#include "http.hpp"
#include "async_io_executor.hpp"
#include "io.hpp"

#include <nghttp2/nghttp2.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <vector>

struct SessionDeleter {
  void operator()(nghttp2_session* value) const {
    nghttp2_session_del(value);
  }
};

struct CallbacksDeleter {
  void operator()(nghttp2_session_callbacks* value) const {
    nghttp2_session_callbacks_del(value);
  }
};

nghttp2_nv header(std::string_view name, std::string_view value) {
  return nghttp2_nv{
      .name = reinterpret_cast<uint8_t*>(
          const_cast<char*>(name.data())),
      .value = reinterpret_cast<uint8_t*>(
          const_cast<char*>(value.data())),
      .namelen = name.size(),
      .valuelen = value.size(),
      .flags = NGHTTP2_NV_FLAG_NONE,
  };
}

struct ServerState {
  nghttp2_session* session = nullptr;
  std::vector<std::byte> body;
  size_t offset = 0;
  bool response_submitted = false;
  bool padded = false;
  std::string content_length;
  std::string content_range;
};

ssize_t read_body(nghttp2_session*, int32_t, uint8_t* buffer,
                  size_t length, uint32_t* flags,
                  nghttp2_data_source* source, void*) {
  auto& state = *static_cast<ServerState*>(source->ptr);
  const size_t count =
      std::min({length, state.body.size() - state.offset,
                state.padded ? size_t(8191) : SIZE_MAX});
  memcpy(buffer, state.body.data() + state.offset, count);
  state.offset += count;
  if (state.offset == state.body.size()) {
    *flags |= NGHTTP2_DATA_FLAG_EOF;
  }
  return static_cast<ssize_t>(count);
}

int on_frame_recv(nghttp2_session* session, const nghttp2_frame* frame,
                  void* user_data) {
  auto& state = *static_cast<ServerState*>(user_data);
  if (frame->hd.type != NGHTTP2_HEADERS ||
      frame->headers.cat != NGHTTP2_HCAT_REQUEST ||
      (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) == 0) {
    return 0;
  }

  std::array response_headers{
      header(":status", "206"),
      header("content-length", state.content_length),
      header("content-range", state.content_range),
      header("etag", "\"mock-etag\""),
  };
  nghttp2_data_provider provider{
      .source = {.ptr = &state},
      .read_callback = read_body,
  };
  const int result = nghttp2_submit_response(
      session, frame->hd.stream_id, response_headers.data(),
      response_headers.size(), &provider);
  if (result != 0) {
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  state.response_submitted = true;
  return 0;
}

void flush_server(nghttp2_session* session, int socket_fd) {
  for (;;) {
    const uint8_t* bytes = nullptr;
    const ssize_t length = nghttp2_session_mem_send(session, &bytes);
    assert(length >= 0);
    if (length == 0) {
      return;
    }
    write_all(
        socket_fd,
        std::span(reinterpret_cast<const std::byte*>(bytes),
                  static_cast<size_t>(length)));
  }
}

nghttp2_ssize select_padding(nghttp2_session*, const nghttp2_frame* frame,
                              size_t maximum, void*) {
  return nghttp2_ssize(frame->hd.type == NGHTTP2_DATA
      ? std::min(frame->hd.length + 7, maximum) : frame->hd.length);
}

void run_server(int listener, std::vector<std::byte> body, bool padded) {
  UniqueFd socket(::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC));
  assert(socket);
  nghttp2_session_callbacks* raw_callbacks = nullptr;
  assert(nghttp2_session_callbacks_new(&raw_callbacks) == 0);
  std::unique_ptr<nghttp2_session_callbacks, CallbacksDeleter> callbacks(
      raw_callbacks);
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks.get(),
                                                        on_frame_recv);
  if (padded) {
    nghttp2_session_callbacks_set_select_padding_callback2(callbacks.get(),
                                                           select_padding);
  }

  ServerState state;
  state.padded = padded;
  state.body = std::move(body);
  state.content_length = std::to_string(state.body.size());
  state.content_range =
      "bytes 0-" + std::to_string(state.body.size() - 1) + '/' +
      std::to_string(state.body.size());

  nghttp2_session* raw_session = nullptr;
  assert(nghttp2_session_server_new(&raw_session, callbacks.get(), &state) ==
         0);
  std::unique_ptr<nghttp2_session, SessionDeleter> session(raw_session);
  state.session = session.get();
  assert(nghttp2_submit_settings(session.get(), NGHTTP2_FLAG_NONE, nullptr,
                                 0) == 0);
  flush_server(session.get(), socket.get());

  std::array<uint8_t, 64U * 1024U> incoming{};
  while (state.offset != state.body.size()) {
    const ssize_t count = ::read(socket.get(), incoming.data(), incoming.size());
    assert(count > 0);
    assert(nghttp2_session_mem_recv(
               session.get(), incoming.data(),
               static_cast<size_t>(count)) == count);
    flush_server(session.get(), socket.get());
  }

  // Model a persistent S3 connection. Closing after an arbitrary WINDOW_UPDATE
  // can discard body bytes still queued in the client and turn the close into
  // a TCP reset because the server also has unread client-side window credit.
  for (;;) {
    const ssize_t count =
        ::read(socket.get(), incoming.data(), incoming.size());
    if (count == 0) {
      return;
    }
    assert(count > 0);
    assert(nghttp2_session_mem_recv(
               session.get(), incoming.data(),
               static_cast<size_t>(count)) == count);
    flush_server(session.get(), socket.get());
  }
}

class TestFileSink final : public RangeFileSink {
 public:
  TestFileSink(int fd, uint64_t offset) noexcept
      : RangeFileSink(fd, offset) {}

  void progress(const Response& response, bool complete) override {
    if (!expected.empty()) {
      assert(response.body_bytes >= produced && response.body_bytes <= expected.size());
      std::vector<std::byte> visible(response.body_bytes - produced);
      assert(::pread(fd(), visible.data(), visible.size(), off_t(produced)) ==
             ssize_t(visible.size()));
      assert(std::equal(visible.begin(), visible.end(), expected.begin() + produced));
    }
    ++calls;
    produced = response.body_bytes;
    completed |= complete;
  }

  size_t calls = 0;
  std::span<const std::byte> expected;
  size_t produced = 0;
  bool completed = false;
};

class TestMemorySink final : public RangeMemorySink {
 public:
  explicit TestMemorySink(size_t size) : data(size) {}
  std::span<std::byte> writable(size_t maximum) override {
    const size_t length = std::min({maximum, data.size() - produced,
                                     size_t(2 * 1024 * 1024) -
                                         produced % (2 * 1024 * 1024)});
    return std::span(data).subspan(produced, length);
  }
  void commit(size_t length) override { produced += length; }
  bool receive_waitall() const noexcept override { return produced >= 64 * 1024; }
  void progress(const Response& response, bool complete) override {
    assert(response.body_bytes == produced);
    ++calls;
    completed |= complete;
  }

  std::vector<std::byte> data;
  size_t calls = 0;
  size_t produced = 0;
  bool completed = false;
};

int run_test(std::string_view mode, int socket_error = 0, int file_error = 0) {
  const bool memory = mode == "--async-memory" || mode == "--async-memory-padded";
  const bool asynchronous = mode == "--async" || memory;
  const bool padded = mode == "--async-memory-padded";
  UniqueFd listener(
      ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP));
  assert(listener);
  sockaddr_in address{};
  address.sin_family      = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port        = 0;
  assert(::bind(listener.get(), reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) == 0);
  assert(::listen(listener.get(), 1) == 0);
  socklen_t address_size = sizeof(address);
  assert(::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address),
                       &address_size) == 0);

  std::vector<std::byte> expected(8U * 1024U * 1024U);
  for (size_t i = 0; i < expected.size(); ++i) {
    expected[i] = static_cast<std::byte>((i * 17U) & 0xffU);
  }

  std::jthread server(run_server, listener.get(), expected, padded);
  auto client = HttpClient::connect(
      "127.0.0.1", ntohs(address.sin_port), "mock-s3");
  UniqueFd file(::memfd_create("h2-range-file", MFD_CLOEXEC));
  assert(file);
  TestFileSink sink(file.get(), 0);
  sink.expected = expected;
  TestMemorySink memory_sink(expected.size());
  const size_t& produced = memory ? memory_sink.produced : sink.produced;
  const bool& completed = memory ? memory_sink.completed : sink.completed;
  Response response;
  if (asynchronous) {
    TestAsyncIoExecutor executor(4093);
    executor.socket_splice_error = socket_error;
    executor.file_splice_error = file_error;
    AsyncHttpRequest request;
    request.path = "/bucket/key";
    request.range = true;
    request.length = expected.size();
    if (memory) request.memory_destination = &memory_sink;
    else request.destination = &sink;
    request.capture_headers = false;
    request.measure_transport = true;
    struct Completion {
      Response* response;
      std::exception_ptr error;
      bool done = false;
    } completion{&response, {}};
    auto operation = client->make_async_request(executor, std::move(request),
        [](void* context, Response&& result, std::exception_ptr error) noexcept {
          auto& completion = *static_cast<Completion*>(context);
          *completion.response = std::move(result);
          completion.error = std::move(error);
          completion.done = true;
        }, &completion);
    operation->start();
    while (produced < 64U * 1024U && !completion.done) {
      assert(executor.run_one());
    }
    assert(produced >= 64U * 1024U);
    assert(produced < expected.size());
    assert(!completed);
    executor.run();
    assert(completion.done);
    if (completion.error) std::rethrow_exception(completion.error);
    assert(executor.parameters_preserved());
    assert(executor.saw_background_file_write() == !memory);
    if (socket_error != 0 || file_error != 0) {
      assert(executor.rejected_splices == 1);
    }
  } else {
    auto download = client->begin_range_to_fd(
        "/bucket/key", 0, expected.size(), sink, {}, false, true);
    const size_t first_received = download->receive_at_least(64U * 1024U);
    assert(first_received >= 64U * 1024U);
    assert(first_received < expected.size());
    assert(!sink.completed);
    response = download->finish();
  }
  assert(response.status == 206);
  assert(response.body_bytes == expected.size());
  const bool copying = socket_error != 0 || file_error != 0;
  assert(response.externally_spliced_bytes == (memory || copying ? 0 : expected.size()));
  if (copying) assert(response.fallback_copied_bytes == expected.size());
  else assert(padded ? response.fallback_copied_bytes > 0
                     : response.fallback_copied_bytes == 0);
  assert(response.fallback_copied_bytes <= expected.size());
  assert(response.wire_start_ns != 0);
  assert(response.wire_last_data_ns >= response.wire_start_ns);
  assert(response.headers.empty());
  assert(sso_view(response.content_range) ==
         "bytes 0-8388607/8388608");
  assert((memory ? memory_sink.calls : sink.calls) > 1);
  assert(completed);

  std::vector<std::byte> actual(expected.size());
  if (memory) actual = std::move(memory_sink.data);
  else read_all(file.get(), actual);
  assert(actual == expected);
  return 0;
}

int main(int argc, char** argv) {
  const std::string_view mode = argc == 2 ? argv[1] : "";
  run_test(mode);
  if (mode == "--async") {
    // Eight MiB exceeds the four-MiB receive window: a missing shadow
    // consume/WINDOW_UPDATE on either copy fallback would stall this transfer.
    run_test(mode, EPERM, 0);
    run_test(mode, ENOSYS, 0);
    run_test(mode, 0, EPERM);
    run_test(mode, 0, EXDEV);
  }
  return 0;
}
