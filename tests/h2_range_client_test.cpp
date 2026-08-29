#include "http.hpp"
#include "io.hpp"

#include <nghttp2/nghttp2.h>

#include <arpa/inet.h>
#include <netinet/in.h>
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
  bool connection_credit_received = false;
  std::string content_length;
};

ssize_t read_body(nghttp2_session*, int32_t, uint8_t* buffer,
                  size_t length, uint32_t* flags,
                  nghttp2_data_source* source, void*) {
  auto& state = *static_cast<ServerState*>(source->ptr);
  const size_t count =
      std::min(length, state.body.size() - state.offset);
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
  if (frame->hd.type == NGHTTP2_WINDOW_UPDATE &&
      frame->hd.stream_id == 0) {
    state.connection_credit_received = true;
    return 0;
  }
  if (frame->hd.type != NGHTTP2_HEADERS ||
      frame->headers.cat != NGHTTP2_HCAT_REQUEST ||
      (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) == 0) {
    return 0;
  }

  std::array response_headers{
      header(":status", "206"),
      header("content-length", state.content_length),
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

void run_server(int listener, std::vector<std::byte> body) {
  UniqueFd socket(::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC));
  assert(socket);
  nghttp2_session_callbacks* raw_callbacks = nullptr;
  assert(nghttp2_session_callbacks_new(&raw_callbacks) == 0);
  std::unique_ptr<nghttp2_session_callbacks, CallbacksDeleter> callbacks(
      raw_callbacks);
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks.get(),
                                                        on_frame_recv);

  ServerState state;
  state.body = std::move(body);
  state.content_length = std::to_string(state.body.size());

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

  // Keep the peer alive until the client returns receive-window credit after
  // draining its pipe. A real S3 connection remains persistent as well.
  state.connection_credit_received = false;
  while (!state.connection_credit_received) {
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

int main() {
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

  std::vector<std::byte> expected(128U * 1024U);
  for (size_t i = 0; i < expected.size(); ++i) {
    expected[i] = static_cast<std::byte>((i * 17U) & 0xffU);
  }

  std::jthread server(run_server, listener.get(), expected);
  auto client = HttpClient::connect(
      "127.0.0.1", ntohs(address.sin_port), "mock-s3");
  auto pipe = Pipe::create();
  const auto response = client->get_range(
      "/bucket/key", 0, expected.size(), pipe, {}, true, true);
  assert(response.status == 206);
  assert(response.body_bytes == expected.size());
  assert(response.externally_spliced_bytes == expected.size());
  assert(response.fallback_copied_bytes == 0);
  assert(response.wire_start_ns != 0);
  assert(response.wire_last_data_ns >= response.wire_start_ns);
  assert(response.headers.at("etag") == "\"mock-etag\"");

  std::vector<std::byte> actual(expected.size());
  read_all(pipe.read_fd(), actual);
  assert(actual == expected);
  client->consume(response);
  return 0;
}
