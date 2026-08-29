#include "http.hpp"
#include "io.hpp"

#include <nghttp2/nghttp2.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <exception>
#include <memory>
#include <span>
#include <string_view>
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
  std::vector<std::byte> received;
  bool response_submitted = false;
};

int on_data_chunk(nghttp2_session*, uint8_t, int32_t,
                  const uint8_t* data, size_t length,
                  void* user_data) {
  auto& state = *static_cast<ServerState*>(user_data);
  const size_t old_size = state.received.size();
  state.received.resize(old_size + length);
  memcpy(state.received.data() + old_size, data, length);
  return 0;
}

int on_frame_recv(nghttp2_session* session, const nghttp2_frame* frame,
                  void* user_data) {
  auto& state = *static_cast<ServerState*>(user_data);
  if (frame->hd.type != NGHTTP2_DATA ||
      (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) == 0) {
    return 0;
  }
  const std::array response_headers{
      header(":status", "200"),
      header("etag", "\"put-etag\""),
      header("content-length", "0"),
  };
  const int result = nghttp2_submit_response(
      session, frame->hd.stream_id, response_headers.data(),
      response_headers.size(), nullptr);
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

void run_server(int listener,
                const std::vector<std::byte>& expected,
                std::exception_ptr& failure) noexcept {
  try {
    UniqueFd socket(::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC));
    assert(socket);
    nghttp2_session_callbacks* raw_callbacks = nullptr;
    assert(nghttp2_session_callbacks_new(&raw_callbacks) == 0);
    std::unique_ptr<nghttp2_session_callbacks, CallbacksDeleter> callbacks(
        raw_callbacks);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
        callbacks.get(), on_data_chunk);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks.get(),
                                                          on_frame_recv);

    ServerState state;
    nghttp2_session* raw_session = nullptr;
    assert(nghttp2_session_server_new(&raw_session, callbacks.get(), &state) ==
           0);
    std::unique_ptr<nghttp2_session, SessionDeleter> session(raw_session);
    assert(nghttp2_submit_settings(session.get(), NGHTTP2_FLAG_NONE, nullptr,
                                   0) == 0);
    flush_server(session.get(), socket.get());

    std::array<uint8_t, 64U * 1024U> incoming{};
    while (!state.response_submitted) {
      const ssize_t count = ::read(socket.get(), incoming.data(),
                                   incoming.size());
      assert(count > 0);
      assert(nghttp2_session_mem_recv(
                 session.get(), incoming.data(),
                 static_cast<size_t>(count)) == count);
      flush_server(session.get(), socket.get());
    }
    assert(state.received == expected);
  } catch (...) {
    failure = std::current_exception();
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

  std::vector<std::byte> expected(512U * 1024U + 19U);
  for (size_t i = 0; i < expected.size(); ++i) {
    expected[i] = static_cast<std::byte>((i * 31U + 11U) & 0xffU);
  }

  UniqueFd memory(
      ::memfd_create("ngs3fs-put-test", MFD_CLOEXEC));
  assert(memory);
  write_all(memory.get(), expected);

  std::exception_ptr server_failure;
  std::jthread server(run_server, listener.get(),
                      std::cref(expected), std::ref(server_failure));
  auto client = HttpClient::connect(
      "127.0.0.1", ntohs(address.sin_port), "mock-s3");
  const std::array extra_headers{
      Header{"content-type", "application/octet-stream"},
  };
  const auto response = client->put_from_fd(
      "/bucket/key", extra_headers, memory.get(), 0, expected.size());
  assert(response.status == 200);
  assert(response.headers.at("etag") == "\"put-etag\"");
  assert(response.externally_sent_bytes == expected.size());
  assert(response.body.empty());

  server.join();
  if (server_failure) {
    std::rethrow_exception(server_failure);
  }
  return 0;
}
