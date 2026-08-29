#include "http.hpp"
#include "io.hpp"

#include <nghttp2/nghttp2.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>


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
  bool send_goaway = false;
  bool responded = false;
  std::string sequence;
};

int on_frame_recv(nghttp2_session* session, const nghttp2_frame* frame,
                  void* user_data) {
  auto& state = *static_cast<ServerState*>(user_data);
  if (frame->hd.type != NGHTTP2_HEADERS ||
      frame->headers.cat != NGHTTP2_HCAT_REQUEST ||
      (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) == 0) {
    return 0;
  }
  if (state.send_goaway) {
    const int result = nghttp2_submit_goaway(
        session, NGHTTP2_FLAG_NONE, frame->hd.stream_id, NGHTTP2_NO_ERROR,
        nullptr, 0);
    if (result != 0) {
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
  }
  const std::array response_headers{
      header(":status", "200"),
      header("content-length", "0"),
      header("x-sequence", state.sequence),
  };
  const int result = nghttp2_submit_response(
      session, frame->hd.stream_id, response_headers.data(),
      response_headers.size(), nullptr);
  if (result != 0) {
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  state.responded = true;
  return 0;
}

void flush_server(nghttp2_session* session, int socket) {
  for (;;) {
    const uint8_t* bytes = nullptr;
    const ssize_t length = nghttp2_session_mem_send(session, &bytes);
    assert(length >= 0);
    if (length == 0) {
      return;
    }
    write_all(
        socket,
        std::span(reinterpret_cast<const std::byte*>(bytes),
                  static_cast<size_t>(length)));
  }
}

UniqueFd accept_one(int listener) {
  for (;;) {
    const int socket = ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
    if (socket >= 0) {
      return UniqueFd(socket);
    }
    assert(errno == EINTR);
  }
}

void serve_connection(int listener, bool send_goaway,
                      std::string sequence) {
  UniqueFd socket = accept_one(listener);
  nghttp2_session_callbacks* raw_callbacks = nullptr;
  assert(nghttp2_session_callbacks_new(&raw_callbacks) == 0);
  std::unique_ptr<nghttp2_session_callbacks, CallbacksDeleter> callbacks(
      raw_callbacks);
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks.get(),
                                                        on_frame_recv);

  ServerState state{
      .send_goaway = send_goaway,
      .sequence = std::move(sequence),
  };
  nghttp2_session* raw_session = nullptr;
  assert(nghttp2_session_server_new(&raw_session, callbacks.get(), &state) ==
         0);
  std::unique_ptr<nghttp2_session, SessionDeleter> session(raw_session);
  assert(nghttp2_submit_settings(session.get(), NGHTTP2_FLAG_NONE, nullptr,
                                 0) == 0);
  flush_server(session.get(), socket.get());

  std::array<uint8_t, 64U * 1024U> input{};
  while (!state.responded) {
    const ssize_t count = ::read(socket.get(), input.data(), input.size());
    assert(count > 0);
    assert(nghttp2_session_mem_recv(
               session.get(), input.data(), static_cast<size_t>(count)) ==
           count);
    flush_server(session.get(), socket.get());
  }

  assert(::shutdown(socket.get(), SHUT_WR) == 0);
  for (;;) {
    const ssize_t count = ::read(socket.get(), input.data(), input.size());
    if (count > 0 || (count < 0 && errno == EINTR)) {
      continue;
    }
    if (count < 0) {
      assert(errno == ECONNRESET);
    }
    break;
  }
}

void run_server(int listener) {
  serve_connection(listener, true, "first");
  serve_connection(listener, false, "second");
}

int main() {
  UniqueFd listener(
      ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP));
  assert(listener);
  int enabled = 1;
  assert(::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &enabled,
                      sizeof(enabled)) == 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  assert(::bind(listener.get(), reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) == 0);
  assert(::listen(listener.get(), 2) == 0);
  socklen_t address_size = sizeof(address);
  assert(::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address),
                       &address_size) == 0);

  std::jthread server(run_server, listener.get());
  auto client = HttpClient::connect(
      "127.0.0.1", ntohs(address.sin_port), "mock-s3");
  const auto first = client->request_no_body("HEAD", "/first");
  assert(first.status == 200);
  assert(first.headers.at("x-sequence") == "first");

  // GOAWAY on the first connection must cause a transparent HTTP/2 reconnect
  // before allocating the next stream.
  const auto second = client->request_no_body("HEAD", "/second");
  assert(second.status == 200);
  assert(second.headers.at("x-sequence") == "second");
  return 0;
}
