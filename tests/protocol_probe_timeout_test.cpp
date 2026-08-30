#include "http.hpp"
#include "io.hpp"

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
#include <chrono>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <span>
#include <string>
#include <thread>


UniqueFd accept_one(int listener) {
  for (;;) {
    const int socket = ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
    if (socket >= 0) {
      return UniqueFd(socket);
    }
    assert(errno == EINTR);
  }
}

void drain_probe(UniqueFd socket) {
  std::array<std::byte, 4096> discarded{};
  for (;;) {
    const ssize_t count = ::read(socket.get(), discarded.data(),
                                 discarded.size());
    if (count > 0 || (count < 0 && errno == EINTR)) {
      continue;
    }
    assert(count == 0 || errno == ECONNRESET);
    return;
  }
}

void send_text(int socket, std::string_view text) {
  write_all(
      socket,
      std::span(reinterpret_cast<const std::byte*>(text.data()), text.size()));
}

void serve_fallback(int listener) {
  UniqueFd fallback = accept_one(listener);
  std::string request;
  while (!request.ends_with("\r\n\r\n")) {
    char byte = 0;
    const ssize_t count = ::read(fallback.get(), &byte, 1);
    assert(count == 1);
    request.push_back(byte);
    assert(request.size() <= 64U * 1024U);
  }
  assert(request.starts_with("HEAD /health HTTP/1.1\r\n"));
  send_text(fallback.get(),
            "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n"
            "connection: close\r\n\r\n");
}

void run_timeout_server(int listener) {
  UniqueFd probe = accept_one(listener);
  std::array<std::byte, 24> client_magic{};
  read_all(probe.get(), client_magic);
  constexpr std::string_view expected_magic =
      "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
  assert(memcmp(client_magic.data(), expected_magic.data(),
                expected_magic.size()) == 0);

  // Keep the first connection silent. The main server thread remains free to
  // accept the HTTP/1.1 fallback connection after the bounded probe expires.
  std::jthread probe_drainer(drain_probe, std::move(probe));
  serve_fallback(listener);
}

void run_reset_server(int listener) {
  UniqueFd probe = accept_one(listener);
  linger reset{
      .l_onoff = 1,
      .l_linger = 0,
  };
  assert(::setsockopt(probe.get(), SOL_SOCKET, SO_LINGER, &reset,
                      sizeof(reset)) == 0);
  probe.reset();
  serve_fallback(listener);
}

UniqueFd make_listener(sockaddr_in& address) {
  UniqueFd listener(
      ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP));
  assert(listener);
  int enabled = 1;
  assert(::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &enabled,
                      sizeof(enabled)) == 0);

  address = {};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  assert(::bind(listener.get(), reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) == 0);
  assert(::listen(listener.get(), 2) == 0);
  socklen_t address_size = sizeof(address);
  assert(::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address),
                       &address_size) == 0);
  return listener;
}

void run_client(void (*serve)(int), bool expect_timeout) {
  sockaddr_in address{};
  UniqueFd listener = make_listener(address);
  std::jthread server(serve, listener.get());
  const auto started = std::chrono::steady_clock::now();
  auto client = HttpClient::connect(
      "127.0.0.1", ntohs(address.sin_port), "mock-s3");
  const auto elapsed = std::chrono::steady_clock::now() - started;
  if (expect_timeout) {
    assert(elapsed >= std::chrono::milliseconds(500));
  }
  assert(elapsed < std::chrono::seconds(4));

  const auto response = client->request_no_body("HEAD", "/health");
  assert(response.status == 200);
  assert(response.body_bytes == 0);
}

int main() {
  run_client(run_timeout_server, true);
  run_client(run_reset_server, false);
  return 0;
}
