#include "http.hpp"
#include "io.hpp"

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
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <chrono>
#include <charconv>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

UniqueFd accept_one(int listener) {
  for (;;) {
    const int socket = ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
    if (socket >= 0) {
      return UniqueFd(socket);
    }
    assert(errno == EINTR);
  }
}

std::string read_request_head(int socket, const char* request_name) {
  std::string result;
  while (!result.ends_with("\r\n\r\n")) {
    char byte = 0;
    const ssize_t received = ::recv(socket, &byte, 1, 0);
    if (received != 1) {
      std::cerr << "failed while reading " << request_name
                << " request header: recv=" << received << " errno=" << errno
                << '\n';
    }
    assert(received == 1);
    result.push_back(byte);
    assert(result.size() <= 64U * 1024U);
  }
  return result;
}

size_t content_length(std::string_view headers) {
  constexpr std::string_view prefix = "content-length: ";
  const size_t position = headers.find(prefix);
  assert(position != std::string_view::npos);
  const size_t begin = position + prefix.size();
  const size_t end = headers.find("\r\n", begin);
  assert(end != std::string_view::npos);
  return static_cast<size_t>(
      std::stoull(std::string(headers.substr(begin, end - begin))));
}

std::string read_line(int socket) {
  std::string line;
  while (!line.ends_with("\r\n")) {
    char ch = 0;
    const ssize_t received = ::recv(socket, &ch, 1, 0);
    assert(received == 1);
    line.push_back(ch);
    assert(line.size() <= 128);
  }
  line.resize(line.size() - 2);
  return line;
}

std::vector<std::byte> read_chunked_body(int socket) {
  std::vector<std::byte> body;
  for (;;) {
    const std::string line = read_line(socket);
    size_t length = 0;
    const auto parsed = std::from_chars(
        line.data(), line.data() + line.size(), length, 16);
    assert(parsed.ec == std::errc{} &&
           parsed.ptr == line.data() + line.size());
    if (length == 0) {
      assert(read_line(socket).empty());
      return body;
    }
    const size_t old_size = body.size();
    body.resize(old_size + length);
    read_all(socket, std::span(body).subspan(old_size));
    std::array<std::byte, 2> end{};
    read_all(socket, end);
    assert(end[0] == std::byte{'\r'} && end[1] == std::byte{'\n'});
  }
}

void send_text(int socket, std::string_view text) {
  write_all(
      socket,
      std::span(reinterpret_cast<const std::byte*>(text.data()), text.size()));
}

void finish_probe_response(int socket) {
  if (::shutdown(socket, SHUT_WR) != 0) {
    assert(errno == ENOTCONN);
  }
  std::array<std::byte, 4096> discarded{};
  for (;;) {
    const ssize_t received =
        ::read(socket, discarded.data(), discarded.size());
    if (received > 0) {
      continue;
    }
    if (received < 0 && errno == EINTR) {
      continue;
    }
    if (received < 0 && errno == ECONNRESET) {
      return;
    }
    assert(received == 0);
    return;
  }
}

struct ServerInput {
  int listener = -1;
  std::vector<std::byte> download;
  std::vector<std::byte> upload;
  std::vector<std::byte> control;
};

void run_server(ServerInput input) {
  UniqueFd probe = accept_one(input.listener);
  std::array<std::byte, 24> client_magic{};
  read_all(probe.get(), client_magic);
  constexpr std::string_view expected_magic =
      "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
  assert(memcmp(client_magic.data(), expected_magic.data(),
                     expected_magic.size()) == 0);
  send_text(probe.get(),
            "HTTP/1.1 505 HTTP Version Not Supported\r\n"
            "content-length: 0\r\nconnection: close\r\n\r\n");
  // Drain the client's SETTINGS and WINDOW_UPDATE before close. Closing with
  // unread bytes can reset this probe connection and make fallback flaky.
  finish_probe_response(probe.get());
  probe.reset();

  UniqueFd socket = accept_one(input.listener);

  const std::string get = read_request_head(socket.get(), "GET");
  assert(get.starts_with("GET /bucket/key HTTP/1.1\r\n"));
  assert(get.find("host: mock-s3\r\n") != std::string::npos);
  assert(get.find("range: bytes=0-262143\r\n") != std::string::npos);
  constexpr std::string_view response =
      "HTTP/1.1 103 Early Hints\r\n"
      "link: </metadata>; rel=preload\r\n\r\n"
      "HTTP/1.1 206 Partial Content\r\n"
      "content-length: 262144\r\n"
      "etag: \"h1-etag\"\r\n\r\n";
  constexpr size_t prefix = 4096;
  std::vector<std::byte> first(response.size() + prefix);
  memcpy(first.data(), response.data(), response.size());
  memcpy(first.data() + response.size(), input.download.data(), prefix);
  write_all(socket.get(), first);
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  write_all(socket.get(), std::span(input.download).subspan(prefix));

  const std::string put = read_request_head(socket.get(), "PUT");
  assert(put.starts_with("PUT /bucket/key HTTP/1.1\r\n"));
  assert(content_length(put) == input.upload.size());
  std::vector<std::byte> uploaded(input.upload.size());
  read_all(socket.get(), uploaded);
  assert(uploaded == input.upload);
  send_text(socket.get(),
            "HTTP/1.1 200 OK\r\n"
            "transfer-encoding: chunked\r\n\r\n"
            "5\r\nsaved\r\n"
            "0\r\nx-checksum: valid\r\n\r\n");

  const std::string stream = read_request_head(socket.get(), "stream PUT");
  assert(stream.starts_with("PUT /bucket/stream HTTP/1.1\r\n"));
  assert(stream.find("transfer-encoding: chunked\r\n") !=
         std::string::npos);
  assert(stream.find("content-length:") == std::string::npos);
  assert(read_chunked_body(socket.get()) == input.upload);
  send_text(socket.get(),
            "HTTP/1.1 201 Created\r\n"
            "content-length: 0\r\n"
            "etag: \"streamed\"\r\n\r\n");

  const std::string fixed = read_request_head(socket.get(), "fixed PUT");
  assert(fixed.starts_with("PUT /bucket/fixed HTTP/1.1\r\n"));
  assert(content_length(fixed) == input.upload.size());
  assert(fixed.find("transfer-encoding:") == std::string::npos);
  std::vector<std::byte> fixed_body(input.upload.size());
  read_all(socket.get(), fixed_body);
  assert(fixed_body == input.upload);
  send_text(socket.get(),
            "HTTP/1.1 204 No Content\r\n"
            "content-length: 0\r\n\r\n");

  const std::string large = read_request_head(socket.get(), "large GET");
  assert(large.starts_with("GET /bucket/list HTTP/1.1\r\n"));
  send_text(socket.get(),
            "HTTP/1.1 200 OK\r\ncontent-length: " +
                std::to_string(input.control.size()) + "\r\n\r\n");
  write_all(socket.get(), input.control);

  const std::string head = read_request_head(socket.get(), "HEAD");
  assert(head.starts_with("HEAD /bucket/key HTTP/1.1\r\n"));
  send_text(socket.get(),
            "HTTP/1.1 200 OK\r\n"
            "content-length: 131072\r\n"
            "x-amz-version-id: version-1\r\n"
            "connection: close\r\n\r\n");
}

int main() {
  UniqueFd listener(
      ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP));
  assert(listener);

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  assert(::bind(listener.get(), reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) == 0);
  assert(::listen(listener.get(), 2) == 0);
  socklen_t address_length = sizeof(address);
  assert(::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address),
                       &address_length) == 0);
  const uint16_t port = ntohs(address.sin_port);

  std::vector<std::byte> expected_download(256U * 1024U);
  std::vector<std::byte> expected_upload(96U * 1024U);
  std::vector<std::byte> expected_control(384U * 1024U);
  for (size_t i = 0; i < expected_download.size(); ++i) {
    expected_download[i] =
        static_cast<std::byte>((i * 19U + 3U) & 0xffU);
  }
  for (size_t i = 0; i < expected_upload.size(); ++i) {
    expected_upload[i] =
        static_cast<std::byte>((i * 23U + 7U) & 0xffU);
  }
  for (size_t i = 0; i < expected_control.size(); ++i) {
    expected_control[i] =
        static_cast<std::byte>((i * 29U + 11U) & 0xffU);
  }

  std::jthread server(run_server,
                      ServerInput{.listener = listener.get(),
                                  .download = expected_download,
                                  .upload = expected_upload,
                                  .control = expected_control});

  auto client = HttpClient::connect("127.0.0.1", port, "mock-s3");
  auto download_pipe = Pipe::create();
  const auto downloaded = client->get_range(
      "/bucket/key", 0, expected_download.size(), download_pipe);
  assert(downloaded.status == 206);
  assert(downloaded.body_bytes == expected_download.size());
  assert(downloaded.fallback_copied_bytes != 0);
  assert(downloaded.externally_spliced_bytes +
             downloaded.fallback_copied_bytes ==
         expected_download.size());
  assert(downloaded.transport_splice_calls != 0);
  assert(downloaded.headers.at("etag") == "\"h1-etag\"");
  std::vector<std::byte> actual_download(expected_download.size());
  read_all(download_pipe.read_fd(), actual_download);
  assert(actual_download == expected_download);
  client->consume(downloaded);

  UniqueFd upload_memory(
      ::memfd_create("h1-upload", MFD_CLOEXEC));
  assert(upload_memory);
  write_all(upload_memory.get(), expected_upload);
  const auto uploaded = client->put_from_fd(
      "/bucket/key", {}, upload_memory.get(), 0, expected_upload.size());
  assert(uploaded.status == 200);
  assert(uploaded.externally_sent_bytes == expected_upload.size());
  assert(uploaded.fallback_copied_bytes == 5);
  assert(std::string(reinterpret_cast<const char*>(uploaded.body.data()),
                     uploaded.body.size()) == "saved");
  assert(uploaded.headers.at("x-checksum") == "valid");

  const size_t split = expected_upload.size() - 4096;
  client->begin_upload("PUT", "/bucket/stream");
  client->upload_bytes(std::span(expected_upload).first(split));
  Pipe stream_pipe = Pipe::create();
  write_all(stream_pipe.write_fd(),
            std::span(expected_upload).subspan(split));
  client->upload_from_fd(stream_pipe.read_fd(), 0,
                         expected_upload.size() - split, false);
  const Response streamed = client->finish_upload();
  assert(streamed.status == 201);
  assert(streamed.externally_sent_bytes == expected_upload.size());
  assert(streamed.headers.at("etag") == "\"streamed\"");

  client->begin_upload("PUT", "/bucket/fixed", {}, expected_upload.size());
  client->upload_bytes(std::span(expected_upload).first(split));
  Pipe fixed_pipe = Pipe::create();
  write_all(fixed_pipe.write_fd(),
            std::span(expected_upload).subspan(split));
  client->upload_from_fd(fixed_pipe.read_fd(), 0,
                         expected_upload.size() - split, false);
  const Response fixed = client->finish_upload();
  assert(fixed.status == 204);
  assert(fixed.externally_sent_bytes == expected_upload.size());

  const Pipe control_pipe = Pipe::create();
  assert(expected_control.size() > control_pipe.capacity());
  const Response control = client->request_no_body(
      "GET", "/bucket/list", {}, expected_control.size());
  assert(control.status == 200);
  assert(control.body == expected_control);

  const auto metadata = client->request_no_body("HEAD", "/bucket/key");
  assert(metadata.status == 200);
  assert(metadata.body_bytes == 0);
  assert(metadata.headers.at("content-length") == "131072");
  assert(metadata.headers.at("x-amz-version-id") == "version-1");

  server.join();
  std::jthread stalled_server([&] {
    UniqueFd probe = accept_one(listener.get());
    std::array<std::byte, 24> client_magic{};
    read_all(probe.get(), client_magic);
    send_text(probe.get(),
              "HTTP/1.1 505 HTTP Version Not Supported\r\n"
              "content-length: 0\r\nconnection: close\r\n\r\n");
    finish_probe_response(probe.get());
    probe.reset();

    UniqueFd socket = accept_one(listener.get());
    const std::string get = read_request_head(socket.get(), "stalled GET");
    assert(get.starts_with("GET /bucket/stalled HTTP/1.1\r\n"));
    send_text(socket.get(),
              "HTTP/1.1 206 Partial Content\r\n"
              "content-length: 4096\r\n\r\n");
    std::this_thread::sleep_for(std::chrono::seconds(1));
  });

  auto stalled_client = HttpClient::connect(
      "127.0.0.1", port, "mock-s3", false, 100);
  Pipe stalled_pipe = Pipe::create();
  const auto timeout_start = std::chrono::steady_clock::now();
  bool timed_out = false;
  try {
    stalled_client->get_range(
        "/bucket/stalled", 0, 4096, stalled_pipe);
  } catch (const std::exception&) {
    timed_out = true;
  }
  const auto timeout_elapsed = std::chrono::steady_clock::now() -
                               timeout_start;
  assert(timed_out);
  assert(timeout_elapsed < std::chrono::milliseconds(800));
  return 0;
}
