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

class TestFileSink final : public RangeFileSink {
 public:
  TestFileSink(int fd, uint64_t offset) noexcept
      : RangeFileSink(fd, offset) {}

  void progress(const Response&, bool complete) override {
    ++calls;
    completed |= complete;
  }

  size_t calls = 0;
  bool completed = false;
};

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
  const std::string expected_range =
      "range: bytes=0-" + std::to_string(input.download.size() - 1) +
      "\r\n";
  assert(get.find(expected_range) != std::string::npos);
  const std::string response =
      "HTTP/1.1 103 Early Hints\r\n"
      "link: </metadata>; rel=preload\r\n\r\n"
      "HTTP/1.1 206 Partial Content\r\n"
      "content-length: " + std::to_string(input.download.size()) +
      "\r\netag: \"h1-etag\"\r\n\r\n";
  constexpr size_t prefix = 4096;
  std::vector<std::byte> first(response.size() + prefix);
  memcpy(first.data(), response.data(), response.size());
  memcpy(first.data() + response.size(), input.download.data(), prefix);
  write_all(socket.get(), first);
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  write_all(socket.get(), std::span(input.download).subspan(prefix));

  constexpr size_t small_size = 64U * 1024U + 512U;
  const std::string small = read_request_head(socket.get(), "small GET");
  assert(small.starts_with("GET /bucket/small HTTP/1.1\r\n"));
  assert(small.find("range: bytes=0-66047\r\n") != std::string::npos);
  const std::string small_response =
      "HTTP/1.1 206 Partial Content\r\ncontent-length: " +
      std::to_string(small_size) + "\r\n\r\n";
  std::vector<std::byte> small_first(small_response.size() + prefix);
  memcpy(small_first.data(), small_response.data(), small_response.size());
  memcpy(small_first.data() + small_response.size(),
         input.download.data(), prefix);
  write_all(socket.get(), small_first);
  write_all(socket.get(),
            std::span(input.download).subspan(prefix, small_size - prefix));

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

  std::vector<std::byte> expected_download(1024U * 1024U);
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

  auto client = HttpClient::connect(
      "127.0.0.1", port, "mock-s3", false, kRequestIoTimeoutMs,
      kConnectTimeoutMs, kProtocolProbeTimeoutMs);
  auto download_pipe = Pipe::create(expected_download.size());
  const auto downloaded = client->get_range(
      "/bucket/key", 0, expected_download.size(), download_pipe);
  assert(downloaded.status == 206);
  assert(downloaded.body_bytes == expected_download.size());
  assert(downloaded.fallback_copied_bytes != 0);
  assert(downloaded.externally_spliced_bytes +
             downloaded.fallback_copied_bytes ==
         expected_download.size());
  assert(downloaded.transport_splice_calls >= 2);
  assert(downloaded.headers.at("etag") == "\"h1-etag\"");
  std::vector<std::byte> actual_download(expected_download.size());
  read_all(download_pipe.read_fd(), actual_download);
  assert(actual_download == expected_download);
  client->consume(downloaded);

  constexpr size_t small_size = 64U * 1024U + 512U;
  Pipe small_pipe = Pipe::create(small_size);
  const Response small = client->get_range(
      "/bucket/small", 0, small_size, small_pipe);
  assert(small.fallback_copied_bytes > 512);
  std::vector<std::byte> actual_small(small_size);
  read_all(small_pipe.read_fd(), actual_small);
  assert(memcmp(actual_small.data(), expected_download.data(), small_size) == 0);
  client->consume(small);

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

  std::vector<std::byte> delayed_body(128U * 1024U);
  for (size_t i = 0; i < delayed_body.size(); ++i) {
    delayed_body[i] = std::byte((i * 31U + 13U) & 0xffU);
  }
  std::jthread delayed_server([&] {
    UniqueFd probe = accept_one(listener.get());
    std::array<std::byte, 24> client_magic{};
    read_all(probe.get(), client_magic);
    send_text(probe.get(),
              "HTTP/1.1 505 HTTP Version Not Supported\r\n"
              "content-length: 0\r\nconnection: close\r\n\r\n");
    finish_probe_response(probe.get());
    probe.reset();

    UniqueFd socket = accept_one(listener.get());
    const std::string get = read_request_head(socket.get(), "delayed GET");
    assert(get.starts_with("GET /bucket/delayed HTTP/1.1\r\n"));
    const std::string response =
        "HTTP/1.1 206 Partial Content\r\ncontent-length: " +
        std::to_string(delayed_body.size()) + "\r\n\r\n";
    constexpr size_t prefix = 4096;
    std::vector<std::byte> first(response.size() + prefix);
    memcpy(first.data(), response.data(), response.size());
    memcpy(first.data() + response.size(), delayed_body.data(), prefix);
    write_all(socket.get(), first);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    write_all(socket.get(), std::span(delayed_body).subspan(prefix));

    const std::string repeated = read_request_head(
        socket.get(), "repeated delayed GET");
    assert(repeated.starts_with(
        "GET /bucket/delayed-repeat HTTP/1.1\r\n"));
    send_text(socket.get(),
              "HTTP/1.1 103 Early Hints\r\n"
              "link: </next>; rel=preload\r\n\r\n"
              "HTTP/1.1 206 Partial Content\r\ncontent-length: " +
                  std::to_string(delayed_body.size()) + "\r\n\r\n");
    write_all(socket.get(), delayed_body);

    constexpr size_t changed_size = 96U * 1024U;
    const std::string changed = read_request_head(
        socket.get(), "changed-size delayed GET");
    assert(changed.starts_with(
        "GET /bucket/delayed-changed HTTP/1.1\r\n"));
    send_text(socket.get(),
              "HTTP/1.1 206 Partial Content\r\ncontent-length: " +
                  std::to_string(changed_size) + "\r\n\r");
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    send_text(socket.get(), "\n");
    write_all(socket.get(), std::span(delayed_body).first(changed_size));

    constexpr size_t first_tail_chunk = 96U * 1024U;
    const std::string short_tail = read_request_head(
        socket.get(), "short-tail delayed GET");
    assert(short_tail.starts_with(
        "GET /bucket/delayed-short-tail HTTP/1.1\r\n"));
    send_text(socket.get(),
              "HTTP/1.1 206 Partial Content\r\ncontent-length: " +
                  std::to_string(delayed_body.size()) + "\r\n\r\n");
    write_all(socket.get(),
              std::span(delayed_body).first(first_tail_chunk));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    write_all(socket.get(),
              std::span(delayed_body).subspan(first_tail_chunk));

    const std::string head = read_request_head(socket.get(), "delayed HEAD");
    assert(head.starts_with("HEAD /bucket/delayed HTTP/1.1\r\n"));
    send_text(socket.get(),
              "HTTP/1.1 200 OK\r\n"
              "content-length: 65536\r\n\r\n");

    const std::string missing = read_request_head(
        socket.get(), "missing delayed GET");
    assert(missing.starts_with(
        "GET /bucket/delayed-missing HTTP/1.1\r\n"));
    send_text(socket.get(),
              "HTTP/1.1 404 Not Found\r\n"
              "content-length: 9\r\n\r\n"
              "not found");

    const std::string close = read_request_head(socket.get(), "closing HEAD");
    assert(close.starts_with("HEAD /bucket/delayed-close HTTP/1.1\r\n"));
    send_text(socket.get(),
              "HTTP/1.1 200 OK\r\n"
              "content-length: 0\r\n"
              "connection: close\r\n\r\n");
  });

  auto delayed_client = HttpClient::connect(
      "127.0.0.1", port, "mock-s3", false, 250,
      kConnectTimeoutMs, kProtocolProbeTimeoutMs);
  Pipe delayed_pipe = Pipe::create(delayed_body.size());
  const Response delayed = delayed_client->get_range(
      "/bucket/delayed", 0, delayed_body.size(), delayed_pipe);
  assert(delayed.transport_splice_calls >= 2);
  std::vector<std::byte> actual_delayed(delayed_body.size());
  read_all(delayed_pipe.read_fd(), actual_delayed);
  assert(actual_delayed == delayed_body);

  Pipe repeated_pipe = Pipe::create(delayed_body.size());
  const Response repeated = delayed_client->get_range(
      "/bucket/delayed-repeat", 0, delayed_body.size(), repeated_pipe);
  read_all(repeated_pipe.read_fd(), actual_delayed);
  assert(actual_delayed == delayed_body);

  constexpr size_t changed_size = 96U * 1024U;
  Pipe changed_pipe = Pipe::create(changed_size);
  const Response changed = delayed_client->get_range(
      "/bucket/delayed-changed", 0, changed_size, changed_pipe);
  std::vector<std::byte> actual_changed(changed_size);
  read_all(changed_pipe.read_fd(), actual_changed);
  assert(memcmp(actual_changed.data(), delayed_body.data(), changed_size) == 0);

  Pipe short_tail_pipe = Pipe::create(delayed_body.size());
  const Response short_tail = delayed_client->get_range(
      "/bucket/delayed-short-tail", 0, delayed_body.size(), short_tail_pipe);
  std::vector<std::byte> actual_short_tail(delayed_body.size());
  read_all(short_tail_pipe.read_fd(), actual_short_tail);
  assert(actual_short_tail == delayed_body);

  const Response delayed_head = delayed_client->request_no_body(
      "HEAD", "/bucket/delayed");
  assert(delayed_head.status == 200);
  assert(delayed_head.body_bytes == 0);

  Pipe missing_pipe = Pipe::create(delayed_body.size());
  const Response missing = delayed_client->get_range(
      "/bucket/delayed-missing", 0, delayed_body.size(), missing_pipe);
  assert(missing.status == 404);
  assert(std::string(reinterpret_cast<const char*>(missing.body.data()),
                     missing.body.size()) == "not found");
  const Response closing_head = delayed_client->request_no_body(
      "HEAD", "/bucket/delayed-close");
  assert(closing_head.status == 200);
  assert(closing_head.body_bytes == 0);
  delayed_server.join();

  std::vector<std::byte> file_body(1024U * 1024U);
  for (size_t i = 0; i < file_body.size(); ++i) {
    file_body[i] = static_cast<std::byte>((i * 37U + 5U) & 0xffU);
  }
  assert(file_body.size() > Pipe::create().capacity());
  std::jthread file_server([&] {
    UniqueFd probe = accept_one(listener.get());
    std::array<std::byte, 24> client_magic{};
    read_all(probe.get(), client_magic);
    send_text(probe.get(),
              "HTTP/1.1 505 HTTP Version Not Supported\r\n"
              "content-length: 0\r\nconnection: close\r\n\r\n");
    finish_probe_response(probe.get());
    probe.reset();

    UniqueFd socket = accept_one(listener.get());
    const std::string get = read_request_head(socket.get(), "file GET");
    assert(get.starts_with("GET /bucket/file HTTP/1.1\r\n"));
    assert(get.find("range: bytes=0-" +
                    std::to_string(file_body.size() - 1) + "\r\n") !=
           std::string::npos);
    const std::string response =
        "HTTP/1.1 206 Partial Content\r\n"
        "content-length: " + std::to_string(file_body.size()) +
        "\r\ncontent-range: bytes 0-" +
        std::to_string(file_body.size() - 1) + "/" +
        std::to_string(file_body.size()) + "\r\n"
        "etag: \"h1-file-etag\"\r\n\r\n";
    std::vector<std::byte> first(response.size() + 4096);
    memcpy(first.data(), response.data(), response.size());
    memcpy(first.data() + response.size(), file_body.data(), 4096);
    write_all(socket.get(), first);
    write_all(socket.get(), std::span(file_body).subspan(4096));
  });

  auto file_client = HttpClient::connect(
      "127.0.0.1", port, "mock-s3", false, kRequestIoTimeoutMs,
      kConnectTimeoutMs, kProtocolProbeTimeoutMs);
  UniqueFd file(::memfd_create("h1-range-file", MFD_CLOEXEC));
  assert(file);
  TestFileSink sink(file.get(), 0);
  const Response file_response = file_client->get_range_to_fd(
      "/bucket/file", 0, file_body.size(), sink, {}, false, true);
  assert(file_response.status == 206);
  assert(file_response.body_bytes == file_body.size());
  assert(file_response.fallback_copied_bytes > 0);
  assert(file_response.externally_spliced_bytes > 0);
  assert(file_response.externally_spliced_bytes +
             file_response.fallback_copied_bytes == file_body.size());
  assert(file_response.transport_splice_calls > 0);
  assert(file_response.wire_start_ns != 0);
  assert(file_response.wire_last_data_ns >= file_response.wire_start_ns);
  assert(file_response.headers.empty());
  assert(sso_view(file_response.content_range) ==
         "bytes 0-" + std::to_string(file_body.size() - 1) + "/" +
             std::to_string(file_body.size()));
  assert(sink.calls > 1);
  assert(sink.completed);
  std::vector<std::byte> actual_file(file_body.size());
  assert(::lseek(file.get(), 0, SEEK_SET) == 0);
  read_all(file.get(), actual_file);
  assert(actual_file == file_body);
  file_server.join();

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
      "127.0.0.1", port, "mock-s3", false, 100,
      kConnectTimeoutMs, kProtocolProbeTimeoutMs, false);
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
