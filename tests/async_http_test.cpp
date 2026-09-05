#include "http.hpp"
#include "io.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <array>
#include <deque>
#include <exception>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

[[noreturn]] void fail(const char* message) {
  fprintf(stderr, "async_http_test: %s\n", message);
  abort();
}

void check(bool condition, const char* message) {
  if (!condition) fail(message);
}

void print_error(const std::exception_ptr& error) {
  if (!error) return;
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    fprintf(stderr, "completion error: %s\n", exception.what());
  } catch (...) {
    fprintf(stderr, "completion error: unknown exception\n");
  }
}

void write_exact(int fd, const void* data, size_t length) {
  const char* next = static_cast<const char*>(data);
  while (length != 0) {
    const ssize_t written = ::write(fd, next, length);
    if (written < 0 && errno == EINTR) continue;
    check(written > 0, "write failed");
    next += written;
    length -= size_t(written);
  }
}

void send_text(int fd, std::string_view text) {
  write_exact(fd, text.data(), text.size());
}

void read_exact(int fd, void* data, size_t length) {
  char* next = static_cast<char*>(data);
  while (length != 0) {
    const ssize_t received = ::read(fd, next, length);
    if (received < 0 && errno == EINTR) continue;
    check(received > 0, "read failed");
    next += received;
    length -= size_t(received);
  }
}

UniqueFd accept_one(int listener) {
  for (;;) {
    const int fd = ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
    if (fd >= 0) return UniqueFd(fd);
    check(errno == EINTR, "accept failed");
  }
}

std::string read_request_head(int fd) {
  std::string result;
  while (!result.ends_with("\r\n\r\n")) {
    char byte = 0;
    const ssize_t received = ::recv(fd, &byte, 1, 0);
    if (received < 0 && errno == EINTR) continue;
    check(received == 1, "request header ended early");
    result.push_back(byte);
    check(result.size() <= 64U * 1024U, "request header is too large");
  }
  return result;
}

void finish_probe(int fd) {
  std::array<char, 24> magic{};
  read_exact(fd, magic.data(), magic.size());
  constexpr std::string_view expected = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
  check(memcmp(magic.data(), expected.data(), expected.size()) == 0,
        "unexpected HTTP/2 probe");
  send_text(fd,
            "HTTP/1.1 505 HTTP Version Not Supported\r\n"
            "content-length: 0\r\nconnection: close\r\n\r\n");
  const int shutdown_result = ::shutdown(fd, SHUT_WR);
  check(shutdown_result == 0 || errno == ENOTCONN, "probe shutdown failed");
  std::array<char, 4096> discarded{};
  for (;;) {
    const ssize_t received = ::read(fd, discarded.data(), discarded.size());
    if (received > 0 || (received < 0 && errno == EINTR)) continue;
    check(received == 0 || errno == ECONNRESET, "probe drain failed");
    return;
  }
}

class QueueExecutor final : public IoExecutor {
 public:
  bool submit(AsyncIoRequest& request) noexcept override {
    if (request.complete == nullptr || contains(&request)) {
      errno = request.complete == nullptr ? EINVAL : EBUSY;
      return false;
    }
    Pending pending;
    pending.request = &request;
    pending.kind = request.kind;
    pending.fd = request.fd;
    pending.output_fd = request.output_fd;
    pending.data = request.data;
    pending.length = request.length;
    pending.input_offset = request.input_offset;
    pending.output_offset = request.output_offset;
    pending.address = request.address;
    pending.address_length = request.address_length;
    pending.flags = request.flags;
    pending.exact = request.exact;
    pending.force_async = request.force_async;
    pending.processor = request.processor;
    pending.processor_context = request.processor_context;
    pending.complete = request.complete;
    pending.context = request.context;
    queue_.push_back(pending);
    active_.push_back(&request);
    return true;
  }

  bool cancel(AsyncIoRequest& request) noexcept override {
    const auto item = std::find_if(queue_.begin(), queue_.end(),
        [&](const Pending& pending) { return pending.request == &request; });
    if (item == queue_.end()) {
      errno = ENOENT;
      return false;
    }
    Pending cancelled = *item;
    queue_.erase(item);
    cancelled.cancelled = true;
    queue_.push_back(cancelled);
    return true;
  }

  bool run_one() {
    if (queue_.empty()) return false;
    Pending pending = queue_.front();
    queue_.pop_front();
    if (pending.cancelled) {
      complete(pending, -ECANCELED);
      return true;
    }
    ++pending.operations;
    const ssize_t result = execute(pending);
    if (result == -EINTR) {
      queue_.push_back(pending);
      return true;
    }
    if (result <= 0) {
      complete(pending, result);
      return true;
    }
    pending.transferred += size_t(result);
    if (pending.processor != nullptr) {
      const size_t processed_length = pending.exact
          ? pending.transferred : size_t(result);
      if (!pending.exact || pending.transferred == pending.length) {
        const int processed = pending.processor(
            pending.processor_context, processed_length);
        if (processed < 0) {
          complete(pending, processed);
          return true;
        }
        if (processed > 0) {
          complete(pending, ssize_t(pending.transferred));
          return true;
        }
        pending.transferred = 0;
      }
      queue_.push_back(pending);
      return true;
    }
    if (pending.exact && pending.transferred < pending.length) {
      queue_.push_back(pending);
      return true;
    }
    complete(pending, ssize_t(pending.transferred));
    return true;
  }

  void run() { while (run_one()) {} }

  [[nodiscard]] bool empty() const noexcept { return queue_.empty(); }
  [[nodiscard]] bool parameters_preserved() const noexcept {
    return parameters_preserved_;
  }
  [[nodiscard]] bool saw_background_file_write() const noexcept {
    return saw_background_file_write_;
  }
  [[nodiscard]] size_t syscall_count() const noexcept { return syscall_count_; }
  [[nodiscard]] bool saw_connect() const noexcept { return saw_connect_; }

 private:
  struct Pending {
    AsyncIoRequest* request = nullptr;
    AsyncIoRequest::Kind kind = AsyncIoRequest::RECEIVE;
    int fd = -1;
    int output_fd = -1;
    void* data = nullptr;
    size_t length = 0;
    off_t input_offset = -1;
    off_t output_offset = -1;
    const sockaddr* address = nullptr;
    socklen_t address_length = 0;
    unsigned flags = 0;
    bool exact = false;
    bool force_async = false;
    AsyncIoRequest::Processor processor = nullptr;
    void* processor_context = nullptr;
    AsyncIoRequest::Complete complete = nullptr;
    void* context = nullptr;
    size_t transferred = 0;
    size_t operations = 0;
    bool cancelled = false;
  };

  static constexpr size_t kFragmentSize = 7;

  bool contains(AsyncIoRequest* request) const noexcept {
    return std::find(active_.begin(), active_.end(), request) != active_.end();
  }

  ssize_t execute(Pending& pending) noexcept {
    ++syscall_count_;
    const size_t progress = pending.exact ? pending.transferred : 0;
    const size_t remaining = pending.length - progress;
    const size_t length = std::min(remaining, kFragmentSize);
    char* const data = static_cast<char*>(pending.data);
    ssize_t result = -1;
    switch (pending.kind) {
      case AsyncIoRequest::RECEIVE:
        result = ::recv(pending.fd, data + progress, length, int(pending.flags));
        break;
      case AsyncIoRequest::READ:
        result = ::read(pending.fd, data + progress, length);
        break;
      case AsyncIoRequest::PREAD:
        result = ::pread(pending.fd, data + progress, length,
                         pending.input_offset + off_t(progress));
        break;
      case AsyncIoRequest::SEND:
        result = ::send(pending.fd, data + progress, length, int(pending.flags));
        break;
      case AsyncIoRequest::WRITE:
        result = ::write(pending.fd, data + progress, length);
        break;
      case AsyncIoRequest::PWRITE:
        result = ::pwrite(pending.fd, data + progress, length,
                          pending.output_offset + off_t(progress));
        saw_background_file_write_ |= pending.force_async;
        break;
      case AsyncIoRequest::SPLICE: {
        off_t input_offset = pending.input_offset + off_t(progress);
        off_t output_offset = pending.output_offset + off_t(progress);
        off_t* const input = pending.input_offset >= 0 ? &input_offset : nullptr;
        off_t* const output = pending.output_offset >= 0 ? &output_offset : nullptr;
        result = ::splice(pending.fd, input, pending.output_fd, output,
                          length, pending.flags);
        saw_background_file_write_ |=
            pending.force_async && pending.output_offset >= 0;
        break;
      }
      case AsyncIoRequest::CONNECT:
        saw_connect_ = true;
        result = ::connect(pending.fd, pending.address, pending.address_length);
        if (result < 0 && errno == EINPROGRESS) {
          pollfd event{pending.fd, POLLOUT, 0};
          int ready = -1;
          do {
            ready = ::poll(&event, 1, 5000);
          } while (ready < 0 && errno == EINTR);
          if (ready <= 0) return ready == 0 ? -ETIMEDOUT : -errno;
          int error = 0;
          socklen_t size = sizeof(error);
          if (::getsockopt(pending.fd, SOL_SOCKET, SO_ERROR,
                           &error, &size) != 0) return -errno;
          return -error;
        }
        break;
    }
    return result < 0 ? -errno : result;
  }

  bool same_parameters(const Pending& pending) const noexcept {
    const AsyncIoRequest& request = *pending.request;
    return request.kind == pending.kind && request.fd == pending.fd &&
           request.output_fd == pending.output_fd && request.data == pending.data &&
           request.length == pending.length &&
           request.input_offset == pending.input_offset &&
           request.output_offset == pending.output_offset &&
           request.address == pending.address &&
           request.address_length == pending.address_length &&
           request.flags == pending.flags && request.exact == pending.exact &&
           request.force_async == pending.force_async &&
           request.processor == pending.processor &&
           request.processor_context == pending.processor_context &&
           request.complete == pending.complete && request.context == pending.context;
  }

  void complete(const Pending& pending, ssize_t result) noexcept {
    parameters_preserved_ &= same_parameters(pending);
    AsyncIoRequest* const request = pending.request;
    const auto callback = pending.complete;
    void* const context = pending.context;
    request->transferred = pending.transferred;
    request->operations = pending.operations;
    const auto active = std::find(active_.begin(), active_.end(), request);
    check(active != active_.end(), "completed request was not active");
    active_.erase(active);
    callback(context, result);
  }

  std::deque<Pending> queue_;
  std::vector<AsyncIoRequest*> active_;
  bool parameters_preserved_ = true;
  bool saw_background_file_write_ = false;
  bool saw_connect_ = false;
  size_t syscall_count_ = 0;
};

enum class ServerCase { RANGE, HEAD, EARLY_EOF, RECONNECT, CANCEL };

struct ServerInput {
  int listener = -1;
  ServerCase scenario = ServerCase::RANGE;
  std::vector<std::byte> body;
};

void run_server(ServerInput input) {
  UniqueFd probe = accept_one(input.listener);
  finish_probe(probe.get());
  probe.reset();
  UniqueFd socket = accept_one(input.listener);
  if (input.scenario == ServerCase::CANCEL) {
    socket.reset();
    UniqueFd retry = accept_one(input.listener);
    const std::string request = read_request_head(retry.get());
    check(request.starts_with("HEAD /after-cancel HTTP/1.1\r\n"),
          "cancelled client did not reconnect for its next request");
    send_text(retry.get(),
              "HTTP/1.1 204 No Content\r\nconnection: close\r\n\r\n");
    return;
  }

  const std::string request = read_request_head(socket.get());
  if (input.scenario == ServerCase::RANGE) {
    check(request.starts_with("GET /range HTTP/1.1\r\n"),
          "unexpected range request line");
    check(request.find("range: bytes=5-101\r\n") != std::string::npos,
          "range request omitted its exact byte interval");
    const std::string header =
        "HTTP/1.1 206 Partial Content\r\n"
        "content-length: 97\r\n"
        "content-range: bytes 5-101/1000\r\n"
        "x-fragmented: yes\r\n\r\n";
    const size_t coalesced_header_bytes = 2;
    const size_t fragmented_length = header.size() - coalesced_header_bytes;
    for (size_t offset = 0; offset < fragmented_length; offset += 3) {
      const size_t length = std::min<size_t>(3, fragmented_length - offset);
      write_exact(socket.get(), header.data() + offset, length);
    }
    std::vector<std::byte> coalesced(coalesced_header_bytes + input.body.size());
    memcpy(coalesced.data(), header.data() + fragmented_length,
           coalesced_header_bytes);
    memcpy(coalesced.data() + coalesced_header_bytes,
           input.body.data(), input.body.size());
    write_exact(socket.get(), coalesced.data(), coalesced.size());
    return;
  }
  if (input.scenario == ServerCase::HEAD) {
    check(request.starts_with("HEAD /metadata HTTP/1.1\r\n"),
          "unexpected HEAD request line");
    check(request.find("range:") == std::string::npos,
          "HEAD without a destination incorrectly sent a range");
    send_text(socket.get(),
              "HTTP/1.1 200 OK\r\ncontent-length: 12345\r\n"
              "x-metadata: present\r\n\r\n");
    return;
  }
  if (input.scenario == ServerCase::RECONNECT) {
    check(request.starts_with("HEAD /close HTTP/1.1\r\n"),
          "unexpected closing request line");
    send_text(socket.get(),
              "HTTP/1.1 204 No Content\r\nconnection: close\r\n\r\n");
    socket.reset();
    UniqueFd reconnected = accept_one(input.listener);
    const std::string second = read_request_head(reconnected.get());
    check(second.starts_with("HEAD /reconnected HTTP/1.1\r\n"),
          "client did not reconnect after Connection: close");
    send_text(reconnected.get(),
              "HTTP/1.1 204 No Content\r\nconnection: close\r\n\r\n");
    return;
  }
  check(request.starts_with("GET /early-eof HTTP/1.1\r\n"),
        "unexpected EOF request line");
  send_text(socket.get(),
            "HTTP/1.1 200 OK\r\ncontent-length: 20\r\n\r\nshort");
}

class TestServer {
 public:
  TestServer(ServerCase scenario, std::vector<std::byte> body = {})
      : listener_(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP)) {
    check(bool(listener_), "socket failed");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    const int bind_result = ::bind(
        listener_.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address));
    check(bind_result == 0, "bind failed");
    const int listen_result = ::listen(listener_.get(), 2);
    check(listen_result == 0, "listen failed");
    socklen_t address_length = sizeof(address);
    const int name_result = ::getsockname(
        listener_.get(), reinterpret_cast<sockaddr*>(&address), &address_length);
    check(name_result == 0, "getsockname failed");
    port_ = ntohs(address.sin_port);
    thread_ = std::jthread(run_server,
        ServerInput{.listener = listener_.get(), .scenario = scenario,
                    .body = std::move(body)});
  }

  [[nodiscard]] uint16_t port() const noexcept { return port_; }

 private:
  UniqueFd listener_;
  uint16_t port_ = 0;
  std::jthread thread_;
};

class TestFileSink final : public RangeFileSink {
 public:
  TestFileSink(int fd, uint64_t offset, bool background_write) noexcept
      : RangeFileSink(fd, offset, background_write) {}

  void progress(const Response&, bool complete) override {
    ++progress_calls;
    completed |= complete;
  }

  size_t progress_calls = 0;
  bool completed = false;
};

struct CompletionState {
  std::unique_ptr<AsyncHttpOperation> operation;
  Response response;
  std::exception_ptr error;
  size_t calls = 0;
  bool destroyed_in_callback = false;
};

void complete_http(void* context, Response&& response,
                   std::exception_ptr error) noexcept {
  auto& state = *static_cast<CompletionState*>(context);
  ++state.calls;
  state.response = std::move(response);
  state.error = std::move(error);
  state.operation.reset();
  state.destroyed_in_callback = true;
}

std::unique_ptr<HttpClient> connect_client(uint16_t port) {
  return HttpClient::connect(
      "127.0.0.1", port, "async.test", false,
      kRequestIoTimeoutMs, kConnectTimeoutMs, kProtocolProbeTimeoutMs);
}

void test_range_to_fd() {
  std::vector<std::byte> expected(97);
  for (size_t i = 0; i < expected.size(); ++i)
    expected[i] = std::byte((i * 29U + 11U) & 0xffU);
  TestServer server(ServerCase::RANGE, expected);
  auto client = connect_client(server.port());
  UniqueFd file(::memfd_create("async-http-range", MFD_CLOEXEC));
  check(bool(file), "memfd_create failed");
  std::array<std::byte, 128> sentinel{};
  sentinel.fill(std::byte{0xa5});
  write_exact(file.get(), sentinel.data(), sentinel.size());

  constexpr uint64_t destination_offset = 19;
  TestFileSink destination(file.get(), destination_offset, true);
  AsyncHttpRequest request;
  request.path = "/range";
  request.range = true;
  request.offset = 5;
  request.length = expected.size();
  request.destination = &destination;

  QueueExecutor executor;
  CompletionState completion;
  completion.operation = client->make_async_request(
      executor, std::move(request), complete_http, &completion);
  completion.operation->start();
  check(completion.calls == 0, "submit completed inline");
  executor.run();

  check(completion.calls == 1, "range completion count was not one");
  check(!completion.error, "range request failed");
  check(completion.destroyed_in_callback,
        "range operation was not destroyed in its callback");
  check(completion.response.status == 206, "unexpected range status");
  check(completion.response.body_bytes == expected.size(),
        "range byte count mismatch");
  check(completion.response.externally_spliced_bytes +
            completion.response.fallback_copied_bytes == expected.size(),
        "range accounting mismatch");
  check(completion.response.transport_splice_calls > 1,
        "range body was not fragmented across splice operations");
  check(completion.response.headers.at("x-fragmented") == "yes",
        "fragmented response header was not captured");
  if (destination.offset() != destination_offset + expected.size()) {
    fprintf(stderr, "destination offset: actual=%llu expected=%llu\n",
            static_cast<unsigned long long>(destination.offset()),
            static_cast<unsigned long long>(
                destination_offset + expected.size()));
    fail("destination offset was not advanced exactly");
  }
  check(destination.progress_calls > 1 && destination.completed,
        "destination progress did not finish");
  check(executor.parameters_preserved(), "executor changed request parameters");
  check(executor.saw_background_file_write(),
        "background file-write flag was not propagated");
  check(executor.syscall_count() > 20, "I/O was not sufficiently fragmented");

  std::array<std::byte, 128> actual{};
  const ssize_t read_result = ::pread(file.get(), actual.data(), actual.size(), 0);
  check(read_result == ssize_t(actual.size()), "range result read failed");
  check(std::equal(actual.begin(), actual.begin() + destination_offset,
                   sentinel.begin()), "range write changed the file prefix");
  check(std::equal(expected.begin(), expected.end(),
                   actual.begin() + destination_offset),
        "range write produced incorrect bytes");
  check(std::equal(actual.begin() + destination_offset + expected.size(),
                   actual.end(),
                   sentinel.begin() + destination_offset + expected.size()),
        "range write changed the file suffix");
}

void test_head_without_destination() {
  TestServer server(ServerCase::HEAD);
  auto client = connect_client(server.port());
  AsyncHttpRequest request;
  request.method = "HEAD";
  request.path = "/metadata";
  request.offset = 77;
  request.length = 99;
  QueueExecutor executor;
  CompletionState completion;
  completion.operation = client->make_async_request(
      executor, std::move(request), complete_http, &completion);
  completion.operation->start();
  check(completion.calls == 0, "HEAD submit completed inline");
  executor.run();
  check(completion.calls == 1, "HEAD completion count was not one");
  check(!completion.error, "HEAD request failed");
  check(completion.response.status == 200, "unexpected HEAD status");
  check(completion.response.body_bytes == 0, "HEAD consumed a response body");
  check(completion.response.headers.at("content-length") == "12345",
        "HEAD content length was not captured");
  check(completion.destroyed_in_callback,
        "HEAD operation was not destroyed in its callback");
  check(executor.parameters_preserved(), "executor changed HEAD parameters");
}

void test_early_eof() {
  TestServer server(ServerCase::EARLY_EOF);
  auto client = connect_client(server.port());
  AsyncHttpRequest request;
  request.path = "/early-eof";
  request.max_response_body = 64;
  QueueExecutor executor;
  CompletionState completion;
  completion.operation = client->make_async_request(
      executor, std::move(request), complete_http, &completion);
  completion.operation->start();
  check(completion.calls == 0, "EOF submit completed inline");
  executor.run();
  check(completion.calls == 1, "EOF completion count was not one");
  check(bool(completion.error), "early EOF was accepted");
  check(completion.destroyed_in_callback,
        "failed operation was not destroyed in its callback");
  check(executor.empty(), "I/O remained queued after EOF completion");
  check(executor.parameters_preserved(), "executor changed EOF parameters");
}

void test_reconnect_after_close() {
  TestServer server(ServerCase::RECONNECT);
  auto client = connect_client(server.port());
  QueueExecutor executor;

  AsyncHttpRequest first_request;
  first_request.method = "HEAD";
  first_request.path = "/close";
  CompletionState first;
  first.operation = client->make_async_request(
      executor, std::move(first_request), complete_http, &first);
  first.operation->start();
  executor.run();
  check(first.calls == 1 && !first.error && first.response.status == 204,
        "closing response failed");

  AsyncHttpRequest second_request;
  second_request.method = "HEAD";
  second_request.path = "/reconnected";
  CompletionState second;
  second.operation = client->make_async_request(
      executor, std::move(second_request), complete_http, &second);
  second.operation->start();
  check(second.calls == 0, "reconnect completed inline");
  executor.run();
  print_error(second.error);
  check(second.calls == 1 && !second.error && second.response.status == 204,
        "request after Connection: close failed");
  check(second.destroyed_in_callback,
        "reconnected operation was not destroyed in its callback");
  check(executor.saw_connect(), "reconnect did not use asynchronous CONNECT");
  check(executor.parameters_preserved(),
        "executor changed reconnect parameters");
}

void test_queued_cancel() {
  TestServer server(ServerCase::CANCEL);
  auto client = connect_client(server.port());
  AsyncHttpRequest request;
  request.method = "HEAD";
  request.path = "/cancel";
  QueueExecutor executor;
  CompletionState completion;
  completion.operation = client->make_async_request(
      executor, std::move(request), complete_http, &completion);
  completion.operation->start();
  check(completion.calls == 0, "cancel test submit completed inline");
  completion.operation->cancel();
  check(completion.calls == 0, "cancel completed inline");
  executor.run();
  check(completion.calls == 1, "cancel completion count was not one");
  check(bool(completion.error), "cancel did not report an error");
  check(completion.destroyed_in_callback,
        "cancelled operation was not destroyed in its callback");
  check(executor.empty(), "I/O remained queued after cancellation");
  check(executor.parameters_preserved(), "executor changed cancel parameters");

  AsyncHttpRequest retry_request;
  retry_request.method = "HEAD";
  retry_request.path = "/after-cancel";
  CompletionState retry;
  retry.operation = client->make_async_request(
      executor, std::move(retry_request), complete_http, &retry);
  retry.operation->start();
  check(retry.calls == 0, "post-cancel reconnect completed inline");
  executor.run();
  check(retry.calls == 1 && !retry.error && retry.response.status == 204,
        "client was not reusable after cancellation");
  check(executor.saw_connect(), "post-cancel request did not reconnect");
  check(executor.parameters_preserved(),
        "executor changed post-cancel parameters");
}

int main() {
  test_range_to_fd();
  test_head_without_destination();
  test_early_eof();
  test_reconnect_after_close();
  test_queued_cancel();
  return 0;
}
