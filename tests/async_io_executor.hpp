#pragma once

#include "io.hpp"

#include <algorithm>
#include <deque>
#include <vector>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

// Completion-queued test executor. Small fragments exercise partial CQEs while
// snapshots ensure an outstanding request is never mutated before completion.
class TestAsyncIoExecutor final : public IoExecutor {
 public:
  explicit TestAsyncIoExecutor(size_t fragment_size = 7)
      : fragment_size_(std::max<size_t>(fragment_size, 1)) {}

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

  const size_t fragment_size_;

  bool contains(AsyncIoRequest* request) const noexcept {
    return std::find(active_.begin(), active_.end(), request) != active_.end();
  }

  ssize_t execute(Pending& pending) noexcept {
    ++syscall_count_;
    const size_t progress = pending.exact ? pending.transferred : 0;
    const size_t remaining = pending.length - progress;
    const size_t length = std::min(remaining, fragment_size_);
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
          int ready;
          do { ready = ::poll(&event, 1, 5000); } while (ready < 0 && errno == EINTR);
          if (ready <= 0) return ready == 0 ? -ETIMEDOUT : -errno;
          int error = 0;
          socklen_t size = sizeof(error);
          if (::getsockopt(pending.fd, SOL_SOCKET, SO_ERROR, &error, &size) != 0) {
            return -errno;
          }
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
    if (active == active_.end()) {
      fprintf(stderr, "completed test request was not active\n");
      abort();
    }
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
