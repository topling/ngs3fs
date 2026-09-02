#pragma once

#include <unistd.h>

#include <stddef.h>
#include <stdint.h>
#include <span>
#include <string_view>
#include <system_error>
#include <utility>

// File descriptors, pipes, splice helpers, and sockets.
class UniqueFd {
 public:
  UniqueFd() = default;
  explicit UniqueFd(int fd) noexcept : fd_(fd) {}

  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;

  UniqueFd(UniqueFd&& other) noexcept : fd_(other.release()) {}

  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
      reset(other.release());
    }
    return *this;
  }

  ~UniqueFd() { reset(); }

  [[nodiscard]] int get() const noexcept { return fd_; }
  [[nodiscard]] explicit operator bool() const noexcept { return fd_ >= 0; }

  int release() noexcept {
    return std::exchange(fd_, -1);
  }

  void reset(int fd = -1) noexcept {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = fd;
  }

 private:
  int fd_ = -1;
};

inline constexpr size_t kPreferredIoSize = 256U * 1024U;

class Pipe {
 public:
  static Pipe create(size_t preferred_capacity = kPreferredIoSize);

  Pipe() = default;
  Pipe(UniqueFd read_end, UniqueFd write_end, size_t capacity) noexcept;

  [[nodiscard]] int read_fd() const noexcept { return read_end_.get(); }
  [[nodiscard]] int write_fd() const noexcept { return write_end_.get(); }
  [[nodiscard]] size_t capacity() const noexcept { return capacity_; }
  void close_write_end() noexcept { write_end_.reset(); }

 private:
  UniqueFd read_end_;
  UniqueFd write_end_;
  size_t capacity_ = 0;
};

size_t splice_exact(int source_fd, int destination_fd,
                    size_t length, unsigned int flags,
                    size_t* calls = nullptr);

size_t splice_from_fd_exact(int source_fd, uint64_t& source_offset,
                            int destination_fd, size_t length,
                            unsigned int flags);

size_t splice_some(int source_fd, uint64_t* source_offset,
                   int destination_fd, size_t length,
                   unsigned int flags);

size_t tee_exact(int source_fd, int destination_fd,
                 size_t length, unsigned int flags);

size_t splice_to_fd_exact(int source_fd, int destination_fd,
                          uint64_t& destination_offset,
                          size_t length, unsigned int flags);

size_t sendfile_exact(int socket_fd, int source_fd,
                      uint64_t& source_offset, size_t length);

void write_all(int fd, std::span<const std::byte> bytes);
void send_all(int socket_fd, std::span<const std::byte> bytes);
void read_all(int fd, std::span<std::byte> bytes);

inline constexpr int kConnectTimeoutMs = 5'000;
inline constexpr int kRequestIoTimeoutMs = 30'000;
inline constexpr int kProtocolProbeTimeoutMs = 1'000;
inline constexpr size_t kDefaultSocketReceiveBufferSize =
    2U * 1024U * 1024U;

UniqueFd connect_tcp(std::string_view host, uint16_t port,
                     int connect_timeout_ms = kConnectTimeoutMs,
                     int io_timeout_ms = kRequestIoTimeoutMs,
                     size_t receive_buffer_size = 0);

void set_socket_receive_timeout(int fd, int timeout_ms);
