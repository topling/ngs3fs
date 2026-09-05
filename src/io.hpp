#pragma once

#include <unistd.h>

#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h>
#include <span>
#include <atomic>
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

// Short native waits for legacy workers, or try/recheck/eventfd waits for
// explicit I/O continuations. No descriptor is allocated until contention.
class IoMutex {
 public:
  IoMutex() = default;
  ~IoMutex();
  IoMutex(const IoMutex&) = delete;
  IoMutex& operator=(const IoMutex&) = delete;

  bool try_lock() noexcept {
    bool expected = false;
    return locked_.compare_exchange_strong(expected, true,
        std::memory_order_acquire, std::memory_order_relaxed);
  }
  void lock() noexcept {
    while (!try_lock()) locked_.wait(true, std::memory_order_acquire);
  }
  void unlock() noexcept;
  int begin_async_wait();
  void end_async_wait() noexcept {
    waiters_.fetch_sub(1, std::memory_order_acq_rel);
  }

 private:
  std::atomic<bool> locked_{false};
  std::atomic<unsigned> waiters_{0};
  std::atomic<int> event_{-1};
};

inline constexpr size_t kPreferredIoSize = 256U * 1024U;

class IoExecutor;
class FuseReactor;

// The caller owns this request, its descriptors, buffers, and callback context
// until complete(context, result). A result is bytes transferred or -errno.
// Submit/cancel and completion all run on the executor's owner reactor.
struct AsyncIoRequest {
  enum Kind { RECEIVE, READ, PREAD, SEND, WRITE, PWRITE, SPLICE, CONNECT };
  using Complete = void (*)(void*, ssize_t) noexcept;
  using Processor = int (*)(void*, size_t) noexcept;

  Kind kind                    = RECEIVE;
  int fd                       = -1;
  int output_fd                = -1;
  void* data                   = nullptr;
  size_t length                = 0;
  off_t input_offset           = -1;
  off_t output_offset          = -1;
  const sockaddr* address      = nullptr;
  socklen_t address_length     = 0;
  unsigned flags               = 0;
  int timeout_ms               = 0;
  bool exact                   = false;
  bool force_async             = false;
  Processor processor          = nullptr;
  void* processor_context      = nullptr;
  Complete complete            = nullptr;
  void* context                = nullptr;
  // Written before completion, including partial progress on failure.
  size_t transferred           = 0;
  size_t operations            = 0;

  [[nodiscard]] bool pending() const noexcept { return implementation_ != nullptr; }

 private:
  friend class FuseReactor;
  void* implementation_        = nullptr;
  IoExecutor* owner_            = nullptr;
};

class IoExecutor {
 public:
  using ReceiveProcessor = int (*)(void*, size_t) noexcept;

  virtual ~IoExecutor() = default;

  // false: not accepted, errno explains why, and no completion will run.
  // true: exactly one non-inline completion, after the original kernel I/O
  // has retired. The callback may destroy or resubmit the same request.
  virtual bool submit(AsyncIoRequest& request) noexcept;
  // true means cancellation was requested; completion still owns retirement.
  virtual bool cancel(AsyncIoRequest& request) noexcept;


};

IoExecutor* io_executor() noexcept;

class IoExecutorScope {
 public:
  IoExecutorScope(IoExecutor* executor, int timeout_ms) noexcept;
  ~IoExecutorScope();

  IoExecutorScope(const IoExecutorScope&) = delete;
  IoExecutorScope& operator=(const IoExecutorScope&) = delete;

 private:
  IoExecutor* previous_executor_ = nullptr;
  int previous_timeout_ms_       = 0;
};

ssize_t io_receive(int fd, void* data, size_t length, int flags = 0,
                   int timeout_ms = 0) noexcept;
ssize_t io_read(int fd, void* data, size_t length,
                int timeout_ms = 0) noexcept;
ssize_t io_pread(int fd, void* data, size_t length, off_t offset,
                 int timeout_ms = 0) noexcept;
ssize_t io_receive_exact(int fd, void* data, size_t length, int flags = 0,
                         int timeout_ms = 0) noexcept;
ssize_t io_receive_exact_then(
    int fd, void* data, size_t length, int flags,
    IoExecutor::ReceiveProcessor processor, void* context,
    int timeout_ms = 0) noexcept;
ssize_t io_receive_until(int fd, void* data, size_t length, int flags,
                         IoExecutor::ReceiveProcessor processor,
                         void* context,
                         int timeout_ms = 0) noexcept;
ssize_t io_send(int fd, const void* data, size_t length, int flags,
                int timeout_ms = 0) noexcept;
ssize_t io_send_exact(int fd, const void* data, size_t length, int flags,
                      int timeout_ms = 0) noexcept;
ssize_t io_pwrite(int fd, const void* data, size_t length,
                  off_t offset, int timeout_ms = 0,
                  bool force_async = false) noexcept;
ssize_t io_splice(int input_fd, off_t* input_offset,
                  int output_fd, off_t* output_offset,
                  size_t length, unsigned flags,
                  int timeout_ms = 0,
                  bool force_async = false) noexcept;
ssize_t io_splice_exact(int input_fd, off_t* input_offset,
                        int output_fd, off_t* output_offset,
                        size_t length, unsigned flags,
                        int timeout_ms = 0,
                        size_t* calls = nullptr,
                        bool force_async = false) noexcept;
int io_connect(int fd, const sockaddr* address,
               socklen_t address_length, int timeout_ms) noexcept;

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
void receive_all(int socket_fd, std::span<std::byte> bytes);
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
void configure_blocking_socket(int fd, int io_timeout_ms);
bool configure_socket_receive_buffer(int fd, size_t requested) noexcept;
