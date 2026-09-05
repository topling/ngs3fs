#include "io.hpp"

#include <errno.h>
#include <sys/eventfd.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <chrono>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>

IoMutex::~IoMutex() {
  const int fd = event_.load(std::memory_order_relaxed);
  if (fd >= 0) ::close(fd);
}

int IoMutex::begin_async_wait() {
  int fd = event_.load(std::memory_order_acquire);
  if (fd < 0) {
    UniqueFd candidate(::eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE));
    if (!candidate) {
      throw std::system_error(errno, std::generic_category(), "eventfd(directory mutex)");
    }
    fd = -1;
    if (event_.compare_exchange_strong(fd, candidate.get(),
            std::memory_order_release, std::memory_order_acquire)) {
      fd = candidate.release();
    }
  }
  waiters_.fetch_add(1, std::memory_order_acq_rel);
  return fd;
}

void IoMutex::unlock() noexcept {
  locked_.store(false, std::memory_order_release);
  locked_.notify_one();
  const uint64_t waiters = waiters_.load(std::memory_order_acquire);
  if (waiters == 0) return;
  const int saved_errno = errno;
  const int fd = event_.load(std::memory_order_acquire);
  ssize_t result;
  do {
    result = ::write(fd, &waiters, sizeof(waiters));
  } while (result < 0 && errno == EINTR);
  errno = saved_errno;
}

thread_local IoExecutor* current_io_executor = nullptr;
thread_local int current_io_timeout_ms       = 0;

IoExecutor* io_executor() noexcept {
  return current_io_executor;
}

bool IoExecutor::submit(AsyncIoRequest&) noexcept {
  errno = ENOTSUP;
  return false;
}

bool IoExecutor::cancel(AsyncIoRequest&) noexcept {
  errno = ENOTSUP;
  return false;
}

IoExecutorScope::IoExecutorScope(IoExecutor* executor,
                                 int timeout_ms) noexcept
    : previous_executor_(current_io_executor),
      previous_timeout_ms_(current_io_timeout_ms) {
  current_io_executor   = executor;
  current_io_timeout_ms = timeout_ms;
}

IoExecutorScope::~IoExecutorScope() {
  current_io_executor   = previous_executor_;
  current_io_timeout_ms = previous_timeout_ms_;
}

int effective_io_timeout(int timeout_ms) noexcept {
  return timeout_ms > 0 ? timeout_ms : current_io_timeout_ms;
}

ssize_t io_receive(int fd, void* data, size_t length, int flags,
                   int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::recv(fd, data, length, flags);
}

ssize_t io_read(int fd, void* data, size_t length,
                int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::read(fd, data, length);
}

ssize_t io_pread(int fd, void* data, size_t length, off_t offset,
                 int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::pread(fd, data, length, offset);
}

ssize_t io_receive_exact(int fd, void* data, size_t length, int flags,
                         int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  size_t offset = 0;
  while (offset != length) {
    const ssize_t result = ::recv(
        fd, static_cast<u_char*>(data) + offset, length - offset, flags);
    if (result > 0) {
      offset += size_t(result);
      continue;
    }
    if (result == 0) {
      break;
    }
    if (errno != EINTR) {
      return -1;
    }
  }
  return ssize_t(offset);
}

ssize_t io_receive_exact_then(
    int fd, void* data, size_t length, int flags,
    IoExecutor::ReceiveProcessor processor, void* context,
    int timeout_ms) noexcept {
  if (processor == nullptr) {
    errno = EINVAL;
    return -1;
  }
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  const ssize_t received = io_receive_exact(
      fd, data, length, flags, timeout_ms);
  if (received != ssize_t(length)) {
    return received;
  }
  const int processed = processor(context, length);
  if (processed > 0) {
    return received;
  }
  errno = processed < 0 ? -processed : EIO;
  return -1;
}

ssize_t io_receive_until(int fd, void* data, size_t length, int flags,
                         IoExecutor::ReceiveProcessor processor,
                         void* context, int) noexcept {
  if (processor == nullptr || length == 0) {
    errno = EINVAL;
    return -1;
  }
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  size_t total = 0;
  for (;;) {
    const ssize_t result = ::recv(fd, data, length, flags);
    if (result > 0) {
      total += size_t(result);
      const int processed = processor(context, size_t(result));
      if (processed > 0) {
        return ssize_t(total);
      }
      if (processed < 0) {
        errno = -processed;
        return -1;
      }
      continue;
    }
    if (result == 0) {
      return ssize_t(total);
    }
    if (errno != EINTR) {
      return -1;
    }
  }
}

ssize_t io_send(int fd, const void* data, size_t length, int flags,
                int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::send(fd, data, length, flags);
}

ssize_t io_send_exact(int fd, const void* data, size_t length, int flags,
                      int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  size_t offset = 0;
  while (offset != length) {
    const ssize_t result = ::send(
        fd, static_cast<const u_char*>(data) + offset,
        length - offset, flags);
    if (result > 0) {
      offset += size_t(result);
      continue;
    }
    if (result == 0) {
      errno = EIO;
      return -1;
    }
    if (errno != EINTR) {
      return -1;
    }
  }
  return ssize_t(offset);
}

ssize_t io_pwrite(int fd, const void* data, size_t length,
                  off_t offset, int,
                  bool) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::pwrite(fd, data, length, offset);
}

ssize_t io_splice(int input_fd, off_t* input_offset,
                  int output_fd, off_t* output_offset,
                  size_t length, unsigned flags,
                  int, bool) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::splice(input_fd, input_offset, output_fd, output_offset,
                  length, flags);
}

ssize_t io_splice_exact(int input_fd, off_t* input_offset,
                        int output_fd, off_t* output_offset,
                        size_t length, unsigned flags,
                        int, size_t* calls,
                        bool) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  size_t transferred = 0;
  while (transferred != length) {
    if (calls != nullptr) {
      ++*calls;
    }
    const ssize_t result = ::splice(
        input_fd, input_offset, output_fd, output_offset,
        length - transferred, flags);
    if (result > 0) {
      transferred += size_t(result);
      continue;
    }
    if (result == 0) {
      break;
    }
    if (errno != EINTR) {
      return -1;
    }
  }
  return ssize_t(transferred);
}

int io_connect(int fd, const sockaddr* address,
               socklen_t address_length, int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::connect(fd, address, address_length);
}

// Pipe and descriptor data movement.
[[noreturn]] void pipe_throw_errno(const char* operation) {
  throw std::system_error(errno, std::generic_category(), operation);
}

Pipe::Pipe(UniqueFd read_end, UniqueFd write_end,
           size_t capacity) noexcept
    : read_end_(std::move(read_end)),
      write_end_(std::move(write_end)),
      capacity_(capacity) {}

Pipe Pipe::create(size_t preferred_capacity) {
  int raw_fds[2] = {-1, -1};
  if (::pipe2(raw_fds, O_CLOEXEC | O_NONBLOCK) != 0) {
    pipe_throw_errno("pipe2");
  }

  UniqueFd read_end(raw_fds[0]);
  UniqueFd write_end(raw_fds[1]);

  int capacity = ::fcntl(write_end.get(), F_GETPIPE_SZ);
  if (capacity < 0) {
    pipe_throw_errno("fcntl(F_GETPIPE_SZ)");
  }

  if (preferred_capacity > static_cast<size_t>(capacity)) {
    const size_t bounded = std::min(
        preferred_capacity,
        static_cast<size_t>(std::numeric_limits<int>::max()));
    const int requested = static_cast<int>(bounded);
    const int resized = ::fcntl(write_end.get(), F_SETPIPE_SZ, requested);
    if (resized >= 0) {
      capacity = resized;
    } else if (errno != EPERM && errno != EINVAL) {
      pipe_throw_errno("fcntl(F_SETPIPE_SZ)");
    }
  }

  return Pipe(std::move(read_end), std::move(write_end),
              static_cast<size_t>(capacity));
}

size_t splice_exact(int source_fd, int destination_fd,
                    size_t length, unsigned int flags, size_t* calls) {
  const ssize_t result = io_splice_exact(
      source_fd, nullptr, destination_fd, nullptr, length, flags, 0, calls,
      false);
  if (result < 0) {
    pipe_throw_errno("splice");
  }
  if (size_t(result) != length) {
    throw std::system_error(
        std::make_error_code(std::errc::connection_reset),
        "splice reached EOF before the requested length");
  }
  return length;
}

size_t splice_from_fd_exact(int source_fd, uint64_t& source_offset,
                            int destination_fd, size_t length,
                            unsigned int flags) {
  size_t transferred = 0;
  while (transferred < length) {
    off_t offset = off_t(source_offset);
    const ssize_t result = io_splice(
        source_fd, &offset, destination_fd, nullptr,
        length - transferred, flags);
    if (result > 0) {
      transferred += size_t(result);
      source_offset = uint64_t(offset);
      continue;
    }
    if (result == 0) {
      throw std::system_error(
          std::make_error_code(std::errc::connection_reset),
          "splice reached EOF before the requested length");
    }
    if (errno == EINTR) {
      continue;
    }
    pipe_throw_errno("splice(from fd)");
  }
  return transferred;
}

size_t splice_some(int source_fd, uint64_t* source_offset,
                   int destination_fd, size_t length,
                   unsigned int flags) {
  for (;;) {
    off_t offset = source_offset == nullptr ? 0 : off_t(*source_offset);
    off_t* position = source_offset == nullptr ? nullptr : &offset;
    const ssize_t result = io_splice(source_fd, position, destination_fd,
                                     nullptr, length, flags);
    if (result > 0) {
      if (source_offset != nullptr) {
        *source_offset = uint64_t(offset);
      }
      return size_t(result);
    }
    if (result == 0) {
      throw std::system_error(
          std::make_error_code(std::errc::connection_reset),
          "splice reached EOF before the requested length");
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return 0;
    }
    pipe_throw_errno("splice");
  }
}

size_t tee_exact(int source_fd, int destination_fd,
                 size_t length, unsigned int flags) {
  for (;;) {
    const ssize_t result = ::tee(source_fd, destination_fd, length, flags);
    if (result == ssize_t(length)) {
      return length;
    }
    if (result > 0) {
      throw std::system_error(
          std::make_error_code(std::errc::io_error),
          "tee returned a short clone");
    }
    if (result == 0) {
      throw std::system_error(
          std::make_error_code(std::errc::connection_reset),
          "tee reached EOF before the requested length");
    }
    if (errno == EINTR) {
      continue;
    }
    pipe_throw_errno("tee");
  }
}

size_t splice_to_fd_exact(int source_fd, int destination_fd,
                          uint64_t& destination_offset,
                          size_t length, unsigned int flags) {
  size_t transferred = 0;
  while (transferred < length) {
    off_t offset = static_cast<off_t>(destination_offset);
    const ssize_t result =
        io_splice(source_fd, nullptr, destination_fd, &offset,
                  length - transferred, flags, 0, true);
    if (result > 0) {
      transferred += static_cast<size_t>(result);
      destination_offset = static_cast<uint64_t>(offset);
      continue;
    }
    if (result == 0) {
      throw std::system_error(
          std::make_error_code(std::errc::connection_reset),
          "splice reached EOF before the requested length");
    }
    if (errno == EINTR) {
      continue;
    }
    pipe_throw_errno("splice(to fd)");
  }
  return transferred;
}

size_t sendfile_exact(int socket_fd, int source_fd,
                      uint64_t& source_offset, size_t length) {
  size_t transferred = 0;
  while (transferred < length) {
    off_t offset = static_cast<off_t>(source_offset);
    const ssize_t result = ::sendfile(socket_fd, source_fd, &offset,
                                      length - transferred);
    if (result > 0) {
      transferred += static_cast<size_t>(result);
      source_offset = static_cast<uint64_t>(offset);
      continue;
    }
    if (result == 0) {
      throw std::system_error(
          std::make_error_code(std::errc::io_error),
          "sendfile reached EOF before the requested length");
    }
    if (errno == EINTR) {
      continue;
    }
    pipe_throw_errno("sendfile");
  }
  return transferred;
}

void write_all(int fd, std::span<const std::byte> bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t result = ::write(fd, bytes.data() + offset,
                                   bytes.size() - offset);
    if (result > 0) {
      offset += static_cast<size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    pipe_throw_errno("write");
  }
}

void send_all(int socket_fd, std::span<const std::byte> bytes) {
  const ssize_t result = io_send_exact(
      socket_fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
  if (result < 0) {
    pipe_throw_errno("send");
  }
  if (size_t(result) != bytes.size()) {
    throw std::system_error(
        std::make_error_code(std::errc::io_error),
        "send reached EOF before the requested length");
  }
}

void receive_all(int socket_fd, std::span<std::byte> bytes) {
  const ssize_t result = io_receive_exact(
      socket_fd, bytes.data(), bytes.size());
  if (result < 0) {
    pipe_throw_errno("recv");
  }
  if (size_t(result) != bytes.size()) {
    throw std::system_error(
        std::make_error_code(std::errc::connection_reset),
        "recv reached EOF before the requested length");
  }
}

void read_all(int fd, std::span<std::byte> bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t result = ::read(fd, bytes.data() + offset,
                                  bytes.size() - offset);
    if (result > 0) {
      offset += static_cast<size_t>(result);
      continue;
    }
    if (result == 0) {
      throw std::system_error(
          std::make_error_code(std::errc::connection_reset),
          "read reached EOF before the requested length");
    }
    if (errno == EINTR) {
      continue;
    }
    pipe_throw_errno("read");
  }
}

// TCP connection and timeout handling.
using Clock = std::chrono::steady_clock;

int remaining_ms(Clock::time_point deadline) {
  const auto remaining =
      std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                            Clock::now());
  if (remaining.count() <= 0) {
    return 0;
  }
  return static_cast<int>(
      std::min<int64_t>(remaining.count(), INT_MAX));
}

short wait_for_events(int fd, short events, int timeout_ms) {
  const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
  for (;;) {
    pollfd descriptor{
        .fd = fd,
        .events = events,
        .revents = 0,
    };
    const int result = ::poll(&descriptor, 1, remaining_ms(deadline));
    if (result > 0) {
      return descriptor.revents;
    }
    if (result == 0) {
      return 0;
    }
    if (errno != EINTR) {
      throw std::system_error(errno, std::generic_category(), "poll");
    }
    if (remaining_ms(deadline) == 0) {
      return 0;
    }
  }
}

void configure_blocking_socket(int fd, int io_timeout_ms) {
  const int flags = ::fcntl(fd, F_GETFL);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "fcntl(blocking socket)");
  }

  set_socket_receive_timeout(fd, io_timeout_ms);
  timeval timeout{
      .tv_sec = io_timeout_ms / 1'000,
      .tv_usec = (io_timeout_ms % 1'000) * 1'000,
  };
  if (::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                   sizeof(timeout)) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "setsockopt(socket timeout)");
  }
  const int one = 1;
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "setsockopt(TCP_NODELAY)");
  }
}

size_t socket_receive_buffer_capacity(int fd) noexcept {
  int actual = 0;
  socklen_t size = sizeof(actual);
  if (::getsockopt(fd, SOL_SOCKET, SO_RCVBUF,
                   &actual, &size) != 0 || size != sizeof(actual)) {
    return 0;
  }
  // Linux reports twice the user-visible receive-buffer allowance.
  return actual > 0 ? size_t(actual) / 2 : 0;
}

bool configure_socket_receive_buffer(int fd, size_t requested) noexcept {
  if (requested == 0) {
    return true;
  }

  static std::atomic_flag warned = ATOMIC_FLAG_INIT;
  const auto warn = [&](const char* reason, size_t effective) {
    if (!warned.test_and_set(std::memory_order_relaxed)) {
      fprintf(stderr,
              "warning: unable to obtain the requested %zu-byte TCP "
              "receive buffer (%s, effective %zu bytes); reads will "
              "fall back to TCP receive autotuning; consider raising "
              "net.core.rmem_max\n",
              requested, reason, effective);
    }
  };

  if (requested > size_t(INT_MAX)) {
    warn("value exceeds INT_MAX", 0);
    return false;
  }
  const int value = int(requested);
  if (::setsockopt(fd, SOL_SOCKET, SO_RCVBUF,
                   &value, sizeof(value)) != 0) {
    const int error = errno;
    warn(strerror(error), socket_receive_buffer_capacity(fd));
    return false;
  }

  const size_t effective = socket_receive_buffer_capacity(fd);
  if (effective == 0) {
    warn("SO_RCVBUF verification failed", 0);
    return false;
  }
  if (effective < requested) {
    warn("the kernel capped SO_RCVBUF", effective);
    return false;
  }
  return true;
}

UniqueFd connect_tcp(std::string_view host, uint16_t port,
                     int connect_timeout_ms, int io_timeout_ms,
                     size_t receive_buffer_size) {
  if (connect_timeout_ms <= 0 || io_timeout_ms <= 0) {
    throw std::invalid_argument("socket timeouts must be positive");
  }

  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  const std::string host_string(host);
  const std::string service = std::to_string(port);
  addrinfo* raw_addresses = nullptr;
  const int lookup = ::getaddrinfo(host_string.c_str(), service.c_str(),
                                   &hints, &raw_addresses);
  if (lookup != 0) {
    throw std::runtime_error(std::string("getaddrinfo: ") +
                             gai_strerror(lookup));
  }
  std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> addresses(
      raw_addresses, ::freeaddrinfo);

  int last_error = ECONNREFUSED;
  for (addrinfo* address = addresses.get(); address != nullptr;
       address = address->ai_next) {
    UniqueFd socket(::socket(address->ai_family,
                             address->ai_socktype | SOCK_CLOEXEC |
                                 SOCK_NONBLOCK,
                             address->ai_protocol));
    if (!socket) {
      last_error = errno;
      continue;
    }
    if (!configure_socket_receive_buffer(
            socket.get(), receive_buffer_size)) {
      socket.reset(::socket(address->ai_family,
                            address->ai_socktype | SOCK_CLOEXEC |
                                SOCK_NONBLOCK,
                            address->ai_protocol));
      if (!socket) {
        last_error = errno;
        continue;
      }
    }

    if (io_connect(socket.get(), address->ai_addr, address->ai_addrlen,
                   connect_timeout_ms) != 0) {
      if (errno != EINPROGRESS) {
        last_error = errno;
        continue;
      }
      const short events = wait_for_events(socket.get(), POLLOUT,
                                           connect_timeout_ms);
      if (events == 0) {
        last_error = ETIMEDOUT;
        continue;
      }
      int socket_error = 0;
      socklen_t error_size = sizeof(socket_error);
      if (::getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, &socket_error,
                       &error_size) != 0) {
        last_error = errno;
        continue;
      }
      if (socket_error != 0) {
        last_error = socket_error;
        continue;
      }
    }

    configure_blocking_socket(socket.get(), io_timeout_ms);
    return socket;
  }

  throw std::system_error(last_error, std::generic_category(), "connect");
}

void set_socket_receive_timeout(int fd, int timeout_ms) {
  if (fd < 0 || timeout_ms <= 0) {
    throw std::invalid_argument("invalid receive timeout argument");
  }
  timeval timeout{
      .tv_sec = timeout_ms / 1'000,
      .tv_usec = (timeout_ms % 1'000) * 1'000,
  };
  if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                   sizeof(timeout)) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "setsockopt(SO_RCVTIMEO)");
  }
}
