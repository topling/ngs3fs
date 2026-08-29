#include "io.hpp"

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <chrono>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>

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
  size_t transferred = 0;
  while (transferred < length) {
    const size_t remaining = length - transferred;
    if (calls != nullptr) {
      ++*calls;
    }
    const ssize_t result = ::splice(source_fd, nullptr, destination_fd, nullptr,
                                    remaining, flags);
    if (result > 0) {
      transferred += static_cast<size_t>(result);
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
    pipe_throw_errno("splice");
  }
  return transferred;
}

size_t splice_from_fd_exact(int source_fd, uint64_t& source_offset,
                            int destination_fd, size_t length,
                            unsigned int flags) {
  size_t transferred = 0;
  while (transferred < length) {
    off_t offset = off_t(source_offset);
    const ssize_t result = ::splice(
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
    const ssize_t result = ::splice(source_fd, position, destination_fd,
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
        ::splice(source_fd, nullptr, destination_fd, &offset,
                 length - transferred, flags);
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
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t result =
        ::send(socket_fd, bytes.data() + offset, bytes.size() - offset,
               MSG_NOSIGNAL);
    if (result > 0) {
      offset += static_cast<size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    pipe_throw_errno("send");
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
}

UniqueFd connect_tcp(std::string_view host, uint16_t port,
                     int connect_timeout_ms, int io_timeout_ms) {
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

    if (::connect(socket.get(), address->ai_addr, address->ai_addrlen) != 0) {
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
