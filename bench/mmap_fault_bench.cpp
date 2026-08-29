#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <errno.h>
#include <charconv>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <system_error>

uint64_t parse_unsigned(std::string_view value) {
  uint64_t result = 0;
  const auto parsed = std::from_chars(
      value.data(), value.data() + value.size(), result);
  if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()) {
    throw std::invalid_argument("invalid unsigned integer argument");
  }
  return result;
}

uint64_t monotonic_ns() {
  timespec value{};
  if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
    throw std::system_error(errno, std::generic_category(), "clock_gettime");
  }
  return static_cast<uint64_t>(value.tv_sec) * 1'000'000'000ULL +
         static_cast<uint64_t>(value.tv_nsec);
}

int main(int argc, char** argv) {
  if (argc < 2 || argc > 5) {
    std::cerr << "usage: mmap_fault_bench FILE [BYTES=262144] "
                 "[ITERATIONS=100] [STRIDE=BYTES]\n";
    return 2;
  }

  try {
    const size_t length = argc >= 3
                                   ? static_cast<size_t>(
                                         parse_unsigned(argv[2]))
                                   : 256U * 1024U;
    const size_t iterations = argc >= 4
                                       ? static_cast<size_t>(
                                             parse_unsigned(argv[3]))
                                       : 100;
    const size_t stride = argc >= 5
                                    ? static_cast<size_t>(
                                          parse_unsigned(argv[4]))
                                    : length;
    if (length == 0 || iterations == 0 || stride == 0) {
      throw std::invalid_argument(
          "length, iterations, and stride must be nonzero");
    }
    if (stride % length != 0) {
      throw std::invalid_argument("stride must be a multiple of length");
    }

    const int fd = ::open(argv[1], O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
      throw std::system_error(errno, std::generic_category(), "open");
    }
    struct stat status{};
    if (::fstat(fd, &status) != 0) {
      const int error = errno;
      ::close(fd);
      throw std::system_error(error, std::generic_category(), "fstat");
    }
    if (status.st_size < 0 ||
        static_cast<uint64_t>(status.st_size) < length) {
      ::close(fd);
      throw std::invalid_argument("benchmark file is smaller than BYTES");
    }

    const long page_size_value = ::sysconf(_SC_PAGESIZE);
    if (page_size_value <= 0) {
      ::close(fd);
      throw std::runtime_error("sysconf(_SC_PAGESIZE) failed");
    }
    const size_t page_size = static_cast<size_t>(page_size_value);
    const uint64_t file_size = static_cast<uint64_t>(status.st_size);
    const uint64_t slots = std::max<uint64_t>(1, file_size / length);
    const uint64_t step = stride / length;
    volatile uint64_t checksum = 0;

    for (size_t iteration = 0; iteration < iterations; ++iteration) {
      const uint64_t logical_offset = (iteration * step % slots) * length;
      const uint64_t mapping_offset =
          logical_offset - (logical_offset % page_size);
      const size_t prefix =
          static_cast<size_t>(logical_offset - mapping_offset);
      const size_t mapping_length = prefix + length;
      const int advise_result =
          ::posix_fadvise(fd, static_cast<off_t>(logical_offset),
                          static_cast<off_t>(length), POSIX_FADV_DONTNEED);
      if (advise_result != 0) {
        throw std::system_error(advise_result, std::generic_category(),
                                "posix_fadvise(POSIX_FADV_DONTNEED)");
      }

      void* mapping = ::mmap(nullptr, mapping_length, PROT_READ, MAP_PRIVATE,
                             fd, static_cast<off_t>(mapping_offset));
      if (mapping == MAP_FAILED) {
        const int error = errno;
        ::close(fd);
        throw std::system_error(error, std::generic_category(), "mmap");
      }
      if (::madvise(mapping, mapping_length, MADV_DONTNEED) != 0) {
        const int error = errno;
        ::munmap(mapping, mapping_length);
        throw std::system_error(error, std::generic_category(),
                                "madvise(MADV_DONTNEED)");
      }
      const auto* bytes = static_cast<const unsigned char*>(mapping) + prefix;
      const uint64_t started = monotonic_ns();
      for (size_t i = 0; i < length; i += page_size) {
        checksum = checksum + bytes[i];
      }
      checksum = checksum + bytes[length - 1];
      const uint64_t completed = monotonic_ns();
      ::munmap(mapping, mapping_length);
      std::cout << "{\"event\":\"mmap_fault\",\"iteration\":"
                << iteration << ",\"bytes\":" << length
                << ",\"total_ns\":" << (completed - started) << "}\n";
    }
    ::close(fd);
    std::cout << "{\"event\":\"checksum\",\"value\":" << checksum
              << "}\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "mmap_fault_bench: " << error.what() << '\n';
    return 1;
  }
}
