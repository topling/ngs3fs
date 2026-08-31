#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <algorithm>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <thread>
#include <vector>

void write_object(const std::string& path, unsigned seed, size_t bytes) {
  const int fd = ::open(path.c_str(),
                        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
  assert(fd >= 0);
  std::vector<unsigned char> block(64U * 1024U);
  size_t offset = 0;
  while (offset != bytes) {
    const size_t wanted = std::min(block.size(), bytes - offset);
    for (size_t i = 0; i < wanted; ++i) {
      block[i] = u_char((offset + i) * 131U + seed);
    }
    const ssize_t count = ::write(fd, block.data(), wanted);
    assert(count > 0);
    offset += size_t(count);
  }
  assert(::close(fd) == 0);
}

void verify_object(const std::string& path, unsigned seed, size_t bytes) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  assert(fd >= 0);
  struct stat status{};
  assert(::fstat(fd, &status) == 0);
  assert(uint64_t(status.st_size) == bytes);
  void* raw = ::mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0);
  assert(raw != MAP_FAILED);
  const auto* data = static_cast<const unsigned char*>(raw);
  for (size_t i = 0; i < bytes; ++i) {
    assert(data[i] == u_char(i * 131U + seed));
  }
  assert(::munmap(raw, bytes) == 0);
  assert(::close(fd) == 0);
}

int main(int argc, char** argv) {
  assert(argc == 2);
  const std::string root = argv[1];
  const std::string large = root + "/large.bin";
  const std::string renamed = root + "/renamed.bin";
  const std::string first = root + "/parallel-a.bin";
  const std::string second = root + "/parallel-b.bin";
  constexpr size_t large_bytes = 12U * 1024U * 1024U + 12345U;
  constexpr size_t small_bytes = 1024U * 1024U + 17U;

  write_object(large, 17, large_bytes);
  verify_object(large, 17, large_bytes);
  assert(::rename(large.c_str(), renamed.c_str()) == 0);
  verify_object(renamed, 17, large_bytes);

  std::thread a(write_object, first, 31, small_bytes);
  std::thread b(write_object, second, 47, small_bytes);
  a.join();
  b.join();
  verify_object(first, 31, small_bytes);
  verify_object(second, 47, small_bytes);

  assert(::unlink(first.c_str()) == 0);
  assert(::unlink(second.c_str()) == 0);
  assert(::unlink(renamed.c_str()) == 0);
  fprintf(stdout, "provider mmap/multipart/concurrent-write/rename smoke passed\n");
  return 0;
}
