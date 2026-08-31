#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <algorithm>
#include <barrier>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <thread>
#include <vector>

struct WriterResult {
  std::string path;
  unsigned char fill = 0;
  size_t bytes = 0;
  int open_error  = 0;
  int write_error = 0;
  int close_error = 0;
};

void run_writer(WriterResult& result, std::barrier<>& opened) {
  const int fd = ::open(result.path.c_str(),
                        O_WRONLY | O_TRUNC | O_CLOEXEC);
  if (fd < 0) {
    result.open_error = errno;
  }
  opened.arrive_and_wait();
  if (fd < 0) {
    return;
  }

  std::vector<unsigned char> block(64U * 1024U, result.fill);
  size_t written = 0;
  while (written != result.bytes) {
    const size_t wanted = std::min(block.size(), result.bytes - written);
    const ssize_t count = ::write(fd, block.data(), wanted);
    if (count < 0) {
      result.write_error = errno;
      break;
    }
    assert(count != 0);
    written += size_t(count);
  }
  if (::close(fd) != 0) {
    result.close_error = errno;
  }
}

std::vector<unsigned char> read_object(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  assert(fd >= 0);
  struct stat status{};
  assert(::fstat(fd, &status) == 0);
  assert(status.st_size > 0);
  std::vector<unsigned char> data(size_t(status.st_size));
  size_t offset = 0;
  while (offset != data.size()) {
    const ssize_t bytes = ::read(fd, data.data() + offset,
                                 data.size() - offset);
    assert(bytes > 0);
    offset += size_t(bytes);
  }
  unsigned char extra = 0;
  assert(::read(fd, &extra, 1) == 0);
  assert(::close(fd) == 0);
  return data;
}

int main(int argc, char** argv) {
  assert(argc == 3);
  WriterResult first{
      .path = argv[1], .fill = 0x3a, .bytes = 1024U * 1024U};
  WriterResult second{
      .path = argv[2], .fill = 0xc5, .bytes = 1088U * 1024U};
  std::barrier opened(2);
  std::thread a(run_writer, std::ref(first), std::ref(opened));
  std::thread b(run_writer, std::ref(second), std::ref(opened));
  a.join();
  b.join();

  assert(first.open_error == 0);
  assert(second.open_error == 0);
  const bool first_ok = first.write_error == 0 && first.close_error == 0;
  const bool second_ok = second.write_error == 0 && second.close_error == 0;
  if (!first_ok && !second_ok) {
    fprintf(stderr,
            "both cross-mount overwrites failed: "
            "first=(%d,%d) second=(%d,%d)\n",
            first.write_error, first.close_error,
            second.write_error, second.close_error);
    return 1;
  }

  const std::vector<unsigned char> from_first = read_object(first.path);
  const std::vector<unsigned char> from_second = read_object(second.path);
  assert(from_first == from_second);
  const unsigned char expected = from_first.front();
  assert(expected == first.fill || expected == second.fill);
  assert(from_first.size() ==
         (expected == first.fill ? first.bytes : second.bytes));
  if (!first_ok) {
    assert(expected == second.fill);
  }
  if (!second_ok) {
    assert(expected == first.fill);
  }
  for (unsigned char value : from_first) {
    assert(value == expected);
  }
  return 0;
}
