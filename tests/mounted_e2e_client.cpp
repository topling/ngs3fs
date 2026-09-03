#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <fcntl.h>
#include <algorithm>
#include <iostream>
#include <exception>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <thread>
#include <vector>

class FileDescriptor {
 public:
  explicit FileDescriptor(int value = -1) : value_(value) {}
  ~FileDescriptor() {
    if (value_ >= 0) {
      ::close(value_);
    }
  }

  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;

  int get() const { return value_; }

 private:
  int value_;
};

[[noreturn]] void fail_errno(const std::string& operation) {
  throw std::runtime_error(operation + ": " + strerror(errno));
}

std::vector<std::byte> read_reference(const char* path) {
  FileDescriptor fd(::open(path, O_RDONLY | O_CLOEXEC));
  if (fd.get() < 0) {
    fail_errno("open reference");
  }

  struct stat status {};
  if (::fstat(fd.get(), &status) != 0) {
    fail_errno("stat reference");
  }

  std::vector<std::byte> bytes(static_cast<size_t>(status.st_size));
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t count = ::read(fd.get(), bytes.data() + offset,
                                 bytes.size() - offset);
    if (count < 0) {
      fail_errno("read reference");
    }
    if (count == 0) {
      throw std::runtime_error("short reference file");
    }
    offset += static_cast<size_t>(count);
  }
  return bytes;
}

void verify_mapping(int fd, const std::vector<std::byte>& expected,
                    const char* phase) {
  void* mapping = ::mmap(nullptr, expected.size(), PROT_READ, MAP_PRIVATE,
                         fd, 0);
  if (mapping == MAP_FAILED) {
    fail_errno(std::string("mmap ") + phase);
  }
  const bool equal = memcmp(mapping, expected.data(), expected.size()) == 0;
  if (::munmap(mapping, expected.size()) != 0) {
    fail_errno(std::string("munmap ") + phase);
  }
  if (!equal) {
    throw std::runtime_error(std::string(phase) + " bytes differ");
  }
}

void write_all(int fd, const std::vector<std::byte>& bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const size_t length = std::min<size_t>(256U * 1024U,
                                           bytes.size() - offset);
    const ssize_t count = ::write(fd, bytes.data() + offset, length);
    if (count > 0) {
      offset += static_cast<size_t>(count);
    } else if (count < 0 && errno == EINTR) {
      continue;
    } else {
      fail_errno("write mounted object");
    }
  }
}

void create_and_write(const std::string& path,
                      const std::vector<std::byte>& bytes) {
  FileDescriptor fd(::open(path.c_str(),
                           O_WRONLY | O_CREAT | O_EXCL | O_TRUNC | O_CLOEXEC,
                           0644));
  if (fd.get() < 0) {
    fail_errno("create mounted object");
  }
  write_all(fd.get(), bytes);
}

void verify_file(const std::string& path,
                 const std::vector<std::byte>& expected,
                 const char* phase) {
  FileDescriptor fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (fd.get() < 0) {
    fail_errno(std::string("open ") + phase);
  }
  verify_mapping(fd.get(), expected, phase);
}

void verify_paged_directory(const std::string& path) {
  DIR* directory = ::opendir(path.c_str());
  if (directory == nullptr) {
    fail_errno("opendir paginated directory");
  }
  std::vector<std::string> names;
  errno = 0;
  while (dirent* entry = ::readdir(directory)) {
    if (strcmp(entry->d_name, ".") != 0 &&
        strcmp(entry->d_name, "..") != 0) {
      names.emplace_back(entry->d_name);
    }
  }
  const int read_error = errno;
  if (::closedir(directory) != 0) {
    fail_errno("closedir paginated directory");
  }
  if (read_error != 0) {
    errno = read_error;
    fail_errno("readdir paginated directory");
  }
  std::sort(names.begin(), names.end());
  std::vector<std::string> expected;
  expected.reserve(1001);
  char name[sizeof("page-0000")];
  for (unsigned i = 0; i <= 1000; ++i) {
    snprintf(name, sizeof(name), "page-%04u", i);
    expected.emplace_back(name);
  }
  std::sort(expected.begin(), expected.end());
  if (names != expected) {
    throw std::runtime_error(
        "paginated readdir skipped or duplicated directory entries");
  }
}

int main(int argc, char** argv) {
  try {
    if (argc != 4) {
      std::cerr << "usage: mounted_e2e_client FILE REFERENCE RENAMED_FILE\n";
      return 2;
    }

    std::vector<std::byte> expected = read_reference(argv[2]);
    if (expected.size() <= 4096) {
      throw std::runtime_error("reference file is too small");
    }

    {
      FileDescriptor fd(::open(argv[1], O_RDONLY | O_CLOEXEC));
      if (fd.get() < 0) {
        fail_errno("open mounted object for read");
      }
      verify_mapping(fd.get(), expected, "initial read");
    }

    expected.resize(expected.size() - 4096);
    for (std::byte& value : expected) {
      value ^= std::byte{0xa5};
    }

    {
      FileDescriptor fd(::open(argv[1], O_WRONLY | O_TRUNC | O_CLOEXEC));
      if (fd.get() < 0) {
        fail_errno("open mounted object for write");
      }
      write_all(fd.get(), expected);
      if (::fsync(fd.get()) != 0) {
        fail_errno("fsync mounted object");
      }
    }

    {
      FileDescriptor fd(::open(argv[1], O_RDONLY | O_CLOEXEC));
      if (fd.get() < 0) {
        fail_errno("reopen mounted object");
      }
      verify_mapping(fd.get(), expected, "reopen read");
    }

    if (::rename(argv[1], argv[3]) != 0) {
      fail_errno("rename mounted object");
    }
    if (::access(argv[1], F_OK) == 0 || errno != ENOENT) {
      throw std::runtime_error("old name still exists after rename");
    }

    {
      FileDescriptor fd(::open(argv[3], O_RDONLY | O_CLOEXEC));
      if (fd.get() < 0) {
        fail_errno("open renamed object");
      }
      verify_mapping(fd.get(), expected, "renamed read");
    }

    const std::string renamed = argv[3];
    const size_t slash = renamed.find_last_of('/');
    if (slash == std::string::npos) {
      throw std::runtime_error("renamed path has no parent directory");
    }
    const std::string mount = renamed.substr(0, slash);
    const std::string first_path  = mount + "/parallel-a.bin";
    const std::string second_path = mount + "/parallel-b.bin";
    std::vector<std::byte> first_expected = expected;
    std::vector<std::byte> second_expected = expected;
    for (std::byte& value : first_expected) {
      value ^= std::byte{0x31};
    }
    for (std::byte& value : second_expected) {
      value ^= std::byte{0xc7};
    }
    std::exception_ptr first_error;
    std::exception_ptr second_error;
    {
      std::jthread first([&] {
        try {
          create_and_write(first_path, first_expected);
        } catch (...) {
          first_error = std::current_exception();
        }
      });
      std::jthread second([&] {
        try {
          create_and_write(second_path, second_expected);
        } catch (...) {
          second_error = std::current_exception();
        }
      });
    }
    if (first_error) {
      std::rethrow_exception(first_error);
    }
    if (second_error) {
      std::rethrow_exception(second_error);
    }
    verify_file(first_path, first_expected, "first parallel object");
    verify_file(second_path, second_expected, "second parallel object");

    const std::string directory = mount + "/subdir";
    if (::mkdir(directory.c_str(), 0755) != 0) {
      fail_errno("mkdir mounted directory");
    }
    const std::string nested = directory + "/nested.bin";
    std::vector<std::byte> nested_expected(first_expected.begin(),
                                           first_expected.begin() + 1048576);
    create_and_write(nested, nested_expected);
    verify_file(nested, nested_expected, "nested object");

    const std::string moved = directory + "/moved.bin";
    if (::rename(first_path.c_str(), moved.c_str()) != 0) {
      fail_errno("rename object across directories");
    }
    verify_file(moved, first_expected, "cross-directory renamed object");
    const std::string renamed_directory = mount + "/renamed-dir";
    if (::rename(directory.c_str(), renamed_directory.c_str()) == 0 ||
        errno != EXDEV) {
      throw std::runtime_error(
          "nonempty mounted directory rename did not return EXDEV");
    }
    verify_file(nested, nested_expected,
                "object after rejected directory rename");
    verify_file(moved, first_expected,
                "large object after rejected directory rename");
    if (::unlink(second_path.c_str()) != 0 ||
        ::unlink(nested.c_str()) != 0 || ::unlink(moved.c_str()) != 0) {
      fail_errno("unlink mounted objects");
    }
    if (::rmdir(directory.c_str()) != 0) {
      fail_errno("rmdir mounted directory");
    }

    const std::string paged_directory = mount + "/paged-dir";
    const std::string paged_renamed   = mount + "/paged-renamed";
    verify_paged_directory(paged_directory);
    if (::rename(paged_directory.c_str(), paged_renamed.c_str()) == 0 ||
        errno != EXDEV) {
      throw std::runtime_error(
          "paginated mounted directory rename did not return EXDEV");
    }
    const std::string second_page = paged_directory + "/page-1000";
    struct stat second_page_status{};
    if (::stat(second_page.c_str(), &second_page_status) != 0) {
      fail_errno("stat second-page object after rejected rename");
    }
    if (!S_ISREG(second_page_status.st_mode)) {
      throw std::runtime_error("second-page renamed object is not a file");
    }

    std::cout << "mounted multi-object mmap/read/write/concurrent-create/"
                 "directory-rename-EXDEV/unlink passed: "
              << expected.size() << " bytes per large object\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "mounted_e2e_client: " << error.what() << '\n';
    return 1;
  }
}
