#include <fuse_lowlevel.h>

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <errno.h>
#include <memory>
#include <new>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <thread>

constexpr fuse_ino_t kFileInode = 2;
constexpr char kOldContents[]   = "immutable-passthrough-old-generation";
constexpr char kNewContents[]   = "replacement-generation-new-contents";

struct SharedState {
  std::atomic<bool> enable_passthrough{false};
  std::atomic<bool> mounted{false};
  std::atomic<bool> capable{false};
  std::atomic<unsigned> normal_handles{0};
  std::atomic<unsigned> passthrough_handles{0};
  std::atomic<unsigned> registration_calls{0};
  std::atomic<unsigned> read_callbacks{0};
  std::atomic<int> registration_error{0};
};

struct FileHandle {
  int fd = -1;
  bool passthrough = false;
};

struct Fixture {
  const char* source = nullptr;
  SharedState* shared = nullptr;
  int backing_id = 0;
};

Fixture& fixture(fuse_req_t request) {
  return *static_cast<Fixture*>(fuse_req_userdata(request));
}

void fill_stat(Fixture& state, fuse_ino_t inode, struct stat& status) {
  memset(&status, 0, sizeof(status));
  status.st_ino = ino_t(inode);
  status.st_uid = ::getuid();
  status.st_gid = ::getgid();
  if (inode == FUSE_ROOT_ID) {
    status.st_mode  = S_IFDIR | 0755;
    status.st_nlink = 2;
    return;
  }
  if (inode != kFileInode) {
    errno = ENOENT;
    return;
  }
  struct stat source_status{};
  if (::stat(state.source, &source_status) != 0) return;
  status.st_mode  = S_IFREG | 0444;
  status.st_nlink = 1;
  status.st_size  = source_status.st_size;
}

void fixture_init(void* userdata, fuse_conn_info* connection) {
  auto& state = *static_cast<Fixture*>(userdata);
  if ((connection->capable & FUSE_CAP_PASSTHROUGH) != 0) {
    connection->want |= FUSE_CAP_PASSTHROUGH;
    connection->max_backing_stack_depth = FUSE_BACKING_STACKED_UNDER;
    state.shared->capable.store(true, std::memory_order_release);
  } else {
    state.shared->registration_error.store(ENOSYS, std::memory_order_release);
  }
}

void fixture_lookup(fuse_req_t request, fuse_ino_t parent,
                    const char* name) {
  if (parent != FUSE_ROOT_ID || name == nullptr ||
      strcmp(name, "object") != 0) {
    fuse_reply_err(request, ENOENT);
    return;
  }
  fuse_entry_param entry{};
  entry.ino = kFileInode;
  entry.generation = 1;
  errno = 0;
  fill_stat(fixture(request), kFileInode, entry.attr);
  if (errno == ENOENT) {
    fuse_reply_err(request, ENOENT);
    return;
  }
  fuse_reply_entry(request, &entry);
}

void fixture_getattr(fuse_req_t request, fuse_ino_t inode,
                     fuse_file_info*) {
  struct stat status{};
  errno = 0;
  fill_stat(fixture(request), inode, status);
  if (errno != 0) {
    fuse_reply_err(request, errno);
    return;
  }
  fuse_reply_attr(request, &status, 0.0);
}

void fixture_open(fuse_req_t request, fuse_ino_t inode,
                  fuse_file_info* file) {
  Fixture& state = fixture(request);
  if (inode != kFileInode || (file->flags & O_ACCMODE) != O_RDONLY) {
    fuse_reply_err(request, EACCES);
    return;
  }
  auto handle = std::make_unique<FileHandle>();
  handle->fd = ::open(state.source, O_RDONLY | O_CLOEXEC);
  if (handle->fd < 0) {
    fuse_reply_err(request, errno);
    return;
  }

  const bool requested = state.shared->enable_passthrough.load(
      std::memory_order_acquire);
  if (requested && state.shared->capable.load(std::memory_order_acquire) &&
      state.shared->normal_handles.load(std::memory_order_acquire) == 0) {
    if (state.backing_id == 0) {
      errno = 0;
      state.backing_id = fuse_passthrough_open(request, handle->fd);
      state.shared->registration_calls.fetch_add(1, std::memory_order_relaxed);
      if (state.backing_id == 0) {
        state.shared->registration_error.store(
            errno == 0 ? EIO : errno, std::memory_order_release);
      }
    }
    if (state.backing_id != 0) {
      handle->passthrough = true;
      state.shared->passthrough_handles.fetch_add(
          1, std::memory_order_release);
      file->backing_id = state.backing_id;
      file->keep_cache = 0;
    }
  }
  if (!handle->passthrough) {
    state.shared->normal_handles.fetch_add(1, std::memory_order_release);
    file->keep_cache = 0;
  }
  file->fh = reinterpret_cast<uint64_t>(handle.release());
  fuse_reply_open(request, file);
}

void fixture_read(fuse_req_t request, fuse_ino_t, size_t size, off_t offset,
                  fuse_file_info* file) {
  auto* handle = reinterpret_cast<FileHandle*>(uintptr_t(file->fh));
  if (handle == nullptr || handle->fd < 0 || handle->passthrough) {
    fuse_reply_err(request, EIO);
    return;
  }
  char* data = static_cast<char*>(malloc(size));
  if (data == nullptr && size != 0) {
    fuse_reply_err(request, ENOMEM);
    return;
  }
  ssize_t count;
  do {
    count = ::pread(handle->fd, data, size, offset);
  } while (count < 0 && errno == EINTR);
  if (count < 0) {
    free(data);
    fuse_reply_err(request, errno);
    return;
  }
  fixture(request).shared->read_callbacks.fetch_add(
      1, std::memory_order_relaxed);
  fuse_reply_buf(request, data, size_t(count));
  free(data);
}

void fixture_release(fuse_req_t request, fuse_ino_t,
                     fuse_file_info* file) {
  Fixture& state = fixture(request);
  std::unique_ptr<FileHandle> handle(
      reinterpret_cast<FileHandle*>(uintptr_t(file->fh)));
  file->fh = 0;
  if (handle->passthrough) {
    const unsigned previous = state.shared->passthrough_handles.fetch_sub(
        1, std::memory_order_acq_rel);
    if (previous == 1 && state.backing_id != 0) {
      fuse_passthrough_close(request, state.backing_id);
      state.backing_id = 0;
    }
  } else {
    state.shared->normal_handles.fetch_sub(1, std::memory_order_acq_rel);
  }
  ::close(handle->fd);
  fuse_reply_err(request, 0);
}

bool write_contents(const char* path, const char* contents) {
  const int fd = ::open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
                        0600);
  if (fd < 0) return false;
  const size_t length = strlen(contents);
  size_t offset = 0;
  while (offset != length) {
    const ssize_t count = ::write(fd, contents + offset, length - offset);
    if (count > 0) offset += size_t(count);
    else if (count < 0 && errno == EINTR) continue;
    else {
      ::close(fd);
      return false;
    }
  }
  return ::close(fd) == 0;
}

bool wait_until(const std::atomic<unsigned>& value, unsigned wanted) {
  for (unsigned attempt = 0; attempt != 500; ++attempt) {
    if (value.load(std::memory_order_acquire) == wanted) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return false;
}

bool read_matches(int fd, const char* expected) {
  constexpr size_t kBufferSize =
      sizeof(kOldContents) > sizeof(kNewContents)
          ? sizeof(kOldContents) : sizeof(kNewContents);
  char data[kBufferSize]{};
  const size_t length = strlen(expected);
  if (length > sizeof(data)) return false;
  size_t offset = 0;
  while (offset != length) {
    const ssize_t count = ::pread(
        fd, data + offset, length - offset, off_t(offset));
    if (count > 0) offset += size_t(count);
    else if (count < 0 && errno == EINTR) continue;
    else return false;
  }
  return memcmp(data, expected, length) == 0;
}

int run_daemon(const char* source, const char* mountpoint,
               SharedState& shared) {
  Fixture state{.source = source, .shared = &shared};
  fuse_args arguments = FUSE_ARGS_INIT(0, nullptr);
  if (fuse_opt_add_arg(&arguments, "native-passthrough-fixture") != 0) {
    return 2;
  }
  fuse_lowlevel_ops operations{};
  operations.init    = fixture_init;
  operations.lookup  = fixture_lookup;
  operations.getattr = fixture_getattr;
  operations.open    = fixture_open;
  operations.read    = fixture_read;
  operations.release = fixture_release;
  fuse_session* session = fuse_session_new(
      &arguments, &operations, sizeof(operations), &state);
  if (session == nullptr) return 2;
  if (fuse_set_signal_handlers(session) != 0 ||
      fuse_session_mount(session, mountpoint) != 0) {
    fuse_session_destroy(session);
    fuse_opt_free_args(&arguments);
    return 2;
  }
  shared.mounted.store(true, std::memory_order_release);
  const int result = fuse_session_loop(session);
  fuse_session_unmount(session);
  fuse_remove_signal_handlers(session);
  fuse_session_destroy(session);
  fuse_opt_free_args(&arguments);
  return result == 0 || result == SIGTERM ? 0 : 2;
}

int fail(pid_t child, const char* message) {
  fprintf(stderr, "native passthrough integration failure: %s\n", message);
  if (child > 0) {
    ::kill(child, SIGTERM);
    int status = 0;
    while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {}
  }
  return 1;
}

int main(int argc, char** argv) {
  const bool require_passthrough =
      argc == 2 && strcmp(argv[1], "--require-passthrough") == 0;
  char directory_template[] = "/tmp/ngs3fs-passthrough-XXXXXX";
  char* directory = ::mkdtemp(directory_template);
  if (directory == nullptr) return fail(-1, "mkdtemp");
  char source[PATH_MAX];
  char replacement[PATH_MAX];
  char mountpoint[PATH_MAX];
  char object[PATH_MAX];
  const int source_length =
      snprintf(source, sizeof(source), "%s/source", directory);
  const int replacement_length = snprintf(
      replacement, sizeof(replacement), "%s/replacement", directory);
  const int mountpoint_length =
      snprintf(mountpoint, sizeof(mountpoint), "%s/mount", directory);
  const int object_length =
      snprintf(object, sizeof(object), "%s/mount/object", directory);
  if (source_length < 0 || size_t(source_length) >= sizeof(source) ||
      replacement_length < 0 ||
      size_t(replacement_length) >= sizeof(replacement) ||
      mountpoint_length < 0 ||
      size_t(mountpoint_length) >= sizeof(mountpoint) ||
      object_length < 0 || size_t(object_length) >= sizeof(object)) {
    return fail(-1, "prepare fixture paths");
  }
  if (::mkdir(mountpoint, 0700) != 0 ||
      !write_contents(source, kOldContents)) {
    return fail(-1, "prepare fixture files");
  }
  void* mapping = ::mmap(nullptr, sizeof(SharedState), PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (mapping == MAP_FAILED) return fail(-1, "mmap shared state");
  auto* shared = new (mapping) SharedState;

  const pid_t child = ::fork();
  if (child < 0) return fail(-1, "fork");
  if (child == 0) {
    _exit(run_daemon(source, mountpoint, *shared));
  }
  for (unsigned attempt = 0; attempt != 500 &&
       !shared->mounted.load(std::memory_order_acquire); ++attempt) {
    if (::kill(child, 0) != 0) return fail(child, "daemon exited before mount");
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  if (!shared->mounted.load(std::memory_order_acquire)) {
    return fail(child, "mount timeout");
  }

  const int normal_fd = ::open(object, O_RDONLY | O_CLOEXEC);
  if (normal_fd < 0) return fail(child, "open normal handle");
  struct stat normal_status{};
  if (::fstat(normal_fd, &normal_status) != 0 ||
      normal_status.st_ino != ino_t(kFileInode)) {
    return fail(child, "normal open did not preserve FUSE inode number");
  }
  void* old_mapping = ::mmap(nullptr, strlen(kOldContents), PROT_READ,
                             MAP_SHARED, normal_fd, 0);
  if (old_mapping == MAP_FAILED || ::close(normal_fd) != 0) {
    return fail(child, "map normal handle");
  }
  shared->enable_passthrough.store(true, std::memory_order_release);
  const int blocked_fd = ::open(object, O_RDONLY | O_CLOEXEC);
  if (blocked_fd < 0 || !read_matches(blocked_fd, kOldContents) ||
      shared->registration_calls.load(std::memory_order_acquire) != 0) {
    return fail(child, "normal mmap did not block passthrough promotion");
  }
  ::close(blocked_fd);
  if (::munmap(old_mapping, strlen(kOldContents)) != 0 ||
      !wait_until(shared->normal_handles, 0)) {
    return fail(child, "normal mmap lifetime did not drain");
  }

  const unsigned fallback_reads =
      shared->read_callbacks.load(std::memory_order_acquire);
  const int first = ::open(object, O_RDONLY | O_CLOEXEC);
  if (first < 0 || !read_matches(first, kOldContents)) {
    return fail(child, "open passthrough candidate");
  }
  struct stat passthrough_status{};
  if (::fstat(first, &passthrough_status) != 0 ||
      passthrough_status.st_ino != normal_status.st_ino) {
    return fail(child, "passthrough open exposed the backing inode number");
  }
  if (shared->passthrough_handles.load(std::memory_order_acquire) == 0) {
    const int error = shared->registration_error.load(std::memory_order_acquire);
    if (error != ENOSYS && error != EPERM && error != EACCES) {
      return fail(child, "passthrough fallback was not a verified unsupported/permission error");
    }
    if (shared->read_callbacks.load(std::memory_order_acquire) ==
        fallback_reads) {
      return fail(child, "ordinary fallback did not serve the read");
    }
    fprintf(stderr,
            "warning: verified native FUSE passthrough unsupported: %s; "
            "ordinary fallback passed\n", strerror(error));
    ::close(first);
    ::kill(child, SIGTERM);
    int status = 0;
    while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {}
    return require_passthrough ? 77 : 0;
  }

  const int second = ::open(object, O_RDONLY | O_CLOEXEC);
  if (second < 0 || !read_matches(second, kOldContents) ||
      shared->registration_calls.load(std::memory_order_acquire) != 1 ||
      shared->passthrough_handles.load(std::memory_order_acquire) != 2) {
    return fail(child, "simultaneous opens did not share one backing registration");
  }
  void* passthrough_mapping = ::mmap(
      nullptr, strlen(kOldContents), PROT_READ, MAP_SHARED, first, 0);
  if (passthrough_mapping == MAP_FAILED || ::close(first) != 0 ||
      ::close(second) != 0 ||
      !wait_until(shared->passthrough_handles, 0)) {
    return fail(child, "close passthrough handles");
  }
  if (!write_contents(replacement, kNewContents) ||
      ::rename(replacement, source) != 0 ||
      memcmp(passthrough_mapping, kOldContents, strlen(kOldContents)) != 0) {
    return fail(child, "old passthrough mmap changed after inode replacement");
  }
  const int fresh = ::open(object, O_RDONLY | O_CLOEXEC);
  struct stat fresh_status{};
  if (fresh < 0 || ::fstat(fresh, &fresh_status) != 0 ||
      fresh_status.st_ino != normal_status.st_ino ||
      !read_matches(fresh, kNewContents) ||
      shared->registration_calls.load(std::memory_order_acquire) != 2) {
    return fail(child, "replacement generation did not get a fresh backing inode");
  }
  ::close(fresh);
  ::munmap(passthrough_mapping, strlen(kOldContents));
  ::kill(child, SIGTERM);
  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {}
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    return fail(-1, "daemon shutdown");
  }
  return 0;
}
