#include "cache.hpp"
#include "credentials.hpp"
#include "fuse_reactor.hpp"
#include "http.hpp"
#include "io.hpp"
#include "s3.hpp"

#include <fuse_lowlevel.h>
#include <xxhash.h>

#include <fcntl.h>
#include <getopt.h>
#include <linux/fs.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <sys/statvfs.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <assert.h>
#include <atomic>
#include <errno.h>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <ctype.h>
#include <deque>
#include <functional>
#include <fstream>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <exception>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

constexpr size_t kMaximumListResponseSize      = 8U * 1024U * 1024U;
constexpr unsigned kDirectoryListLimit         = 1000;
constexpr uint32_t kKernelReadAheadSize        = 256U * 1024U;
constexpr unsigned kFuseReactorQueueDepth      = 256;
constexpr char kMaxPrefetchWindowEnvironment[] =
    "UNSTABLE_NGS3FS_MAX_PREFETCH_WINDOW_SIZE";

enum ChecksumService {
  CHECKSUM_SERVICE_UNKNOWN,
  CHECKSUM_SERVICE_AWS,
  CHECKSUM_SERVICE_OSS,
  CHECKSUM_SERVICE_GCS,
};

enum IoEngine {
  IO_ENGINE_AUTO,
  IO_ENGINE_LEGACY,
  IO_ENGINE_URING,
};

struct MountConfig {
  std::string endpoint_host;
  uint16_t endpoint_port            = 80;
  std::string authority;
  std::string bucket;
  std::string bucket_path;
  std::string prefix;
  std::string cache_dir;
  std::string region                = "us-east-1";
  std::string expected_bucket_owner;
  size_t maximum_read_size          = kPreferredIoSize;
  size_t maximum_write_size         = kPreferredIoSize;
  size_t socket_receive_buffer_size = kDefaultSocketReceiveBufferSize;
  uint64_t part_size                = 8ULL * 1024ULL * 1024ULL;
  uint64_t cache_size               = 0;
  uint64_t cache_reserve            = 5;
  uint64_t max_pinned_memory        = 256ULL * 1024ULL * 1024ULL;
  uint64_t directory_cache_ns       = 1000ULL * 1000ULL * 1000ULL;
  size_t max_cached_inodes          = 1'000'000;
  uint32_t stats_interval_seconds   = 0;
  size_t max_prefetch_window_size   = kDefaultMaxPrefetchWindowSize;
  size_t max_prefetch_memory        = 0;
  size_t max_file_prefetch_memory   = 0;
  unsigned max_uploads              = 4;
  unsigned max_connections          = 8;
  int connect_timeout_ms            = kConnectTimeoutMs;
  int request_timeout_ms            = kRequestIoTimeoutMs;
  int protocol_probe_timeout_ms     = kProtocolProbeTimeoutMs;
  int metadata_timeout_ms           = 1000;
  ChecksumAlgorithm checksum        = CHECKSUM_AUTO;
  ChecksumService checksum_service  = CHECKSUM_SERVICE_UNKNOWN;
  IoEngine io_engine                = IO_ENGINE_LEGACY;
  unsigned reactor_count            = 1;
  uid_t uid                         = ::getuid();
  gid_t gid                         = ::getgid();
  mode_t file_mode                  = 0644;
  mode_t directory_mode             = 0755;
  bool report_metrics                  = false;
  bool directory_bucket                = false;
  bool tls                             = false;
  bool verify_read_checksum            = false;
  bool socket_receive_buffer_explicit  = false;
  bool cache_reserve_is_percent        = true;
  bool requester_pays                  = false;
};

ChecksumService checksum_service_from_host(std::string_view host) noexcept;

ChecksumService effective_checksum_service(
    const MountConfig& config) noexcept {
  if (config.checksum_service != CHECKSUM_SERVICE_UNKNOWN) {
    return config.checksum_service;
  }
  ChecksumService service = checksum_service_from_host(config.endpoint_host);
  return service == CHECKSUM_SERVICE_UNKNOWN
             ? checksum_service_from_host(config.authority)
             : service;
}

std::string cache_namespace_id(const MountConfig& config) {
  std::string result;
  result.reserve(config.endpoint_host.size() + config.authority.size() +
                 config.bucket.size() + config.bucket_path.size() +
                 config.prefix.size() + 64);
  const auto append = [&](std::string_view value) {
    result.append(value);
    result.push_back('\0');
  };
  append(config.tls ? "https" : "http");
  append(config.endpoint_host);
  append(std::to_string(config.endpoint_port));
  append(config.authority);
  append(config.bucket);
  append(config.bucket_path);
  append(config.prefix);
  append(config.directory_bucket ? "directory" : "general");
  append(config.expected_bucket_owner);
  append(config.requester_pays ? "requester" : "");
  return result;
}

enum WriteState {
  WRITE_OPEN,
  WRITE_SEALING,
  WRITE_SEALED,
  WRITE_FAILED,
};

enum PipeInput {
  PIPE_INPUT_NONE,
  PIPE_INPUT_MEMORY,
  PIPE_INPUT_FD,
};

struct PipeSegment {
  Pipe pipe;
  size_t bytes    = 0;
  PipeInput input = PIPE_INPUT_NONE;
};

struct RetainedPart {
  // TODO: charge physical pipe page slots, not only logical part bytes.
  // Tiny FD-backed writes can exhaust slots long before payload capacity;
  // the current payload budget then understates pinned memory and fd usage.
  std::vector<PipeSegment> segments;
  ChecksumValue checksum;
  uint64_t bytes  = 0;
  unsigned number = 0;
};

struct State;
class AsyncExpressSession;
struct OpenHandle;

class FileReader {
 public:
  virtual ~FileReader() = default;
  virtual void read(State& state, OpenHandle& handle,
                    fuse_req_t request, fuse_ino_t inode,
                    size_t size, off_t offset) = 0;
};

class FileWriter {
 public:
  virtual ~FileWriter() = default;
  virtual void write(State& state, OpenHandle& handle,
                     fuse_req_t request, fuse_ino_t inode,
                     fuse_bufvec* input, off_t offset) = 0;
  virtual void write_async(State& state, OpenHandle& handle,
                           fuse_req_t request, fuse_ino_t inode,
                           fuse_bufvec* input, off_t offset,
                           FuseReactor& reactor) = 0;
  virtual void flush(State& state, OpenHandle& handle) = 0;
  virtual void fsync(State& state, OpenHandle& handle, bool data_only) = 0;
  virtual void release(State& state, OpenHandle& handle) noexcept = 0;
};

std::shared_ptr<FileReader> make_file_reader(State& state,
                                              OpenHandle& handle);
std::unique_ptr<FileWriter> make_file_writer(State& state,
                                              OpenHandle& handle);

class HttpPool {
 private:
  struct Slot {
    std::unique_ptr<HttpClient> client;
    std::atomic<bool> busy{false};
  };

 public:
  class Lease {
   public:
    Lease() = default;
    Lease(HttpPool* owner, size_t slot) noexcept
        : owner_(owner), slot_(slot) {}
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    Lease(Lease&& other) noexcept
        : owner_(other.owner_), slot_(other.slot_) {
      other.owner_ = nullptr;
    }
    Lease& operator=(Lease&& other) noexcept {
      if (this != &other) {
        reset();
        owner_ = other.owner_;
        slot_  = other.slot_;
        other.owner_ = nullptr;
      }
      return *this;
    }
    ~Lease() { reset(); }

    HttpClient& client() const { return *owner_->slots_[slot_]->client; }
    HttpClient* operator->() const { return &client(); }
    explicit operator bool() const noexcept { return owner_ != nullptr; }

   private:
    void reset() noexcept {
      if (owner_ != nullptr) {
        owner_->release(slot_);
        owner_ = nullptr;
      }
    }

    HttpPool* owner_ = nullptr;
    size_t slot_ = 0;
  };

  explicit HttpPool(const MountConfig& config)
      : release_pipe_(Pipe::create(4096)) {
    const int release_flags = ::fcntl(
        release_pipe_.write_fd(), F_GETFL, 0);
    if (release_flags < 0 ||
        ::fcntl(release_pipe_.write_fd(), F_SETFL,
                release_flags | O_NONBLOCK) != 0) {
      throw std::system_error(errno, std::generic_category(),
                              "fcntl(HTTP release notification)");
    }
    slots_.resize(config.max_connections);
    std::vector<std::exception_ptr> errors(config.max_connections);
    std::vector<std::thread> threads;
    threads.reserve(config.max_connections);
    for (size_t i = 0; i < slots_.size(); ++i) {
      slots_[i] = std::make_unique<Slot>();
      threads.emplace_back([&, i] {
        try {
          slots_[i]->client = HttpClient::connect(
              config.endpoint_host, config.endpoint_port,
              config.authority, config.tls, config.request_timeout_ms,
              config.connect_timeout_ms,
              config.protocol_probe_timeout_ms,
              config.socket_receive_buffer_size);
        } catch (...) {
          errors[i] = std::current_exception();
        }
      });
    }
    for (std::thread& thread : threads) {
      thread.join();
    }
    for (const std::exception_ptr& error : errors) {
      if (error) {
        std::rethrow_exception(error);
      }
    }
  }

  Lease acquire() {
    return acquire_slots(slots_.size());
  }

  Lease acquire_bulk() {
    return acquire_slots(slots_.size() - 1);
  }

  Lease try_acquire_bulk() noexcept {
    return try_acquire_slots(slots_.size() - 1);
  }

  Lease try_acquire() noexcept {
    return try_acquire_slots(slots_.size());
  }

  int begin_async_wait() noexcept {
    reactor_waiters_.fetch_add(1, std::memory_order_acq_rel);
    return release_pipe_.read_fd();
  }

  void end_async_wait() noexcept {
    reactor_waiters_.fetch_sub(1, std::memory_order_acq_rel);
  }

 private:
  Lease try_acquire_slots(size_t usable) noexcept {
    const size_t begin = next_.fetch_add(1, std::memory_order_relaxed) %
        usable;
    for (size_t n = 0; n < usable; ++n) {
      const size_t i = (begin + n) % usable;
      bool available = false;
      if (slots_[i]->busy.compare_exchange_strong(
              available, true, std::memory_order_acquire,
              std::memory_order_relaxed)) {
        return Lease(this, i);
      }
    }
    return {};
  }

  Lease acquire_slots(size_t usable) {
    if (current_fuse_reactor() != nullptr) {
      throw std::system_error(EDEADLK, std::generic_category(), "blocking HTTP acquisition on reactor");
    }
    for (;;) {
      const uint64_t released = released_.load(std::memory_order_acquire);
      const size_t begin = next_.fetch_add(1, std::memory_order_relaxed) %
          usable;
      for (size_t n = 0; n < usable; ++n) {
        const size_t i = (begin + n) % usable;
        bool available = false;
        if (slots_[i]->busy.compare_exchange_strong(
                available, true, std::memory_order_acquire,
                std::memory_order_relaxed)) {
          return Lease(this, i);
        }
      }
      released_.wait(released, std::memory_order_acquire);
    }
  }

  void release(size_t i) noexcept {
    slots_[i]->busy.store(false, std::memory_order_release);
    released_.fetch_add(1, std::memory_order_release);
    released_.notify_one();
    if (reactor_waiters_.load(std::memory_order_acquire) != 0) {
      const std::byte notification{};
      ssize_t result;
      do {
        result = ::write(release_pipe_.write_fd(), &notification,
                         sizeof(notification));
      } while (result < 0 && errno == EINTR);
      // A full pipe is already readable, so dropping this coalesced wakeup
      // cannot strand a waiter and must never block a reactor thread.
      (void)result;
    }
  }

  std::vector<std::unique_ptr<Slot>> slots_;
  std::atomic<size_t> next_{0};
  std::atomic<uint64_t> released_{0};
  Pipe release_pipe_;
  std::atomic<size_t> reactor_waiters_{0};
};

class UploadScheduler {
 private:
  struct Job {
    const void* owner;
    FuseReactor* reactor;
    std::function<void()> run;
    bool upload = false;
    bool asynchronous = false;
  };

 public:
  UploadScheduler(unsigned concurrency, int io_timeout_ms)
      : io_timeout_ms_(io_timeout_ms), max_uploads_(concurrency) {
    threads_.reserve(concurrency);
    for (unsigned i = 0; i < concurrency; ++i) {
      threads_.emplace_back([this] { worker(); });
    }
  }

  UploadScheduler(const UploadScheduler&) = delete;
  UploadScheduler& operator=(const UploadScheduler&) = delete;

  ~UploadScheduler() { shutdown(); }

  void shutdown() noexcept {
    {
      std::lock_guard guard(mutex_);
      stopping_ = true;
    }
    condition_.notify_all();
    for (std::thread& thread : threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

  void submit(const void* owner, std::function<void()> run) {
    {
      std::lock_guard guard(mutex_);
      if (stopping_) {
        throw std::system_error(ECANCELED, std::generic_category(),
                                "upload scheduler stopped");
      }
      jobs_.push_back(Job{owner, current_fuse_reactor(), std::move(run)});
    }
    condition_.notify_one();
  }

  void submit_upload(const void* owner, std::function<void()> run,
                     bool asynchronous = false) {
    {
      std::lock_guard guard(mutex_);
      if (stopping_) {
        throw std::system_error(ECANCELED, std::generic_category(),
                                "upload scheduler stopped");
      }
      jobs_.push_back(Job{owner, current_fuse_reactor(), std::move(run),
                          true, asynchronous});
    }
    condition_.notify_one();
  }

  // Asynchronous upload admission lasts through its final network/local
  // completion, not merely until the CPU preparation worker returns.
  void finish_upload() noexcept {
    {
      std::lock_guard guard(mutex_);
      if (active_uploads_ == 0) abort();
      --active_uploads_;
    }
    condition_.notify_all();
  }

 private:
  void worker() noexcept {
    for (;;) {
      Job job;
      {
        std::unique_lock guard(mutex_);
        const auto eligible = [&](const Job& candidate) {
          return !candidate.upload || active_uploads_ < max_uploads_;
        };
        condition_.wait(guard, [&] {
          return (stopping_ && jobs_.empty()) ||
              std::any_of(jobs_.begin(), jobs_.end(), eligible);
        });
        if (jobs_.empty()) {
          return;
        }
        auto selected = std::find_if(jobs_.begin(), jobs_.end(), eligible);
        if (last_owner_ != nullptr) {
          const auto different = std::find_if(
              jobs_.begin(), jobs_.end(),
              [&](const Job& candidate) {
                return candidate.owner != last_owner_ && eligible(candidate);
              });
          if (different != jobs_.end()) {
            selected = different;
          }
        }
        job = std::move(*selected);
        jobs_.erase(selected);
        last_owner_ = job.owner;
        if (job.upload) ++active_uploads_;
      }
      try {
        std::optional<FuseReactorReplyScope> scope;
        if (job.reactor != nullptr && !job.asynchronous) {
          scope.emplace(job.reactor, io_timeout_ms_);
        }
        job.run();
      } catch (const std::exception& error) {
        fprintf(stderr, "unhandled upload job error: %s\n", error.what());
      } catch (...) {
        fprintf(stderr, "unhandled upload job error\n");
      }
      if (job.upload && !job.asynchronous) finish_upload();
    }
  }

  std::vector<std::thread> threads_;
  std::deque<Job> jobs_;
  std::mutex mutex_;
  std::condition_variable condition_;
  int io_timeout_ms_ = kRequestIoTimeoutMs;
  size_t max_uploads_ = 0;
  size_t active_uploads_ = 0;
  const void* last_owner_ = nullptr;
  bool stopping_ = false;
};

class ReactorSharedMutex {
 public:
  ReactorSharedMutex() : event_(::eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE)) {
    if (!event_) {
      throw std::system_error(errno, std::generic_category(),
                              "eventfd(reactor shared mutex)");
    }
  }

  ReactorSharedMutex(const ReactorSharedMutex&) = delete;
  ReactorSharedMutex& operator=(const ReactorSharedMutex&) = delete;

  void lock() {
    waiting_writers_.fetch_add(1, std::memory_order_acq_rel);
    try {
      for (;;) {
        const uint32_t generation = wake_generation_.load(
            std::memory_order_acquire);
        uint32_t expected = 0;
        if (state_.compare_exchange_strong(
                expected, kWriter, std::memory_order_acquire,
                std::memory_order_relaxed)) {
          waiting_writers_.fetch_sub(1, std::memory_order_release);
          return;
        }
        wait(generation);
      }
    } catch (...) {
      waiting_writers_.fetch_sub(1, std::memory_order_release);
      notify();
      throw;
    }
  }

  bool try_lock() noexcept {
    uint32_t expected = 0;
    return state_.compare_exchange_strong(
        expected, kWriter, std::memory_order_acquire,
        std::memory_order_relaxed);
  }

  void unlock() noexcept {
    state_.store(0, std::memory_order_release);
    notify();
  }

  void lock_shared() {
    for (;;) {
      const uint32_t generation = wake_generation_.load(
          std::memory_order_acquire);
      if (try_lock_shared()) {
        return;
      }
      wait(generation);
    }
  }

  bool try_lock_shared() noexcept {
    if (waiting_writers_.load(std::memory_order_acquire) != 0) {
      return false;
    }
    uint32_t state = state_.load(std::memory_order_relaxed);
    do {
      if ((state & kWriter) != 0 || (state & kReaders) == kReaders) {
        return false;
      }
    } while (!state_.compare_exchange_weak(
        state, state + 1, std::memory_order_acquire,
        std::memory_order_relaxed));
    return true;
  }

  void unlock_shared() noexcept {
    const uint32_t previous =
        state_.fetch_sub(1, std::memory_order_release);
    if ((previous & kReaders) == 0 || (previous & kWriter) != 0) {
      abort();
    }
    if (previous == 1) {
      notify();
    }
  }

  int begin_async_wait(bool writer = false) noexcept {
    if (writer) waiting_writers_.fetch_add(1, std::memory_order_acq_rel);
    reactor_waiters_.fetch_add(1, std::memory_order_acq_rel);
    return event_.get();
  }

  void end_async_wait(bool writer = false) noexcept {
    reactor_waiters_.fetch_sub(1, std::memory_order_acq_rel);
    if (writer) {
      waiting_writers_.fetch_sub(1, std::memory_order_release);
      notify();
    }
  }

 private:
  void wait(uint32_t observed) {
    if (current_fuse_reactor() != nullptr) {
      throw std::system_error(EDEADLK, std::generic_category(), "blocking identity lock on reactor");
    }
    wake_generation_.wait(observed, std::memory_order_acquire);
  }

  void notify() noexcept {
    // A pending writer can make a reader wait while state_ is already zero.
    // Use a generation so a complete lock/unlock cycle cannot hide the wakeup.
    wake_generation_.fetch_add(1, std::memory_order_release);
    wake_generation_.notify_all();
    const uint64_t waiters =
        reactor_waiters_.load(std::memory_order_acquire);
    if (waiters == 0) {
      return;
    }
    ssize_t result;
    do {
      result = ::write(event_.get(), &waiters, sizeof(waiters));
    } while (result < 0 && errno == EINTR);
  }

  static constexpr uint32_t kWriter  = 1U << 31;
  static constexpr uint32_t kReaders = kWriter - 1;
  UniqueFd event_;
  std::atomic<uint32_t> state_{0};
  std::atomic<uint32_t> wake_generation_{0};
  std::atomic<unsigned> waiting_writers_{0};
  std::atomic<unsigned> reactor_waiters_{0};
};

struct OpenHandle {
  OpenHandle()
      : state_event(::eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE)) {
    if (!state_event) {
      throw std::system_error(errno, std::generic_category(),
                              "eventfd(open handle)");
    }
  }

  fuse_ino_t inode  = 0;
  uint64_t id       = 0;
  InodeFile* item   = nullptr;
  std::string object_path;
  std::string key;
  std::string etag;
  std::string version_id;
  std::string upload_id;
  std::string write_id;
  std::shared_ptr<CacheEntry> cache_entry;
  std::shared_ptr<FileReader> reader;
  std::unique_ptr<FileWriter> writer;
  std::vector<std::string> part_etags;
  std::vector<ssostr<96>> part_checksums;
  std::vector<uint64_t> part_checksum_values;
  std::vector<uint64_t> part_sizes;
  std::vector<std::pair<uint64_t, uint64_t>> partial_write_pages;
  std::unique_ptr<RetainedPart> current_part;
  UniqueFd state_event;
  Pipe cache_write_pipe;
  uint64_t size                = 0;
  uint64_t stream_offset       = 0;
  uint64_t cache_last_read_end = 0;
  uint64_t generation_epoch    = 0;
  size_t cache_read_window     = 0;
  size_t pending_parts         = 0;
  unsigned next_part_number    = 1;
  WriteState write_state       = WRITE_OPEN;
  int write_error              = 0;
  bool current_reservation     = false;
  bool multipart_starting      = false;
  bool multipart_required      = false;
  bool part_limit_warned       = false;
  bool write_in_progress       = false;
  bool writable                = false;
  bool create_exclusive        = false;
  bool registered              = false;
  bool inode_counted           = false;
  bool recovery_read           = false;
  bool page_cache_store_failed = false;
  bool cache_read_seen         = false;
  bool write_after_flush_warned = false;
  // The initial reference belongs to the application's open handle. Release
  // closes admission and drops it; the final asynchronous request retires us.
  std::atomic<uint64_t> request_state{1};
  void (*release_ready)(void*) noexcept = nullptr;
  void* release_context = nullptr;
  std::atomic<bool> stale{false};
  std::atomic<bool> read_checksum_bad{false};
  std::atomic<bool> unlinked{false};
  std::atomic<unsigned> reactor_waiters{0};
  mutable ReactorSharedMutex identity_mutex;
  std::mutex mutex;
  std::condition_variable condition;
};

void drop_open_request(OpenHandle& handle) noexcept {
  constexpr uint64_t closing = 1ULL << 63;
  const uint64_t previous = handle.request_state.fetch_sub(
      1, std::memory_order_acq_rel);
  if (previous == (closing | 1)) {
    // This decrement owns retirement. Neither release nor another guard may
    // delete the handle concurrently; callback publication precedes closing.
    handle.release_ready(handle.release_context);
  }
}

void notify_handle(OpenHandle& handle) noexcept {
  const uint64_t wake =
      handle.reactor_waiters.load(std::memory_order_acquire);
  if (wake != 0) {
    ssize_t result;
    do {
      result = ::write(handle.state_event.get(), &wake, sizeof(wake));
    } while (result < 0 && errno == EINTR);
  }
  handle.condition.notify_all();
}

void wait_handle(OpenHandle& handle,
                 std::unique_lock<std::mutex>& guard) {
  if (current_fuse_reactor() != nullptr) {
    throw std::system_error(EDEADLK, std::generic_category(), "blocking handle state wait on reactor");
  }
  handle.condition.wait(guard);
}

class OpenRequestGuard {
 public:
  explicit OpenRequestGuard(OpenHandle& handle) : handle_(handle) {
    uint64_t state = handle_.request_state.load(std::memory_order_relaxed);
    for (;;) {
      if ((state & kClosing) != 0) {
        throw std::system_error(EBADF, std::generic_category(),
                                "closing file handle");
      }
      if (state == kActiveMask) {
        throw std::system_error(EOVERFLOW, std::generic_category(),
                                "too many active file requests");
      }
      if (handle_.request_state.compare_exchange_weak(
              state, state + 1, std::memory_order_acquire,
              std::memory_order_relaxed)) {
        break;
      }
    }
  }

  ~OpenRequestGuard() {
    drop_open_request(handle_);
  }

  // A continuation belongs to an already admitted request. It may retain that
  // request after close, but must not admit a new application operation.
  OpenRequestGuard(const OpenRequestGuard& other) : handle_(other.handle_) {
    uint64_t state = handle_.request_state.load(std::memory_order_relaxed);
    for (;;) {
      if ((state & kActiveMask) == kActiveMask) {
        throw std::system_error(EOVERFLOW, std::generic_category(),
                                "too many active file continuations");
      }
      if (handle_.request_state.compare_exchange_weak(
              state, state + 1, std::memory_order_acquire,
              std::memory_order_relaxed)) break;
    }
  }
  OpenRequestGuard& operator=(const OpenRequestGuard&) = delete;
  OpenRequestGuard(OpenRequestGuard&&) = delete;
  OpenRequestGuard& operator=(OpenRequestGuard&&) = delete;

 private:
  static constexpr uint64_t kClosing    = 1ULL << 63;
  static constexpr uint64_t kActiveMask = kClosing - 1;
  OpenHandle& handle_;
};

struct DirHandle {
  InodeDir* item = nullptr;
  std::atomic<uint8_t> initialized{0};
};

void notify_dir_handle(DirHandle& handle) noexcept {
  handle.initialized.notify_all();
}

void wait_dir_handle(DirHandle& handle) {
  handle.initialized.wait(1, std::memory_order_acquire);
}

time_t wall_time_seconds() {
  struct timespec now{};
  if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
    throw std::system_error(errno, std::generic_category(), "clock_gettime");
  }
  return now.tv_sec;
}

class AmzDateTimeCache {
 public:
  AmzDateTimeCache() {
    publish(amz_datetime_now());
    thread_ = std::thread([this] {
      while (!stopping_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (!stopping_.load(std::memory_order_acquire)) {
          update();
        }
      }
    });
  }

  AmzDateTimeCache(const AmzDateTimeCache&) = delete;
  AmzDateTimeCache& operator=(const AmzDateTimeCache&) = delete;

  ~AmzDateTimeCache() {
    stopping_.store(true, std::memory_order_release);
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  std::array<uint64_t, 2> now() const noexcept {
    for (;;) {
      const uint64_t before = sequence_.load(std::memory_order_acquire);
      if ((before & 1) != 0) {
        continue;
      }
      const uint64_t first = first_.load(std::memory_order_relaxed);
      const uint64_t second = second_.load(std::memory_order_relaxed);
      const uint64_t after = sequence_.load(std::memory_order_acquire);
      if (before != after) {
        continue;
      }
      return {first, second};
    }
  }

 private:
  void update() noexcept {
    try {
      publish(amz_datetime_now());
    } catch (...) {
    }
  }

  void publish(const ssostr<32>& date) noexcept {
    uint64_t first;
    uint64_t second;
    memcpy(&first, date.data(), sizeof(first));
    memcpy(&second, date.data() + sizeof(first), sizeof(second));
    sequence_.fetch_add(1, std::memory_order_relaxed);
    first_.store(first, std::memory_order_relaxed);
    second_.store(second, std::memory_order_relaxed);
    sequence_.fetch_add(1, std::memory_order_release);
  }

  std::atomic<uint64_t> sequence_{0};
  std::atomic<uint64_t> first_{0};
  std::atomic<uint64_t> second_{0};
  std::atomic<bool> stopping_{false};
  std::thread thread_;
};

struct State;
void cache_reclaim_loop(std::stop_token stop, State* state) noexcept;
void cache_recovery_loop(std::stop_token stop, State* state) noexcept;
void stats_report_loop(std::stop_token stop, State* state) noexcept;

size_t prefetch_capacity(const MountConfig& config, size_t page) {
  if (config.max_prefetch_memory) return config.max_prefetch_memory;
  const long pages = ::sysconf(_SC_PHYS_PAGES);
  if (pages <= 0 || uint64_t(pages) > UINT64_MAX / page) {
    throw std::runtime_error("cannot determine physical RAM for prefetch budget");
  }
  return std::max(config.maximum_read_size,
      PrefetchBudget::default_capacity(uint64_t(pages) * page, page));
}

struct State {
  explicit State(MountConfig value)
      : config(std::move(value)),
        credentials(CredentialProviderOptions{
            .region = config.region,
            .connect_timeout_ms = config.connect_timeout_ms,
            .request_timeout_ms = config.request_timeout_ms,
            .protocol_probe_timeout_ms =
                config.protocol_probe_timeout_ms,
            .metadata_timeout_ms = config.metadata_timeout_ms,
        }),
        directory_mtime(wall_time_seconds()),
        page_size(size_t(::sysconf(_SC_PAGESIZE))),
        prefetch_budget(prefetch_capacity(config, page_size),
                        config.maximum_read_size, page_size),
        root_item(std::make_unique<InodeDir>()),
        read_ahead_pool(std::make_shared<ReadAheadStoragePool>()),
        http(std::make_unique<HttpPool>(config)),
        uploads(std::make_unique<UploadScheduler>(
            config.max_uploads, config.request_timeout_ms)),
        publications(std::make_unique<UploadScheduler>(1, config.request_timeout_ms)),
        cache_reclaimer([this](std::stop_token stop) {
          cache_reclaim_loop(stop, this);
        }) {
    config.max_prefetch_memory = prefetch_capacity(config, page_size);
    root_item->set_parent(root_item.get());
    if (config.stats_interval_seconds != 0) {
      stats_reporter = std::jthread([this](std::stop_token stop) {
        stats_report_loop(stop, this);
      });
    }
  }

  void stop_background_tasks() noexcept {
    prefetch_budget.stop();
    cache_recovery.request_stop();
    if (cache_recovery.joinable()) {
      cache_recovery.join();
    }
    {
      std::lock_guard guard(stats_wait_mutex);
      stats_reporter.request_stop();
    }
    stats_condition.notify_all();
    if (stats_reporter.joinable()) {
      stats_reporter.join();
    }
    {
      std::lock_guard guard(cache_mutex);
      cache_reclaimer.request_stop();
    }
    cache_condition.notify_all();
    if (cache_reclaimer.joinable()) {
      cache_reclaimer.join();
    }
    std::unique_ptr<UploadScheduler> stopped_publications;
    {
      std::lock_guard guard(legacy_publication_mutex);
      stopped_publications = std::move(publications);
    }
    stopped_publications.reset();
    uploads.reset();
  }

  ~State() {
    stop_background_tasks();
    for (const auto& [inode, retired] : retired_items) {
      (void)inode;
      detach_parent_slot_if_owned(*retired.item, retired.parent);
      delete_inode(retired.item);
    }
    retired_items.clear();
  }

  struct OpenFileState {
    std::vector<OpenHandle*> handles;
    std::weak_ptr<FileReader> uncached_reader;
    fuse_ino_t reader_inode = 0;
    uint64_t reader_epoch   = 0;
    size_t readers = 0;
    bool writer     = false;
  };

  struct BlockedPath {
    const void* owner;
    std::string path;
    bool prefix;
  };

  struct RetiredItem {
    InodeBase* item;
    InodeBase* parent;
  };

  struct PendingDelete {
    std::string key;
    std::string restore_key;
    std::string replacement_etag;
    bool rollback = false;
    bool deleting = false;
  };

  struct PendingInvalidation {
    fuse_ino_t inode;
    off_t offset;
    off_t length;
    uint64_t generation_epoch;
    std::shared_ptr<InodeFile> item;
  };

  MountConfig config;
  CredentialProvider credentials;
  time_t directory_mtime;
  size_t page_size;
  PrefetchBudget prefetch_budget;
  std::atomic<time_t> prefetch_warning_time{0};
  std::unique_ptr<InodeDir> root_item;
  std::shared_ptr<ReadAheadStoragePool> read_ahead_pool;
  std::unique_ptr<LocalCache> local_cache;
  AmzDateTimeCache date_time;
  std::mutex retired_mutex;
  std::mutex open_files_mutex;
  std::condition_variable open_files_condition;
  std::mutex cache_mutex;
  std::mutex session_mutex;
  // Only speculative STORE and invalidation participate. Ordinary READ
  // replies must bypass this fence: STORE may be waiting on their folios.
  ReactorSharedMutex prefetch_publication_mutex;
  std::mutex metrics_mutex;
  std::mutex credentials_mutex;
  std::mutex rename_probe_mutex;
  std::condition_variable credentials_condition;
  std::mutex budget_mutex;
  std::condition_variable budget_condition;
  UniqueFd budget_event;
  std::atomic<unsigned> budget_reactor_waiters{0};
  std::condition_variable cache_condition;
  std::condition_variable stats_condition;
  std::mutex stats_wait_mutex;
  std::map<fuse_ino_t, RetiredItem> retired_items;
  std::atomic<size_t> retired_count{0};
  std::map<std::string, OpenFileState, std::less<>> open_files;
  std::map<std::string, PendingDelete, std::less<>> pending_deletes;
  std::deque<PendingInvalidation> pending_invalidations;
  std::set<std::string, std::less<>> recovery_paths;
  std::vector<BlockedPath> blocked_paths;
  std::vector<std::shared_ptr<CacheEntry>> recovery_entries;
  std::unique_ptr<HttpPool> http;
  std::unique_ptr<UploadScheduler> uploads;
  std::unique_ptr<UploadScheduler> publications;
  std::mutex legacy_publication_mutex;
  fuse_session* session = nullptr;
  Credentials express_credentials;
  AsyncExpressSession* express_async_waiters = nullptr;
  uint64_t express_expiration_ns   = 0;
  bool express_refreshing          = false;
  uint64_t pinned_bytes            = 0;
  unsigned waiting_writers         = 0;
  uint64_t budget_warning_ns       = 0;
  uint64_t fallback_warning_ns     = 0;
  uint64_t fallback_write_bytes    = 0;
  uint64_t next_budget_ticket      = 0;
  uint64_t serving_budget_ticket   = 0;
  uint64_t next_handle_id          = 0;
  InodeDir* cache_clock_hand       = nullptr;
  size_t cache_clock_size          = 0;
  std::atomic<size_t> cached_inodes{0};
  std::atomic<uint64_t> remote_reads{0};
  std::atomic<uint64_t> remote_read_bytes{0};
  std::atomic<uint64_t> fuse_writes{0};
  std::atomic<uint64_t> fuse_write_bytes{0};
  std::atomic<uint64_t> request_errors{0};
  bool budget_exhausted            = false;
  bool cache_budget_warned         = false;
  uint64_t cache_warning_ns        = 0;
  std::atomic<uint64_t> random_read_warning_ns{0};
  std::atomic<uint64_t> cache_bypass_warning_ns{0};
  std::atomic<uint64_t> request_error_warning_ns{0};
  std::atomic<bool> page_cache_store_warned{false};
  std::atomic<bool> page_cache_invalidate_warned{false};
  bool splice_available            = true;
  bool atomic_o_trunc              = false;
  std::atomic<int> rename_object_support{0};
  std::atomic<int> range_signing_mode{0};
  std::jthread cache_reclaimer;
  std::jthread cache_recovery;
  std::jthread stats_reporter;
};

void emit_runtime_stats(State& state, const char* event) noexcept {
  uint64_t pinned_bytes = 0;
  uint64_t fallback_bytes = 0;
  size_t open_handles = 0;
  {
    std::lock_guard guard(state.budget_mutex);
    pinned_bytes = state.pinned_bytes;
  }
  {
    std::lock_guard guard(state.metrics_mutex);
    fallback_bytes = state.fallback_write_bytes;
  }
  {
    std::lock_guard guard(state.open_files_mutex);
    for (const auto& [path, opened] : state.open_files) {
      (void)path;
      open_handles += opened.readers + unsigned(opened.writer);
    }
  }
  fprintf(stderr,
          "{\"event\":\"%s\",\"remote_reads\":%" PRIu64
          ",\"remote_read_bytes\":%" PRIu64
          ",\"fuse_writes\":%" PRIu64
          ",\"fuse_write_bytes\":%" PRIu64
          ",\"request_errors\":%" PRIu64
          ",\"cached_inodes\":%zu,\"open_handles\":%zu"
          ",\"pinned_bytes\":%" PRIu64
          ",\"copied_write_bytes\":%" PRIu64
          ",\"prefetch_bytes\":%zu,\"prefetch_peak_bytes\":%zu"
          ",\"prefetch_file_peak_bytes\":%zu"
          ",\"credential_source\":\"%s\"}\n",
          event,
          state.remote_reads.load(std::memory_order_relaxed),
          state.remote_read_bytes.load(std::memory_order_relaxed),
          state.fuse_writes.load(std::memory_order_relaxed),
          state.fuse_write_bytes.load(std::memory_order_relaxed),
          state.request_errors.load(std::memory_order_relaxed),
          state.cached_inodes.load(std::memory_order_relaxed), open_handles,
          pinned_bytes, fallback_bytes, state.prefetch_budget.snapshot().used,
          state.prefetch_budget.snapshot().peak, state.prefetch_budget.snapshot().file_peak,
          state.credentials.source_name());
}

void stats_report_loop(std::stop_token stop, State* state) noexcept {
  const auto interval = std::chrono::seconds(
      state->config.stats_interval_seconds);
  std::unique_lock guard(state->stats_wait_mutex);
  while (!stop.stop_requested()) {
    if (state->stats_condition.wait_for(
            guard, interval, [&] { return stop.stop_requested(); })) {
      break;
    }
    guard.unlock();
    emit_runtime_stats(*state, "stats");
    guard.lock();
  }
  guard.unlock();
  emit_runtime_stats(*state, "shutdown_stats");
}

struct AsyncPageCacheFence {
  State& state;
  FuseReactor& reactor;
  fuse_ino_t inode;
  off_t offset;
  off_t length;
  FuseReactor::NotifyFunction done;
  void* context;
  std::unique_lock<ReactorSharedMutex> fence;
  FuseReactor::ReactorTask task;
  AsyncIoRequest wait;
  uint64_t notification = 0;
  bool waiting = false;
  bool invalidate;

  AsyncPageCacheFence(State& s, FuseReactor& r, fuse_ino_t ino,
                        off_t off, off_t len,
                        FuseReactor::NotifyFunction fn, void* ctx, bool notify)
      : state(s), reactor(r), inode(ino), offset(off), length(len),
        done(fn), context(ctx),
        fence(s.prefetch_publication_mutex, std::defer_lock), invalidate(notify) {
    task = {start, cancel, this};
  }
  ~AsyncPageCacheFence() {
    if (waiting) state.prefetch_publication_mutex.end_async_wait(true);
  }
  static void finish(void* context, int result) noexcept {
    std::unique_ptr<AsyncPageCacheFence> self(
        static_cast<AsyncPageCacheFence*>(context));
    auto done = self->done;
    void* value = self->context;
    self.reset(); // Reopen publication before a reentrant completion.
    if (done) done(value, result);
  }
  static void cancel(void* context) noexcept { finish(context, -ECANCELED); }
  static void available(void* context, ssize_t result) noexcept {
    if (result < 0) finish(context, int(result));
    else start(context);
  }
  static void start(void* context) noexcept {
    auto* self = static_cast<AsyncPageCacheFence*>(context);
    try {
      if (!self->fence.try_lock()) {
        if (!self->waiting) {
          self->wait.fd = self->state.prefetch_publication_mutex.begin_async_wait(true);
          self->waiting = true;
        }
        if (!self->fence.try_lock()) {
          self->wait.kind     = AsyncIoRequest::READ;
          self->wait.data     = &self->notification;
          self->wait.length   = sizeof(self->notification);
          self->wait.complete = available;
          self->wait.context  = self;
          if (self->reactor.submit(self->wait)) return;
          finish(self, -errno);
          return;
        }
      }
      if (self->waiting) {
        self->state.prefetch_publication_mutex.end_async_wait(true);
        self->waiting = false;
      }
      if (!self->invalidate) {
        finish(self, 0);
        return;
      }
      if (!self->reactor.notify_inval_inode(
              self->inode, self->offset, self->length, finish, self)) {
        finish(self, -errno);
      }
    } catch (...) { finish(self, -EIO); }
  }
};

bool async_page_cache_fence(State& state, FuseReactor& reactor,
                                 fuse_ino_t inode, off_t offset, off_t length,
                                 FuseReactor::NotifyFunction done,
                                 void* context, bool invalidate = true) noexcept {
  try {
    auto task = std::make_unique<AsyncPageCacheFence>(
        state, reactor, inode, offset, length, done, context, invalidate);
    if (!reactor.post(&task->task)) return false;
    task.release();
    return true;
  } catch (...) {
    errno = ENOMEM;
    return false;
  }
}

void invalidated_page_cache(void* context, int result) noexcept {
  auto& state = *static_cast<State*>(context);
  if (result != 0 && result != -ENOENT &&
      !state.page_cache_invalidate_warned.exchange(true, std::memory_order_relaxed)) {
    fprintf(stderr, "warning: unable to invalidate stale page cache: %s\n",
            strerror(-result));
  }
}

void invalidate_page_cache(State& state, fuse_ino_t inode,
                           off_t offset = 0, off_t length = 0) noexcept {
  if (auto* reactor = current_fuse_reactor()) {
    if (!async_page_cache_fence(state, *reactor, inode, offset, length,
                                     invalidated_page_cache, &state)) {
      invalidated_page_cache(&state, -errno);
    }
    return;
  }
  int error = 0;
  {
    std::unique_lock fence(state.prefetch_publication_mutex);
    std::lock_guard guard(state.session_mutex);
    if (state.session == nullptr) {
      return;
    }
    const int result = fuse_lowlevel_notify_inval_inode(
        state.session, inode, offset, length);
    if (result == 0 || result == -ENOENT) {
      return;
    }
    error = -result;
  }
  if (!state.page_cache_invalidate_warned.exchange(
          true, std::memory_order_relaxed)) {
    fprintf(stderr,
            "warning: unable to invalidate stale page cache: "
            "inode=%" PRIu64 ": %s\n",
            uint64_t(inode), strerror(error));
  }
}

void retain_inode_count(std::atomic<uint32_t>& count,
                         const char* operation);
bool release_inode_count(std::atomic<uint32_t>& count) noexcept;

void queue_page_invalidation(State& state, OpenHandle& handle,
                             off_t offset, off_t length) noexcept {
  try {
    std::lock_guard guard(state.cache_mutex);
    const auto duplicate = std::find_if(
        state.pending_invalidations.begin(),
        state.pending_invalidations.end(),
        [&](const State::PendingInvalidation& pending) {
          return pending.inode == handle.inode && pending.offset == offset &&
              pending.length == length &&
              pending.generation_epoch == handle.generation_epoch;
        });
    if (duplicate == state.pending_invalidations.end()) {
      // The admitted read still owns the handle's inode pin here. Retain a
      // separate pin until this deferred notification is consumed, so forget
      // cannot recycle its pointer-valued inode number in the meantime.
      retain_inode_count(handle.item->open_count, "pin pending invalidation");
      std::shared_ptr<InodeFile> item(handle.item, [](InodeFile* file) {
        release_inode_count(file->open_count);
      });
      state.pending_invalidations.push_back(
          State::PendingInvalidation{handle.inode, offset, length,
                                      handle.generation_epoch, std::move(item)});
    }
    state.cache_condition.notify_all();
  } catch (...) {
    if (!state.page_cache_invalidate_warned.exchange(
            true, std::memory_order_relaxed)) {
      fprintf(stderr,
              "warning: unable to queue stale page-cache invalidation: "
              "inode=%" PRIu64 "\n",
              uint64_t(handle.inode));
    }
  }
}

bool blocked_path_contains(const State::BlockedPath& blocked,
                           std::string_view path) noexcept {
  return blocked.prefix ? path.starts_with(blocked.path)
                        : path == blocked.path;
}

class PathMutationGuard {
 public:
  PathMutationGuard(State& state,
                    std::string_view first, bool first_prefix,
                    std::string_view second, bool second_prefix,
                    const char* operation, bool allow_readers = false,
                    bool allow_writers = false)
      : state_(&state), count_(second.empty() ? 1 : 2) {
    pending_[0] = State::BlockedPath{this, std::string(first), first_prefix};
    if (count_ == 2) {
      pending_[1] = State::BlockedPath{
          this, std::string(second), second_prefix};
    }

    std::lock_guard guard(state.open_files_mutex);
    for (const std::string& path : state.recovery_paths) {
      for (size_t i = 0; i < count_; ++i) {
        if (blocked_path_contains(pending_[i], path)) {
          throw std::system_error(EBUSY, std::generic_category(), operation);
        }
      }
    }
    for (const auto& [path, opened] : state.open_files) {
      for (size_t i = 0; i < count_; ++i) {
        if (blocked_path_contains(pending_[i], path) &&
            ((opened.writer && !allow_writers) ||
             (opened.readers != 0 && !allow_readers))) {
          throw std::system_error(EBUSY, std::generic_category(), operation);
        }
      }
    }
    for (const State::BlockedPath& blocked : state.blocked_paths) {
      for (size_t i = 0; i < count_; ++i) {
        if (blocked_path_contains(blocked, pending_[i].path) ||
            blocked_path_contains(pending_[i], blocked.path)) {
          throw std::system_error(EBUSY, std::generic_category(), operation);
        }
      }
    }
    state.blocked_paths.reserve(state.blocked_paths.size() + count_);
    for (size_t i = 0; i < count_; ++i) {
      state.blocked_paths.push_back(std::move(pending_[i]));
    }
  }

  PathMutationGuard(const PathMutationGuard&) = delete;
  PathMutationGuard& operator=(const PathMutationGuard&) = delete;

  ~PathMutationGuard() {
    if (state_ == nullptr) {
      return;
    }
    std::lock_guard guard(state_->open_files_mutex);
    std::erase_if(state_->blocked_paths, [&](const State::BlockedPath& path) {
      return path.owner == this;
    });
  }

 private:
  State* state_;
  std::array<State::BlockedPath, 2> pending_{};
  size_t count_;
};

struct WorkerState {
  const State* owner = nullptr;
  Pipe transport_pipe;
  HeaderList request_headers;
  std::array<uint64_t, 2> authorization_date{};
  uint64_t authorization_handle_id = 0;
  size_t authorization_count       = 0;
  bool authorization_valid         = false;
};

WorkerState& worker_state(State& state) {
  // Each FUSE worker reuses its transport pipe and header scratch. HTTP
  // connections are leased from the mount-global pool.
  thread_local WorkerState worker;
  if (worker.owner != &state) {
    worker.transport_pipe           = Pipe();
    worker.request_headers          = HeaderList();
    worker.authorization_date       = {};
    worker.authorization_handle_id = 0;
    worker.authorization_count      = 0;
    worker.authorization_valid      = false;
    worker.owner                    = &state;
  }
  return worker;
}

void sweep_retired_items(State& state) noexcept;
void retain_inode_count(std::atomic<uint32_t>& count,
                        const char* operation);
bool release_inode_count(std::atomic<uint32_t>& count) noexcept;
void remove_item(State& state, InodeBase& item);

bool parse_unsigned(std::string_view text, uint64_t& value) {
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(),
                                      value);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

bool parse_byte_size(std::string_view text, uint64_t& value) {
  struct Unit {
    std::string_view suffix;
    uint64_t multiplier;
  };
  constexpr Unit units[] = {
      {"KiB", 1024ULL},
      {"MiB", 1024ULL * 1024ULL},
      {"GiB", 1024ULL * 1024ULL * 1024ULL},
      {"KB", 1024ULL},
      {"MB", 1024ULL * 1024ULL},
      {"GB", 1024ULL * 1024ULL * 1024ULL},
      {"K", 1024ULL},
      {"M", 1024ULL * 1024ULL},
      {"G", 1024ULL * 1024ULL * 1024ULL},
  };

  uint64_t multiplier = 1;
  for (const Unit& unit : units) {
    if (text.ends_with(unit.suffix)) {
      text.remove_suffix(unit.suffix.size());
      multiplier = unit.multiplier;
      break;
    }
  }
  uint64_t number = 0;
  if (text.empty() || !parse_unsigned(text, number) ||
      number > std::numeric_limits<uint64_t>::max() / multiplier) {
    return false;
  }
  value = number * multiplier;
  return true;
}

void decode_mountinfo_path(char* path) noexcept {
  char* d = path;
  for (const char* s = path; *s != '\0';) {
    if (s[0] == '\\' && s[1] >= '0' && s[1] <= '7' &&
        s[2] >= '0' && s[2] <= '7' && s[3] >= '0' && s[3] <= '7') {
      *d++ = static_cast<char>(((s[1] - '0') << 6) |
                              ((s[2] - '0') << 3) | (s[3] - '0'));
      s += 4;
    } else {
      *d++ = *s++;
    }
  }
  *d = '\0';
}

bool find_mount_bdi(const char* mountpoint, unsigned& major,
                    unsigned& minor) noexcept {
  FILE* input = fopen("/proc/self/mountinfo", "r");
  if (input == nullptr) {
    return false;
  }
  char* line = nullptr;
  size_t capacity = 0;
  bool found = false;
  while (getline(&line, &capacity, input) >= 0) {
    char path[PATH_MAX];
    unsigned maj;
    unsigned min;
    if (sscanf(line, "%*u %*u %u:%u %*s %4095s", &maj, &min, path) != 3) {
      continue;
    }
    decode_mountinfo_path(path);
    if (strcmp(path, mountpoint) == 0) {
      major = maj;
      minor = min;
      found = true;
      break;
    }
  }
  free(line);
  fclose(input);
  return found;
}

bool set_kernel_read_ahead(const char* mountpoint, uint32_t& bytes,
                           std::string& error) {
  if (bytes == 0) {
    return true;
  }
  unsigned major;
  unsigned minor;
  if (!find_mount_bdi(mountpoint, major, minor)) {
    error = "mount BDI not found in /proc/self/mountinfo";
    bytes = 0;
    return false;
  }

  char path[128];
  const int path_length = snprintf(
      path, sizeof(path), "/sys/class/bdi/%u:%u/read_ahead_kb", major, minor);
  if (path_length < 0 || size_t(path_length) >= sizeof(path)) {
    error = "mount BDI path is too long";
    bytes = 0;
    return false;
  }
  const uint64_t target = (uint64_t(bytes) + 1023) / 1024;
  uint64_t current = 0;
  {
    UniqueFd input(::open(path, O_RDONLY | O_CLOEXEC));
    if (!input) {
      error = std::string("open ") + path + ": " + strerror(errno);
      bytes = 0;
      return false;
    }
    char text[32];
    const ssize_t length = ::read(input.get(), text, sizeof(text) - 1);
    if (length < 0) {
      error = std::string("read ") + path + ": " + strerror(errno);
      bytes = 0;
      return false;
    }
    text[length] = '\0';
    std::string_view value(text, size_t(length));
    while (!value.empty() &&
           (value.back() == '\n' || value.back() == '\r')) {
      value.remove_suffix(1);
    }
    if (!parse_unsigned(value, current)) {
      error = std::string("invalid value in ") + path;
      bytes = 0;
      return false;
    }
    if (current >= target) {
      return true;
    }
  }

  UniqueFd output(::open(path, O_WRONLY | O_CLOEXEC));
  if (!output) {
    error = std::string("open ") + path + ": " + strerror(errno);
    bytes = static_cast<uint32_t>(current * 1024);
    return false;
  }
  char text[32];
  const auto converted = std::to_chars(text, text + sizeof(text), target);
  if (converted.ec != std::errc{}) {
    error = "unable to format kernel read-ahead size";
    bytes = static_cast<uint32_t>(current * 1024);
    return false;
  }
  *converted.ptr = '\n';
  const size_t length = size_t(converted.ptr - text) + 1;
  const ssize_t written = ::write(output.get(), text, length);
  if (written != ssize_t(length)) {
    error = std::string("write ") + path + ": " +
            (written < 0 ? strerror(errno) : "short write");
    bytes = static_cast<uint32_t>(current * 1024);
    return false;
  }
  return true;
}

void assign_string(std::string& d, const ssostr<32>& s) {
  d.assign(s.data(), s.size());
}

bool parse_mode(std::string_view text, mode_t& value) {
  uint64_t parsed = 0;
  const auto result = std::from_chars(text.data(), text.data() + text.size(),
                                      parsed, 8);
  if (result.ec != std::errc{} || result.ptr != text.data() + text.size() ||
      parsed > 07777U) {
    return false;
  }
  value = static_cast<mode_t>(parsed);
  return true;
}

uint64_t fuse_monotonic_ns() {
  timespec value{};
  if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
    throw std::system_error(errno, std::generic_category(), "clock_gettime");
  }
  return static_cast<uint64_t>(value.tv_sec) * 1'000'000'000ULL +
         static_cast<uint64_t>(value.tv_nsec);
}

uint64_t fuse_monotonic_ns_noexcept() noexcept {
  timespec value{};
  if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
    return 0;
  }
  return uint64_t(value.tv_sec) * 1'000'000'000ULL + uint64_t(value.tv_nsec);
}

uint64_t fuse_thread_cpu_ns() {
  timespec value{};
  if (::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "clock_gettime(CLOCK_THREAD_CPUTIME_ID)");
  }
  return static_cast<uint64_t>(value.tv_sec) * 1'000'000'000ULL +
         static_cast<uint64_t>(value.tv_nsec);
}

void warn_random_read(State& state, std::string_view path,
                      uint64_t file_size, uint64_t offset,
                      size_t size) noexcept {
  const size_t read_ahead = std::min(
      state.config.maximum_read_size,
      size_t(kKernelReadAheadSize));
  const size_t threshold = read_ahead / 4;
  if (file_size < threshold || offset == 0 || size > threshold) {
    return;
  }

  uint64_t previous =
      state.random_read_warning_ns.load(std::memory_order_relaxed);
  if (previous != 0) {
    static thread_local unsigned probe = 0;
    if (++probe != 1024) {
      return;
    }
    probe = 0;
  }

  const uint64_t now = fuse_monotonic_ns_noexcept();
  if (now == 0) {
    return;
  }
  constexpr uint64_t interval = 60ULL * 1000ULL * 1000ULL * 1000ULL;
  if (previous != 0 && now - previous < interval) {
    return;
  }
  if (!state.random_read_warning_ns.compare_exchange_strong(
          previous, now, std::memory_order_relaxed)) {
    return;
  }

  timespec wall{};
  (void)::clock_gettime(CLOCK_REALTIME, &wall);
  fprintf(stderr,
          "warning: likely random read: time_unix=%" PRIu64
          ".%09ld path=%.*s offset=%" PRIu64
          " bytes=%zu; disable POSIX_FADV_RANDOM/MADV_RANDOM "
          "for this mount to preserve kernel read-ahead\n",
          uint64_t(wall.tv_sec), wall.tv_nsec,
          int(path.size()), path.data(),
          offset, size);
}

void warn_cache_bypass(State& state, std::string_view path) noexcept {
  const uint64_t now = fuse_monotonic_ns_noexcept();
  if (now == 0) {
    return;
  }
  constexpr uint64_t interval = 60ULL * 1000ULL * 1000ULL * 1000ULL;
  uint64_t previous =
      state.cache_bypass_warning_ns.load(std::memory_order_relaxed);
  if (previous != 0 && now - previous < interval) {
    return;
  }
  if (!state.cache_bypass_warning_ns.compare_exchange_strong(
          previous, now, std::memory_order_relaxed)) {
    return;
  }
  fprintf(stderr,
          "warning: local cache capacity unavailable; bypassing cache for "
          "read: path=%.*s\n",
          int(path.size()), path.data());
}

void retry_delay(unsigned attempt) noexcept {
  const uint64_t jitter = fuse_monotonic_ns_noexcept() % 25;
  const uint64_t milliseconds = (25ULL << attempt) + jitter;
  timespec delay{
      .tv_sec  = time_t(milliseconds / 1000),
      .tv_nsec = long(milliseconds % 1000) * 1000L * 1000L,
  };
  while (::nanosleep(&delay, &delay) != 0 && errno == EINTR) {
  }
}

inline bool retryable_status(int status) noexcept {
  return status == 408 || status == 425 || status == 429 ||
         (status >= 500 && status <= 599 && status != 501 &&
          status != 505);
}

std::string_view response_xml(const Response& response) noexcept {
  const char* data = response.body.empty()
                         ? ""
                         : reinterpret_cast<const char*>(response.body.data());
  return std::string_view(data, response.body.size());
}

bool retryable_s3_code(std::string_view code) noexcept {
  return code == "RequestTimeout" ||
      code == "RequestTimeoutException" ||
      code == "SlowDown" || code == "InternalError" ||
      code == "ServiceUnavailable" || code == "OperationAborted" ||
      code == "ConditionalRequestConflict";
}

bool retryable_response(const Response& response) noexcept {
  if (retryable_status(response.status)) {
    return true;
  }
  S3ErrorInfo error;
  return !response.body.empty() &&
      parse_s3_error(response_xml(response), error) &&
      retryable_s3_code(error.code);
}

bool retry_after_delay(const Response& response) noexcept {
  const auto header = response.headers.find("retry-after");
  if (header == response.headers.end()) {
    return false;
  }
  uint64_t seconds = 0;
  if (!parse_unsigned(sso_view(header->second), seconds)) {
    try {
      const time_t deadline = parse_http_mtime(sso_view(header->second));
      timespec now{};
      if (::clock_gettime(CLOCK_REALTIME, &now) != 0 ||
          deadline <= now.tv_sec) {
        return false;
      }
      seconds = uint64_t(deadline - now.tv_sec);
    } catch (...) {
      return false;
    }
  }
  seconds = std::min<uint64_t>(seconds, 30);
  timespec delay{.tv_sec = time_t(seconds), .tv_nsec = 0};
  while (::nanosleep(&delay, &delay) != 0 && errno == EINTR) {
  }
  return true;
}

thread_local const std::stop_token* request_stop_token = nullptr;

void check_request_stop() {
  if (request_stop_token != nullptr &&
      request_stop_token->stop_requested()) {
    throw std::system_error(ECANCELED, std::generic_category(),
                            "request cancelled");
  }
}

class RequestStopScope {
 public:
  explicit RequestStopScope(const std::stop_token& stop) noexcept
      : previous_(request_stop_token) {
    request_stop_token = &stop;
  }

  ~RequestStopScope() { request_stop_token = previous_; }

  RequestStopScope(const RequestStopScope&) = delete;
  RequestStopScope& operator=(const RequestStopScope&) = delete;

 private:
  const std::stop_token* previous_;
};

template<class Request>
Response request_with_retries(Request&& request, const char* operation,
                              bool* ambiguous = nullptr) {
  std::exception_ptr last_error;
  for (unsigned attempt = 0; attempt != 4; ++attempt) {
    check_request_stop();
    bool delayed = false;
    try {
      Response response = request();
      check_request_stop();
      if (!retryable_response(response) || attempt == 3) {
        return response;
      }
      if (ambiguous != nullptr) {
        *ambiguous = true;
      }
      fprintf(stderr,
              "warning: retrying %s after S3 response status=%d attempt=%u\n",
              operation, response.status, attempt + 1);
      delayed = retry_after_delay(response);
    } catch (...) {
      check_request_stop();
      last_error = std::current_exception();
      if (ambiguous != nullptr) {
        *ambiguous = true;
      }
      if (attempt == 3) {
        std::rethrow_exception(last_error);
      }
      fprintf(stderr,
              "warning: retrying %s after transport failure attempt=%u\n",
              operation, attempt + 1);
    }
    check_request_stop();
    if (!delayed) {
      retry_delay(attempt);
    }
  }
  std::rethrow_exception(last_error);
}

bool add_fuse_argument(fuse_args& arguments, const char* value) {
  return fuse_opt_add_arg(&arguments, value) == 0;
}

std::string format_authority(std::string_view host, uint16_t port) {
  std::string result;
  if (host.find(':') != std::string_view::npos &&
      !host.starts_with('[')) {
    result = '[' + std::string(host) + ']';
  } else {
    result = host;
  }
  result += ':' + std::to_string(port);
  return result;
}

bool fuse_option_present(std::string_view options,
                         std::string_view wanted) noexcept {
  while (!options.empty()) {
    const size_t comma = options.find(',');
    std::string_view option = options.substr(0, comma);
    const size_t equal = option.find('=');
    if (option.substr(0, equal) == wanted) {
      return true;
    }
    if (comma == std::string_view::npos) {
      break;
    }
    options.remove_prefix(comma + 1);
  }
  return false;
}

void load_environment(MountConfig& config) {
  if (const char* value = getenv("AWS_REGION")) {
    config.region = value;
  } else if (const char* value = getenv("AWS_DEFAULT_REGION")) {
    config.region = value;
  }
  if (const char* text = getenv(kMaxPrefetchWindowEnvironment)) {
    uint64_t value = 0;
    const long page_size = ::sysconf(_SC_PAGESIZE);
    constexpr uint64_t minimum = 1024ULL * 1024ULL;
    if (page_size <= 0 || !parse_unsigned(text, value) || value < minimum ||
        value > std::numeric_limits<size_t>::max() ||
        value % uint64_t(page_size) != 0) {
      fprintf(stderr,
              "warning: %s must be a page-aligned integer byte count "
              "of at least 1048576; using default %zu bytes\n",
              kMaxPrefetchWindowEnvironment,
              kDefaultMaxPrefetchWindowSize);
    } else {
      config.max_prefetch_window_size = size_t(value);
      fprintf(stderr,
              "warning: %s overrides the maximum prefetch window to "
              "%zu bytes\n",
              kMaxPrefetchWindowEnvironment,
              config.max_prefetch_window_size);
    }
  }
}

bool parse_arguments(int argc, char** argv, MountConfig& config,
                     fuse_args& fuse_arguments) {
  if (!add_fuse_argument(fuse_arguments, argv[0])) {
    return false;
  }

  constexpr int verify_read_checksum_option       = 257;
  constexpr int connect_timeout_option            = 258;
  constexpr int request_timeout_option            = 259;
  constexpr int protocol_probe_timeout_option     = 260;
  constexpr int metadata_timeout_option           = 261;
  constexpr int stats_interval_option             = 262;
  constexpr int socket_buffer_size_option          = 263;
  constexpr int cache_size_option                  = 264;
  constexpr int cache_reserve_option               = 265;
  constexpr int expected_bucket_owner_option       = 267;
  constexpr int requester_pays_option              = 268;
  constexpr int io_engine_option                   = 269;
  constexpr int reactors_option                    = 270;
  constexpr int max_prefetch_memory_option         = 271;
  constexpr int max_file_prefetch_memory_option    = 272;
  constexpr option long_options[] = {
      {"endpoint-host", required_argument, nullptr, 'e'},
      {"endpoint-port", required_argument, nullptr, 'p'},
      {"authority", required_argument, nullptr, 'a'},
      {"bucket", required_argument, nullptr, 'b'},
      {"prefix", required_argument, nullptr, 'k'},
      {"region", required_argument, nullptr, 'r'},
      {"checksum", required_argument, nullptr, 'K'},
      {"verify-read-checksum", no_argument, nullptr,
       verify_read_checksum_option},
      {"connect-timeout", required_argument, nullptr,
       connect_timeout_option},
      {"request-timeout", required_argument, nullptr,
       request_timeout_option},
      {"protocol-probe-timeout", required_argument, nullptr,
       protocol_probe_timeout_option},
      {"metadata-timeout", required_argument, nullptr,
       metadata_timeout_option},
      {"stats-interval", required_argument, nullptr,
       stats_interval_option},
      {"socket-buffer-size", required_argument, nullptr,
       socket_buffer_size_option},
      {"cache-dir", required_argument, nullptr, 'L'},
      {"cache-size", required_argument, nullptr, cache_size_option},
      {"cache-reserve", required_argument, nullptr,
       cache_reserve_option},
      {"expected-bucket-owner", required_argument, nullptr,
       expected_bucket_owner_option},
      {"requester-pays", no_argument, nullptr, requester_pays_option},
      {"io-engine", required_argument, nullptr, io_engine_option},
      {"reactors", required_argument, nullptr, reactors_option},
      {"max-prefetch-memory", required_argument, nullptr,
       max_prefetch_memory_option},
      {"max-file-prefetch-memory", required_argument, nullptr,
       max_file_prefetch_memory_option},
      {"dir-cache-timeout", required_argument, nullptr, 'T'},
      {"max-cached-inodes", required_argument, nullptr, 'I'},
      {"part-size", required_argument, nullptr, 'P'},
      {"max-uploads", required_argument, nullptr, 'c'},
      {"max-connections", required_argument, nullptr, 'C'},
      {"max-pinned-memory", required_argument, nullptr, 'B'},
      {"uid", required_argument, nullptr, 'u'},
      {"gid", required_argument, nullptr, 'g'},
      {"file-mode", required_argument, nullptr, 'm'},
      {"dir-mode", required_argument, nullptr, 'D'},
      {"metrics", no_argument, nullptr, 'M'},
      {"tls", no_argument, nullptr, 'S'},
      {"help", no_argument, nullptr, 'h'},
      {"version", no_argument, nullptr, 'V'},
      {nullptr, 0, nullptr, 0},
  };
  constexpr std::string_view short_options =
      "-e:p:a:b:k:r:K:L:T:I:P:c:C:B:u:g:m:D:MShVdfso:";

  auto add_fuse_short_option = [&](int value) {
    const char argument[] = {'-', static_cast<char>(value), '\0'};
    return add_fuse_argument(fuse_arguments, argument);
  };
  auto parse_required_unsigned = [&](std::string_view name) {
    uint64_t value = 0;
    if (optarg == nullptr || !parse_unsigned(optarg, value)) {
      throw std::invalid_argument("invalid " + std::string(name));
    }
    return value;
  };
  auto parse_required_size = [&](std::string_view name) {
    uint64_t value = 0;
    if (optarg == nullptr || !parse_byte_size(optarg, value)) {
      throw std::invalid_argument("invalid " + std::string(name));
    }
    return value;
  };
  auto parse_prefetch_memory = [&](std::string_view name) {
    const uint64_t value = parse_required_size(name);
    if (value > std::numeric_limits<size_t>::max()) {
      throw std::invalid_argument(std::string(name) + " exceeds SIZE_MAX");
    }
    return size_t(value);
  };

  opterr = 0;
  optind = 1;
  for (;;) {
    const int parsed = getopt_long(argc, argv, short_options.data(),
                                   long_options, nullptr);
    if (parsed == -1) {
      break;
    }
    switch (parsed) {
      case 1:
        if (!add_fuse_argument(fuse_arguments, optarg)) {
          return false;
        }
        break;
      case 'e':
        config.endpoint_host = optarg;
        break;
      case 'p': {
        const uint64_t value = parse_required_unsigned("--endpoint-port");
        if (value == 0 || value > std::numeric_limits<uint16_t>::max()) {
          throw std::invalid_argument("invalid --endpoint-port");
        }
        config.endpoint_port = static_cast<uint16_t>(value);
        break;
      }
      case 'a':
        config.authority = optarg;
        break;
      case 'b':
        config.bucket = optarg;
        break;
      case 'k':
        config.prefix = optarg;
        break;
      case 'r':
        config.region = optarg;
        break;
      case 'K':
        if (optarg == nullptr ||
            !parse_checksum_algorithm(optarg, config.checksum)) {
          throw std::invalid_argument(
              "invalid --checksum; expected auto, default, none, crc32, "
              "crc32c, crc64nvme, sha1, sha256, md5, xxhash64, xxhash3, "
              "xxhash128, sha512, or crc64xz");
        }
        break;
      case verify_read_checksum_option:
        config.verify_read_checksum = true;
        break;
      case connect_timeout_option:
      case request_timeout_option:
      case protocol_probe_timeout_option:
      case metadata_timeout_option: {
        const char* name = parsed == connect_timeout_option
                               ? "--connect-timeout"
                           : parsed == request_timeout_option
                               ? "--request-timeout"
                           : parsed == protocol_probe_timeout_option
                               ? "--protocol-probe-timeout"
                               : "--metadata-timeout";
        const uint64_t value = parse_required_unsigned(name);
        constexpr uint64_t maximum = 60ULL * 60ULL * 1000ULL;
        if (value == 0 || value > maximum) {
          throw std::invalid_argument(
              std::string(name) + " must be between 1 and 3600000 ms");
        }
        int& destination = parsed == connect_timeout_option
                               ? config.connect_timeout_ms
                           : parsed == request_timeout_option
                               ? config.request_timeout_ms
                           : parsed == protocol_probe_timeout_option
                               ? config.protocol_probe_timeout_ms
                               : config.metadata_timeout_ms;
        destination = int(value);
        break;
      }
      case stats_interval_option: {
        const uint64_t value = parse_required_unsigned("--stats-interval");
        if (value > 24ULL * 60ULL * 60ULL) {
          throw std::invalid_argument(
              "--stats-interval must be at most 86400 seconds");
        }
        config.stats_interval_seconds = uint32_t(value);
        break;
      }
      case socket_buffer_size_option: {
        const uint64_t value = parse_required_size("--socket-buffer-size");
        if (value > uint64_t(std::numeric_limits<int>::max())) {
          throw std::invalid_argument(
              "--socket-buffer-size must be at most INT_MAX");
        }
        config.socket_receive_buffer_size = size_t(value);
        config.socket_receive_buffer_explicit = true;
        break;
      }
      case 'L':
        if (optarg == nullptr || *optarg == '\0') {
          throw std::invalid_argument("--cache-dir must not be empty");
        }
        config.cache_dir = optarg;
        break;
      case cache_size_option:
        config.cache_size = parse_required_size("--cache-size");
        break;
      case cache_reserve_option: {
        if (optarg == nullptr || *optarg == '\0') {
          throw std::invalid_argument("invalid --cache-reserve");
        }
        std::string_view value(optarg);
        if (value.ends_with('%')) {
          value.remove_suffix(1);
          uint64_t percent = 0;
          if (!parse_unsigned(value, percent) || percent > 99) {
            throw std::invalid_argument(
                "--cache-reserve percentage must be between 0% and 99%");
          }
          config.cache_reserve = percent;
          config.cache_reserve_is_percent = true;
        } else {
          uint64_t bytes = 0;
          if (!parse_byte_size(value, bytes)) {
            throw std::invalid_argument("invalid --cache-reserve");
          }
          config.cache_reserve = bytes;
          config.cache_reserve_is_percent = false;
        }
        break;
      }
      case expected_bucket_owner_option:
        if (optarg == nullptr || *optarg == '\0') {
          throw std::invalid_argument(
              "--expected-bucket-owner must not be empty");
        }
        config.expected_bucket_owner = optarg;
        break;
      case requester_pays_option:
        config.requester_pays = true;
        break;
      case io_engine_option: {
        const std::string_view value = optarg == nullptr
                                           ? std::string_view()
                                           : std::string_view(optarg);
        if (value == "auto") {
          config.io_engine = IO_ENGINE_AUTO;
        } else if (value == "legacy") {
          config.io_engine = IO_ENGINE_LEGACY;
        } else if (value == "uring") {
          config.io_engine = IO_ENGINE_URING;
        } else {
          throw std::invalid_argument(
              "invalid --io-engine; expected auto, legacy, or uring");
        }
        break;
      }
      case reactors_option: {
        const uint64_t value = parse_required_unsigned("--reactors");
        if (value == 0 || value > 256) {
          throw std::invalid_argument(
              "--reactors must be between 1 and 256");
        }
        config.reactor_count = unsigned(value);
        break;
      }
      case max_prefetch_memory_option:
        config.max_prefetch_memory =
            parse_prefetch_memory("--max-prefetch-memory");
        break;
      case max_file_prefetch_memory_option:
        config.max_file_prefetch_memory =
            parse_prefetch_memory("--max-file-prefetch-memory");
        break;
      case 'T': {
        const uint64_t milliseconds =
            parse_required_unsigned("--dir-cache-timeout");
        if (milliseconds > UINT64_MAX / (1000ULL * 1000ULL)) {
          throw std::invalid_argument("invalid --dir-cache-timeout");
        }
        config.directory_cache_ns = milliseconds * 1000ULL * 1000ULL;
        break;
      }
      case 'I': {
        const uint64_t value =
            parse_required_unsigned("--max-cached-inodes");
        if (value == 0 || value > std::numeric_limits<size_t>::max()) {
          throw std::invalid_argument("invalid --max-cached-inodes");
        }
        config.max_cached_inodes = size_t(value);
        break;
      }
      case 'P':
        config.part_size = parse_required_size("--part-size");
        break;
      case 'c': {
        const uint64_t value = parse_required_unsigned("--max-uploads");
        if (value == 0 || value > UINT_MAX) {
          throw std::invalid_argument("invalid --max-uploads");
        }
        config.max_uploads = unsigned(value);
        break;
      }
      case 'C': {
        const uint64_t value = parse_required_unsigned("--max-connections");
        if (value == 0 || value > UINT_MAX) {
          throw std::invalid_argument("invalid --max-connections");
        }
        config.max_connections = unsigned(value);
        break;
      }
      case 'B':
        config.max_pinned_memory =
            parse_required_size("--max-pinned-memory");
        break;
      case 'u': {
        const uint64_t value = parse_required_unsigned("--uid");
        if (value > std::numeric_limits<uid_t>::max()) {
          throw std::invalid_argument("invalid --uid");
        }
        config.uid = static_cast<uid_t>(value);
        break;
      }
      case 'g': {
        const uint64_t value = parse_required_unsigned("--gid");
        if (value > std::numeric_limits<gid_t>::max()) {
          throw std::invalid_argument("invalid --gid");
        }
        config.gid = static_cast<gid_t>(value);
        break;
      }
      case 'm':
        if (optarg == nullptr || !parse_mode(optarg, config.file_mode)) {
          throw std::invalid_argument("invalid --file-mode");
        }
        break;
      case 'D':
        if (optarg == nullptr || !parse_mode(optarg, config.directory_mode)) {
          throw std::invalid_argument("invalid --dir-mode");
        }
        break;
      case 'M':
        config.report_metrics = true;
        break;
      case 'S':
        config.tls = true;
        break;
      case 'h':
      case 'V':
      case 'd':
      case 'f':
      case 's':
        if (!add_fuse_short_option(parsed)) {
          return false;
        }
        break;
      case 'o':
        if (optarg != nullptr &&
            (fuse_option_present(optarg, "writeback_cache") ||
             fuse_option_present(optarg, "kernel_cache") ||
             fuse_option_present(optarg, "auto_cache") ||
             fuse_option_present(optarg, "direct_io"))) {
          throw std::invalid_argument(
              "requested FUSE cache/direct-I/O option is incompatible with "
              "ngs3fs consistency and sequential-write invariants");
        }
        if (!add_fuse_short_option(parsed) ||
            !add_fuse_argument(fuse_arguments, optarg)) {
          return false;
        }
        break;
      case '?':
      default:
        throw std::invalid_argument(
            "unknown or incomplete option: " +
            std::string(optind > 0 ? argv[optind - 1] : argv[0]));
    }
  }
  while (optind < argc) {
    if (!add_fuse_argument(fuse_arguments, argv[optind++])) {
      return false;
    }
  }

  if (config.authority.empty() && !config.endpoint_host.empty()) {
    std::string host = config.endpoint_host;
    if (!config.bucket.empty() &&
        checksum_service_from_host(host) == CHECKSUM_SERVICE_OSS &&
        !authority_uses_virtual_bucket(host, config.bucket)) {
      host = config.bucket + '.' + host;
    }
    config.authority = format_authority(host, config.endpoint_port);
  }
  if (config.endpoint_port == 443) {
    config.tls = true;
  }
  while (!config.prefix.empty() && config.prefix.front() == '/') {
    config.prefix.erase(config.prefix.begin());
  }
  if (!config.prefix.empty() && config.prefix.back() != '/') {
    config.prefix.push_back('/');
  }
  if (!config.bucket.empty() &&
      !authority_uses_virtual_bucket(config.authority, config.bucket)) {
    config.bucket_path = '/' + uri_encode(config.bucket, false);
  }
  config.directory_bucket =
      config.bucket.ends_with("--x-s3") ||
      config.authority.find(".s3express-") != std::string::npos;
  if (config.directory_bucket && !config.bucket_path.empty()) {
    throw std::invalid_argument(
        "S3 directory buckets require a virtual-hosted authority");
  }
  constexpr uint64_t minimum_part_size = 5ULL * 1024ULL * 1024ULL;
  constexpr uint64_t maximum_part_size = 5ULL * 1024ULL * 1024ULL * 1024ULL;
  if (config.part_size < minimum_part_size ||
      config.part_size > maximum_part_size) {
    throw std::invalid_argument(
        "--part-size must be between 5 MiB and 5 GiB");
  }
  if (config.max_connections <= config.max_uploads) {
    throw std::invalid_argument(
        "--max-connections must be greater than --max-uploads");
  }
  if (config.max_pinned_memory < config.part_size) {
    throw std::invalid_argument(
        "--max-pinned-memory must be at least --part-size");
  }
  const long page_size = ::sysconf(_SC_PAGESIZE);
  if (page_size <= 0) {
    throw std::runtime_error("unable to determine PAGE_SIZE");
  }
  if (config.max_prefetch_window_size % size_t(page_size) != 0) {
    throw std::logic_error("prefetch window is not page aligned");
  }
  constexpr size_t minimum_prefetch_memory = 256U * 1024U;
  const auto validate_prefetch_memory = [&](size_t value,
                                            std::string_view name) {
    if (value != 0 &&
        (value < minimum_prefetch_memory || value % size_t(page_size) != 0)) {
      throw std::invalid_argument(std::string(name) +
                                  " must be 0 or a page-aligned size of at "
                                  "least 256 KiB");
    }
  };
  validate_prefetch_memory(config.max_prefetch_memory,
                           "--max-prefetch-memory");
  validate_prefetch_memory(config.max_file_prefetch_memory,
                           "--max-file-prefetch-memory");
  return true;
}

bool splice_preflight(std::string& error) noexcept {
  try {
    Pipe source = Pipe::create(4096);
    Pipe destination = Pipe::create(4096);
    const std::byte sent{0x5a};
    std::byte received{};
    write_all(source.write_fd(), std::span(&sent, 1));
    splice_exact(source.read_fd(), destination.write_fd(), 1,
                 SPLICE_F_MOVE);
    read_all(destination.read_fd(), std::span(&received, 1));
    if (received != sent) {
      error = "pipe-to-pipe splice corrupted its probe byte";
      return false;
    }
    UniqueFd zero(::open("/dev/zero", O_RDONLY | O_CLOEXEC));
    if (!zero) {
      throw std::system_error(errno, std::generic_category(),
                              "open(/dev/zero)");
    }
    Pipe file_destination = Pipe::create(4096);
    if (splice_some(zero.get(), nullptr, file_destination.write_fd(), 1,
                    SPLICE_F_MOVE) != 1) {
      throw std::runtime_error("file-to-pipe splice probe made no progress");
    }
    read_all(file_destination.read_fd(), std::span(&received, 1));
    return true;
  } catch (const std::exception& exception) {
    error = exception.what();
    return false;
  }
}

void prepare_file_descriptor_budget(const MountConfig& config,
                                    size_t pipe_capacity) noexcept {
  if (pipe_capacity == 0) {
    return;
  }
  const uint64_t segments =
      (config.max_pinned_memory - 1) / pipe_capacity + 1;
  const uint64_t reserved_parts =
      (config.max_pinned_memory - 1) / config.part_size + 1;
  uint64_t needed = segments;
  const auto add = [&](uint64_t value) {
    if (value > UINT64_MAX - needed) {
      needed = UINT64_MAX;
    } else {
      needed += value;
    }
  };
  add(reserved_parts);
  add(uint64_t(config.max_uploads) * 2);
  add(config.max_connections);
  add(128);

  rlimit limit{};
  if (::getrlimit(RLIMIT_NOFILE, &limit) != 0 ||
      uint64_t(limit.rlim_cur) >= needed) {
    return;
  }
  if (needed <= uint64_t(limit.rlim_max)) {
    rlimit raised = limit;
    raised.rlim_cur = rlim_t(needed);
    if (::setrlimit(RLIMIT_NOFILE, &raised) == 0) {
      return;
    }
  }
  fprintf(stderr,
          "warning: RLIMIT_NOFILE=%" PRIu64
          " is below the estimated %" PRIu64
          " descriptors needed to use --max-pinned-memory=%" PRIu64
          "; writes may fail before reaching the configured memory budget\n",
          uint64_t(limit.rlim_cur), needed, config.max_pinned_memory);
}

State& state_from(fuse_req_t request) {
  return *static_cast<State*>(fuse_req_userdata(request));
}

fuse_ino_t item_inode(const InodeBase* item) {
  const fuse_ino_t inode = fuse_ino_t(uintptr_t(item));
  if (inode == 0 || inode == FUSE_ROOT_ID) {
    throw std::runtime_error("InodeBase pointer is not a valid FUSE inode");
  }
  return inode;
}

fuse_ino_t item_inode(const State& state, const InodeBase* item) {
  return item == state.root_item.get() ? FUSE_ROOT_ID : item_inode(item);
}

std::string item_key(const State& state, const InodeBase& item) {
  struct Scratch {
    Scratch() {
      path.reserve(8);
      guards.reserve(8);
    }

    std::vector<std::pair<ssostr<128>, bool>> path;
    std::vector<std::shared_lock<std::shared_mutex>> guards;
    bool active = false;
  };
  thread_local Scratch scratch;
  if (scratch.active) {
    throw std::logic_error("recursive inode path construction");
  }
  scratch.path.clear();
  scratch.guards.clear();
  scratch.active = true;
  struct Reset {
    ~Reset() {
      scratch.path.clear();
      scratch.guards.clear();
      scratch.active = false;
    }
    Scratch& scratch;
  } reset{scratch};

  const InodeBase* current = &item;
  while (current != state.root_item.get()) {
    InodeBase* parent = current->parent();
    if (parent == nullptr || parent == current) {
      throw std::logic_error("inode parent chain does not reach root");
    }
    const Directory& children = parent->dir_children();
    scratch.guards.emplace_back(children.mutex);
    if (current->parent() != parent || current->detached() ||
        current->dentry_slot == UINT32_MAX ||
        current->dentry_slot >= children.end_i() ||
        children.is_deleted(current->dentry_slot) ||
        children.val(current->dentry_slot) != current) {
      throw std::system_error(
          EAGAIN, std::generic_category(), "inode path changed");
    }
    const terark::fstring name = children.key(current->dentry_slot);
    scratch.path.emplace_back(
        ssostr<128>(name.data(), name.size()), current->directory());
    current = parent;
  }
  std::string key = state.config.prefix;
  for (auto i = scratch.path.rbegin(); i != scratch.path.rend(); ++i) {
    key.append(i->first.data(), i->first.size());
    if (i->second) {
      key.push_back('/');
    }
  }
  return key;
}

InodeBase& inode_item(State& state, fuse_ino_t inode) {
  InodeBase* item = inode == FUSE_ROOT_ID
                        ? state.root_item.get()
                        : reinterpret_cast<InodeBase*>(uintptr_t(inode));
  // A pointer inode travels from LOOKUP through the kernel to another owner.
  // Pair with the lookup-count publication before reading its initialized data.
  if (item != nullptr) (void)item->nlookup.load(std::memory_order_acquire);
  if (item == nullptr || item->detached()) {
    throw std::system_error(ENOENT, std::generic_category(), "inode");
  }
  return *item;
}

InodeBase& inode_reference(State& state, fuse_ino_t inode) {
  InodeBase* item = inode == FUSE_ROOT_ID
                        ? state.root_item.get()
                        : reinterpret_cast<InodeBase*>(uintptr_t(inode));
  if (item == nullptr) {
    throw std::system_error(ENOENT, std::generic_category(), "inode");
  }
  (void)item->nlookup.load(std::memory_order_acquire);
  return *item;
}

void fill_inode_stat(const State& state, fuse_ino_t inode,
                     const InodeBase& item, struct stat& status) {
  status = {};
  status.st_ino = inode;
  status.st_uid = state.config.uid;
  status.st_gid = state.config.gid;
  if (item.regular()) {
    const InodeFile& file = static_cast<const InodeFile&>(item);
    status.st_mode        = S_IFREG | state.config.file_mode;
    status.st_nlink       = 1;
    status.st_size        = off_t(file.fsize.load(std::memory_order_relaxed));
    status.st_mtim.tv_sec = file.mtime.load(std::memory_order_relaxed);
  } else {
    status.st_mode        = S_IFDIR | state.config.directory_mode;
    status.st_nlink       = 2;
    status.st_size        = 0;
    status.st_mtim.tv_sec = state.directory_mtime;
  }
}

std::string object_request_path(const State& state, std::string_view key) {
  std::string path = state.config.bucket_path;
  path.push_back('/');
  path += uri_encode(key, true);
  return path;
}

void reply_callback_error(fuse_req_t request) noexcept {
  State& state = state_from(request);
  if (state.config.stats_interval_seconds != 0) {
    state.request_errors.fetch_add(1, std::memory_order_relaxed);
  }
  const uint64_t now = fuse_monotonic_ns_noexcept();
  constexpr uint64_t interval = 1000ULL * 1000ULL * 1000ULL;
  uint64_t previous =
      state.request_error_warning_ns.load(std::memory_order_relaxed);
  bool log = now != 0 &&
      (previous == 0 || now - previous >= interval) &&
      state.request_error_warning_ns.compare_exchange_strong(
          previous, now, std::memory_order_relaxed);
  try {
    throw;
  } catch (const std::system_error& error) {
    const int value = error.code().value();
    if (log) {
      fprintf(stderr, "ngs3fs request failed: %s\n", error.what());
    }
    fuse_reply_err(request, value > 0 ? value : EIO);
  } catch (const std::exception& error) {
    if (log) {
      fprintf(stderr, "ngs3fs request failed: %s\n", error.what());
    }
    fuse_reply_err(request, EIO);
  } catch (...) {
    fuse_reply_err(request, EIO);
  }
}

OpenHandle* handle_optional(const fuse_file_info* file) noexcept {
  if (file == nullptr || file->fh == 0) {
    return nullptr;
  }
  return reinterpret_cast<OpenHandle*>(file->fh);
}

OpenHandle& handle_required(const fuse_file_info* file) {
  OpenHandle* handle = handle_optional(file);
  if (handle == nullptr) {
    throw std::system_error(EBADF, std::generic_category(),
                            "missing FUSE file handle");
  }
  return *handle;
}

template<class Headers>
void append_mount_request_headers(Headers& headers,
                                  const MountConfig& config) {
  if (config.requester_pays) {
    headers.push_back(Header{
        effective_checksum_service(config) == CHECKSUM_SERVICE_OSS
            ? "x-oss-request-payer"
            : "x-amz-request-payer",
        "requester"});
  }
  if (!config.expected_bucket_owner.empty()) {
    headers.push_back(Header{"x-amz-expected-bucket-owner",
                             config.expected_bucket_owner});
  }
}

void authorization_headers_for_credentials(
    HeaderList& output, const State& state, const Credentials& credentials,
    std::string_view method,
    std::string_view request_path, std::span<const Header> signed_headers,
    std::string_view payload_hash, bool fast_get = false,
    std::string_view datetime = {}) {
  if (credentials.access_key_id.empty()) {
    output.clear();
    return;
  }
  const size_t query_position = request_path.find('?');
  const std::string_view path(request_path);
  const std::string_view canonical_uri =
      query_position == std::string::npos ? path
                                          : path.substr(0, query_position);
  std::string canonical_query;
  if (query_position != std::string::npos) {
    std::string_view remaining = path.substr(query_position + 1);
    std::vector<std::string> parameters;
    while (!remaining.empty()) {
      const size_t ampersand = remaining.find('&');
      std::string parameter(remaining.substr(0, ampersand));
      if (parameter.find('=') == std::string::npos) {
        parameter.push_back('=');
      }
      parameters.push_back(std::move(parameter));
      if (ampersand == std::string_view::npos) {
        break;
      }
      remaining.remove_prefix(ampersand + 1);
    }
    std::sort(parameters.begin(), parameters.end());
    for (const std::string& parameter : parameters) {
      if (!canonical_query.empty()) {
        canonical_query.push_back('&');
      }
      canonical_query += parameter;
    }
  }
  const ssostr<32> generated_date =
      datetime.empty() ? amz_datetime_now() : ssostr<32>{};
  if (datetime.empty()) {
    datetime = sso_view(generated_date);
  }
  sign_v4_headers(
      output, credentials,
      SignRequest{
          .method = method,
          .canonical_uri = canonical_uri,
          .canonical_query = canonical_query,
          .authority = state.config.authority,
          .headers = signed_headers,
          .payload_hash = payload_hash,
          .amz_datetime = datetime,
          .region = state.config.region,
          .fast_get = fast_get,
      });
}

[[noreturn]] void throw_s3_response(const Response& response,
                                    const char* operation);
void invalidate_express_session(State& state) noexcept;
void finish_async_express_waiters(
    State& state, std::exception_ptr error = {}) noexcept;

void ensure_express_session(State& state) {
  if (!state.config.directory_bucket) {
    return;
  }
  const uint64_t now = fuse_monotonic_ns();
  {
    std::unique_lock guard(state.credentials_mutex);
    while (state.express_refreshing) {
      state.credentials_condition.wait(guard);
    }
    if (!state.express_credentials.access_key_id.empty() &&
        state.express_expiration_ns > now + 15ULL * 1'000'000'000ULL) {
      return;
    }
    state.express_refreshing = true;
  }

  Credentials credentials;
  uint64_t expiration = 0;
  try {
    const Response response = request_with_retries([&] {
      std::vector<Header> headers{
          Header{"x-amz-create-session-mode", "ReadWrite"},
      };
      append_mount_request_headers(headers, state.config);
      HeaderList authorization;
      authorization_headers_for_credentials(
          authorization, state, state.credentials.get(), "GET", "/?session",
          headers, kEmptyPayloadSha256);
      headers.insert(headers.end(),
                     std::make_move_iterator(authorization.begin()),
                     std::make_move_iterator(authorization.end()));
      std::unique_ptr<HttpClient> client = HttpClient::connect(
          state.config.endpoint_host, state.config.endpoint_port,
          state.config.authority, state.config.tls,
          state.config.request_timeout_ms, state.config.connect_timeout_ms,
          state.config.protocol_probe_timeout_ms,
          state.config.socket_receive_buffer_size);
      return client->request_no_body(
          "GET", "/?session", headers, kMaximumListResponseSize);
    }, "CreateSession");
    if (response.status != 200) {
      throw_s3_response(response, "CreateSession");
    }
    S3Xml xml(response_xml(response), "CreateSession");
    const tinyxml2::XMLElement& root =
        xml.result_root("CreateSessionResult");
    const tinyxml2::XMLElement& values =
        xml.required_child(root, "Credentials");
    credentials.access_key_id =
        xml.required_text(values, "AccessKeyId");
    credentials.secret_access_key =
        xml.required_text(values, "SecretAccessKey");
    credentials.session_token =
        xml.required_text(values, "SessionToken");
    if (credentials.access_key_id.empty() ||
        credentials.secret_access_key.empty() ||
        credentials.session_token.empty()) {
      throw std::runtime_error(
          "CreateSession response omitted temporary credentials");
    }
    if (credentials.access_key_id.find_first_of("\r\n") !=
            std::string::npos ||
        credentials.session_token.find_first_of("\r\n") !=
            std::string::npos) {
      throw std::runtime_error(
          "CreateSession returned unsafe credential header text");
    }
    expiration = fuse_monotonic_ns() + 285ULL * 1'000'000'000ULL;
  } catch (...) {
    finish_async_express_waiters(state, std::current_exception());
    throw;
  }

  {
    std::lock_guard guard(state.credentials_mutex);
    state.express_credentials = std::move(credentials);
    state.express_expiration_ns = expiration;
  }
  finish_async_express_waiters(state);
}

void authorization_headers(
    HeaderList& output, State& state, std::string_view method,
    std::string_view request_path, std::span<const Header> signed_headers,
    std::string_view payload_hash, bool fast_get = false,
    std::string_view datetime = {}) {
  if (!state.config.directory_bucket) {
    if (!state.config.requester_pays &&
        state.config.expected_bucket_owner.empty()) {
      authorization_headers_for_credentials(
          output, state, state.credentials.get(), method, request_path,
          signed_headers, payload_hash, fast_get, datetime);
      return;
    }
    std::vector<Header> augmented(signed_headers.begin(),
                                  signed_headers.end());
    append_mount_request_headers(augmented, state.config);
    authorization_headers_for_credentials(
        output, state, state.credentials.get(), method, request_path,
        augmented, payload_hash, fast_get, datetime);
    append_mount_request_headers(output, state.config);
    return;
  }

  Credentials credentials;
  if (current_fuse_reactor() == nullptr) {
    ensure_express_session(state);
    std::lock_guard guard(state.credentials_mutex);
    credentials = state.express_credentials;
  } else {
    const uint64_t now = fuse_monotonic_ns();
    std::lock_guard guard(state.credentials_mutex);
    if (state.express_refreshing ||
        state.express_credentials.access_key_id.empty() ||
        state.express_expiration_ns <= now + 15ULL * 1'000'000'000ULL) {
      throw std::system_error(EAGAIN, std::generic_category(),
                              "S3 Express session refresh required");
    }
    credentials = state.express_credentials;
  }
  const std::string token = credentials.session_token;
  credentials.session_token.clear();
  std::vector<Header> augmented(signed_headers.begin(), signed_headers.end());
  append_mount_request_headers(augmented, state.config);
  augmented.push_back(Header{"x-amz-s3session-token", token});
  authorization_headers_for_credentials(
      output, state, credentials, method, request_path, augmented,
      payload_hash);
  output.push_back(Header{"x-amz-s3session-token", token});
  append_mount_request_headers(output, state.config);
}

HeaderList authorization_headers(
    State& state, std::string_view method, std::string_view request_path,
    std::span<const Header> signed_headers,
    std::string_view payload_hash) {
  HeaderList output;
  authorization_headers(output, state, method, request_path, signed_headers,
                        payload_hash);
  return output;
}

HeaderList base_authorization_headers(
    State& state, std::string_view method, std::string_view request_path,
    std::span<const Header> signed_headers,
    std::string_view payload_hash) {
  HeaderList output;
  if (!state.config.requester_pays &&
      state.config.expected_bucket_owner.empty()) {
    authorization_headers_for_credentials(
        output, state, state.credentials.get(), method, request_path,
        signed_headers, payload_hash);
    return output;
  }
  std::vector<Header> augmented(signed_headers.begin(),
                                signed_headers.end());
  append_mount_request_headers(augmented, state.config);
  authorization_headers_for_credentials(
      output, state, state.credentials.get(), method, request_path,
      augmented, payload_hash);
  append_mount_request_headers(output, state.config);
  return output;
}

bool contains_ignore_case(std::string_view text,
                          std::string_view wanted) noexcept {
  if (wanted.size() > text.size()) {
    return false;
  }
  for (size_t begin = 0; begin <= text.size() - wanted.size(); ++begin) {
    bool equal = true;
    for (size_t i = 0; i < wanted.size(); ++i) {
      if (tolower(u_char(text[begin + i])) !=
          tolower(u_char(wanted[i]))) {
        equal = false;
        break;
      }
    }
    if (equal) {
      return true;
    }
  }
  return false;
}

bool domain_suffix_ignore_case(std::string_view host,
                               std::string_view suffix) noexcept {
  if (host.starts_with('[')) {
    return false;
  }
  const size_t colon = host.rfind(':');
  if (colon != std::string_view::npos && host.find(':') == colon) {
    host = host.substr(0, colon);
  }
  if (host.ends_with('.')) {
    host.remove_suffix(1);
  }
  if (host.size() < suffix.size()) {
    return false;
  }
  const std::string_view tail = host.substr(host.size() - suffix.size());
  if (!ascii_equal_ignore_case(tail, suffix)) {
    return false;
  }
  return host.size() == suffix.size() ||
         host[host.size() - suffix.size() - 1] == '.';
}

ChecksumService checksum_service_from_response(
    const Response& response) noexcept {
  if (response.headers.find("x-oss-request-id") != response.headers.end()) {
    return CHECKSUM_SERVICE_OSS;
  }
  if (response.headers.find("x-goog-request-id") != response.headers.end() ||
      response.headers.find("x-guploader-uploadid") !=
          response.headers.end()) {
    return CHECKSUM_SERVICE_GCS;
  }
  const auto server = response.headers.find("server");
  if (server != response.headers.end()) {
    const std::string_view value = sso_view(server->second);
    if (contains_ignore_case(value, "AliyunOSS")) {
      return CHECKSUM_SERVICE_OSS;
    }
    if (contains_ignore_case(value, "AmazonS3")) {
      return CHECKSUM_SERVICE_AWS;
    }
  }
  return CHECKSUM_SERVICE_UNKNOWN;
}

ChecksumService checksum_service_from_host(std::string_view host) noexcept {
  if (domain_suffix_ignore_case(host, "aliyuncs.com") ||
      domain_suffix_ignore_case(host, "aliyuncs.com.cn")) {
    return CHECKSUM_SERVICE_OSS;
  }
  if (domain_suffix_ignore_case(host, "storage.googleapis.com")) {
    return CHECKSUM_SERVICE_GCS;
  }
  if (domain_suffix_ignore_case(host, "amazonaws.com") ||
      domain_suffix_ignore_case(host, "amazonaws.com.cn")) {
    return CHECKSUM_SERVICE_AWS;
  }
  return CHECKSUM_SERVICE_UNKNOWN;
}

void configure_checksum(State& state) {
  ChecksumService service = checksum_service_from_host(
      state.config.endpoint_host);
  if (service == CHECKSUM_SERVICE_UNKNOWN) {
    service = checksum_service_from_host(state.config.authority);
  }
  if (state.config.checksum != CHECKSUM_AUTO) {
    if (service == CHECKSUM_SERVICE_UNKNOWN &&
        state.config.checksum == CHECKSUM_CRC64XZ) {
      service = CHECKSUM_SERVICE_OSS;
    }
    state.config.checksum_service = service;
    return;
  }
  if (service == CHECKSUM_SERVICE_UNKNOWN) {
    try {
      const std::string path = state.config.bucket_path.empty()
                                   ? "/"
                                   : state.config.bucket_path;
      const HeaderList headers = authorization_headers(
          state, "HEAD", path, {}, kEmptyPayloadSha256);
      HttpPool::Lease client = state.http->acquire();
      const Response response = client->request_no_body(
          "HEAD", path, headers, 4096);
      service = checksum_service_from_response(response);
    } catch (const std::exception& error) {
      fprintf(stderr, "warning: checksum provider probe failed: %s\n",
              error.what());
    }
  }
  switch (service) {
    case CHECKSUM_SERVICE_AWS:
      state.config.checksum = CHECKSUM_XXHASH128;
      fprintf(stderr, "checksum auto selected xxhash128 for Amazon S3\n");
      break;
    case CHECKSUM_SERVICE_OSS:
      state.config.checksum = CHECKSUM_CRC64XZ;
      fprintf(stderr, "checksum auto selected crc64xz for Alibaba OSS\n");
      break;
    case CHECKSUM_SERVICE_GCS:
      state.config.checksum = CHECKSUM_PROTOCOL_DEFAULT;
      fprintf(stderr,
              "checksum auto selected the Google Cloud Storage protocol "
              "default\n");
      break;
    case CHECKSUM_SERVICE_UNKNOWN:
      state.config.checksum = CHECKSUM_PROTOCOL_DEFAULT;
      fprintf(stderr,
              "checksum auto selected protocol default because the "
              "endpoint did not advertise a known checksum service\n");
      break;
  }
  state.config.checksum_service = service;
}

void initialize_local_cache(State& state) {
  if (state.config.cache_dir.empty()) {
    return;
  }
  state.local_cache = std::make_unique<LocalCache>(CacheConfig{
      .root = state.config.cache_dir,
      .namespace_id = cache_namespace_id(state.config),
      .maximum_bytes = state.config.cache_size,
      .reserve_bytes = state.config.cache_reserve,
      .reserve_percent = unsigned(state.config.cache_reserve),
      .max_prefetch_window_size = state.config.max_prefetch_window_size,
      .page_size = state.page_size,
      .upload_part_size = state.config.part_size,
      .checksum_algorithm = uint32_t(state.config.checksum),
      .reserve_is_percent = state.config.cache_reserve_is_percent,
  });
  std::vector<std::string> isolated_keys;
  state.recovery_entries = state.local_cache->recover_dirty(&isolated_keys);
  for (CachePendingDelete& record :
       state.local_cache->recover_pending_deletes()) {
    const std::string path = object_request_path(state, record.key);
    state.pending_deletes.try_emplace(
        path, State::PendingDelete{
            .key = std::move(record.key),
            .restore_key = std::move(record.restore_key),
            .replacement_etag = std::move(record.replacement_etag),
            .rollback = record.rollback,
        });
  }
  for (auto i = state.recovery_entries.begin();
       i != state.recovery_entries.end();) {
    const std::string path = object_request_path(state, (*i)->key());
    if (!state.pending_deletes.contains(path)) {
      ++i;
      continue;
    }
    const std::string key((*i)->key());
    (*i)->discard_write();
    state.local_cache->remove(key, false);
    fprintf(stderr,
            "warning: pending unlink superseded cached write recovery: "
            "path=%s\n",
            path.c_str());
    i = state.recovery_entries.erase(i);
  }
  for (const std::shared_ptr<CacheEntry>& entry : state.recovery_entries) {
    std::string path = state.config.bucket_path;
    path.push_back('/');
    path += uri_encode(entry->key(), true);
    state.recovery_paths.insert(std::move(path));
  }
  for (const std::string& key : isolated_keys) {
    std::string path = state.config.bucket_path;
    path.push_back('/');
    path += uri_encode(key, true);
    state.recovery_paths.insert(std::move(path));
  }
  if (!isolated_keys.empty()) {
    fprintf(stderr,
            "error: isolated %zu invalid cached write%s; affected paths "
            "remain blocked until their dirty markers are repaired or removed\n",
            isolated_keys.size(), isolated_keys.size() == 1 ? "" : "s");
  }
  if (!state.recovery_entries.empty() || !state.pending_deletes.empty()) {
    state.cache_recovery = std::jthread([&state](std::stop_token stop) {
      cache_recovery_loop(stop, &state);
    });
  }
}

int errno_for_s3_status(int status) noexcept {
  switch (status) {
    case 400:
    case 416:
      return EINVAL;
    case 401:
    case 403:
      return EACCES;
    case 404:
      return ENOENT;
    case 409:
      return EBUSY;
    case 412:
      return ESTALE;
    case 425:
    case 429:
      return EAGAIN;
    case 507:
      return ENOSPC;
    default:
      return status >= 500 ? EREMOTEIO : EIO;
  }
}

int errno_for_s3_error(int status, std::string_view code) noexcept {
  if (code == "NoSuchBucket" || code == "NoSuchKey" ||
      code == "NoSuchUpload") {
    return ENOENT;
  }
  if (code == "BucketAlreadyExists" ||
      code == "BucketAlreadyOwnedByYou") {
    return EEXIST;
  }
  if (code == "AccessDenied" || code == "InvalidAccessKeyId" ||
      code == "SignatureDoesNotMatch" || code == "ExpiredToken" ||
      code == "TokenRefreshRequired") {
    return EACCES;
  }
  if (code == "InvalidObjectState" ||
      code == "ObjectNotInActiveTierError") {
    return EIO;
  }
  if (code == "InvalidRange" || code == "InvalidArgument" ||
      code == "InvalidRequest" || code == "MalformedXML" ||
      code == "EntityTooSmall" || code == "InvalidPart" ||
      code == "InvalidPartOrder") {
    return EINVAL;
  }
  if (code == "EntityTooLarge" || code == "TooManyParts") {
    return EFBIG;
  }
  if (code == "KeyTooLongError") {
    return ENAMETOOLONG;
  }
  if (code == "NotImplemented" || code == "MethodNotAllowed") {
    return EOPNOTSUPP;
  }
  if (code == "PreconditionFailed") {
    return ESTALE;
  }
  if (retryable_s3_code(code)) {
    return EAGAIN;
  }
  if (code == "InsufficientStorage") {
    return ENOSPC;
  }
  return errno_for_s3_status(status);
}

void append_s3_diagnostic(std::string& destination,
                          std::string_view label,
                          std::string_view value) {
  if (value.empty()) {
    return;
  }
  destination.push_back(' ');
  destination.append(label);
  destination.push_back('=');
  constexpr size_t maximum = 512;
  const size_t length = std::min(value.size(), maximum);
  destination.reserve(destination.size() + length);
  for (size_t i = 0; i < length; ++i) {
    const unsigned char ch = u_char(value[i]);
    destination.push_back(ch < 0x20 || ch == 0x7f ? ' ' : char(ch));
  }
}

[[noreturn]] void throw_s3_response(const Response& response,
                                    const char* operation) {
  S3ErrorInfo error;
  const bool parsed = !response.body.empty() &&
      parse_s3_error(response_xml(response), error);
  std::string message(operation);
  message += " failed with HTTP status ";
  message += std::to_string(response.status);
  if (parsed) {
    append_s3_diagnostic(message, "code", error.code);
    append_s3_diagnostic(message, "message", error.message);
  }
  auto request_id = response.headers.find("x-amz-request-id");
  if (request_id == response.headers.end()) {
    request_id = response.headers.find("x-oss-request-id");
  }
  if (request_id == response.headers.end()) {
    request_id = response.headers.find("x-goog-request-id");
  }
  if (request_id == response.headers.end()) {
    request_id = response.headers.find("x-guploader-uploadid");
  }
  if (request_id != response.headers.end()) {
    append_s3_diagnostic(message, "request_id", sso_view(request_id->second));
  } else if (parsed) {
    append_s3_diagnostic(message, "request_id", error.request_id);
  }
  throw std::system_error(
      parsed ? errno_for_s3_error(response.status, error.code)
             : errno_for_s3_status(response.status),
      std::generic_category(), message);
}

struct ObjectMetadata {
  uint64_t size = 0;
  time_t mtime  = 0;
  ssostr<32> last_modified;
  std::string etag;
  std::string version_id;
  std::string write_id;
};

using ObjectGeneration = uint64_t;

class InodeMetadataGuard {
 public:
  explicit InodeMetadataGuard(InodeFile& file) : file_(file) {
    uint64_t state = file_.generation_epoch.load(std::memory_order_relaxed);
    for (;;) {
      if ((state & kRefreshing) != 0) {
        file_.generation_epoch.wait(state, std::memory_order_relaxed);
        state = file_.generation_epoch.load(std::memory_order_relaxed);
        continue;
      }
      if (file_.generation_epoch.compare_exchange_weak(
              state, state | kRefreshing, std::memory_order_acquire,
              std::memory_order_relaxed)) {
        break;
      }
    }
  }

  ~InodeMetadataGuard() {
    file_.generation_epoch.fetch_and(kEpochMask, std::memory_order_release);
    file_.generation_epoch.notify_all();
  }

  InodeMetadataGuard(const InodeMetadataGuard&) = delete;
  InodeMetadataGuard& operator=(const InodeMetadataGuard&) = delete;

 private:
  static constexpr uint64_t kRefreshing = 1ULL << 63;
  static constexpr uint64_t kEpochMask  = kRefreshing - 1;
  InodeFile& file_;
};

ObjectGeneration object_generation(std::string_view etag,
                                   std::string_view version_id,
                                   uint64_t size, time_t mtime) noexcept {
  if (!version_id.empty()) {
    return XXH3_64bits_withSeed(
        version_id.data(), version_id.size(), 0x56455253494f4eULL);
  }
  if (!etag.empty()) {
    return XXH3_64bits_withSeed(etag.data(), etag.size(), 0x45544147ULL);
  }
  const std::array<uint64_t, 2> fallback{
      size, uint64_t(int64_t(mtime))};
  return XXH3_64bits_withSeed(
      fallback.data(), sizeof(fallback), 0x4d54494d45ULL);
}

uint64_t publish_inode_generation(InodeFile& file,
                                  ObjectGeneration generation) noexcept {
  constexpr uint64_t refreshing = 1ULL << 63;
  constexpr uint64_t epoch_mask = refreshing - 1;
  uint64_t epoch = file.generation_epoch.load(std::memory_order_relaxed) &
      epoch_mask;
  const bool known = epoch != 0;
  const bool changed = known &&
      file.generation_hash.load(std::memory_order_relaxed) != generation;
  if (!known || changed) {
    file.generation_hash.store(generation, std::memory_order_relaxed);
    epoch = epoch == epoch_mask ? 1 : epoch + 1;
    file.generation_epoch.store(epoch | refreshing,
                                std::memory_order_release);
  }
  return epoch;
}

bool stale_old_inode_readers(State& state, InodeFile& file,
                             OpenHandle& current,
                             uint64_t generation_epoch) noexcept {
  bool found = false;
  std::lock_guard guard(state.open_files_mutex);
  for (auto& [path, opened] : state.open_files) {
    (void)path;
    for (OpenHandle* handle : opened.handles) {
      if (handle != nullptr && handle != &current &&
          handle->item == &file && !handle->writable &&
          handle->generation_epoch != 0 &&
          handle->generation_epoch != generation_epoch) {
        handle->stale.store(true, std::memory_order_release);
        found |= (handle->request_state.load(std::memory_order_acquire) >> 63) == 0;
      }
    }
  }
  return found;
}

uint64_t async_retry_milliseconds(const Response& response, unsigned attempt) noexcept {
  const auto header = response.headers.find("retry-after");
  if (header != response.headers.end()) {
    uint64_t seconds = 0;
    if (parse_unsigned(sso_view(header->second), seconds)) {
      // A zero timerfd interval disarms the timer instead of waking immediately.
      return std::max<uint64_t>(1, std::min<uint64_t>(seconds, 30) * 1000);
    }
    try {
      const time_t deadline = parse_http_mtime(sso_view(header->second));
      timespec now{};
      if (::clock_gettime(CLOCK_REALTIME, &now) == 0 && deadline > now.tv_sec) {
        return std::min<uint64_t>(uint64_t(deadline - now.tv_sec), 30) * 1000;
      }
    } catch (...) {
    }
  }
  return (25ULL << std::min(attempt, 3U)) + fuse_monotonic_ns_noexcept() % 25;
}

void strip_request_authorization(std::vector<Header>& headers) {
  std::erase_if(headers, [](const Header& header) {
    return header.name == "authorization" || header.name == "x-amz-date" ||
        header.name == "x-amz-security-token" || header.name == "x-amz-s3session-token" ||
        header.name == "x-amz-content-sha256";
  });
}

// Only used when base credentials have no usable snapshot while their source
// is refreshing. Keep this rare path off both the reactor and CPU worker waits.
struct AsyncCredentialWait {
  UniqueFd timer;
  AsyncIoRequest io;
  uint64_t notification = 0;
  uint64_t deadline = 0;
  void (*complete)(void*, ssize_t) noexcept = nullptr;
  void* context = nullptr;

  bool start(State& state, FuseReactor& reactor,
             void (*done)(void*, ssize_t) noexcept, void* value) {
    const uint64_t now = fuse_monotonic_ns();
    if (deadline == 0) {
      const uint64_t duration = uint64_t(std::max(state.config.request_timeout_ms, 1)) * 1'000'000;
      deadline = now > UINT64_MAX - duration ? UINT64_MAX : now + duration;
    }
    if (now >= deadline) {
      throw std::system_error(ETIMEDOUT, std::generic_category(), "credential refresh wait expired");
    }
    if (!timer) {
      timer.reset(::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC));
      if (!timer) throw std::system_error(errno, std::generic_category(), "timerfd(credentials)");
    }
    const uint64_t delay = std::min<uint64_t>(10'000'000, deadline - now);
    itimerspec timeout{};
    timeout.it_value.tv_nsec = long(delay);
    if (::timerfd_settime(timer.get(), 0, &timeout, nullptr) != 0) {
      throw std::system_error(errno, std::generic_category(), "timerfd_settime(credentials)");
    }
    complete = done;
    context = value;
    io = {};
    io.kind = AsyncIoRequest::READ;
    io.fd = timer.get();
    io.data = &notification;
    io.length = sizeof(notification);
    io.complete = ready;
    io.context = this;
    return reactor.submit(io);
  }

  void cancel(FuseReactor& reactor) noexcept { if (io.pending()) reactor.cancel(io); }

  static void ready(void* context, ssize_t result) noexcept {
    auto* self = static_cast<AsyncCredentialWait*>(context);
    if (result >= 0) {
      const uint64_t now = fuse_monotonic_ns_noexcept();
      if (now == 0 || now >= self->deadline) result = -ETIMEDOUT;
    }
    self->complete(self->context, result);
  }
};

class AsyncS3Request {
 public:
  using Complete = AsyncHttpOperation::Complete;

  AsyncS3Request(State& state, FuseReactor& reactor, AsyncHttpRequest arguments,
                 Complete complete, void* context, unsigned max_attempts = 4)
      : state_(state), reactor_(reactor), arguments_(std::move(arguments)),
        complete_(complete), context_(context), max_attempts_(max_attempts) {
    task_ = {run, cancel_queued, this};
  }

  bool start() noexcept {
    if (started_ || complete_ == nullptr || max_attempts_ == 0) {
      errno = EINVAL;
      return false;
    }
    started_ = true;
    if (reactor_.post(&task_)) return true;
    started_ = false;
    return false;
  }

  void cancel() noexcept {
    cancelled_ = true;
    credentials_wait_.cancel(reactor_);
    if (http_) {
      http_->cancel();
    } else if (wait_.pending()) {
      reactor_.cancel(wait_);
    }
  }

  bool ambiguous() const noexcept { return ambiguous_; }

 private:
  static void run(void* context) noexcept {
    static_cast<AsyncS3Request*>(context)->acquire();
  }

  static void cancel_queued(void* context) noexcept {
    static_cast<AsyncS3Request*>(context)->fail(ECANCELED, "S3 request cancelled");
  }

  void fail(int error, const char* operation) noexcept {
    try {
      throw std::system_error(error, std::generic_category(), operation);
    } catch (...) {
      finish(Response{}, std::current_exception());
    }
  }

  void finish(Response&& response, std::exception_ptr error) noexcept {
    http_.reset();
    lease_ = {};
    const Complete complete = complete_;
    void* context = context_;
    complete_ = nullptr;
    complete(context, std::move(response), std::move(error));
  }

  static void available(void* context, ssize_t result) noexcept {
    auto* request = static_cast<AsyncS3Request*>(context);
    if (request->pool_wait_) {
      request->state_.http->end_async_wait();
      request->pool_wait_ = false;
    }
    if (result < 0 || request->cancelled_) {
      request->fail(request->cancelled_ ? ECANCELED : -int(result),
                    "wait for S3 request");
      return;
    }
    request->acquire();
  }

  void acquire() noexcept {
    if (cancelled_) {
      fail(ECANCELED, "S3 request cancelled");
      return;
    }
    try {
      if (resign_) {
        auto headers = arguments_.headers;
        strip_request_authorization(headers);
        const auto authorization = authorization_headers(state_, sso_view(arguments_.method),
            sso_view(arguments_.path), headers, kEmptyPayloadSha256);
        headers.insert(headers.end(), authorization.begin(), authorization.end());
        arguments_.headers = std::move(headers);
        resign_ = false;
      }
      lease_ = state_.http->try_acquire();
      if (!lease_) {
        const int fd = state_.http->begin_async_wait();
        pool_wait_ = true;
        lease_ = state_.http->try_acquire();
        if (!lease_) {
          wait_ = {};
          wait_.kind       = AsyncIoRequest::READ;
          wait_.fd         = fd;
          wait_.data       = &notification_;
          wait_.length     = 1;
          wait_.timeout_ms = 0;
          wait_.complete   = available;
          wait_.context    = this;
          if (reactor_.submit(wait_)) return;
          const int error = errno;
          state_.http->end_async_wait();
          pool_wait_ = false;
          fail(error, "submit S3 connection wait");
          return;
        }
        state_.http->end_async_wait();
        pool_wait_ = false;
      }
      AsyncHttpRequest arguments = max_attempts_ == 1
          ? std::move(arguments_) : arguments_;
      http_ = lease_->make_async_request(
          reactor_, std::move(arguments), received, this);
      http_->start();
    } catch (const CredentialRefreshPending&) {
      try {
        if (!credentials_wait_.start(state_, reactor_, available, this)) {
          fail(errno, "submit credential refresh wait");
        }
      } catch (...) { finish(Response{}, std::current_exception()); }
    } catch (...) {
      received(this, Response{}, std::current_exception());
    }
  }


  static void received(void* context, Response&& response,
                        std::exception_ptr error) noexcept {
    auto* request = static_cast<AsyncS3Request*>(context);
    // The HTTP response may reside inside the completed operation. Move it
    // out before destroying that operation and returning its pool lease.
    Response value = std::move(response);
    const bool published = request->arguments_.range && request->http_ &&
        (request->http_->response().status == 200 || request->http_->response().status == 206) &&
        request->http_->response().body_bytes != 0;
    request->ambiguous_ |= bool(error);
    request->http_.reset();
    request->lease_ = {};
    if (request->cancelled_) {
      request->fail(ECANCELED, "S3 request cancelled");
      return;
    }
    bool retry = !error && retryable_response(value);
    if (!published && !error && value.status == 403 && request->arguments_.range &&
        !request->state_.config.directory_bucket &&
        request->attempt_ + 1 < request->max_attempts_) {
      try {
        bool signed_range = false;
        for (const Header& header : request->arguments_.headers) {
          if (header.name != "authorization") continue;
          const auto auth = sso_view(header.value);
          const size_t begin = auth.find("SignedHeaders=");
          if (begin == std::string_view::npos) break;
          auto names = auth.substr(begin + 14);
          names = names.substr(0, names.find(','));
          while (!names.empty()) {
            const size_t end = names.find(';');
            if (names.substr(0, end) == "range") signed_range = true;
            if (end == std::string_view::npos) break;
            names.remove_prefix(end + 1);
          }
        }
        if (!signed_range) {
          request->resign_ = true;
          if (request->state_.range_signing_mode.exchange(2, std::memory_order_relaxed) != 2) {
            fprintf(stderr, "warning: S3 endpoint rejected unsigned Range; signing Range for this mount\n");
          }
          retry = true;
        }
      } catch (...) {
        request->finish(std::move(value), std::current_exception());
        return;
      }
    }
    if (published || (!error && !retry) ||
        request->attempt_ + 1 >= request->max_attempts_) {
      request->finish(std::move(value), std::move(error));
      return;
    }
    const uint64_t milliseconds = async_retry_milliseconds(value, request->attempt_);
    ++request->attempt_;
    fprintf(stderr, "warning: retrying asynchronous S3 request status=%d attempt=%u\n",
            value.status, request->attempt_);
    try {
      if (!request->timer_) {
        request->timer_.reset(::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC));
        if (!request->timer_) {
          request->fail(errno, "timerfd_create(S3 retry)");
          return;
        }
      }
      const uint64_t nanoseconds = std::max<uint64_t>(1, milliseconds * 1'000'000);
      itimerspec timer{};
      timer.it_value.tv_sec  = time_t(nanoseconds / 1'000'000'000);
      timer.it_value.tv_nsec = long(nanoseconds % 1'000'000'000);
      if (::timerfd_settime(request->timer_.get(), 0, &timer, nullptr) != 0) {
        request->fail(errno, "timerfd_settime(S3 retry)");
        return;
      }
      request->wait_ = {};
      request->wait_.kind     = AsyncIoRequest::READ;
      request->wait_.fd       = request->timer_.get();
      request->wait_.data     = &request->notification_;
      request->wait_.length   = sizeof(request->notification_);
      request->wait_.complete = available;
      request->wait_.context  = request;
      if (!request->reactor_.submit(request->wait_)) {
        request->fail(errno, "submit S3 retry timer");
      }
    } catch (...) {
      request->finish(Response{}, std::current_exception());
    }
  }

  State& state_;
  FuseReactor& reactor_;
  AsyncHttpRequest arguments_;
  Complete complete_;
  void* context_;
  unsigned max_attempts_;
  unsigned attempt_ = 0;
  HttpPool::Lease lease_;
  std::unique_ptr<AsyncHttpOperation> http_;
  AsyncCredentialWait credentials_wait_;
  AsyncIoRequest wait_;
  FuseReactor::ReactorTask task_;
  UniqueFd timer_;
  uint64_t notification_ = 0;
  bool pool_wait_ = false;
  bool started_ = false;
  bool cancelled_ = false;
  bool ambiguous_ = false;
  bool resign_ = false;
};

class AsyncExpressSession {
 public:
  using Complete = void (*)(void*, std::exception_ptr) noexcept;

  AsyncExpressSession(State& state, FuseReactor& reactor,
                      Complete complete, void* context) noexcept
      : state_(state), reactor_(reactor), complete_(complete),
        context_(context) {
    completion_ = {ready_task, cancel_task, this};
  }

  static bool ready(State& state) noexcept {
    if (!state.config.directory_bucket) return true;
    const uint64_t now = fuse_monotonic_ns_noexcept();
    std::lock_guard guard(state.credentials_mutex);
    return !state.express_refreshing &&
        !state.express_credentials.access_key_id.empty() && now != 0 &&
        state.express_expiration_ns > now + 15ULL * 1'000'000'000ULL;
  }

  bool start() noexcept {
    if (started_ || complete_ == nullptr) {
      errno = EINVAL;
      return false;
    }
    if (!reactor_.reserve_completion(&completion_)) return false;
    started_ = true;
    enter();
    return true;
  }

  void cancel() noexcept { cancelled_ = true; }

 private:
  friend void finish_async_express_waiters(
      State&, std::exception_ptr) noexcept;

  void finish_enter(std::exception_ptr error) noexcept {
    error_ = std::move(error);
    completed_ = true;
    reactor_.complete(&completion_);
  }

  static void base_ready(void* context, ssize_t result) noexcept {
    auto* session = static_cast<AsyncExpressSession*>(context);
    if (result >= 0) { session->enter(); return; }
    try {
      throw std::system_error(-int(result), std::generic_category(), "base credential refresh wait");
    } catch (...) { session->finish_enter(std::current_exception()); }
  }

  void enter() noexcept {
    // This prerequisite is also used by pre-signed native/copy operations.
    // Checking base credentials here avoids turning their transient EAGAIN
    // into either an immediate callback loop or a permanent write failure.
    try {
      if (cancelled_) throw std::system_error(ECANCELED, std::generic_category(), "authorization cancelled");
      (void)state_.credentials.get();
    } catch (const CredentialRefreshPending&) {
      try {
        if (!credentials_wait_.start(state_, reactor_, base_ready, this)) {
          throw std::system_error(errno, std::generic_category(), "submit base credential wait");
        }
      } catch (...) { finish_enter(std::current_exception()); }
      return;
    } catch (...) { finish_enter(std::current_exception()); return; }
    bool refresh = false;
    {
      std::lock_guard guard(state_.credentials_mutex);
      const uint64_t now = fuse_monotonic_ns_noexcept();
      const bool valid = !state_.config.directory_bucket ||
          (!state_.express_credentials.access_key_id.empty() && now != 0 &&
           state_.express_expiration_ns >
               now + 15ULL * 1'000'000'000ULL);
      if (valid) {
        completed_ = true;
      } else {
        next_ = state_.express_async_waiters;
        state_.express_async_waiters = this;
        if (!state_.express_refreshing) {
          state_.express_refreshing = true;
          refresh = true;
        }
      }
    }
    if (completed_) {
      reactor_.complete(&completion_);
    } else if (refresh) {
      begin_refresh();
    }
  }

  static void ready_task(void* context) noexcept {
    auto* session = static_cast<AsyncExpressSession*>(context);
    std::exception_ptr error = std::move(session->error_);
    if (session->cancelled_ && !error) {
      try {
        throw std::system_error(ECANCELED, std::generic_category(),
                                "S3 Express session wait cancelled");
      } catch (...) {
        error = std::current_exception();
      }
    }
    const Complete complete = session->complete_;
    void* const target = session->context_;
    session->complete_ = nullptr;
    complete(target, std::move(error));
  }

  static void cancel_task(void* context) noexcept {
    auto* session = static_cast<AsyncExpressSession*>(context);
    try {
      throw std::system_error(ECANCELED, std::generic_category(),
                              "S3 Express session wait cancelled");
    } catch (...) {
      session->error_ = std::current_exception();
    }
    ready_task(context);
  }

  static void credentials_ready(void* context, ssize_t result) noexcept {
    auto* session = static_cast<AsyncExpressSession*>(context);
    if (result >= 0) { session->begin_refresh(); return; }
    try {
      throw std::system_error(-int(result), std::generic_category(), "wait for CreateSession credentials");
    } catch (...) {
      finish_async_express_waiters(session->state_, std::current_exception());
    }
  }

  void begin_refresh() noexcept {
    try {
      AsyncHttpRequest arguments;
      arguments.method = "GET";
      arguments.path = "/?session";
      arguments.max_response_body = kMaximumListResponseSize;
      arguments.headers = {
          Header{"x-amz-create-session-mode", "ReadWrite"},
      };
      append_mount_request_headers(arguments.headers, state_.config);
      HeaderList authorization;
      authorization_headers_for_credentials(
          authorization, state_, state_.credentials.get(), "GET",
          sso_view(arguments.path), arguments.headers, kEmptyPayloadSha256);
      arguments.headers.insert(
          arguments.headers.end(),
          std::make_move_iterator(authorization.begin()),
          std::make_move_iterator(authorization.end()));
      request_ = std::make_unique<AsyncS3Request>(
          state_, reactor_, std::move(arguments), refreshed, this);
      if (!request_->start()) {
        throw std::system_error(errno, std::generic_category(),
                                "submit asynchronous CreateSession");
      }
    } catch (const CredentialRefreshPending&) {
      try {
        if (!credentials_wait_.start(state_, reactor_, credentials_ready, this)) {
          throw std::system_error(errno, std::generic_category(), "submit CreateSession credential wait");
        }
      } catch (...) {
        finish_async_express_waiters(state_, std::current_exception());
      }
    } catch (...) {
      finish_async_express_waiters(state_, std::current_exception());
    }
  }

  static void refreshed(void* context, Response&& response,
                        std::exception_ptr error) noexcept {
    auto* session = static_cast<AsyncExpressSession*>(context);
    Response value = std::move(response);
    session->request_.reset();
    if (error) {
      finish_async_express_waiters(session->state_, std::move(error));
      return;
    }
    try {
      if (value.status != 200) {
        throw_s3_response(value, "CreateSession");
      }
      S3Xml xml(response_xml(value), "CreateSession");
      const tinyxml2::XMLElement& root =
          xml.result_root("CreateSessionResult");
      const tinyxml2::XMLElement& values =
          xml.required_child(root, "Credentials");
      Credentials credentials;
      credentials.access_key_id =
          xml.required_text(values, "AccessKeyId");
      credentials.secret_access_key =
          xml.required_text(values, "SecretAccessKey");
      credentials.session_token =
          xml.required_text(values, "SessionToken");
      if (credentials.access_key_id.empty() ||
          credentials.secret_access_key.empty() ||
          credentials.session_token.empty()) {
        throw std::runtime_error(
            "CreateSession response omitted temporary credentials");
      }
      if (credentials.access_key_id.find_first_of("\r\n") !=
              std::string::npos ||
          credentials.session_token.find_first_of("\r\n") !=
              std::string::npos) {
        throw std::runtime_error(
            "CreateSession returned unsafe credential header text");
      }
      const uint64_t expiration =
          fuse_monotonic_ns() + 285ULL * 1'000'000'000ULL;
      {
        std::lock_guard guard(session->state_.credentials_mutex);
        session->state_.express_credentials = std::move(credentials);
        session->state_.express_expiration_ns = expiration;
      }
      finish_async_express_waiters(session->state_);
    } catch (...) {
      finish_async_express_waiters(
          session->state_, std::current_exception());
    }
  }

  State& state_;
  FuseReactor& reactor_;
  Complete complete_;
  void* context_;
  AsyncExpressSession* next_ = nullptr;
  AsyncCredentialWait credentials_wait_;
  std::unique_ptr<AsyncS3Request> request_;
  FuseReactor::ReactorTask completion_;
  std::exception_ptr error_;
  bool started_ = false;
  bool completed_ = false;
  bool cancelled_ = false;
};

void finish_async_express_waiters(
    State& state, std::exception_ptr error) noexcept {
  AsyncExpressSession* waiters;
  {
    std::lock_guard guard(state.credentials_mutex);
    state.express_refreshing = false;
    waiters = std::exchange(state.express_async_waiters, nullptr);
  }
  state.credentials_condition.notify_all();
  while (waiters != nullptr) {
    AsyncExpressSession* const next = waiters->next_;
    waiters->next_ = nullptr;
    waiters->request_.reset();
    waiters->error_ = error;
    waiters->completed_ = true;
    waiters->reactor_.complete(&waiters->completion_);
    waiters = next;
  }
}

// Metadata requests carry unsigned operation headers until the asynchronous
// Express credential prerequisite has completed. Ordinary S3 takes no extra
// reactor turn: only the socket request itself is queued.
class AsyncSignedS3Request {
 public:
  using Complete = AsyncHttpOperation::Complete;

  AsyncSignedS3Request(State& state, FuseReactor& reactor, AsyncHttpRequest args,
                        Complete complete, void* context, unsigned attempts = 4,
                        std::string_view payload_hash = kEmptyPayloadSha256)
      : state_(state), reactor_(reactor), args_(std::move(args)),
        complete_(complete), context_(context), attempts_(attempts),
        payload_hash_(payload_hash) {}

  bool start() {
    if (started_ || complete_ == nullptr) {
      errno = EINVAL;
      return false;
    }
    started_ = true;
    return advance();
  }

  bool advance() {
    if (cancelled_) throw std::system_error(ECANCELED, std::generic_category(), "signed S3 request cancelled");
    if (AsyncExpressSession::ready(state_)) {
      try {
        return send();
      } catch (const CredentialRefreshPending&) {
        return credentials_wait_.start(state_, reactor_, credentials_ready, this);
      } catch (const std::system_error& error) {
        if (error.code().value() != EAGAIN || !state_.config.directory_bucket) throw;
      }
    }
    session_ = std::make_unique<AsyncExpressSession>(state_, reactor_, authorized, this);
    return session_->start();
  }

  void cancel() noexcept {
    cancelled_ = true;
    credentials_wait_.cancel(reactor_);
    if (session_) session_->cancel();
    if (request_) request_->cancel();
  }

  bool ambiguous() const noexcept { return ambiguous_; }

 private:
  static void credentials_ready(void* context, ssize_t result) noexcept {
    auto* task = static_cast<AsyncSignedS3Request*>(context);
    try {
      if (result < 0) throw std::system_error(-int(result), std::generic_category(), "credential refresh wait");
      if (!task->advance()) throw std::system_error(errno, std::generic_category(), "submit signed S3 request");
    } catch (...) {
      task->complete_(task->context_, Response{}, std::current_exception());
    }
  }

  bool send() {
    AsyncHttpRequest arguments = args_;
    const auto headers = authorization_headers(
        state_, sso_view(arguments.method), sso_view(arguments.path),
        arguments.headers,
        sso_view(payload_hash_));
    arguments.headers.insert(
        arguments.headers.end(), headers.begin(), headers.end());
    request_ = std::make_unique<AsyncS3Request>(
        state_, reactor_, std::move(arguments), received, this, attempts_);
    return request_->start();
  }

  static void authorized(void* context, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncSignedS3Request*>(context);
    task->session_.reset();
    try {
      if (error) std::rethrow_exception(error);
      if (!task->advance()) throw std::system_error(errno, std::generic_category(), "submit signed S3 request");
    } catch (...) {
      task->complete_(task->context_, Response{}, std::current_exception());
    }
  }

  static void received(void* context, Response&& response, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncSignedS3Request*>(context);
    task->ambiguous_ |= task->request_->ambiguous();
    task->request_.reset();
    if (!error && (response.status == 401 || response.status == 403) &&
        task->state_.config.directory_bucket && task->attempts_ > 1 &&
        task->session_attempt_++ < 3) {
      invalidate_express_session(task->state_);
      try {
        if (!task->advance()) {
          throw std::system_error(errno, std::generic_category(),
                                  "resubmit signed S3 request");
        }
      } catch (...) {
        task->complete_(task->context_, Response{},
                        std::current_exception());
      }
      return;
    }
    if (!error) task->session_attempt_ = 0;
    task->complete_(task->context_, std::move(response), std::move(error));
  }

  State& state_;
  FuseReactor& reactor_;
  AsyncHttpRequest args_;
  Complete complete_;
  void* context_;
  unsigned attempts_;
  ssostr<72> payload_hash_;
  unsigned session_attempt_ = 0;
  bool ambiguous_ = false;
  bool started_ = false;
  bool cancelled_ = false;
  AsyncCredentialWait credentials_wait_;
  std::unique_ptr<AsyncExpressSession> session_;
  std::unique_ptr<AsyncS3Request> request_;
};

ObjectMetadata decode_head_response(const Response& response) {
  if (response.status == 404) {
    throw std::system_error(ENOENT, std::generic_category(), "S3 HEAD");
  }
  if (response.status != 200) {
    throw_s3_response(response, "S3 HEAD");
  }
  const auto content_length = response.headers.find("content-length");
  if (content_length == response.headers.end()) {
    throw std::runtime_error("S3 HEAD omitted content-length");
  }
  ObjectMetadata metadata;
  if (!parse_unsigned(sso_view(content_length->second), metadata.size)) {
    throw std::runtime_error("S3 HEAD returned invalid content-length");
  }
  const auto last_modified = response.headers.find("last-modified");
  if (last_modified == response.headers.end()) {
    throw std::runtime_error("S3 HEAD omitted Last-Modified");
  }
  metadata.last_modified.assign(sso_view(last_modified->second));
  metadata.mtime = parse_http_mtime(sso_view(metadata.last_modified));
  const auto etag = response.headers.find("etag");
  if (etag != response.headers.end()) {
    assign_string(metadata.etag, etag->second);
    if (metadata.etag.find_first_of(" \t\r\n") != std::string::npos) {
      throw std::runtime_error("S3 HEAD returned an unsafe ETag");
    }
  }
  auto version = response.headers.find("x-amz-version-id");
  if (version == response.headers.end()) {
    version = response.headers.find("x-oss-version-id");
  }
  if (version != response.headers.end() && version->second != "null") {
    assign_string(metadata.version_id, version->second);
  }
  auto write_id = response.headers.find("x-amz-meta-ngs3fs-write-id");
  if (write_id == response.headers.end()) {
    write_id = response.headers.find("x-oss-meta-ngs3fs-write-id");
  }
  if (write_id != response.headers.end()) {
    assign_string(metadata.write_id, write_id->second);
  }
  return metadata;
}

ObjectMetadata head_object(State& state, std::string_view path) {
  const Response response = request_with_retries([&] {
    const auto authorization = authorization_headers(
        state, "HEAD", path, {}, kEmptyPayloadSha256);
    HttpPool::Lease client = state.http->acquire();
    return client->request_no_body("HEAD", path, authorization);
  }, "HeadObject");
  return decode_head_response(response);
}

bool recover_write_commit(State& state, const OpenHandle& handle,
                          Response& response) noexcept {
  try {
    const ObjectMetadata metadata = head_object(state, handle.object_path);
    if (metadata.size != handle.stream_offset ||
        (!handle.write_id.empty() && metadata.write_id != handle.write_id) ||
        (handle.write_id.empty() && !handle.etag.empty() &&
         metadata.etag == handle.etag)) {
      return false;
    }
    response = {};
    response.status = 200;
    if (!metadata.etag.empty()) {
      response.headers.insert_or_assign(
          ssostr<32>("etag"), ssostr<32>(metadata.etag));
    }
    if (!metadata.version_id.empty()) {
      response.headers.insert_or_assign(
          ssostr<32>("x-amz-version-id"), ssostr<32>(metadata.version_id));
    }
    response.headers.insert_or_assign(
        ssostr<32>("last-modified"), std::move(metadata.last_modified));
    fprintf(stderr,
            "warning: recovered an ambiguous S3 write commit with "
            "HeadObject: %s\n",
            handle.object_path.c_str());
    return true;
  } catch (...) {
    return false;
  }
}

struct OpenGenerationConflict : std::system_error {
  explicit OpenGenerationConflict(bool conflict = true)
      : std::system_error(EBUSY, std::generic_category(),
                            "object generation requires cache invalidation"),
        old_readers(conflict) {}
  bool old_readers;
};

bool publish_open_metadata(State& state, OpenHandle& handle, ObjectMetadata metadata) {
  InodeFile& item = *handle.item;
  const ObjectGeneration generation = object_generation(
      metadata.etag, metadata.version_id, metadata.size, metadata.mtime);
  bool changed;
  bool keep_cache;
  {
    // Every open performs its own HeadObject.  Serialize only the short
    // in-memory publication so a reactor never blocks while another
    // operation owns this state across network I/O.
    InodeMetadataGuard metadata_guard(item);
    const uint64_t previous_epoch =
        item.generation_epoch.load(std::memory_order_acquire) &
        ((1ULL << 63) - 1);
    changed = previous_epoch != 0 &&
        item.generation_hash.load(std::memory_order_relaxed) != generation;
    keep_cache = item.page_cache_valid() && !changed &&
        item.fsize.load(std::memory_order_relaxed) == metadata.size;
    if (changed) {
      item.set_page_cache_valid(false);
    }
    item.fsize.store(metadata.size, std::memory_order_relaxed);
    item.mtime.store(metadata.mtime, std::memory_order_relaxed);
    handle.generation_epoch = publish_inode_generation(item, generation);
    handle.stale.store(false, std::memory_order_release);
    item.set_page_cache_valid(keep_cache);
  }
  handle.size        = metadata.size;
  handle.etag        = std::move(metadata.etag);
  handle.version_id  = std::move(metadata.version_id);
  const bool old_readers = stale_old_inode_readers(
      state, item, handle, handle.generation_epoch);
  if (changed) {
    fprintf(stderr, "warning: S3 object generation changed: path=%s\n",
            handle.object_path.c_str());
  }
  if (changed || old_readers) {
    item.set_page_cache_valid(false);
    // Conflicting reactor opens must await notification completion before
    // replying EBUSY, otherwise the old descriptor can still hit stale pages.
    // Ordinary reads must remain free to reply while invalidation waits for
    // any locked folios; serializing all FUSE output would deadlock them.
    if (current_fuse_reactor() != nullptr) throw OpenGenerationConflict(old_readers);
    invalidate_page_cache(state, handle.inode);
  }
  if (old_readers) {
    throw OpenGenerationConflict();
  }
  return keep_cache;
}

bool refresh_open_metadata(State& state, OpenHandle& handle) {
  return publish_open_metadata(state, handle, head_object(state, handle.object_path));
}

void register_open_handle(State& state, fuse_ino_t inode,
                          OpenHandle& handle, bool wait_for_release = true) {
  std::unique_lock open_files_guard(state.open_files_mutex);
  InodeBase& base = inode_item(state, inode);
  if (!base.regular()) {
    throw std::system_error(EISDIR, std::generic_category(), "open");
  }
  InodeFile& item = static_cast<InodeFile&>(base);
  const std::string key = item_key(state, item);
  handle.inode       = inode;
  handle.item        = &item;
  handle.key         = key;
  handle.object_path = object_request_path(state, key);
  handle.size        = item.fsize.load(std::memory_order_relaxed);
  if (state.recovery_paths.contains(handle.object_path)) {
    if (handle.writable) {
      throw std::system_error(EBUSY, std::generic_category(),
                              "cached object recovery is in progress");
    }
    for (const std::shared_ptr<CacheEntry>& entry :
         state.recovery_entries) {
      if (entry && entry->key() == handle.key) {
        handle.cache_entry  = entry;
        handle.size         = entry->written_end();
        handle.recovery_read = true;
        item.set_fsize(handle.size);
        break;
      }
    }
    if (!handle.recovery_read) {
      throw std::system_error(EIO, std::generic_category(),
                              "cached object recovery is isolated");
    }
  }
  for (const State::BlockedPath& blocked : state.blocked_paths) {
    if (blocked_path_contains(blocked, handle.object_path)) {
      throw std::system_error(EBUSY, std::generic_category(),
                              "object path is being changed");
    }
  }
  const auto conflicts = [&] {
    const auto position = state.open_files.find(handle.object_path);
    if (position == state.open_files.end()) {
      return false;
    }
    const State::OpenFileState& opened = position->second;
    return (handle.writable && (opened.writer || opened.readers != 0)) ||
           (!handle.writable && opened.writer);
  };
  if (conflicts()) {
    if (!wait_for_release) {
      throw std::system_error(EAGAIN, std::generic_category(),
                              "open is waiting for queued release");
    }
    // FUSE_RELEASE can be processed just after a close returns while a later
    // FUSE_OPEN is already running on another worker. Give that queued release
    // a bounded chance to retire its handle before reporting a real conflict.
    state.open_files_condition.wait_for(
        open_files_guard, std::chrono::milliseconds(100),
        [&] { return !conflicts(); });
  }
  if (state.recovery_paths.contains(handle.object_path)) {
    if (handle.writable || !handle.recovery_read) {
      throw std::system_error(EBUSY, std::generic_category(),
                              "cached object recovery is in progress");
    }
  }
  for (const State::BlockedPath& blocked : state.blocked_paths) {
    if (blocked_path_contains(blocked, handle.object_path)) {
      throw std::system_error(EBUSY, std::generic_category(),
                              "object path is being changed");
    }
  }
  auto [position, inserted] = state.open_files.try_emplace(
      handle.object_path, State::OpenFileState{});
  State::OpenFileState& opened = position->second;
  if ((handle.writable && (opened.writer || opened.readers != 0)) ||
      (!handle.writable && opened.writer)) {
    if (inserted) {
      state.open_files.erase(position);
    }
    throw std::system_error(EBUSY, std::generic_category(),
                            "object already open with conflicting access");
  }
  uint32_t count = item.open_count.load(std::memory_order_relaxed);
  if (count == UINT32_MAX ||
      (!handle.writable && opened.readers == SIZE_MAX)) {
    if (inserted) {
      state.open_files.erase(position);
    }
    throw std::system_error(
        EMFILE, std::generic_category(), "inode open count");
  }
  if (state.next_handle_id == UINT64_MAX) {
    if (inserted) {
      state.open_files.erase(position);
    }
    throw std::system_error(
        EMFILE, std::generic_category(), "open handle ID exhausted");
  }
  if (handle.writable) {
    opened.writer = true;
  } else {
    ++opened.readers;
  }
  try {
    opened.handles.push_back(&handle);
  } catch (...) {
    if (handle.writable) {
      opened.writer = false;
    } else {
      --opened.readers;
    }
    if (!opened.writer && opened.readers == 0) {
      state.open_files.erase(position);
    }
    throw;
  }
  while (!item.open_count.compare_exchange_weak(
      count, count + 1, std::memory_order_relaxed,
      std::memory_order_relaxed)) {
    if (count == UINT32_MAX) {
        std::erase(opened.handles, &handle);
      if (handle.writable) {
        opened.writer = false;
      } else {
        --opened.readers;
      }
      if (!opened.writer && opened.readers == 0) {
        state.open_files.erase(position);
      }
      throw std::system_error(
          EMFILE, std::generic_category(), "inode open count");
    }
  }
  handle.id            = ++state.next_handle_id;
  handle.registered    = true;
  handle.inode_counted = true;
}

void release_open_inode(State& state, OpenHandle& handle) noexcept {
  if (!handle.inode_counted) {
    return;
  }
  handle.inode_counted = false;
  if (handle.item != nullptr && release_inode_count(handle.item->open_count)) {
    sweep_retired_items(state);
  }
}

void unregister_open_handle(State& state, std::string_view path,
                            OpenHandle& handle,
                            bool release_inode = true) noexcept {
  {
    std::lock_guard open_files_guard(state.open_files_mutex);
    auto position = state.open_files.find(path);
    if (position == state.open_files.end() ||
        std::find(position->second.handles.begin(),
                  position->second.handles.end(), &handle) ==
            position->second.handles.end()) {
      position = std::find_if(
          state.open_files.begin(), state.open_files.end(),
          [&](const auto& opened) {
            return std::find(opened.second.handles.begin(),
                             opened.second.handles.end(), &handle) !=
                opened.second.handles.end();
          });
    }
    if (position != state.open_files.end()) {
      State::OpenFileState& opened = position->second;
      const size_t erased = std::erase(opened.handles, &handle);
      if (erased != 1) {
        fprintf(stderr,
                "warning: open-handle registry contained %zu copies of "
                "one handle\n",
                erased);
      }
      if (handle.writable) {
        opened.writer = false;
      } else if (opened.readers != 0) {
        --opened.readers;
      }
      if (!opened.writer && opened.readers == 0) {
        state.open_files.erase(position);
      }
    }
  }
  state.open_files_condition.notify_all();
  if (release_inode) {
    release_open_inode(state, handle);
  }
}

struct OpenHandleIdentityLocks {
  std::vector<OpenHandle*> handles;
  std::vector<std::unique_ptr<OpenRequestGuard>> pins;
  std::vector<std::unique_lock<ReactorSharedMutex>> identities;
};

bool reader_snapshot_matches(std::span<OpenHandle* const> current,
                               std::span<OpenHandle* const> snapshot) noexcept {
  for (OpenHandle* handle : current) {
    if (std::find(snapshot.begin(), snapshot.end(), handle) == snapshot.end() &&
        (handle->request_state.load(std::memory_order_acquire) >> 63) == 0) return false;
  }
  for (OpenHandle* handle : snapshot) {
    if (std::find(current.begin(), current.end(), handle) == current.end() &&
        (handle->request_state.load(std::memory_order_acquire) >> 63) == 0) return false;
  }
  return true;
}

OpenHandleIdentityLocks lock_open_handle_identities(
    State& state, std::string_view path, const char* operation) {
  OpenHandleIdentityLocks result;
  {
    std::lock_guard guard(state.open_files_mutex);
    const auto opened = state.open_files.find(path);
    if (opened == state.open_files.end()) {
      return result;
    }
    if (opened->second.writer ||
        opened->second.handles.size() != opened->second.readers) {
      throw std::logic_error(
          std::string(operation) + " reader registry is inconsistent");
    }
    result.handles.reserve(opened->second.handles.size());
    result.pins.reserve(opened->second.handles.size());
    for (OpenHandle* handle : opened->second.handles) {
      if (handle == nullptr || handle->writable) {
        throw std::logic_error(
            std::string(operation) + " reader registry is invalid");
      }
      try {
        result.pins.push_back(std::make_unique<OpenRequestGuard>(*handle));
        result.handles.push_back(handle);
      } catch (const std::system_error& error) {
        if (error.code().value() != EBADF) throw;
      }
    }
  }
  result.identities.reserve(result.handles.size());
  for (OpenHandle* handle : result.handles) {
    result.identities.emplace_back(handle->identity_mutex);
  }
  return result;
}

bool renamed_destination_fully_cached(State& state, std::string_view path) {
  if (!state.local_cache) {
    return false;
  }
  bool can_preserve = false;
  {
    std::lock_guard guard(state.open_files_mutex);
    const auto opened = state.open_files.find(path);
    if (opened == state.open_files.end() || opened->second.readers == 0) {
      return false;
    }
    can_preserve = std::all_of(
        opened->second.handles.begin(), opened->second.handles.end(),
        [](const OpenHandle* handle) {
          return handle != nullptr && !handle->writable &&
              handle->cache_entry && handle->cache_entry->fully_clean();
        });
  }
  return can_preserve;
}

// Snapshot under open_files_mutex, without taking a cache entry's mutex.
std::vector<std::shared_ptr<CacheEntry>> cached_reader_entries(
    std::span<OpenHandle* const> handles) {
  std::vector<std::shared_ptr<CacheEntry>> entries;
  entries.reserve(handles.size());
  for (const OpenHandle* handle : handles) {
    if (handle == nullptr || handle->writable || !handle->cache_entry) return {};
    entries.push_back(handle->cache_entry);
  }
  return entries;
}

// Consume the snapshot in the storage worker, including the last reference's
// possible local fd/metadata-mapping cleanup.
bool cached_readers_fully_clean(std::vector<std::shared_ptr<CacheEntry>> entries) {
  return !entries.empty() && std::all_of(entries.begin(), entries.end(),
      [](const auto& entry) { return entry->fully_clean(); });
}

bool preserve_renamed_destination(State& state, std::string_view path,
                                  std::string_view key) {
  return renamed_destination_fully_cached(state, path) && state.local_cache->remove(key, true);
}

void finish_open_file_rename(State& state,
                             std::string_view source_path,
                             std::string_view destination_path,
                             std::string_view destination_key,
                             bool destination_preserved,
                             const std::vector<OpenHandle*>& source_handles) {
  std::lock_guard guard(state.open_files_mutex);
  auto destination = state.open_files.find(destination_path);
  if (destination != state.open_files.end() && !destination_preserved) {
    for (OpenHandle* handle : destination->second.handles) {
      if (handle != nullptr) {
        handle->stale.store(true, std::memory_order_release);
      }
    }
  }

  auto source = state.open_files.find(source_path);
  if (source == state.open_files.end()) {
    if (!reader_snapshot_matches({}, source_handles)) {
      throw std::logic_error("rename source reader registry disappeared");
    }
    return;
  }
  if (!reader_snapshot_matches(source->second.handles, source_handles)) {
    throw std::logic_error("rename source reader registry changed");
  }
  for (OpenHandle* handle : source->second.handles) {
    if (handle == nullptr ||
        std::find(source_handles.begin(), source_handles.end(), handle) == source_handles.end()) {
      continue;
    }
    handle->object_path.assign(destination_path);
    handle->key.assign(destination_key);
  }
  if (destination == state.open_files.end()) {
    auto node = state.open_files.extract(source);
    node.key().assign(destination_path);
    state.open_files.insert(std::move(node));
    return;
  }
  destination->second.handles.reserve(
      destination->second.handles.size() + source->second.handles.size());
  destination->second.handles.insert(
      destination->second.handles.end(), source->second.handles.begin(),
      source->second.handles.end());
  destination->second.readers += source->second.readers;
  destination->second.writer = destination->second.writer ||
      source->second.writer;
  state.open_files.erase(source);
}

class ResponseCreditGuard {
 public:
  ResponseCreditGuard(HttpClient& client, const Response& response)
      : client_(client),
        response_(response),
        pending_(response.requires_consume) {}

  ResponseCreditGuard(const ResponseCreditGuard&) = delete;
  ResponseCreditGuard& operator=(const ResponseCreditGuard&) = delete;

  ~ResponseCreditGuard() {
    if (pending_) {
      try {
        client_.consume(response_);
      } catch (const std::exception& error) {
        fprintf(stderr, "failed to discard transport receive credit: %s\n",
                error.what());
      }
    }
  }

  void consume() {
    if (pending_) {
      pending_ = false;
      client_.consume(response_);
    }
  }

 private:
  HttpClient& client_;
  const Response& response_;
  bool pending_;
};

struct AuthorizedRangeRequest {
  const std::string* base_path = nullptr;
  std::string versioned_path;
  std::span<const Header> headers;
  bool range_signed = false;
  bool verify_checksum = false;

  std::string_view path() const {
    return versioned_path.empty() ? std::string_view(*base_path)
                                  : std::string_view(versioned_path);
  }
};

AuthorizedRangeRequest make_range_request(State& state,
                                           const OpenHandle& handle,
                                           WorkerState& worker,
                                           uint64_t offset,
                                           size_t length, bool authorize = true) {
  HeaderList& headers = worker.request_headers;
  AuthorizedRangeRequest request{
      .base_path = &handle.object_path,
      .versioned_path = {},
      .headers = {},
      .range_signed = false,
      .verify_checksum = false,
  };
  if (!handle.version_id.empty()) {
    request.versioned_path = handle.object_path;
    request.versioned_path +=
        request.versioned_path.find('?') == std::string::npos ? '?' : '&';
    request.versioned_path +=
        "versionId=" + uri_encode(handle.version_id, false);
  }
  const uint64_t inclusive_end =
      offset + static_cast<uint64_t>(length) - 1;
  std::array<char, 6 + 20 + 1 + 20> range;
  char* p = std::copy_n("bytes=", 6, range.data());
  const auto offset_result =
      std::to_chars(p, range.data() + range.size(), offset);
  if (offset_result.ec != std::errc{}) {
    throw std::runtime_error("unable to format S3 range offset");
  }
  p = offset_result.ptr;
  *p++ = '-';
  const auto end_result =
      std::to_chars(p, range.data() + range.size(), inclusive_end);
  if (end_result.ec != std::errc{}) {
    throw std::runtime_error("unable to format S3 range end");
  }
  p = end_result.ptr;
  const bool use_if_match =
      handle.version_id.empty() && !handle.etag.empty();
  const bool verify_checksum = state.config.verify_read_checksum &&
      offset == 0 && uint64_t(length) == handle.size;
  const bool request_checksum = verify_checksum &&
      state.config.checksum_service != CHECKSUM_SERVICE_OSS &&
      state.config.checksum_service != CHECKSUM_SERVICE_GCS;
  request.verify_checksum = verify_checksum;
  const int signing_mode =
      state.range_signing_mode.load(std::memory_order_relaxed);
  const bool sign_range = state.config.directory_bucket || signing_mode == 2;
  request.range_signed = sign_range;
  const std::array<uint64_t, 2> date = state.date_time.now();
  const bool cacheable = authorize && !state.config.directory_bucket && !sign_range &&
      !request_checksum;
  const bool cache_hit =
      cacheable && worker.authorization_valid &&
      worker.authorization_handle_id == handle.id &&
      worker.authorization_date == date &&
      worker.authorization_count < headers.size() &&
      headers.data()[worker.authorization_count].name == "range";
  const size_t range_length = size_t(p - range.data());
  if (cache_hit) {
    headers.data()[worker.authorization_count].value.assign(
        range.data(), range_length);
  } else {
    std::array<char, 16> date_text;
    memcpy(date_text.data(), &date[0], sizeof(date[0]));
    memcpy(date_text.data() + sizeof(date[0]),
           &date[1], sizeof(date[1]));
    const Header if_match{"if-match", handle.etag};
    std::array<Header, 3> canonical_headers;
    size_t canonical_count = 0;
    if (use_if_match) {
      canonical_headers[canonical_count++] = if_match;
    }
    if (sign_range) {
      canonical_headers[canonical_count++] =
          Header{"range", ssostr<32>(range.data(), range_length)};
    }
    if (request_checksum) {
      canonical_headers[canonical_count++] =
          Header{"x-amz-checksum-mode", "ENABLED"};
    }
    if (authorize) {
      authorization_headers(
          headers, state, "GET", request.path(),
          std::span(canonical_headers.data(), canonical_count),
          kEmptyPayloadSha256, true,
          std::string_view(date_text.data(), date_text.size()));
    } else {
      headers.clear();
    }
    worker.authorization_count = headers.size();
    headers.push_back(
        Header{"range", ssostr<32>(range.data(), range_length)});
    if (request_checksum) {
      headers.push_back(Header{"x-amz-checksum-mode", "ENABLED"});
    }
    if (use_if_match) {
      headers.push_back(if_match);
    }
    worker.authorization_valid = cacheable;
    if (cacheable) {
      worker.authorization_handle_id = handle.id;
      worker.authorization_date      = date;
    }
  }
  request.headers = std::span(headers.data(), headers.size());
  return request;
}

constexpr unsigned kMaximumMultipartParts = 10'000;
constexpr uint64_t kMaximumObjectSize =
    5ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL;
constexpr std::string_view kInternalDeleteDirectory =
    ".~ngs3fs~.pending-delete";

std::string query_path(std::string_view path, std::string_view query) {
  std::string result(path);
  result.push_back(path.find('?') == std::string_view::npos ? '?' : '&');
  result.append(query);
  return result;
}

unsigned hex_value(char ch) {
  if (ch >= '0' && ch <= '9') {
    return unsigned(ch - '0');
  }
  ch = char(ch | 0x20);
  return unsigned(ch - 'a') + 10;
}

bool valid_fuse_component(std::string_view name) {
  return !name.empty() && name.size() <= NAME_MAX &&
         name != "." && name != ".." &&
         name != kInternalDeleteDirectory &&
         name.find('\0') == std::string_view::npos &&
         name.find('/') == std::string_view::npos;
}

std::string percent_decode(std::string_view text) {
  std::string result;
  result.reserve(text.size());
  for (size_t p = 0; p < text.size(); ++p) {
    if (text[p] != '%') {
      result.push_back(text[p]);
      continue;
    }
    if (p + 2 >= text.size() || !isxdigit(u_char(text[p + 1])) ||
        !isxdigit(u_char(text[p + 2]))) {
      throw std::runtime_error("invalid URL encoding in ListObjectsV2");
    }
    result.push_back(char((hex_value(text[p + 1]) << 4) |
                          hex_value(text[p + 2])));
    p += 2;
  }
  return result;
}

struct ListedChild {
  ssostr<64> name;
  time_t mtime   = 0;
  uint64_t size  = 0;
  bool directory = false;
};

struct ListedObject {
  std::string key;
  std::string etag;
  time_t mtime = 0;
  uint64_t size = 0;
};

struct ListedPage {
  std::vector<ListedChild> children;
  std::string token;
  bool truncated = false;
};

struct ListedXmlPage {
  std::vector<ListedObject> objects;
  std::vector<std::string> prefixes;
  std::string token;
  bool truncated = false;
};

std::string_view trim_xml_space(std::string_view value) noexcept {
  while (!value.empty() && isspace(u_char(value.front()))) {
    value.remove_prefix(1);
  }
  while (!value.empty() && isspace(u_char(value.back()))) {
    value.remove_suffix(1);
  }
  return value;
}

ListedXmlPage parse_list_xml(const Response& response,
                             const char* operation) {
  S3Xml xml(response_xml(response), operation);
  const tinyxml2::XMLElement& root =
      xml.result_root("ListBucketResult");
  ListedXmlPage page;
  page.objects.reserve(kDirectoryListLimit);
  page.prefixes.reserve(kDirectoryListLimit);
  for (const tinyxml2::XMLElement* child = root.FirstChildElement();
       child != nullptr; child = child->NextSiblingElement()) {
    if (S3Xml::named(*child, "Contents")) {
      ListedObject object;
      object.key = percent_decode(xml.required_text(*child, "Key"));
      const std::string size = xml.required_text(*child, "Size");
      if (!parse_unsigned(trim_xml_space(size), object.size)) {
        throw std::runtime_error(
            std::string(operation) + " returned invalid Size");
      }
      object.etag = xml.optional_text(*child, "ETag");
      const std::string modified =
          xml.required_text(*child, "LastModified");
      object.mtime = parse_s3_mtime(trim_xml_space(modified));
      page.objects.push_back(std::move(object));
    } else if (S3Xml::named(*child, "CommonPrefixes")) {
      page.prefixes.push_back(
          percent_decode(xml.required_text(*child, "Prefix")));
    }
  }
  page.truncated = xml.required_bool(root, "IsTruncated");
  page.token = xml.optional_text(root, "NextContinuationToken");
  if (page.truncated == page.token.empty()) {
    throw std::runtime_error(
        std::string(operation) +
        " returned inconsistent IsTruncated and NextContinuationToken");
  }
  return page;
}

std::string directory_request_path(State& state, std::string_view prefix,
                                    std::string_view token) {
  std::string query = "delimiter=%2F&encoding-type=url&list-type=2&prefix=";
  query += uri_encode(prefix, false);
  if (!token.empty()) {
    query += "&continuation-token=" + uri_encode(token, false);
  }
  query += "&max-keys=" + std::to_string(kDirectoryListLimit);
  return query_path(
      state.config.bucket_path.empty() ? "/" : state.config.bucket_path,
      query);
}

ListedPage decode_directory_response(const Response& response, std::string_view prefix) {
  if (response.status != 200) {
    throw_s3_response(response, "ListObjectsV2");
  }
  ListedXmlPage listed = parse_list_xml(response, "ListObjectsV2");
  ListedPage page;
  page.children.reserve(kDirectoryListLimit);
  for (const ListedObject& object : listed.objects) {
    const std::string& key = object.key;
    if (!key.starts_with(prefix) || key.size() == prefix.size()) {
      continue;
    }
    std::string_view relative(key.data() + prefix.size(),
                              key.size() - prefix.size());
    const bool marker = relative.ends_with('/');
    if (marker) {
      relative.remove_suffix(1);
    }
    if (!valid_fuse_component(relative)) {
      continue;
    }
    ListedChild child;
    child.name.assign(relative);
    child.size      = object.size;
    child.directory = marker;
    child.mtime     = object.mtime;
    page.children.push_back(std::move(child));
  }
  for (std::string& key : listed.prefixes) {
    if (!key.starts_with(prefix) ||
        key.size() <= prefix.size() || key.back() != '/') {
      continue;
    }
    key.pop_back();
    const std::string_view relative(
        key.data() + prefix.size(), key.size() - prefix.size());
    if (!valid_fuse_component(relative)) {
      continue;
    }
    ListedChild child;
    child.name.assign(relative);
    child.directory = true;
    page.children.push_back(std::move(child));
  }
  page.truncated = listed.truncated;
  page.token     = std::move(listed.token);
  return page;
}

ListedPage list_directory_page(State& state, std::string_view prefix,
                               std::string_view token) {
  const std::string path = directory_request_path(state, prefix, token);
  const Response response = request_with_retries([&] {
    const HeaderList headers = authorization_headers(
        state, "GET", path, {}, kEmptyPayloadSha256);
    HttpPool::Lease client = state.http->acquire();
    return client->request_no_body("GET", path, headers, kMaximumListResponseSize);
  }, "ListObjectsV2");
  return decode_directory_response(response, prefix);
}

template<class Function>
void for_each_directory_page(State& state, std::string_view prefix,
                             Function function) {
  std::string token;
  for (;;) {
    ListedPage page = list_directory_page(state, prefix, token);
    function(std::move(page.children));
    if (!page.truncated) {
      return;
    }
    token = std::move(page.token);
  }
}

InodeBase* allocate_inode(InodeBase* parent, bool directory) {
  return directory ? static_cast<InodeBase*>(new InodeDir(parent))
                   : static_cast<InodeBase*>(new InodeFile(parent));
}

void retain_inode_count(std::atomic<uint32_t>& count,
                        const char* operation) {
  uint32_t value = count.load(std::memory_order_relaxed);
  do {
    if (value == UINT32_MAX) {
      throw std::system_error(
          EOVERFLOW, std::generic_category(), operation);
    }
  } while (!count.compare_exchange_weak(
      value, value + 1, std::memory_order_acq_rel,
      std::memory_order_relaxed));
}

bool release_inode_count(std::atomic<uint32_t>& count) noexcept {
  uint32_t value = count.load(std::memory_order_relaxed);
  while (value != 0) {
    if (count.compare_exchange_weak(
            value, value - 1, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
      return value == 1;
    }
  }
  return false;
}

class InodePin {
 public:
  InodePin() noexcept = default;

  InodePin(State& state, InodeBase& item) noexcept
      : state_(&state), item_(&item) {}

  InodePin(const InodePin&) = delete;
  InodePin& operator=(const InodePin&) = delete;

  InodePin(InodePin&& other) noexcept
      : state_(other.state_), item_(other.item_) {
    other.item_ = nullptr;
  }

  InodePin& operator=(InodePin&& other) noexcept {
    if (this != &other) {
      reset();
      state_      = other.state_;
      item_       = other.item_;
      other.item_ = nullptr;
    }
    return *this;
  }

  ~InodePin() {
    reset();
  }

  [[nodiscard]] InodeBase* get() const noexcept {
    return item_;
  }

  [[nodiscard]] InodeBase& operator*() const noexcept {
    return *item_;
  }

  [[nodiscard]] InodeBase* operator->() const noexcept {
    return item_;
  }

  explicit operator bool() const noexcept {
    return item_ != nullptr;
  }

  void reset() noexcept {
    if (item_ == nullptr) {
      return;
    }
    InodeBase* item = item_;
    item_           = nullptr;
    if (release_inode_count(item->open_count)) {
      sweep_retired_items(*state_);
    }
  }

 private:
  State* state_    = nullptr;
  InodeBase* item_ = nullptr;
};

InodePin pin_cached_child(State& state, InodeBase& parent,
                          std::string_view name) {
  if (!parent.directory()) {
    return {};
  }
  Directory& children = parent.dir_children();
  std::shared_lock guard(children.mutex);
  const auto i = children.find(
      terark::fstring(name.data(), ptrdiff_t(name.size())));
  if (i == children.end() || i->second->detached()) {
    return {};
  }
  retain_inode_count(i->second->open_count, "inode operation count");
  return InodePin(state, *i->second);
}

bool item_tree_unreferenced(const InodeBase& item) noexcept {
  if (item.nlookup.load(std::memory_order_acquire) != 0 ||
      item.open_count.load(std::memory_order_acquire) != 0) {
    return false;
  }
  if (item.directory()) {
    const Directory& children = item.dir_children();
    std::shared_lock guard(children.mutex);
    for (auto [name, child] : children) {
      (void)name;
      if (!item_tree_unreferenced(*child)) {
        return false;
      }
    }
  }
  return true;
}

bool item_tree_reclaimable(const InodeBase& item) noexcept {
  return item.detached() && item_tree_unreferenced(item);
}

size_t item_tree_size(const InodeBase& item) noexcept {
  size_t count = 1;
  if (item.directory()) {
    const Directory& children = item.dir_children();
    std::shared_lock guard(children.mutex);
    for (auto [name, child] : children) {
      (void)name;
      count += item_tree_size(*child);
    }
  }
  return count;
}

void cache_touch(InodeDir& item) noexcept {
  item.children.clock_referenced.store(true, std::memory_order_relaxed);
}

void cache_register_directory(State& state, InodeDir& item) noexcept {
  if (item.detached()) {
    return;
  }
  Directory& directory = item.children;
  cache_touch(item);
  {
    std::lock_guard guard(state.cache_mutex);
    if (directory.clock_linked || item.detached()) {
      return;
    }
    if (state.cache_clock_hand == nullptr) {
      directory.clock_prev    = &item;
      directory.clock_next    = &item;
      state.cache_clock_hand  = &item;
    } else {
      InodeDir* hand            = state.cache_clock_hand;
      InodeDir* tail            = hand->children.clock_prev;
      directory.clock_prev      = tail;
      directory.clock_next      = hand;
      tail->children.clock_next = &item;
      hand->children.clock_prev = &item;
    }
    directory.clock_linked = true;
    ++state.cache_clock_size;
  }
  state.cache_condition.notify_all();
}

void cache_unregister_directory(State& state, InodeDir& item) noexcept {
  Directory& directory = item.children;
  std::lock_guard guard(state.cache_mutex);
  if (!directory.clock_linked) {
    return;
  }
  if (directory.clock_next == &item) {
    state.cache_clock_hand = nullptr;
  } else {
    directory.clock_prev->children.clock_next = directory.clock_next;
    directory.clock_next->children.clock_prev = directory.clock_prev;
    if (state.cache_clock_hand == &item) {
      state.cache_clock_hand = directory.clock_next;
    }
  }
  directory.clock_prev   = nullptr;
  directory.clock_next   = nullptr;
  directory.clock_linked = false;
  --state.cache_clock_size;
}

void cache_inode_allocated(State& state) noexcept {
  state.cached_inodes.fetch_add(1, std::memory_order_release);
  state.cache_condition.notify_all();
}

void cache_inode_tree_deleted(State& state, const InodeBase& item) noexcept {
  const size_t count = item_tree_size(item);
  size_t old = state.cached_inodes.load(std::memory_order_relaxed);
  while (old >= count && !state.cached_inodes.compare_exchange_weak(
      old, old - count, std::memory_order_acq_rel,
      std::memory_order_relaxed)) {
  }
  if (old < count) {
    fprintf(stderr,
            "error: inode cache accounting underflow: used=%zu "
            "deleting=%zu\n",
            old, count);
  }
  state.cache_condition.notify_all();
}

void mark_item_tree_detached(State& state, InodeBase& item) {
  item.set_detached(true);
  if (!item.directory()) {
    return;
  }
  cache_unregister_directory(state, static_cast<InodeDir&>(item));
  std::vector<InodePin> children;
  {
    std::unique_lock guard(item.dir_children().mutex);
    children.reserve(item.dir_children().size());
    for (auto [name, child] : item.dir_children()) {
      (void)name;
      retain_inode_count(child->open_count, "inode detach count");
      children.emplace_back(state, *child);
    }
  }
  for (InodePin& child : children) {
    mark_item_tree_detached(state, *child);
  }
}

void sweep_retired_items(State& state) noexcept {
  if (state.retired_count.load(std::memory_order_acquire) == 0) {
    return;
  }
  std::lock_guard guard(state.retired_mutex);
  bool progress;
  do {
    progress = false;
    for (auto i = state.retired_items.begin();
         i != state.retired_items.end();) {
      InodeBase* item   = i->second.item;
      InodeBase* parent = i->second.parent;
      if (!item_tree_reclaimable(*item)) {
        ++i;
        continue;
      }
      detach_parent_slot_if_owned(*item, parent);
      i = state.retired_items.erase(i);
      state.retired_count.fetch_sub(1, std::memory_order_relaxed);
      cache_inode_tree_deleted(state, *item);
      delete_inode(item);
      if (parent != nullptr) {
        release_inode_count(parent->open_count);
      }
      progress = true;
    }
  } while (progress);
}

void retire_item(State& state, InodeBase& item) {
  InodeBase* parent = item.parent();
  mark_item_tree_detached(state, item);
  if (parent != nullptr && parent != &item) {
    retain_inode_count(parent->open_count, "retired inode parent count");
  } else {
    parent = nullptr;
  }
  std::lock_guard guard(state.retired_mutex);
  const auto retired = state.retired_items.emplace(
      item_inode(&item), State::RetiredItem{&item, parent});
  if (!retired.second) {
    if (parent != nullptr) {
      release_inode_count(parent->open_count);
    }
    throw std::logic_error("inode is already retired");
  }
  state.retired_count.fetch_add(1, std::memory_order_release);
}

bool detach_cached_item(State& state, InodeBase& item) {
  InodeBase& parent    = *item.parent();
  Directory& children = parent.dir_children();
  {
    std::unique_lock guard(children.mutex);
    if (item.detached()) {
      return false;
    }
    if (item.dentry_slot == UINT32_MAX ||
        item.dentry_slot >= children.end_i() ||
        children.is_deleted(item.dentry_slot) ||
        children.val(item.dentry_slot) != &item) {
      throw std::logic_error("detached inode dentry changed");
    }
    children.erase_i(item.dentry_slot);
    item.set_detached(true);
    ++children.mutation_epoch;
  }
  item.set_pending(false);
  item.set_truncate_pending(false);
  retire_item(state, item);
  return true;
}

struct DirectoryListingChanged {};
struct DirectoryListingDeferred {};

// Only the remote-mutation/publication interval is fenced, not the preliminary
// refreshes made while holding mutation_mutex. Concurrent readers keep the
// pre-mutation cache; it remains expired for the next lookup after completion.
struct DirectoryPublicationGuard {
  InodeBase* items[4]{};
  unsigned count = 0;

  DirectoryPublicationGuard() = default;
  DirectoryPublicationGuard(const DirectoryPublicationGuard&) = delete;
  DirectoryPublicationGuard& operator=(const DirectoryPublicationGuard&) = delete;

  static void update(InodeBase& item, bool entering) {
    Directory& children = item.dir_children();
    std::unique_lock guard(children.mutex);
    if (entering) ++children.remote_mutations;
    else --children.remote_mutations;
    ++children.mutation_epoch;
    item.expire.store(0, std::memory_order_release);
  }

  void start(InodeBase& a, InodeBase& b,
             InodeBase* source = nullptr, InodeBase* destination = nullptr) {
    if (count) return;
    for (InodeBase* item : {&a, &b, source, destination}) {
      if (!item || !item->directory()) continue;
      bool duplicate = false;
      for (unsigned i = 0; i != count; ++i) duplicate |= items[i] == item;
      if (duplicate) continue;
      update(*item, true);
      items[count++] = item;
    }
  }

  ~DirectoryPublicationGuard() {
    while (count) update(*items[--count], false);
  }
};

void update_truncate_pending(InodeBase& item, bool pending) {
  Directory& children = item.parent()->dir_children();
  std::unique_lock guard(children.mutex);
  if (item.truncate_pending() != pending) {
    item.set_truncate_pending(pending);
    ++children.mutation_epoch;
  }
}

struct InodeWriteState {
  uint64_t size;
  bool truncate_pending;
};

InodeWriteState begin_inode_write(InodeFile& item) {
  Directory& children = item.parent()->dir_children();
  std::unique_lock guard(children.mutex);
  const InodeWriteState previous{item.fsize.load(std::memory_order_relaxed),
                                item.truncate_pending()};
  // LOOKUP must not restore the remote object's old length during O_TRUNC
  // and sequential writes. Keep LIST from overwriting the local size until
  // the first flush publishes the committed metadata.
  item.set_truncate_pending(true);
  item.set_fsize(0);
  ++children.mutation_epoch;
  return previous;
}

void cancel_inode_write(InodeFile& item, InodeWriteState previous) {
  Directory& children = item.parent()->dir_children();
  std::unique_lock guard(children.mutex);
  item.set_fsize(previous.size);
  item.set_truncate_pending(previous.truncate_pending);
  ++children.mutation_epoch;
}

fuse_ino_t install_item(State& state, fuse_ino_t parent,
                        ListedChild child,
                        uint32_t listing_generation = 0,
                        bool exclusive = false,
                        bool pending = false,
                        bool lookup = false,
                        bool defer_file_over_directory = false,
                        const uint64_t* mutation_epoch = nullptr) {
  InodeBase& parent_item = inode_item(state, parent);
  if (!parent_item.directory()) {
    throw std::system_error(ENOTDIR, std::generic_category(), "directory");
  }
  Directory& children = parent_item.dir_children();
  InodeBase* stale     = nullptr;
  InodeBase* item      = nullptr;
  bool allocated       = false;
  fuse_ino_t invalidate_inode = 0;
  {
    std::unique_lock guard(children.mutex);
    if (mutation_epoch && children.remote_mutations) throw DirectoryListingDeferred{};
    if (mutation_epoch && children.mutation_epoch != *mutation_epoch) {
      throw DirectoryListingChanged{};
    }
    if (parent_item.detached()) {
      throw std::system_error(ESTALE, std::generic_category(), "directory");
    }
    const terark::fstring key(
        child.name.data(), ptrdiff_t(child.name.size()));
    auto position = children.find(key);
    item = position == children.end() ? nullptr : position->second;
    if (item != nullptr && exclusive) {
      throw std::system_error(EEXIST, std::generic_category(), "create");
    }
    const bool local = item != nullptr &&
                       (item->pending() || item->truncate_pending());
    if (item != nullptr && item->directory() != child.directory && !local) {
      if (item->directory() && !child.directory &&
          defer_file_over_directory) {
        return 0;
      }
      InodeBase* replacement = allocate_inode(&parent_item, child.directory);
      stale                  = item;
      item                   = replacement;
      position->second       = item;
      item->dentry_slot      = uint32_t(position.get_index());
      stale->set_detached(true);
      allocated              = true;
    }
    if (item == nullptr) {
      InodeBase* created = allocate_inode(&parent_item, child.directory);
      try {
        const auto inserted = children.insert_i(key, created);
        if (!inserted.second) {
          throw std::logic_error("directory child insertion raced");
        }
        item              = created;
        item->dentry_slot = uint32_t(inserted.first);
        allocated         = true;
      } catch (...) {
        delete_inode(created);
        throw;
      }
    }
    item->set_parent(&parent_item);
    item->set_detached(false);
    if (pending) {
      item->set_pending(true);
    }
    if (lookup) {
      retain_inode_count(item->nlookup, "inode lookup count");
    }
    if (listing_generation != 0) {
      item->listing_generation.store(
          listing_generation, std::memory_order_relaxed);
    } else {
      ++children.mutation_epoch;
    }
    if (!child.directory && item->regular() &&
        !item->pending() && !item->truncate_pending()) {
      InodeFile& file = static_cast<InodeFile&>(*item);
      const bool changed = !allocated &&
          (file.mtime.load(std::memory_order_relaxed) != child.mtime ||
           file.fsize.load(std::memory_order_relaxed) != child.size);
      if (changed) {
        file.set_page_cache_valid(false);
        invalidate_inode = item_inode(item);
      }
      file.mtime.store(child.mtime, std::memory_order_relaxed);
      file.fsize.store(child.size, std::memory_order_relaxed);
    }
  }
  if (invalidate_inode != 0) {
    invalidate_page_cache(state, invalidate_inode);
  }
  if (allocated) {
    cache_inode_allocated(state);
  }
  if (stale != nullptr) {
    retire_item(state, *stale);
  }
  return item_inode(item);
}

void invalidate_directory(State& state, fuse_ino_t inode) noexcept;

struct DirectoryGuards {
  std::vector<Directory*> directories;
  std::vector<std::unique_lock<IoMutex>> guards;
};

DirectoryGuards lock_directories(State& state,
                                 std::vector<fuse_ino_t> sorted) {
  DirectoryGuards result;
  result.directories.reserve(sorted.size());
  for (fuse_ino_t inode : sorted) {
    if (inode == 0) {
      continue;
    }
    InodeBase& item = inode_item(state, inode);
    if (item.directory()) {
      result.directories.push_back(&item.dir_children());
    }
  }
  std::sort(result.directories.begin(), result.directories.end(),
            std::less<Directory*>());
  result.directories.erase(
      std::unique(result.directories.begin(), result.directories.end()),
      result.directories.end());
  result.guards.reserve(result.directories.size());
  for (Directory* directory : result.directories) {
    result.guards.emplace_back(directory->mutation_mutex);
  }
  return result;
}

DirectoryGuards lock_directories(State& state,
                                 std::initializer_list<fuse_ino_t> inodes) {
  return lock_directories(state, std::vector<fuse_ino_t>(inodes));
}

void prune_directory_generation(State& state, InodeBase& directory,
                                uint32_t generation,
                                const uint64_t* mutation_epoch = nullptr) {
  std::vector<InodeBase*> stale;
  {
    Directory& children = directory.dir_children();
    std::unique_lock guard(children.mutex);
    if (mutation_epoch && children.remote_mutations) throw DirectoryListingDeferred{};
    if (mutation_epoch && children.mutation_epoch != *mutation_epoch) {
      throw DirectoryListingChanged{};
    }
    stale.reserve(children.size());
    for (size_t slot = 0; slot < children.end_i(); ++slot) {
      if (children.is_deleted(slot)) {
        continue;
      }
      InodeBase* child = children.val(slot);
      if (child->pending() || child->truncate_pending()) {
        child->listing_generation.store(
            generation, std::memory_order_relaxed);
        continue;
      }
      if (child->listing_generation.load(std::memory_order_relaxed) ==
          generation) {
        continue;
      }
      children.erase_i(slot);
      child->set_detached(true);
      stale.push_back(child);
    }
  }
  for (InodeBase* child : stale) {
    retire_item(state, *child);
  }
}

struct DirectoryContinuation {
  using Complete = void (*)(void*, std::exception_ptr) noexcept;
  DirectoryContinuation* next = nullptr;
  FuseReactor* reactor = nullptr;
  Complete complete = nullptr;
  void* context = nullptr;
  std::exception_ptr error;
  FuseReactor::ReactorTask task;

  static void ready(void* context) noexcept {
    auto* waiter = static_cast<DirectoryContinuation*>(context);
    const Complete complete = waiter->complete;
    void* target = waiter->context;
    auto error = std::move(waiter->error);
    complete(target, std::move(error));
  }

  static void cancelled(void* context) noexcept {
    auto* waiter = static_cast<DirectoryContinuation*>(context);
    try {
      throw std::system_error(ECANCELED, std::generic_category(), "directory refresh cancelled");
    } catch (...) {
      waiter->error = std::current_exception();
    }
    ready(context);
  }
};

void finish_directory_refresh(Directory& children, std::exception_ptr error) noexcept {
  DirectoryContinuation* waiters;
  {
    std::unique_lock guard(children.mutex);
    waiters = std::exchange(children.refresh_waiters, nullptr);
    children.refreshing.store(false, std::memory_order_release);
  }
  children.refreshing.notify_all();
  while (waiters != nullptr) {
    DirectoryContinuation* next = waiters->next;
    waiters->next = nullptr;
    waiters->error = error;
    waiters->reactor->complete(&waiters->task);
    waiters = next;
  }
}

struct AsyncDirectoryRefresh {
  State& state;
  FuseReactor& reactor;
  InodePin pin;
  fuse_ino_t inode;
  std::string key;
  std::string token;
  uint32_t generation = 0;
  uint64_t mutation_epoch = 0;
  std::vector<ListedChild> conflicts;
  std::unique_ptr<AsyncSignedS3Request> request;

  AsyncDirectoryRefresh(State& s, FuseReactor& r, InodeBase& item, fuse_ino_t ino)
      : state(s), reactor(r), inode(ino), key(item_key(s, item)) {
    retain_inode_count(item.open_count, "directory refresh pin");
    pin = InodePin(state, item);
  }

  void finish(std::exception_ptr error = {}) noexcept {
    std::unique_ptr<AsyncDirectoryRefresh> owner(this);
    request.reset();
    if (error) invalidate_directory(state, inode);
    finish_directory_refresh(pin->dir_children(), std::move(error));
  }

  void next_page() noexcept {
    try {
      AsyncHttpRequest args;
      args.path = directory_request_path(state, key, token);
      args.max_response_body = kMaximumListResponseSize;
      request = std::make_unique<AsyncSignedS3Request>(
          state, reactor, std::move(args), received, this);
      if (!request->start()) {
        throw std::system_error(errno, std::generic_category(), "submit directory listing");
      }
    } catch (...) {
      finish(std::current_exception());
    }
  }

  static void received(void* context, Response&& response,
                        std::exception_ptr error) noexcept {
    auto* listing = static_cast<AsyncDirectoryRefresh*>(context);
    try {
      listing->request.reset();
      if (error) std::rethrow_exception(error);
      if (listing->pin->detached() || item_key(listing->state, *listing->pin) != listing->key) {
        throw std::system_error(ESTALE, std::generic_category(), "directory changed during listing");
      }
      ListedPage page = decode_directory_response(response, listing->key);
      Directory& children = listing->pin->dir_children();
      {
        std::unique_lock guard(children.mutex);
        children.reserve(children.end_i() + page.children.size());
      }
      for (ListedChild& child : page.children) {
        if (install_item(listing->state, listing->inode, child, listing->generation,
                          false, false, false, true, &listing->mutation_epoch) == 0) {
          listing->conflicts.push_back(std::move(child));
        }
      }
      if (page.truncated) {
        if (page.token == listing->token) {
          throw std::runtime_error("ListObjectsV2 repeated a continuation token");
        }
        listing->token = std::move(page.token);
        listing->next_page();
        return;
      }
      for (ListedChild& child : listing->conflicts) {
        bool directory_won = false;
        {
          std::shared_lock guard(children.mutex);
          const auto i = children.find(terark::fstring(child.name.data(), ptrdiff_t(child.name.size())));
          if (i != children.end()) {
            directory_won = i->second->directory() &&
                i->second->listing_generation.load(std::memory_order_relaxed) == listing->generation;
          }
        }
        if (!directory_won) {
          install_item(listing->state, listing->inode, std::move(child), listing->generation,
                       false, false, false, false, &listing->mutation_epoch);
        }
      }
      prune_directory_generation(listing->state, *listing->pin, listing->generation,
                                 &listing->mutation_epoch);
      const uint64_t now = fuse_monotonic_ns();
      const uint64_t ttl = listing->state.config.directory_cache_ns;
      {
        std::unique_lock guard(children.mutex);
        if (children.remote_mutations) throw DirectoryListingDeferred{};
        if (children.mutation_epoch != listing->mutation_epoch) throw DirectoryListingChanged{};
        listing->pin->expire.store(ttl > UINT64_MAX - now ? UINT64_MAX : now + ttl,
                                   std::memory_order_release);
      }
      sweep_retired_items(listing->state);
      listing->finish();
    } catch (const DirectoryListingDeferred&) {
      listing->finish();
    } catch (const DirectoryListingChanged&) {
      // An old LIST must neither erase a completed local create nor resurrect
      // a local unlink. Restart without marking the stale snapshot complete.
      Directory& children = listing->pin->dir_children();
      {
        std::unique_lock guard(children.mutex);
        listing->mutation_epoch = children.mutation_epoch;
        listing->generation = ++children.listing_generation;
        if (listing->generation == 0) {
          listing->generation = ++children.listing_generation;
          for (auto [name, child] : children) {
            (void)name;
            child->listing_generation.store(0, std::memory_order_relaxed);
          }
        }
      }
      listing->token.clear();
      listing->conflicts.clear();
      listing->next_page();
    } catch (...) {
      listing->finish(std::current_exception());
    }
  }
};

// The caller keeps its continuation and inode pin alive until completion.
// false means the cache was fresh and no callback was registered.
bool await_directory_refresh(State& state, FuseReactor& reactor, fuse_ino_t inode,
                              DirectoryContinuation& waiter, bool force = false) {
  InodeBase& item = inode_item(state, inode);
  Directory& children = item.dir_children();
  auto listing = std::make_unique<AsyncDirectoryRefresh>(state, reactor, item, inode);
  bool start = false;
  {
    std::unique_lock guard(children.mutex);
    if (children.remote_mutations) return false;
    if (!force && !children.refreshing.load(std::memory_order_acquire) &&
        item.expire.load(std::memory_order_acquire) > fuse_monotonic_ns()) {
      return false;
    }
    waiter.task = {DirectoryContinuation::ready, DirectoryContinuation::cancelled, &waiter};
    if (!reactor.reserve_completion(&waiter.task)) {
      throw std::system_error(errno, std::generic_category(), "reserve directory continuation");
    }
    if (!children.refreshing.load(std::memory_order_relaxed)) {
      start = true;
      children.refreshing.store(true, std::memory_order_release);
      listing->mutation_epoch = children.mutation_epoch;
      listing->generation = ++children.listing_generation;
      if (listing->generation == 0) {
        listing->generation = ++children.listing_generation;
        for (auto [name, child] : children) {
          (void)name;
          child->listing_generation.store(0, std::memory_order_relaxed);
        }
      }
    }
    waiter.reactor = &reactor;
    waiter.next = children.refresh_waiters;
    children.refresh_waiters = &waiter;
  }
  if (start) {
    try {
      cache_register_directory(state, static_cast<InodeDir&>(item));
    } catch (...) {
      listing.release()->finish(std::current_exception());
      return true;
    }
    listing.release()->next_page();
  }
  return true;
}

void refresh_directory_once(State& state, fuse_ino_t inode, bool force) {
  const uint64_t now = fuse_monotonic_ns();
  std::string key;
  InodeBase& directory = inode_item(state, inode);
  if (!directory.directory()) {
    throw std::system_error(ENOTDIR, std::generic_category(), "readdir");
  }
  if (!force && directory.expire.load(std::memory_order_relaxed) > now) {
    return;
  }
  key = item_key(state, directory);
  Directory& children = directory.dir_children();
  uint64_t mutation_epoch;
  uint32_t generation;
  {
    std::unique_lock guard(children.mutex);
    if (children.remote_mutations) return;
    mutation_epoch = children.mutation_epoch;
    generation = ++children.listing_generation;
    if (generation == 0) {
      generation = ++children.listing_generation;
      for (auto [name, child] : children) {
        (void)name;
        child->listing_generation.store(0, std::memory_order_relaxed);
      }
    }
  }
  cache_register_directory(state, static_cast<InodeDir&>(directory));
  std::vector<ListedChild> file_conflicts;
  try {
    for_each_directory_page(
        state, key, [&](std::vector<ListedChild> page) {
          InodeBase& current = inode_item(state, inode);
          if (item_key(state, current) != key) {
            throw std::system_error(ESTALE, std::generic_category(),
                                    "directory changed during listing");
          }
          {
            std::unique_lock guard(children.mutex);
            children.reserve(children.end_i() + page.size());
          }
          for (ListedChild& child : page) {
            const fuse_ino_t installed = install_item(
                state, inode, child, generation,
                false, false, false, true, &mutation_epoch);
            if (installed == 0) {
              file_conflicts.push_back(std::move(child));
            }
          }
        });
    for (ListedChild& child : file_conflicts) {
      bool directory_won = false;
      {
        std::shared_lock guard(children.mutex);
        const auto i = children.find(
            terark::fstring(
                child.name.data(), ptrdiff_t(child.name.size())));
        if (i != children.end()) {
          InodeBase* item = i->second;
          directory_won = item->directory() &&
              item->listing_generation.load(std::memory_order_relaxed) ==
                  generation;
        }
      }
      if (!directory_won) {
        install_item(state, inode, std::move(child), generation,
                     false, false, false, false, &mutation_epoch);
      }
    }
    prune_directory_generation(state, inode_item(state, inode), generation, &mutation_epoch);
    const uint64_t refreshed = fuse_monotonic_ns();
    const uint64_t ttl       = state.config.directory_cache_ns;
    const uint64_t expire    = ttl > UINT64_MAX - refreshed
                                   ? UINT64_MAX
                                   : refreshed + ttl;
    {
      std::unique_lock guard(children.mutex);
      if (children.remote_mutations) throw DirectoryListingDeferred{};
      if (children.mutation_epoch != mutation_epoch) throw DirectoryListingChanged{};
      directory.expire.store(expire, std::memory_order_relaxed);
    }
  } catch (...) {
    invalidate_directory(state, inode);
    throw;
  }
  sweep_retired_items(state);
}

void refresh_directory_snapshot(State& state, fuse_ino_t inode,
                              bool force = false) {
  for (;;) {
    try {
      refresh_directory_once(state, inode, force);
      return;
    } catch (const DirectoryListingDeferred&) {
      return;
    } catch (const DirectoryListingChanged&) {
      force = true;
    }
  }
}

void refresh_directory(State& state, fuse_ino_t inode, bool force = false) {
  InodeBase& item = inode_item(state, inode);
  if (!item.directory()) {
    throw std::system_error(ENOTDIR, std::generic_category(), "directory");
  }
  cache_touch(static_cast<InodeDir&>(item));
  Directory& children = item.dir_children();
  for (;;) {
    {
      std::unique_lock guard(children.mutex);
      if (!children.refreshing.load(std::memory_order_relaxed)) {
        children.refreshing.store(true, std::memory_order_release);
        break;
      }
    }
    if (current_fuse_reactor() != nullptr) {
      throw std::system_error(EAGAIN, std::generic_category(),
                              "directory refresh requires asynchronous completion");
    }
    children.refreshing.wait(true, std::memory_order_acquire);
  }
  try {
    refresh_directory_snapshot(state, inode, force);
    finish_directory_refresh(children, {});
  } catch (...) {
    finish_directory_refresh(children, std::current_exception());
    throw;
  }
}

void invalidate_directory(State& state, fuse_ino_t inode) noexcept {
  InodeBase* item = inode == FUSE_ROOT_ID
                        ? state.root_item.get()
                        : reinterpret_cast<InodeBase*>(uintptr_t(inode));
  if (item != nullptr && !item->detached()) {
    item->expire.store(0, std::memory_order_relaxed);
  }
}

double remaining_directory_timeout(const InodeBase& item) noexcept {
  const uint64_t expire = item.expire.load(std::memory_order_relaxed);
  const uint64_t now    = fuse_monotonic_ns_noexcept();
  return expire > now ? double(expire - now) / 1'000'000'000.0 : 0.0;
}

double remaining_inode_timeout(State& state,
                               const InodeBase& item) noexcept {
  const InodeBase* parent = item.parent();
  if (parent == nullptr || parent == &item) {
    parent = state.root_item.get();
  }
  return remaining_directory_timeout(*parent);
}

size_t reclaim_cached_children(State& state, InodeDir& item) {
  std::lock_guard mutation_guard(item.children.mutation_mutex);
  if (item.detached() ||
      item.children.refreshing.load(std::memory_order_acquire) ||
      item.open_count.load(std::memory_order_acquire) != 1 ||
      item.expire.load(std::memory_order_relaxed) > fuse_monotonic_ns()) {
    return 0;
  }

  std::vector<InodeBase*> reclaimed;
  {
    std::unique_lock guard(item.children.mutex);
    reclaimed.reserve(item.children.size());
    for (size_t slot = 0; slot < item.children.end_i(); ++slot) {
      if (item.children.is_deleted(slot)) {
        continue;
      }
      InodeBase* child = item.children.val(slot);
      if (child->pending() || child->truncate_pending() ||
          !item_tree_unreferenced(*child)) {
        continue;
      }
      item.children.erase_i(slot);
      child->dentry_slot = UINT32_MAX;
      reclaimed.push_back(child);
    }
  }

  size_t count = 0;
  for (InodeBase* child : reclaimed) {
    mark_item_tree_detached(state, *child);
    count += item_tree_size(*child);
    cache_inode_tree_deleted(state, *child);
    delete_inode(child);
  }
  return count;
}

void warn_inode_cache_budget(State& state) noexcept {
  const uint64_t now = fuse_monotonic_ns_noexcept();
  constexpr uint64_t interval = 60ULL * 1000ULL * 1000ULL * 1000ULL;
  if (state.cache_budget_warned && now - state.cache_warning_ns < interval) {
    return;
  }
  state.cache_budget_warned = true;
  state.cache_warning_ns    = now;
  fprintf(stderr,
          "warning: inode cache remains above soft limit: used=%zu "
          "limit=%zu; referenced, open, pending, or fresh entries are "
          "retained\n",
          state.cached_inodes.load(std::memory_order_relaxed),
          state.config.max_cached_inodes);
}

void cache_reclaim_loop(std::stop_token stop, State* state) noexcept {
  while (!stop.stop_requested()) {
    size_t scan_limit = 0;
    std::deque<State::PendingInvalidation> invalidations;
    {
      std::unique_lock guard(state->cache_mutex);
      state->cache_condition.wait_for(
          guard, std::chrono::milliseconds(250), [&] {
            return stop.stop_requested() ||
                !state->pending_invalidations.empty() ||
                state->cached_inodes.load(std::memory_order_acquire) >
                    state->config.max_cached_inodes;
          });
      if (stop.stop_requested()) {
        return;
      }
      invalidations.swap(state->pending_invalidations);
      if (state->cached_inodes.load(std::memory_order_acquire) <=
          state->config.max_cached_inodes) {
        state->cache_budget_warned = false;
      } else {
        scan_limit = state->cache_clock_size * 2 + 1;
      }
    }
    for (const State::PendingInvalidation& pending : invalidations) {
      if (pending.generation_epoch != 0 &&
          (pending.item->generation_epoch.load(std::memory_order_acquire) &
           ((1ULL << 63) - 1)) != pending.generation_epoch) {
        continue;
      }
      invalidate_page_cache(*state, pending.inode,
                            pending.offset, pending.length);
    }
    if (!invalidations.empty()) {
      invalidations.clear();
      sweep_retired_items(*state);
    }
    if (scan_limit == 0) {
      continue;
    }

    size_t reclaimed = 0;
    for (size_t scan = 0; scan < scan_limit; ++scan) {
      InodeDir* item = nullptr;
      {
        std::lock_guard guard(state->cache_mutex);
        item = state->cache_clock_hand;
        if (item == nullptr) {
          break;
        }
        state->cache_clock_hand = item->children.clock_next;
        if (item->children.clock_referenced.exchange(
                false, std::memory_order_relaxed)) {
          continue;
        }
        uint32_t count = item->open_count.load(std::memory_order_relaxed);
        do {
          if (count == UINT32_MAX) {
            item = nullptr;
            break;
          }
        } while (!item->open_count.compare_exchange_weak(
            count, count + 1, std::memory_order_acquire,
            std::memory_order_relaxed));
      }
      if (item == nullptr) {
        continue;
      }
      try {
        reclaimed += reclaim_cached_children(*state, *item);
      } catch (const std::exception& error) {
        fprintf(stderr, "warning: inode cache reclamation failed: %s\n",
                error.what());
      } catch (...) {
      }
      if (release_inode_count(item->open_count)) {
        sweep_retired_items(*state);
      }
      if (state->cached_inodes.load(std::memory_order_acquire) <=
          state->config.max_cached_inodes) {
        state->cache_budget_warned = false;
        break;
      }
    }
    if (reclaimed == 0 &&
        state->cached_inodes.load(std::memory_order_acquire) >
            state->config.max_cached_inodes) {
      warn_inode_cache_budget(*state);
    }
  }
}

void append_authorization(std::vector<Header>& headers, State& state,
                          std::string_view method, std::string_view path,
                          std::string_view payload_hash) {
  HeaderList authorization = authorization_headers(
      state, method, path, headers, payload_hash);
  headers.insert(headers.end(),
                 std::make_move_iterator(authorization.begin()),
                 std::make_move_iterator(authorization.end()));
}

bool checksum_has_digest(ChecksumAlgorithm algorithm) noexcept {
  return checksum_is_s3(algorithm) || algorithm == CHECKSUM_CRC64XZ;
}

void checksum_pipe(DataChecksum& checksum, Pipe& copy,
                   int source_fd, size_t bytes) {
  std::array<std::byte, kPreferredIoSize> buffer;
  size_t remaining = bytes;
  while (remaining != 0) {
    const size_t wanted = std::min(
        {remaining, copy.capacity(), buffer.size()});
    ssize_t result;
    do {
      result = ::tee(source_fd, copy.write_fd(), wanted, 0);
    } while (result < 0 && errno == EINTR);
    if (result <= 0) {
      const int error = result == 0 ? ECONNRESET : errno;
      throw std::system_error(error, std::generic_category(),
                              "tee(checksum)");
    }
    const size_t count = size_t(result);
    read_all(copy.read_fd(), std::span(buffer).first(count));
    checksum.update(std::span(buffer).first(count));
    remaining -= count;
  }
}

ChecksumValue retained_checksum(ChecksumAlgorithm algorithm,
                                const RetainedPart* part) {
  DataChecksum checksum(algorithm);
  if (part != nullptr && !part->segments.empty()) {
    Pipe copy = Pipe::create(kPreferredIoSize);
    for (const PipeSegment& segment : part->segments) {
      checksum_pipe(checksum, copy, segment.pipe.read_fd(), segment.bytes);
    }
  }
  return checksum.finish();
}

void append_upload_checksum(std::vector<Header>& headers,
                            ChecksumAlgorithm algorithm,
                            const ChecksumValue& checksum) {
  if (checksum_is_s3(algorithm)) {
    headers.push_back(Header{checksum_header_name(algorithm),
                             checksum.base64});
  }
}

void verify_upload_checksum(const Response& response,
                            ChecksumAlgorithm algorithm,
                            const ChecksumValue& checksum,
                            const char* operation) {
  if (!checksum_has_digest(algorithm)) {
    return;
  }
  const std::string_view name = checksum_header_name(algorithm);
  const auto header = response.headers.find(name);
  if (header == response.headers.end()) {
    throw std::runtime_error(std::string(operation) +
                             " response omitted " + std::string(name));
  }
  if (algorithm == CHECKSUM_CRC64XZ) {
    uint64_t received = 0;
    if (!parse_unsigned(sso_view(header->second), received) ||
        received != checksum.integer) {
      throw std::runtime_error(std::string(operation) +
                               " returned a mismatched OSS CRC64");
    }
  } else if (sso_view(header->second) != sso_view(checksum.base64)) {
    throw std::runtime_error(std::string(operation) +
                             " returned a mismatched S3 checksum");
  }
}

bool read_checksum_from_response(const Response& response,
                                 ChecksumAlgorithm preferred,
                                 ChecksumAlgorithm& algorithm,
                                 std::string_view& expected) {
  const auto goog = response.headers.find("x-goog-hash");
  if (goog != response.headers.end() &&
      gcs_checksum_from_header(sso_view(goog->second), preferred,
                               algorithm, expected)) {
    return true;
  }
  const auto type = response.headers.find("x-amz-checksum-type");
  if (type != response.headers.end() && type->second == "COMPOSITE") {
    return false;
  }
  if (preferred == CHECKSUM_CRC64XZ) {
    const auto header = response.headers.find(
        checksum_header_name(CHECKSUM_CRC64XZ));
    if (header != response.headers.end()) {
      algorithm = CHECKSUM_CRC64XZ;
      expected  = sso_view(header->second);
      return true;
    }
  }
  constexpr std::array algorithms{
      CHECKSUM_XXHASH128,
      CHECKSUM_XXHASH3,
      CHECKSUM_CRC64NVME,
      CHECKSUM_CRC32C,
      CHECKSUM_CRC32,
      CHECKSUM_XXHASH64,
      CHECKSUM_SHA512,
      CHECKSUM_SHA256,
      CHECKSUM_SHA1,
      CHECKSUM_MD5,
  };
  if (checksum_is_s3(preferred)) {
    const auto header = response.headers.find(
        checksum_header_name(preferred));
    if (header != response.headers.end() &&
        sso_view(header->second).find('-') == std::string_view::npos) {
      algorithm = preferred;
      expected  = sso_view(header->second);
      return true;
    }
  }
  for (const ChecksumAlgorithm candidate : algorithms) {
    const auto header = response.headers.find(
        checksum_header_name(candidate));
    if (header != response.headers.end()) {
      if (sso_view(header->second).find('-') != std::string_view::npos) {
        continue;
      }
      algorithm = candidate;
      expected  = sso_view(header->second);
      return true;
    }
  }
  const auto oss = response.headers.find(
      checksum_header_name(CHECKSUM_CRC64XZ));
  if (oss != response.headers.end()) {
    algorithm = CHECKSUM_CRC64XZ;
    expected  = sso_view(oss->second);
    return true;
  }
  return false;
}

void verify_read_checksum(const Response& response, Pipe& pipe,
                          size_t bytes, ChecksumAlgorithm preferred) {
  ChecksumAlgorithm algorithm = CHECKSUM_NONE;
  std::string_view expected;
  if (!read_checksum_from_response(response, preferred,
                                   algorithm, expected)) {
    return;
  }
  DataChecksum checksum(algorithm);
  Pipe copy = Pipe::create(kPreferredIoSize);
  checksum_pipe(checksum, copy, pipe.read_fd(), bytes);
  const ChecksumValue actual = checksum.finish();
  if (algorithm == CHECKSUM_CRC64XZ) {
    uint64_t received = 0;
    if (!parse_unsigned(expected, received) ||
        received != actual.integer) {
      throw std::runtime_error("GetObject returned a mismatched OSS CRC64");
    }
  } else if (expected != sso_view(actual.base64)) {
    throw std::runtime_error(
        response.headers.find("x-goog-hash") != response.headers.end()
            ? "GetObject returned a mismatched Google Cloud Storage checksum"
            : "GetObject returned a mismatched S3 checksum");
  }
}

void publish_written_metadata(OpenHandle& handle,
                              const Response& response,
                              const ObjectMetadata* metadata,
                              std::string_view body_etag = {}) {
  time_t mtime;
  const auto modified = response.headers.find("last-modified");
  if (modified != response.headers.end()) {
    mtime = parse_http_mtime(sso_view(modified->second));
  } else {
    if (metadata == nullptr) {
      throw std::logic_error("write commit lacks server mtime");
    }
    mtime = metadata->mtime;
  }
  const auto etag = response.headers.find("etag");
  if (etag != response.headers.end()) {
    assign_string(handle.etag, etag->second);
  } else if (!body_etag.empty()) {
    handle.etag.assign(body_etag);
  } else if (metadata != nullptr) {
    handle.etag = metadata->etag;
  } else {
    handle.etag.clear();
  }
  auto version = response.headers.find("x-amz-version-id");
  if (version == response.headers.end()) {
    version = response.headers.find("x-oss-version-id");
  }
  if (version != response.headers.end() && version->second != "null") {
    assign_string(handle.version_id, version->second);
  } else if (metadata != nullptr) {
    handle.version_id = metadata->version_id;
  } else {
    handle.version_id.clear();
  }
  InodeFile& item = *handle.item;
  InodeMetadataGuard metadata_guard(item);
  std::unique_lock<std::shared_mutex> directory_guard;
  // Recovery uses a private inode, not yet attached to the mounted tree.
  if (InodeBase* parent = item.parent()) {
    Directory& children = parent->dir_children();
    directory_guard = std::unique_lock(children.mutex);
    ++children.mutation_epoch;
  }
  item.fsize.store(handle.stream_offset, std::memory_order_relaxed);
  item.mtime.store(mtime, std::memory_order_relaxed);
  item.set_page_cache_valid(!handle.page_cache_store_failed);
  item.set_pending(false);
  handle.generation_epoch = publish_inode_generation(
      item, object_generation(handle.etag, handle.version_id,
                              handle.stream_offset, mtime));
  item.set_truncate_pending(false);
}

void update_written_metadata(State& state, OpenHandle& handle,
                             const Response& response,
                             std::string_view body_etag = {}) {
  std::optional<ObjectMetadata> metadata;
  if (response.headers.find("last-modified") == response.headers.end()) {
    metadata = head_object(state, handle.object_path);
  }
  publish_written_metadata(handle, response,
                            metadata ? &*metadata : nullptr, body_etag);
}

void require_s3_success(const Response& response, const char* operation) {
  S3ErrorInfo error;
  const bool embedded_error = response.status >= 200 &&
      response.status < 300 && !response.body.empty() &&
      parse_s3_error(response_xml(response), error);
  if (response.status < 200 || response.status >= 300 || embedded_error) {
    throw_s3_response(response, operation);
  }
}

void fail_write(OpenHandle& handle, int error) noexcept {
  if (handle.write_state != WRITE_FAILED) {
    handle.write_state = WRITE_FAILED;
    handle.write_error = error > 0 ? error : EIO;
  }
  notify_handle(handle);
}

void warn_budget_locked(State& state) noexcept {
  const uint64_t now = fuse_monotonic_ns_noexcept();
  constexpr uint64_t interval = 5ULL * 1000ULL * 1000ULL * 1000ULL;
  if (!state.budget_exhausted ||
      now - state.budget_warning_ns >= interval) {
    fprintf(stderr,
            "warning: pinned write budget exhausted: used=%" PRIu64
            " limit=%" PRIu64 " part_size=%" PRIu64
            " waiting_handles=%u\n",
            state.pinned_bytes, state.config.max_pinned_memory,
            state.config.part_size, state.waiting_writers +
                state.budget_reactor_waiters.load(std::memory_order_relaxed));
    state.budget_warning_ns = now;
  }
  state.budget_exhausted = true;
}

bool reserve_part_budget(State& state, bool wait) {
  std::unique_lock guard(state.budget_mutex);
  const auto available = [&] {
    return state.pinned_bytes <=
           state.config.max_pinned_memory - state.config.part_size;
  };
  if (!available() || state.waiting_writers != 0) {
    warn_budget_locked(state);
    if (!wait) {
      return false;
    }
    const uint64_t ticket = state.next_budget_ticket++;
    ++state.waiting_writers;
    state.budget_condition.wait(guard, [&] {
      return ticket == state.serving_budget_ticket && available();
    });
    --state.waiting_writers;
    ++state.serving_budget_ticket;
  }
  state.pinned_bytes += state.config.part_size;
  guard.unlock();
  state.budget_condition.notify_all();
  return true;
}

void release_part_budget(State& state) noexcept {
  {
    std::lock_guard guard(state.budget_mutex);
    if (state.pinned_bytes < state.config.part_size) {
      fprintf(stderr,
              "warning: pinned write budget accounting underflow\n");
      state.pinned_bytes = 0;
    } else {
      state.pinned_bytes -= state.config.part_size;
    }
    if (state.budget_exhausted &&
        state.pinned_bytes <=
            state.config.max_pinned_memory - state.config.part_size) {
      fprintf(stderr,
              "pinned write budget recovered: used=%" PRIu64
              " limit=%" PRIu64 "\n",
              state.pinned_bytes, state.config.max_pinned_memory);
      state.budget_exhausted = false;
    }
  }
  state.budget_condition.notify_all();
  const uint64_t wake = state.budget_reactor_waiters.load(std::memory_order_acquire);
  if (wake != 0) {
    ssize_t result;
    do { result = ::write(state.budget_event.get(), &wake, sizeof(wake)); }
    while (result < 0 && errno == EINTR);
  }
}

PipeSegment& writable_segment(RetainedPart& part, PipeInput input) {
  if (part.segments.empty() ||
      part.segments.back().pipe.write_fd() < 0 ||
      part.segments.back().bytes == part.segments.back().pipe.capacity() ||
      (part.segments.back().input != PIPE_INPUT_NONE &&
       part.segments.back().input != input)) {
    if (!part.segments.empty()) {
      part.segments.back().pipe.close_write_end();
    }
    Pipe pipe = Pipe::create(kPreferredIoSize);
    part.segments.push_back(
        PipeSegment{std::move(pipe), 0, PIPE_INPUT_NONE});
  }
  PipeSegment& segment = part.segments.back();
  segment.input = input;
  return segment;
}

void retain_bytes(RetainedPart& part, std::span<const std::byte> bytes) {
  while (!bytes.empty()) {
    PipeSegment& segment = writable_segment(part, PIPE_INPUT_MEMORY);
    const size_t room = segment.pipe.capacity() - segment.bytes;
    const size_t count = std::min(room, bytes.size());
    write_all(segment.pipe.write_fd(), bytes.first(count));
    segment.bytes += count;
    part.bytes    += count;
    bytes = bytes.subspan(count);
  }
}

void record_memory_fallback(State& state, size_t bytes) noexcept;

size_t copy_fd_to_pipe(int fd, uint64_t& offset, bool seek,
                       int pipe_fd, size_t length) {
  std::array<std::byte, 64U * 1024U> buffer;
  const size_t wanted = std::min(length, buffer.size());
  for (;;) {
    ssize_t read_bytes;
    if (seek) {
      read_bytes = ::pread(fd, buffer.data(), wanted, off_t(offset));
    } else {
      read_bytes = ::read(fd, buffer.data(), wanted);
    }
    if (read_bytes > 0) {
      write_all(pipe_fd, std::span(buffer).first(size_t(read_bytes)));
      if (seek) {
        offset += uint64_t(read_bytes);
      }
      return size_t(read_bytes);
    }
    if (read_bytes == 0) {
      throw std::system_error(ECONNRESET, std::generic_category(),
                              "FUSE write source reached EOF");
    }
    if (errno != EINTR) {
      throw std::system_error(errno, std::generic_category(),
                              seek ? "pread(FUSE write fallback)"
                                   : "read(FUSE write fallback)");
    }
  }
}

void retain_fd(State& state, RetainedPart& part, int fd, uint64_t& offset,
               size_t length, bool seek) {
  bool copied = false;
  while (length != 0) {
    PipeSegment& segment = writable_segment(
        part, copied ? PIPE_INPUT_MEMORY : PIPE_INPUT_FD);
    const size_t room = segment.pipe.capacity() - segment.bytes;
    const size_t count = std::min(room, length);
    size_t moved = 0;
    if (!copied) {
      try {
        uint64_t* position = seek ? &offset : nullptr;
        moved = splice_some(fd, position, segment.pipe.write_fd(),
                            count, SPLICE_F_MOVE);
      } catch (const std::system_error& error) {
        const int value = error.code().value();
        if (value != EPERM && value != ENOSYS && value != EINVAL &&
            value != EOPNOTSUPP && value != EXDEV) {
          throw;
        }
        copied = true;
        moved = copy_fd_to_pipe(fd, offset, seek,
                                segment.pipe.write_fd(), count);
        record_memory_fallback(state, moved);
      }
    } else {
      moved = copy_fd_to_pipe(fd, offset, seek,
                              segment.pipe.write_fd(), count);
      record_memory_fallback(state, moved);
    }
    if (moved == 0) {
      if (segment.bytes == 0) {
        throw std::system_error(EAGAIN, std::generic_category(),
                                "FUSE write source has no data");
      }
      segment.pipe.close_write_end();
      continue;
    }
    segment.bytes += moved;
    part.bytes    += moved;
    length        -= moved;
  }
}

void send_retained_body(HttpClient& client, std::string_view method,
                        std::string_view path,
                        std::span<const Header> headers,
                        const RetainedPart& part) {
  client.begin_upload(method, path, headers, size_t(part.bytes));
  try {
    for (const PipeSegment& segment : part.segments) {
      Pipe clone = Pipe::create(segment.bytes);
      if (clone.capacity() < segment.bytes) {
        throw std::system_error(ENOBUFS, std::generic_category(),
                                "retry pipe is smaller than source segment");
      }
      tee_exact(segment.pipe.read_fd(), clone.write_fd(), segment.bytes, 0);
      clone.close_write_end();
      client.upload_from_fd(clone.read_fd(), 0, segment.bytes, false);
    }
  } catch (...) {
    client.cancel_upload();
    throw;
  }
}

bool store_page_cache(State& state, const OpenHandle& handle,
                      RetainedPart& part, uint64_t offset) noexcept {
  if (part.bytes == 0) {
    return true;
  }

  int error = 0;
  try {
    const size_t alloc_size = offsetof(fuse_bufvec, buf) +
        part.segments.size() * sizeof(fuse_buf);
    auto storage = std::make_unique<std::byte[]>(alloc_size);
    memset(storage.get(), 0, alloc_size);
    auto* bufv = reinterpret_cast<fuse_bufvec*>(storage.get());
    bufv->count = part.segments.size();
    auto* bufs = reinterpret_cast<fuse_buf*>(
        storage.get() + offsetof(fuse_bufvec, buf));
    for (size_t i = 0; i < part.segments.size(); ++i) {
      bufs[i].size  = part.segments[i].bytes;
      bufs[i].flags = fuse_buf_flags(
          FUSE_BUF_IS_FD | FUSE_BUF_FD_RETRY);
      bufs[i].fd    = part.segments[i].pipe.read_fd();
    }

    std::lock_guard guard(state.session_mutex);
    if (state.session == nullptr) {
      return false;
    }
    const int result = fuse_lowlevel_notify_store(
        state.session, handle.inode, off_t(offset), bufv,
        FUSE_BUF_SPLICE_MOVE);
    if (result == 0) {
      return true;
    }
    error = -result;
  } catch (const std::bad_alloc&) {
    error = ENOMEM;
  } catch (...) {
    error = EIO;
  }

  if (!state.page_cache_store_warned.exchange(
          true, std::memory_order_relaxed)) {
    fprintf(stderr,
            "warning: unable to retain uploaded data in page cache: "
            "path=%s offset=%" PRIu64 " bytes=%" PRIu64 ": %s\n",
            handle.object_path.c_str(), offset, part.bytes,
            strerror(error));
  }
  return false;
}

void remember_partial_write_pages(OpenHandle& handle, uint64_t start,
                                  uint64_t end, size_t page_size) {
  const auto append = [&](uint64_t offset) {
    const uint64_t page_end = offset > UINT64_MAX - page_size
                                  ? UINT64_MAX
                                  : offset + page_size;
    if (!handle.partial_write_pages.empty() &&
        offset <= handle.partial_write_pages.back().second) {
      handle.partial_write_pages.back().second = std::max(
          handle.partial_write_pages.back().second, page_end);
    } else {
      handle.partial_write_pages.emplace_back(offset, page_end);
    }
  };
  if (start % page_size != 0) {
    append(start - start % page_size);
  }
  if (end % page_size != 0) {
    append(end - end % page_size);
  }
}

bool store_cached_partial_pages(State& state,
                                const OpenHandle& handle) noexcept {
  const uint64_t maximum_chunk = state.config.part_size;
  int error = 0;
  try {
    for (const auto& [range_start, range_end] :
         handle.partial_write_pages) {
      uint64_t offset = range_start;
      const uint64_t end = std::min<uint64_t>(
          range_end, handle.stream_offset);
      while (offset < end) {
        const size_t length = size_t(std::min<uint64_t>(
            end - offset, maximum_chunk));
        fuse_bufvec buffers{
            .count = 1,
            .idx   = 0,
            .off   = 0,
            .buf   = {{
                .size     = length,
                .flags    = fuse_buf_flags(0),
                .mem      = nullptr,
                .fd       = -1,
                .pos      = 0,
                .mem_size = 0,
            }},
        };
        buffers.buf[0].flags = fuse_buf_flags(
            FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK | FUSE_BUF_FD_RETRY);
        buffers.buf[0].fd  = handle.cache_entry->data_fd();
        buffers.buf[0].pos = off_t(offset);
        {
          std::lock_guard guard(state.session_mutex);
          if (state.session == nullptr) {
            throw std::system_error(
                ENOTCONN, std::generic_category(), "FUSE_NOTIFY_STORE");
          }
          const int result = fuse_lowlevel_notify_store(
              state.session, handle.inode, off_t(offset), &buffers,
              FUSE_BUF_SPLICE_MOVE);
          if (result != 0) {
            error = -result;
            throw std::system_error(
                error, std::generic_category(), "FUSE_NOTIFY_STORE");
          }
        }
        offset += length;
      }
    }
    return true;
  } catch (const std::bad_alloc&) {
    error = ENOMEM;
  } catch (const std::system_error& exception) {
    error = exception.code().value();
  } catch (...) {
    error = EIO;
  }
  if (!state.page_cache_store_warned.exchange(
          true, std::memory_order_relaxed)) {
    fprintf(stderr,
            "warning: unable to retain partial cached writes in page cache: "
            "path=%s: %s\n",
            handle.object_path.c_str(), strerror(error));
  }
  return false;
}

void ensure_multipart(State& state, OpenHandle& handle) {
  {
    std::unique_lock guard(handle.mutex);
    while (handle.multipart_starting) {
      wait_handle(handle, guard);
    }
    if (!handle.upload_id.empty()) {
      return;
    }
    if (handle.write_state == WRITE_FAILED) {
      throw std::system_error(handle.write_error, std::generic_category(),
                              "write handle failed");
    }
    handle.multipart_starting = true;
  }

  try {
    const std::string path = query_path(handle.object_path, "uploads=");
    Response response;
    try {
      std::vector<Header> headers;
      if (checksum_is_s3(state.config.checksum)) {
        const std::string_view type = checksum_multipart_type(
            state.config.checksum);
        headers.push_back(Header{"x-amz-checksum-algorithm",
                                 checksum_s3_name(state.config.checksum)});
        headers.push_back(Header{"x-amz-checksum-type", type});
      }
      if (!handle.write_id.empty()) {
        headers.push_back(Header{
            "x-amz-meta-ngs3fs-write-id", handle.write_id});
      }
      append_authorization(headers, state, "POST", path,
                           kEmptyPayloadSha256);
      HttpPool::Lease client = state.http->acquire();
      response = client->request_no_body("POST", path, headers);
    } catch (...) {
      fprintf(stderr,
              "error: CreateMultipartUpload outcome unknown; an orphan "
              "upload may require lifecycle cleanup: %s\n",
              handle.object_path.c_str());
      throw;
    }
    if (retryable_response(response)) {
      fprintf(stderr,
              "error: not retrying ambiguous CreateMultipartUpload: "
              "status=%d path=%s\n",
              response.status, handle.object_path.c_str());
    }
    require_s3_success(response, "CreateMultipartUpload");
    if (checksum_is_s3(state.config.checksum)) {
      const auto algorithm = response.headers.find(
          "x-amz-checksum-algorithm");
      const auto type = response.headers.find("x-amz-checksum-type");
      if (algorithm == response.headers.end() ||
          sso_view(algorithm->second) !=
              checksum_s3_name(state.config.checksum) ||
          type == response.headers.end() ||
          sso_view(type->second) !=
              checksum_multipart_type(state.config.checksum)) {
        throw std::runtime_error(
            "CreateMultipartUpload did not accept the requested checksum");
      }
    }
    S3Xml xml(response_xml(response), "CreateMultipartUpload");
    const tinyxml2::XMLElement& root =
        xml.result_root("InitiateMultipartUploadResult");
    std::string upload_id = xml.required_text(root, "UploadId");
    if (upload_id.empty()) {
      throw std::runtime_error(
          "CreateMultipartUpload response omitted UploadId");
    }
    if (handle.cache_entry) {
      handle.cache_entry->set_upload_id(upload_id);
    }
    {
      std::lock_guard guard(handle.mutex);
      handle.upload_id = std::move(upload_id);
      handle.part_etags.reserve(kMaximumMultipartParts);
      if (checksum_multipart_type(state.config.checksum) == "COMPOSITE") {
        handle.part_checksums.reserve(kMaximumMultipartParts);
      }
      if (state.config.checksum == CHECKSUM_CRC64NVME ||
          state.config.checksum == CHECKSUM_CRC64XZ) {
        handle.part_checksum_values.reserve(kMaximumMultipartParts);
        handle.part_sizes.reserve(kMaximumMultipartParts);
      }
      handle.multipart_starting = false;
      notify_handle(handle);
    }
  } catch (...) {
    std::lock_guard guard(handle.mutex);
    handle.multipart_starting = false;
    fail_write(handle, EIO);
    throw;
  }
}

std::string upload_part(State& state, OpenHandle& handle,
                        RetainedPart& part) {
  ensure_multipart(state, handle);
  if (checksum_has_digest(state.config.checksum)) {
    part.checksum = retained_checksum(state.config.checksum, &part);
  }
  std::string upload_id;
  {
    std::lock_guard guard(handle.mutex);
    upload_id = handle.upload_id;
  }
  std::string query = "partNumber=" + std::to_string(part.number);
  query += "&uploadId=" + uri_encode(upload_id, false);
  const std::string path = query_path(handle.object_path, query);
  const Response response = request_with_retries([&] {
    std::vector<Header> headers;
    append_upload_checksum(headers, state.config.checksum, part.checksum);
    append_authorization(headers, state, "PUT", path, kUnsignedPayload);
    HttpPool::Lease client = state.http->acquire_bulk();
    send_retained_body(client.client(), "PUT", path, headers, part);
    return client->finish_upload();
  }, "UploadPart");
  require_s3_success(response, "UploadPart");
  verify_upload_checksum(response, state.config.checksum,
                         part.checksum, "UploadPart");
  const auto etag = response.headers.find("etag");
  if (etag == response.headers.end() || etag->second.empty()) {
    throw std::runtime_error("UploadPart response omitted ETag");
  }
  return std::string(etag->second.data(), etag->second.size());
}

void upload_part_job(State& state, OpenHandle& handle,
                     std::shared_ptr<RetainedPart> part) noexcept {
  const unsigned part_number = part->number;
  std::string etag;
  ssostr<96> checksum;
  uint64_t checksum_value  = 0;
  const uint64_t part_size = part->bytes;
  int error_code           = 0;
  bool cache_store_failed  = false;
  try {
    {
      std::lock_guard guard(handle.mutex);
      if (handle.unlinked.load(std::memory_order_acquire)) {
        throw std::system_error(ESTALE, std::generic_category(),
                                "write was unlinked");
      }
      if (handle.write_state == WRITE_FAILED) {
        throw std::system_error(handle.write_error,
                                std::generic_category(),
                                "write handle failed");
      }
    }
    etag = upload_part(state, handle, *part);
    checksum       = part->checksum.base64;
    checksum_value = part->checksum.integer;
    cache_store_failed = !store_page_cache(
        state, handle, *part,
        uint64_t(part_number - 1) * state.config.part_size);
  } catch (const std::system_error& error) {
    error_code = error.code().value();
  } catch (...) {
    error_code = EIO;
  }
  part.reset();
  release_part_budget(state);
  std::lock_guard guard(handle.mutex);
  if (handle.unlinked.load(std::memory_order_acquire)) {
    error_code = ESTALE;
  }
  if (error_code == 0) {
    handle.page_cache_store_failed |= cache_store_failed;
    handle.part_etags[part_number - 1] = std::move(etag);
    if (checksum_multipart_type(state.config.checksum) == "COMPOSITE") {
      handle.part_checksums[part_number - 1] = std::move(checksum);
    }
    if (state.config.checksum == CHECKSUM_CRC64NVME ||
        state.config.checksum == CHECKSUM_CRC64XZ) {
      handle.part_checksum_values[part_number - 1] = checksum_value;
      handle.part_sizes[part_number - 1] = part_size;
    }
  } else {
    fail_write(handle, error_code);
  }
  --handle.pending_parts;
  notify_handle(handle);
}

ChecksumValue checksum_file_range(ChecksumAlgorithm algorithm, int fd,
                                  uint64_t offset, size_t length);

struct AsyncPartUpload {
  AsyncPartUpload(State& s, OpenHandle& h, FuseReactor& r,
                  std::shared_ptr<RetainedPart> p, unsigned n,
                  uint64_t off, size_t len, const OpenRequestGuard* parent)
      : state(s), handle(h), reactor(r),
        active(parent ? OpenRequestGuard(*parent) : OpenRequestGuard(h)),
        part(std::move(p)),
        number(n), offset(off), length(len) {}

  State& state;
  OpenHandle& handle;
  FuseReactor& reactor;
  OpenRequestGuard active;
  std::shared_ptr<RetainedPart> part;
  unsigned number;
  uint64_t offset;
  size_t length;
  ChecksumValue checksum;
  ssostr<64> etag;
  ssostr<248> created_id;
  std::vector<Pipe> replay;
  std::unique_ptr<AsyncSignedS3Request> http;
  FuseReactor::ReactorTask continuation;
  AsyncIoRequest wait;
  UniqueFd timer;
  uint64_t notification = 0;
  std::exception_ptr worker_error;
  unsigned attempt = 0;
  bool admitted = false;
  bool creating = false;
  bool waiting = false;
  bool store_failed = false;

  static int error_code(std::exception_ptr error) noexcept {
    if (!error) return 0;
    try { std::rethrow_exception(error); }
    catch (const std::system_error& value) { return value.code().value(); }
    catch (const std::bad_alloc&) { return ENOMEM; }
    catch (...) { return EIO; }
  }

  void finish(int error) noexcept {
    std::unique_ptr<AsyncPartUpload> self(this);
    http.reset();
    replay.clear();
    if (part) {
      part.reset();
      release_part_budget(state);
    }
    try {
      std::lock_guard guard(handle.mutex);
      if (creating) handle.multipart_starting = false;
      if (handle.unlinked.load(std::memory_order_acquire)) error = ESTALE;
      if (error == 0) {
        handle.page_cache_store_failed |= store_failed;
        handle.part_etags[number - 1].assign(etag.data(), etag.size());
        if (checksum_multipart_type(state.config.checksum) == "COMPOSITE") {
          handle.part_checksums[number - 1] = std::move(checksum.base64);
        }
        if (state.config.checksum == CHECKSUM_CRC64NVME ||
            state.config.checksum == CHECKSUM_CRC64XZ) {
          handle.part_checksum_values[number - 1] = checksum.integer;
          handle.part_sizes[number - 1] = length;
        }
      } else {
        fail_write(handle, error);
      }
      --handle.pending_parts;
      notify_handle(handle);
    } catch (...) {
      std::lock_guard guard(handle.mutex);
      fail_write(handle, error_code(std::current_exception()));
      --handle.pending_parts;
      notify_handle(handle);
    }
    if (admitted) state.uploads->finish_upload();
  }

  void reserve_completion(void (*run)(void*) noexcept) {
    continuation = {run, run, this};
    if (!reactor.reserve_completion(&continuation)) {
      throw std::system_error(errno, std::generic_category(), "reserve multipart completion");
    }
  }

  template<class Work>
  void local(void (*run)(void*) noexcept, Work work) {
    reserve_completion(run);
    try {
      state.uploads->submit(&handle, [this, work = std::move(work)]() mutable {
        try {
          IoExecutorScope local_only(nullptr, 0);
          work();
        } catch (...) { worker_error = std::current_exception(); }
        reactor.complete(&continuation);
      });
    } catch (...) {
      worker_error = std::current_exception();
      reactor.complete(&continuation);
    }
  }

  void prepare_replay() {
    replay.clear();
    if (!part) return;
    replay.reserve(part->segments.size());
    for (const PipeSegment& segment : part->segments) {
      if (segment.bytes == 0) continue;
      Pipe copy = Pipe::create(segment.pipe.capacity());
      if (copy.capacity() < segment.bytes) {
        throw std::system_error(ENOBUFS, std::generic_category(),
                                "multipart replay pipe is too small");
      }
      tee_exact(segment.pipe.read_fd(), copy.write_fd(), segment.bytes, 0);
      copy.close_write_end();
      replay.push_back(std::move(copy));
    }
  }

  void prepare() noexcept {
    admitted = true;
    try {
      IoExecutorScope local_only(nullptr, 0);
      if (checksum_has_digest(state.config.checksum)) {
        checksum = part ? retained_checksum(state.config.checksum, part.get())
            : checksum_file_range(state.config.checksum,
                handle.cache_entry->data_fd(), offset, length);
      }
      prepare_replay();
    } catch (...) { worker_error = std::current_exception(); }
    reactor.complete(&continuation);
  }

  static void prepared(void* context) noexcept {
    auto* self = static_cast<AsyncPartUpload*>(context);
    if (self->worker_error) {
      self->finish(error_code(self->worker_error));
      return;
    }
    self->begin();
  }

  static void awakened(void* context, ssize_t result) noexcept {
    auto* self = static_cast<AsyncPartUpload*>(context);
    if (self->waiting) {
      self->handle.reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
      self->waiting = false;
    }
    if (result < 0) {
      self->finish(-int(result));
      return;
    }
    self->begin();
  }

  void begin() noexcept {
    try {
      {
        std::lock_guard guard(handle.mutex);
        if (handle.write_state == WRITE_FAILED || handle.unlinked.load()) {
          throw std::system_error(handle.unlinked.load() ? ESTALE : handle.write_error,
                                  std::generic_category(), "multipart write failed");
        }
        if (handle.multipart_starting && !creating) {
          handle.reactor_waiters.fetch_add(1, std::memory_order_acq_rel);
          waiting = true;
        } else if (!creating && handle.upload_id.empty()) {
          handle.multipart_starting = true;
          creating = true;
        }
      }
      if (waiting) {
        wait = {};
        wait.kind       = AsyncIoRequest::READ;
        wait.fd         = handle.state_event.get();
        wait.data       = &notification;
        wait.length     = sizeof(notification);
        wait.timeout_ms = 0; // The creating request owns its network deadline.
        wait.complete   = awakened;
        wait.context    = this;
        if (reactor.submit(wait)) return;
        handle.reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
        waiting = false;
        throw std::system_error(errno, std::generic_category(), "wait multipart start");
      }
      AsyncHttpRequest args;
      if (creating) {
        args.method = "POST";
        args.path = query_path(handle.object_path, "uploads=");
        if (checksum_is_s3(state.config.checksum)) {
          args.headers.push_back({"x-amz-checksum-algorithm",
                                  checksum_s3_name(state.config.checksum)});
          args.headers.push_back({"x-amz-checksum-type",
                                  checksum_multipart_type(state.config.checksum)});
        }
        if (!handle.write_id.empty()) {
          args.headers.push_back({"x-amz-meta-ngs3fs-write-id", handle.write_id});
        }
      } else {
        args.method = "PUT";
        const std::string query = "partNumber=" + std::to_string(number) +
            "&uploadId=" + uri_encode(handle.upload_id, false);
        args.path = query_path(handle.object_path, query);
        args.upload = true;
        if (part) {
          size_t i = 0;
          args.source_segments.reserve(replay.size());
          for (const auto& segment : part->segments) {
            if (segment.bytes == 0) continue;
            args.source_segments.push_back(
                {replay[i++].read_fd(), 0, segment.bytes, false});
          }
        } else {
          args.source_fd = handle.cache_entry->data_fd();
          args.source_offset = offset;
          args.source_length = length;
        }
        append_upload_checksum(args.headers, state.config.checksum, checksum);
      }
      http = std::make_unique<AsyncSignedS3Request>(
          state, reactor, std::move(args), received, this, 1,
          creating ? kEmptyPayloadSha256 : kUnsignedPayload);
      if (!http->start()) {
        throw std::system_error(errno, std::generic_category(), "start multipart request");
      }
    } catch (...) { finish(error_code(std::current_exception())); }
  }

  static void retry_ready(void* context, ssize_t result) noexcept {
    auto* self = static_cast<AsyncPartUpload*>(context);
    if (result < 0) { self->finish(-int(result)); return; }
    try {
      self->local(prepared, [self] { self->prepare_replay(); });
    } catch (...) { self->finish(error_code(std::current_exception())); }
  }

  void retry(const Response& response) {
    const uint64_t ms = async_retry_milliseconds(response, attempt++);
    timer.reset(::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC));
    if (!timer) throw std::system_error(errno, std::generic_category(), "multipart retry timer");
    itimerspec spec{};
    spec.it_value.tv_sec = time_t(ms / 1000);
    spec.it_value.tv_nsec = long((ms % 1000) * 1'000'000);
    if (::timerfd_settime(timer.get(), 0, &spec, nullptr) != 0) {
      throw std::system_error(errno, std::generic_category(), "arm multipart retry");
    }
    wait = {};
    wait.kind = AsyncIoRequest::READ;
    wait.fd = timer.get();
    wait.data = &notification;
    wait.length = sizeof(notification);
    wait.complete = retry_ready;
    wait.context = this;
    if (!reactor.submit(wait)) {
      throw std::system_error(errno, std::generic_category(), "submit multipart retry");
    }
  }

  static void created(void* context) noexcept {
    auto* self = static_cast<AsyncPartUpload*>(context);
    if (self->worker_error) {
      self->finish(error_code(self->worker_error));
      return;
    }
    try {
      std::lock_guard guard(self->handle.mutex);
      self->handle.upload_id.assign(self->created_id.data(), self->created_id.size());
      self->handle.multipart_starting = false;
      self->creating = false;
      notify_handle(self->handle);
    } catch (...) {
      self->finish(error_code(std::current_exception()));
      return;
    }
    self->begin();
  }

  static void stored(void* context) noexcept {
    auto* self = static_cast<AsyncPartUpload*>(context);
    self->finish(error_code(self->worker_error));
  }

  static void received(void* context, Response&& response,
                       std::exception_ptr error) noexcept {
    auto* self = static_cast<AsyncPartUpload*>(context);
    self->http.reset();
    try {
      if (!error && self->state.config.directory_bucket &&
          (response.status == 401 || response.status == 403) && self->attempt < 3) {
        invalidate_express_session(self->state);
        // The response may have consumed a pipe prefix; only a fresh tee is
        // replayable. CreateMultipartUpload has no request body to replay.
        self->retry(response);
        return;
      }
      if (!self->creating && (error || retryable_response(response)) &&
          error_code(error) != ECANCELED && self->attempt < 3) {
        self->retry(response);
        return;
      }
      if (error) std::rethrow_exception(error);
      if (self->creating) {
        require_s3_success(response, "CreateMultipartUpload");
        if (checksum_is_s3(self->state.config.checksum)) {
          const auto algorithm = response.headers.find("x-amz-checksum-algorithm");
          const auto type = response.headers.find("x-amz-checksum-type");
          if (algorithm == response.headers.end() || type == response.headers.end() ||
              sso_view(algorithm->second) != checksum_s3_name(self->state.config.checksum) ||
              sso_view(type->second) != checksum_multipart_type(self->state.config.checksum)) {
            throw std::runtime_error("CreateMultipartUpload rejected its checksum");
          }
        }
        S3Xml xml(response_xml(response), "CreateMultipartUpload");
        self->created_id = xml.required_text(
            xml.result_root("InitiateMultipartUploadResult"), "UploadId");
        if (self->created_id.empty()) throw std::runtime_error("missing UploadId");
        if (self->handle.cache_entry) {
          self->local(created, [self] {
            self->handle.cache_entry->set_upload_id(sso_view(self->created_id));
          });
        } else {
          created(self);
        }
        return;
      }
      require_s3_success(response, "UploadPart");
      verify_upload_checksum(response, self->state.config.checksum,
                             self->checksum, "UploadPart");
      const auto etag = response.headers.find("etag");
      if (etag == response.headers.end() || etag->second.empty()) {
        throw std::runtime_error("UploadPart response omitted ETag");
      }
      self->etag.assign(etag->second.data(), etag->second.size());
      if (!self->part) { self->finish(0); return; }
      self->local(stored, [self] {
        self->store_failed = !store_page_cache(
            self->state, self->handle, *self->part, self->offset);
      });
    } catch (...) {
      if (self->creating) {
        fprintf(stderr, "error: CreateMultipartUpload outcome unknown; orphan cleanup may be required: %s\n",
                self->handle.object_path.c_str());
      }
      self->finish(error_code(std::current_exception()));
    }
  }
};

void submit_async_part(State& state, OpenHandle& handle, FuseReactor& reactor,
                        std::shared_ptr<RetainedPart> part, unsigned number,
                        uint64_t offset, size_t length,
                        const OpenRequestGuard* parent) {
  auto* task = new AsyncPartUpload(
      state, handle, reactor, std::move(part), number, offset, length, parent);
  try {
    task->reserve_completion(AsyncPartUpload::prepared);
  } catch (...) {
    delete task;
    throw;
  }
  try {
    state.uploads->submit_upload(&handle, [task] { task->prepare(); }, true);
  } catch (...) {
    // Admission already pins the reactor and this handle. Complete on its
    // owner even when the CPU queue rejects the preparation job.
    task->worker_error = std::current_exception();
    reactor.complete(&task->continuation);
  }
}

void submit_part(State& state, OpenHandle& handle,
                 std::shared_ptr<RetainedPart> part,
                 const OpenRequestGuard* parent = nullptr) {
  if (handle.next_part_number > kMaximumMultipartParts) {
    throw std::system_error(EFBIG, std::generic_category(),
                            "S3 multipart part limit exceeded");
  }
  const unsigned number     = handle.next_part_number;
  const bool multipart      = handle.multipart_required;
  handle.part_etags.resize(number);
  if (checksum_multipart_type(state.config.checksum) == "COMPOSITE") {
    handle.part_checksums.resize(number);
  }
  if (state.config.checksum == CHECKSUM_CRC64NVME ||
      state.config.checksum == CHECKSUM_CRC64XZ) {
    handle.part_checksum_values.resize(number);
    handle.part_sizes.resize(number);
  }
  part->number              = number;
  handle.next_part_number   = number + 1;
  handle.multipart_required = true;
  ++handle.pending_parts;
  try {
    if (FuseReactor* reactor = current_fuse_reactor();
        reactor != nullptr && !state.config.tls) {
      const size_t length = size_t(part->bytes);
      submit_async_part(state, handle, *reactor, std::move(part), number,
                         uint64_t(number - 1) * state.config.part_size, length, parent);
    } else {
      state.uploads->submit_upload(
          &handle, [&state, &handle, part = std::move(part)] {
            upload_part_job(state, handle, part);
          });
    }
  } catch (...) {
    --handle.pending_parts;
    handle.next_part_number   = number;
    handle.multipart_required = multipart;
    handle.part_etags.resize(number - 1);
    if (checksum_multipart_type(state.config.checksum) == "COMPOSITE") {
      handle.part_checksums.resize(number - 1);
    }
    if (state.config.checksum == CHECKSUM_CRC64NVME ||
        state.config.checksum == CHECKSUM_CRC64XZ) {
      handle.part_checksum_values.resize(number - 1);
      handle.part_sizes.resize(number - 1);
    }
    throw;
  }
}

void ensure_current_part(State& state, OpenHandle& handle,
                         std::unique_lock<std::mutex>& guard) {
  if (!handle.current_reservation) {
    guard.unlock();
    reserve_part_budget(state, true);
    guard.lock();
    if (handle.write_state != WRITE_OPEN) {
      guard.unlock();
      release_part_budget(state);
      guard.lock();
      throw std::system_error(EIO, std::generic_category(),
                              "write after flush");
    }
    if (handle.current_reservation) {
      guard.unlock();
      release_part_budget(state);
      guard.lock();
    } else {
      handle.current_reservation = true;
    }
  }
  if (!handle.current_part) {
    auto part = std::make_unique<RetainedPart>();
    Pipe pipe = Pipe::create(kPreferredIoSize);
    const size_t capacity      = pipe.capacity();
    const size_t segment_count = size_t(
        (state.config.part_size - 1) / capacity + 1);
    part->segments.reserve(segment_count);
    part->segments.push_back(PipeSegment{std::move(pipe), 0});
    handle.current_part = std::move(part);
  }
}

void submit_current_part(State& state, OpenHandle& handle,
                          const OpenRequestGuard* parent = nullptr) {
  if (!handle.current_part ||
      handle.current_part->bytes != state.config.part_size) {
    return;
  }
  std::shared_ptr<RetainedPart> part(handle.current_part.release());
  if (!part->segments.empty()) {
    part->segments.back().pipe.close_write_end();
  }
  handle.current_reservation   = false;
  try {
    submit_part(state, handle, std::move(part), parent);
  } catch (...) {
    release_part_budget(state);
    throw;
  }
}

void record_memory_fallback(State& state, size_t bytes) noexcept {
  std::lock_guard guard(state.metrics_mutex);
  state.fallback_write_bytes += bytes;
  const uint64_t now = fuse_monotonic_ns_noexcept();
  constexpr uint64_t interval = 5ULL * 1000ULL * 1000ULL * 1000ULL;
  if (state.fallback_warning_ns == 0 ||
      now - state.fallback_warning_ns >= interval) {
    fprintf(stderr,
            "warning: copying memory-backed FUSE_WRITE data: bytes=%zu "
            "cumulative_bytes=%" PRIu64 "\n",
            bytes, state.fallback_write_bytes);
    state.fallback_warning_ns = now;
  }
}

std::string complete_body(const OpenHandle& handle,
                          ChecksumAlgorithm algorithm) {
  std::string body;
  body.reserve(64 + handle.part_etags.size() * 144);
  body += "<CompleteMultipartUpload>";
  const std::string_view checksum_name =
      checksum_multipart_type(algorithm) == "COMPOSITE"
          ? checksum_xml_name(algorithm)
          : std::string_view{};
  for (size_t i = 0; i < handle.part_etags.size(); ++i) {
    body += "<Part><PartNumber>";
    body += std::to_string(i + 1);
    body += "</PartNumber><ETag>";
    body += handle.part_etags[i];
    body += "</ETag>";
    if (!checksum_name.empty()) {
      body.push_back('<');
      body.append(checksum_name);
      body.push_back('>');
      body.append(sso_view(handle.part_checksums[i]));
      body += "</";
      body.append(checksum_name);
      body.push_back('>');
    }
    body += "</Part>";
  }
  body += "</CompleteMultipartUpload>";
  return body;
}

ChecksumValue combined_multipart_checksum(const OpenHandle& handle,
                                           ChecksumAlgorithm algorithm) {
  if (handle.part_checksum_values.empty() ||
      handle.part_checksum_values.size() != handle.part_sizes.size()) {
    throw std::logic_error("multipart CRC64 part metadata is incomplete");
  }
  uint64_t value = handle.part_checksum_values.front();
  uint64_t size  = handle.part_sizes.front();
  for (size_t i = 1; i < handle.part_checksum_values.size(); ++i) {
    value = combine_crc64(algorithm, value,
                          handle.part_checksum_values[i],
                          handle.part_sizes[i]);
    size += handle.part_sizes[i];
  }
  if (size != handle.stream_offset) {
    throw std::logic_error("multipart CRC64 byte count is inconsistent");
  }
  return crc64_checksum_value(value);
}

Response put_object(State& state, OpenHandle& handle,
                    const RetainedPart* part) {
  ChecksumValue checksum;
  if (checksum_has_digest(state.config.checksum)) {
    checksum = retained_checksum(state.config.checksum, part);
  }
  bool ambiguous = false;
  try {
    Response response = request_with_retries([&] {
      std::vector<Header> headers{
          Header{"content-type", "application/octet-stream"},
      };
      if (!handle.write_id.empty()) {
        headers.push_back(Header{
            "x-amz-meta-ngs3fs-write-id", handle.write_id});
      }
      if (!handle.etag.empty()) {
        headers.push_back(Header{"if-match", handle.etag});
      } else if (handle.create_exclusive) {
        headers.push_back(Header{"if-none-match", "*"});
      }
      append_upload_checksum(headers, state.config.checksum, checksum);
      const bool empty = part == nullptr || part->bytes == 0;
      append_authorization(headers, state, "PUT", handle.object_path,
                           empty ? kEmptyPayloadSha256 : kUnsignedPayload);
      HttpPool::Lease client = state.http->acquire_bulk();
      if (empty) {
        client->begin_upload("PUT", handle.object_path, headers, 0);
        return client->finish_upload();
      }
      send_retained_body(client.client(), "PUT", handle.object_path,
                         headers, *part);
      return client->finish_upload();
    }, "PutObject", &ambiguous);
    if (ambiguous && response.status == 412) {
      Response recovered;
      if (recover_write_commit(state, handle, recovered)) {
        return recovered;
      }
      throw std::system_error(EIO, std::generic_category(),
                              "PutObject commit outcome unknown");
    }
    if (response.status == 412 && handle.create_exclusive) {
      throw std::system_error(EEXIST, std::generic_category(),
                              "PutObject destination exists");
    }
    require_s3_success(response, "PutObject");
    verify_upload_checksum(response, state.config.checksum,
                           checksum, "PutObject");
    return response;
  } catch (...) {
    if (ambiguous) {
      Response recovered;
      if (recover_write_commit(state, handle, recovered)) {
        return recovered;
      }
      fprintf(stderr, "error: PutObject commit outcome unknown: %s\n",
              handle.object_path.c_str());
      throw std::system_error(EIO, std::generic_category(),
                              "PutObject commit outcome unknown");
    }
    throw;
  }
}

void complete_multipart(State& state, OpenHandle& handle) {
  const std::string path = query_path(
      handle.object_path,
      "uploadId=" + uri_encode(handle.upload_id, false));
  const std::string body = complete_body(handle, state.config.checksum);
  const bool crc64 = state.config.checksum == CHECKSUM_CRC64NVME ||
                     state.config.checksum == CHECKSUM_CRC64XZ;
  const ChecksumValue checksum = crc64
      ? combined_multipart_checksum(handle, state.config.checksum)
      : ChecksumValue{};
  std::vector<Header> headers{
      Header{"content-type", "application/xml"},
  };
  if (state.config.checksum == CHECKSUM_CRC64NVME) {
    headers.push_back(Header{checksum_header_name(state.config.checksum),
                             checksum.base64});
    headers.push_back(Header{"x-amz-mp-object-size",
                             std::to_string(handle.stream_offset)});
  }
  if (!handle.etag.empty()) {
    headers.push_back(Header{"if-match", handle.etag});
  } else if (handle.create_exclusive) {
    headers.push_back(Header{"if-none-match", "*"});
  }
  bool ambiguous = false;
  Response response;
  try {
    response = request_with_retries([&] {
      std::vector<Header> request_headers = headers;
      append_authorization(request_headers, state, "POST", path,
                           kUnsignedPayload);
      HttpPool::Lease client = state.http->acquire();
      client->begin_upload("POST", path, request_headers, body.size());
      client->upload_bytes(std::span(
          reinterpret_cast<const std::byte*>(body.data()), body.size()));
      return client->finish_upload();
    }, "CompleteMultipartUpload", &ambiguous);
  } catch (...) {
    if (ambiguous) {
      Response recovered;
      if (recover_write_commit(state, handle, recovered)) {
        update_written_metadata(state, handle, recovered);
        handle.upload_id.clear();
        handle.part_etags.clear();
        handle.part_checksums.clear();
        handle.part_checksum_values.clear();
        handle.part_sizes.clear();
        return;
      }
      fprintf(stderr,
              "error: CompleteMultipartUpload commit outcome unknown: %s\n",
              handle.object_path.c_str());
      throw std::system_error(EIO, std::generic_category(),
                              "multipart commit outcome unknown");
    }
    throw;
  }
  S3ErrorInfo complete_error;
  const bool embedded_error = response.status >= 200 &&
      response.status < 300 && !response.body.empty() &&
      parse_s3_error(response_xml(response), complete_error);
  if (ambiguous &&
      (response.status < 200 || response.status >= 300 || embedded_error)) {
    Response recovered;
    if (recover_write_commit(state, handle, recovered)) {
      update_written_metadata(state, handle, recovered);
      handle.upload_id.clear();
      handle.part_etags.clear();
      handle.part_checksums.clear();
      handle.part_checksum_values.clear();
      handle.part_sizes.clear();
      return;
    }
    fprintf(stderr,
            "error: CompleteMultipartUpload commit outcome unknown: %s\n",
            handle.object_path.c_str());
    throw std::system_error(EIO, std::generic_category(),
                            "multipart commit outcome unknown");
  }
  if (response.status == 409) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "CompleteMultipartUpload conflict");
  }
  if (response.status == 412 && handle.create_exclusive) {
    throw std::system_error(EEXIST, std::generic_category(),
                            "multipart destination exists");
  }
  require_s3_success(response, "CompleteMultipartUpload");
  S3Xml xml(response_xml(response), "CompleteMultipartUpload");
  const tinyxml2::XMLElement& root =
      xml.result_root("CompleteMultipartUploadResult");
  if (state.config.checksum == CHECKSUM_CRC64NVME) {
    const std::string received = xml.required_text(
        root, checksum_xml_name(state.config.checksum));
    if (received != sso_view(checksum.base64)) {
      throw std::runtime_error(
          "CompleteMultipartUpload returned a mismatched S3 checksum");
    }
  } else if (state.config.checksum == CHECKSUM_CRC64XZ) {
    verify_upload_checksum(response, state.config.checksum,
                           checksum, "CompleteMultipartUpload");
  }
  update_written_metadata(
      state, handle, response, xml.optional_text(root, "ETag"));
  handle.upload_id.clear();
  handle.part_etags.clear();
  handle.part_checksums.clear();
  handle.part_checksum_values.clear();
  handle.part_sizes.clear();
}

void seal_write(State& state, OpenHandle& handle) {
  std::unique_lock guard(handle.mutex);
  while (handle.write_in_progress) {
    wait_handle(handle, guard);
  }
  while (handle.write_state == WRITE_SEALING) {
    wait_handle(handle, guard);
  }
  if (handle.write_state == WRITE_SEALED) {
    return;
  }
  if (handle.write_state == WRITE_FAILED) {
    throw std::system_error(handle.write_error, std::generic_category(),
                            "write handle failed");
  }
  handle.write_state = WRITE_SEALING;

  try {
    if (!handle.multipart_required) {
      std::unique_ptr<RetainedPart> part = std::move(handle.current_part);
      if (part && !part->segments.empty()) {
        part->segments.back().pipe.close_write_end();
      }
      const bool reserved = handle.current_reservation;
      handle.current_reservation = false;
      guard.unlock();
      Response response;
      try {
        response = put_object(state, handle, part.get());
      } catch (...) {
        part.reset();
        if (reserved) {
          release_part_budget(state);
        }
        throw;
      }
      if (part && !store_page_cache(state, handle, *part, 0)) {
        handle.page_cache_store_failed = true;
      }
      part.reset();
      if (reserved) {
        release_part_budget(state);
      }
      update_written_metadata(state, handle, response);
      guard.lock();
    } else {
      if (handle.current_part && handle.current_part->bytes != 0) {
        std::shared_ptr<RetainedPart> tail(handle.current_part.release());
        if (!tail->segments.empty()) {
          tail->segments.back().pipe.close_write_end();
        }
        handle.current_reservation = false;
        try {
          submit_part(state, handle, std::move(tail));
        } catch (...) {
          release_part_budget(state);
          throw;
        }
      } else if (handle.current_reservation) {
        handle.current_reservation = false;
        guard.unlock();
        release_part_budget(state);
        guard.lock();
      }
      while (handle.pending_parts != 0 &&
             handle.write_state != WRITE_FAILED) {
        wait_handle(handle, guard);
      }
      if (handle.write_state == WRITE_FAILED) {
        throw std::system_error(handle.write_error,
                                std::generic_category(),
                                "multipart upload failed");
      }
      for (size_t i = 0; i < handle.part_etags.size(); ++i) {
        if (handle.part_etags[i].empty()) {
          throw std::system_error(EIO, std::generic_category(),
                                  "multipart upload omitted a part ETag");
        }
        if (checksum_multipart_type(state.config.checksum) == "COMPOSITE" &&
            (i >= handle.part_checksums.size() ||
             handle.part_checksums[i].empty())) {
          throw std::system_error(
              EIO, std::generic_category(),
              "multipart upload omitted a part checksum");
        }
        if ((state.config.checksum == CHECKSUM_CRC64NVME ||
             state.config.checksum == CHECKSUM_CRC64XZ) &&
            (i >= handle.part_checksum_values.size() ||
             i >= handle.part_sizes.size() || handle.part_sizes[i] == 0)) {
          throw std::system_error(
              EIO, std::generic_category(),
              "multipart upload omitted CRC64 part metadata");
        }
      }
      guard.unlock();
      complete_multipart(state, handle);
      guard.lock();
    }
    handle.write_state = WRITE_SEALED;
    notify_handle(handle);
  } catch (const std::system_error& error) {
    if (!guard.owns_lock()) {
      guard.lock();
    }
    fail_write(handle, error.code().value());
    throw;
  } catch (...) {
    if (!guard.owns_lock()) {
      guard.lock();
    }
    fail_write(handle, EIO);
    throw;
  }
}

void abort_multipart(State& state, OpenHandle& handle) noexcept {
  if (handle.upload_id.empty()) {
    return;
  }
  try {
    const std::string path = query_path(
        handle.object_path,
        "uploadId=" + uri_encode(handle.upload_id, false));
    const HeaderList headers = authorization_headers(
        state, "DELETE", path, {}, kEmptyPayloadSha256);
    HttpPool::Lease client = state.http->acquire();
    client->request_no_body("DELETE", path, headers);
  } catch (const std::exception& error) {
    fprintf(stderr, "failed to abort S3 multipart upload: %s\n",
            error.what());
  }
  handle.upload_id.clear();
}

std::string destination_path(std::string_view source_path,
                             std::string_view new_name) {
  const size_t query = source_path.find('?');
  source_path = source_path.substr(0, query);
  const size_t slash = source_path.find_last_of('/');
  const std::string directory =
      slash == std::string_view::npos
          ? std::string{"/"}
          : std::string(source_path.substr(0, slash + 1));
  return directory + uri_encode(new_name, false);
}

bool rename_object_unsupported(const Response& response) {
  if (response.status == 405 || response.status == 501) {
    return true;
  }
  if (response.status != 400) {
    return false;
  }
  if (response.body.empty()) {
    return true;
  }
  S3Xml xml(response_xml(response), "RenameObject capability probe");
  if (!xml.root_is("Error")) {
    return false;
  }
  const std::string code = xml.required_text(xml.root(), "Code");
  return code == "NotImplemented" || code == "InvalidRequest";
}

bool rename_object_source_missing(const Response& response) {
  if (response.status != 400 && response.status != 404) {
    return false;
  }
  if (response.body.empty()) {
    return false;
  }
  S3Xml xml(response_xml(response), "RenameObject capability probe");
  if (!xml.root_is("Error")) {
    return false;
  }
  const std::string code = xml.required_text(xml.root(), "Code");
  return code == "NoSuchKey" || code == "NoSuchObject";
}

std::string rename_client_token() {
  static std::atomic<uint64_t> sequence{0};
  char token[64];
  const int length = snprintf(
      token, sizeof(token), "ngs3fs-%x-%llx-%llx", unsigned(::getpid()),
      static_cast<unsigned long long>(fuse_monotonic_ns()),
      static_cast<unsigned long long>(
          sequence.fetch_add(1, std::memory_order_relaxed)));
  if (length <= 0 || size_t(length) >= sizeof(token)) {
    throw std::runtime_error("unable to create RenameObject client token");
  }
  return std::string(token, size_t(length));
}

void invalidate_express_session(State& state) noexcept {
  if (!state.config.directory_bucket) {
    return;
  }
  std::lock_guard guard(state.credentials_mutex);
  state.express_expiration_ns = 0;
}

void delete_probe_object(State& state, std::string_view path) {
  const Response response = request_with_retries([&] {
    const auto authorization = authorization_headers(
        state, "DELETE", path, {}, kEmptyPayloadSha256);
    HttpPool::Lease client = state.http->acquire();
    return client->request_no_body("DELETE", path, authorization);
  }, "DeleteObject rename probe");
  if (response.status != 200 && response.status != 204 &&
      response.status != 404) {
    throw_s3_response(response,
                      "DeleteObject RenameObject capability probe");
  }
}

bool probe_rename_object(State& state, std::string_view source) {
  const std::string probe_name =
      ".ngs3fs-rename-probe-" + std::to_string(::getpid()) + '-' +
      std::to_string(fuse_monotonic_ns());
  const std::string destination = destination_path(source, probe_name);
  const std::string missing_source = destination + ".missing";
  const std::string path = destination + "?renameObject";
  std::vector headers{
      Header{"x-amz-rename-source", missing_source},
      Header{"if-none-match", "*"},
      Header{"x-amz-client-token", rename_client_token()},
  };
  const Response response = request_with_retries([&] {
    std::vector<Header> request_headers = headers;
    auto authorization = authorization_headers(
        state, "PUT", path, request_headers, kEmptyPayloadSha256);
    request_headers.insert(
        request_headers.end(),
        std::make_move_iterator(authorization.begin()),
        std::make_move_iterator(authorization.end()));
    HttpPool::Lease client = state.http->acquire();
    return client->request_no_body("PUT", path, request_headers);
  }, "RenameObject capability probe");

  if (response.status == 200 || response.status == 201 ||
      response.status == 204) {
    // A server that ignores the renameObject subresource interprets this as
    // an ordinary empty PutObject. Remove the probe object and use CopyObject.
    delete_probe_object(state, destination);
    return false;
  }
  if (rename_object_source_missing(response)) {
    return true;
  }
  if (rename_object_unsupported(response)) {
    return false;
  }
  throw_s3_response(response, "RenameObject capability probe");
}

void delete_object(State& state, std::string_view key,
                   std::string_view etag);
void put_empty_object(State& state, std::string_view path,
                      bool no_replace = false);

void abort_copy_upload(State& state, std::string_view destination,
                       std::string_view upload_id) noexcept {
  try {
    const std::string path = query_path(
        destination, "uploadId=" + uri_encode(upload_id, false));
    const HeaderList headers = authorization_headers(
        state, "DELETE", path, {}, kEmptyPayloadSha256);
    HttpPool::Lease client = state.http->acquire();
    client->request_no_body("DELETE", path, headers);
  } catch (const std::exception& error) {
    fprintf(stderr, "warning: failed to abort multipart copy: %s\n",
            error.what());
  }
}

void multipart_copy_object(State& state, uint64_t size,
                           std::string_view etag,
                           std::string_view version_id,
                           std::string_view source,
                           std::string_view destination,
                           bool no_replace) {
  const std::string create_path = query_path(destination, "uploads=");
  const HeaderList create_headers = authorization_headers(
      state, "POST", create_path, {}, kEmptyPayloadSha256);
  HttpPool::Lease create_client = state.http->acquire();
  Response created;
  try {
    created = create_client->request_no_body(
        "POST", create_path, create_headers);
  } catch (...) {
    fprintf(stderr,
            "error: multipart-copy CreateMultipartUpload outcome unknown; "
            "lifecycle cleanup may be required: %.*s\n",
            int(destination.size()), destination.data());
    throw;
  }
  require_s3_success(created, "multipart-copy CreateMultipartUpload");
  S3Xml created_xml(
      response_xml(created), "multipart-copy CreateMultipartUpload");
  const tinyxml2::XMLElement& created_root =
      created_xml.result_root("InitiateMultipartUploadResult");
  const std::string upload_id =
      created_xml.required_text(created_root, "UploadId");
  if (upload_id.empty()) {
    throw std::runtime_error(
        "multipart-copy CreateMultipartUpload omitted UploadId");
  }

  bool complete_outcome_unknown = false;
  try {
    constexpr uint64_t part_size = 1024ULL * 1024ULL * 1024ULL;
    const uint64_t part_count = (size + part_size - 1) / part_size;
    if (part_count > kMaximumMultipartParts) {
      throw std::system_error(EFBIG, std::generic_category(),
                              "multipart copy exceeds 10,000 parts");
    }
    const std::string copy_source = make_copy_source(
        state.config.bucket, source, version_id,
        authority_uses_virtual_bucket(
            state.config.authority, state.config.bucket));
    std::vector<std::string> etags;
    etags.reserve(size_t(part_count));
    for (uint64_t part = 0; part < part_count; ++part) {
      const uint64_t begin = part * part_size;
      const uint64_t end = std::min(size, begin + part_size) - 1;
      const std::string path = query_path(
          destination,
          "partNumber=" + std::to_string(part + 1) + "&uploadId=" +
              uri_encode(upload_id, false));
      const std::string range =
          "bytes=" + std::to_string(begin) + '-' + std::to_string(end);
      std::vector<Header> headers{
          Header{"x-amz-copy-source", copy_source},
          Header{"x-amz-copy-source-range", range},
      };
      if (!etag.empty()) {
        headers.push_back(
            Header{"x-amz-copy-source-if-match", etag});
      }
      const Response copied = request_with_retries([&] {
        std::vector<Header> request_headers = headers;
        HeaderList authorization = base_authorization_headers(
            state, "PUT", path, request_headers, kEmptyPayloadSha256);
        request_headers.insert(
            request_headers.end(),
            std::make_move_iterator(authorization.begin()),
            std::make_move_iterator(authorization.end()));
        HttpPool::Lease client = state.http->acquire_bulk();
        return client->request_no_body("PUT", path, request_headers);
      }, "UploadPartCopy");
      require_s3_success(copied, "UploadPartCopy");
      S3Xml copied_xml(response_xml(copied), "UploadPartCopy");
      const tinyxml2::XMLElement& copied_root =
          copied_xml.result_root("CopyPartResult");
      std::string etag = copied_xml.required_text(copied_root, "ETag");
      if (etag.empty()) {
        throw std::runtime_error("UploadPartCopy omitted ETag");
      }
      etags.push_back(std::move(etag));
    }

    std::string body;
    body.reserve(64 + etags.size() * 96);
    body += "<CompleteMultipartUpload>";
    for (size_t i = 0; i < etags.size(); ++i) {
      body += "<Part><PartNumber>" + std::to_string(i + 1) +
              "</PartNumber><ETag>" + etags[i] + "</ETag></Part>";
    }
    body += "</CompleteMultipartUpload>";
    const std::string complete_path = query_path(
        destination, "uploadId=" + uri_encode(upload_id, false));
    std::vector<Header> headers{
        Header{"content-type", "application/xml"},
    };
    if (no_replace) {
      headers.push_back(Header{"if-none-match", "*"});
    }
    Response completed = request_with_retries([&] {
      std::vector<Header> request_headers = headers;
      append_authorization(request_headers, state, "POST", complete_path,
                           kUnsignedPayload);
      HttpPool::Lease client = state.http->acquire();
      client->begin_upload(
          "POST", complete_path, request_headers, body.size());
      client->upload_bytes(std::span(
          reinterpret_cast<const std::byte*>(body.data()), body.size()));
      return client->finish_upload();
    }, "multipart-copy CompleteMultipartUpload",
       &complete_outcome_unknown);
    if (completed.status == 412 && no_replace) {
      throw std::system_error(EEXIST, std::generic_category(),
                              "multipart-copy destination exists");
    }
    require_s3_success(completed, "multipart-copy completion");
    S3Xml completed_xml(
        response_xml(completed), "multipart-copy completion");
    completed_xml.result_root("CompleteMultipartUploadResult");
  } catch (...) {
    if (complete_outcome_unknown) {
      fprintf(stderr,
              "error: multipart-copy completion outcome unknown: %.*s\n",
              int(destination.size()), destination.data());
    } else {
      abort_copy_upload(state, destination, upload_id);
    }
    throw;
  }
}

bool native_rename_supported(State& state, std::string_view source) {
  if (state.rename_object_support.load(std::memory_order_acquire) == 0) {
    std::lock_guard probe_guard(state.rename_probe_mutex);
    if (state.rename_object_support.load(std::memory_order_relaxed) == 0) {
      state.rename_object_support.store(
          probe_rename_object(state, source) ? 1 : -1,
          std::memory_order_release);
    }
  }
  return state.rename_object_support.load(std::memory_order_acquire) > 0;
}

bool try_native_rename_object(State& state, std::string_view key,
                              std::string_view etag,
                              std::string_view destination,
                              bool no_replace) {
  const std::string source = object_request_path(state, key);
  if (!native_rename_supported(state, source)) {
    return false;
  }
  const std::string rename_source = request_path_without_query(source);
  const std::string rename_path   = std::string(destination) +
      "?renameObject";
  std::vector headers{
      Header{"x-amz-rename-source", rename_source},
      Header{"x-amz-client-token", rename_client_token()},
  };
  if (!etag.empty()) {
    headers.push_back(Header{"x-amz-rename-source-if-match", etag});
  }
  if (no_replace) {
    headers.push_back(Header{"if-none-match", "*"});
  }
  const Response response = request_with_retries([&] {
    std::vector<Header> request_headers = headers;
    HeaderList authorization = authorization_headers(
        state, "PUT", rename_path, request_headers, kEmptyPayloadSha256);
    request_headers.insert(
        request_headers.end(),
        std::make_move_iterator(authorization.begin()),
        std::make_move_iterator(authorization.end()));
    HttpPool::Lease client = state.http->acquire();
    Response result = client->request_no_body(
        "PUT", rename_path, request_headers);
    if ((result.status == 401 || result.status == 403) &&
        state.config.directory_bucket) {
      invalidate_express_session(state);
      throw std::runtime_error("RenameObject session rejected");
    }
    return result;
  }, "RenameObject");
  if (response.status == 200) {
    state.rename_object_support.store(1, std::memory_order_release);
    return true;
  }
  if (response.status == 412) {
    throw std::system_error(
        no_replace ? EEXIST : ESTALE, std::generic_category(),
        no_replace ? "RenameObject destination exists"
                   : "RenameObject source changed");
  }
  if (!rename_object_unsupported(response)) {
    throw_s3_response(response, "RenameObject");
  }
  state.rename_object_support.store(-1, std::memory_order_release);
  return false;
}

void rename_remote_object(State& state, std::string_view key,
                          uint64_t size, std::string_view etag,
                          std::string_view version_id,
                          std::string_view destination,
                          bool no_replace) {
  const std::string source = object_request_path(state, key);
  const std::string_view source_etag = etag;

  if (try_native_rename_object(
          state, key, etag, destination, no_replace)) {
    return;
  }

  constexpr uint64_t kMaximumSingleCopySize =
      5ULL * 1024ULL * 1024ULL * 1024ULL;
  if (state.config.bucket.empty()) {
    throw std::system_error(EOPNOTSUPP, std::generic_category(),
                            "CopyObject fallback requires --bucket");
  }

  if (size > kMaximumSingleCopySize) {
    multipart_copy_object(
        state, size, etag, version_id, source, destination, no_replace);
  } else {
    std::vector copy_headers{
        Header{"x-amz-copy-source",
               make_copy_source(
                   state.config.bucket, source, version_id,
                   authority_uses_virtual_bucket(
                       state.config.authority, state.config.bucket))},
    };
    if (!source_etag.empty()) {
      copy_headers.push_back(
          Header{"x-amz-copy-source-if-match", source_etag});
    }
    if (no_replace) {
      copy_headers.push_back(Header{"if-none-match", "*"});
    }
    const Response copied = request_with_retries([&] {
      std::vector<Header> request_headers = copy_headers;
      auto copy_authorization = base_authorization_headers(
          state, "PUT", destination, request_headers, kEmptyPayloadSha256);
      request_headers.insert(
          request_headers.end(),
          std::make_move_iterator(copy_authorization.begin()),
          std::make_move_iterator(copy_authorization.end()));
      HttpPool::Lease client = state.http->acquire_bulk();
      return client->request_no_body("PUT", destination, request_headers);
    }, "CopyObject rename fallback");
    if (copied.status == 412) {
      throw std::system_error(
          no_replace ? EEXIST : ESTALE, std::generic_category(),
          no_replace ? "CopyObject destination exists"
                     : "CopyObject source changed");
    }
    if (copied.status != 200) {
      const bool marker_copy_unsupported =
          size == 0 && key.ends_with('/') &&
          (copied.status == 400 || copied.status == 405 ||
           copied.status == 501);
      if (marker_copy_unsupported) {
        put_empty_object(state, destination);
        delete_object(state, key, etag);
        return;
      }
      throw_s3_response(copied, "CopyObject rename fallback");
    }
    require_s3_success(copied, "CopyObject rename fallback");
    S3Xml xml(response_xml(copied), "CopyObject rename fallback");
    const tinyxml2::XMLElement& root =
        xml.result_root("CopyObjectResult");
    if (xml.required_text(root, "ETag").empty()) {
      throw std::runtime_error("CopyObject response omitted ETag");
    }
  }

  const std::string& delete_path = source;
  std::vector<Header> delete_headers;
  if (!source_etag.empty()) {
    delete_headers.push_back(Header{"if-match", source_etag});
  }
  const Response deleted = request_with_retries([&] {
    std::vector<Header> request_headers = delete_headers;
    auto delete_authorization = authorization_headers(
        state, "DELETE", delete_path, request_headers,
        kEmptyPayloadSha256);
    request_headers.insert(
        request_headers.end(),
        std::make_move_iterator(delete_authorization.begin()),
        std::make_move_iterator(delete_authorization.end()));
    HttpPool::Lease client = state.http->acquire();
    return client->request_no_body(
        "DELETE", delete_path, request_headers);
  }, "DeleteObject rename fallback");
  if (deleted.status == 412) {
    throw std::system_error(
        ESTALE, std::generic_category(),
        "CopyObject succeeded but the source changed before DeleteObject");
  }
  if (deleted.status != 200 && deleted.status != 204) {
    throw_s3_response(
        deleted, "CopyObject succeeded but DeleteObject");
  }
}

class AsyncNativeRename {
 public:
  using Complete = void (*)(void*, bool, std::exception_ptr) noexcept;

  AsyncNativeRename(State& state, FuseReactor& reactor,
                    std::string key, std::string etag,
                    std::string destination, bool no_replace,
                    Complete complete, void* context)
      : state_(state), reactor_(reactor), key_(std::move(key)),
        etag_(std::move(etag)), destination_(std::move(destination)),
        no_replace_(no_replace), complete_(complete), context_(context) {
    task_ = {run, cancel_queued, this};
  }

  bool start() noexcept {
    if (started_ || complete_ == nullptr) {
      errno = EINVAL;
      return false;
    }
    started_ = true;
    if (reactor_.post(&task_)) return true;
    started_ = false;
    return false;
  }

  void cancel() noexcept {
    cancelled_ = true;
    if (session_) session_->cancel();
    if (request_) request_->cancel();
  }

 private:
  enum Stage { PROBE, DELETE_PROBE, RENAME } stage_ = PROBE;
  enum SessionAction {
    RESUME_ADVANCE,
    SUBMIT_PROBE,
    SUBMIT_PROBE_DELETE,
    SUBMIT_RENAME,
  } session_action_ = RESUME_ADVANCE;

  static void run(void* context) noexcept {
    static_cast<AsyncNativeRename*>(context)->advance();
  }

  static void cancel_queued(void* context) noexcept {
    auto* operation = static_cast<AsyncNativeRename*>(context);
    operation->cancelled_ = true;
    try {
      throw std::system_error(ECANCELED, std::generic_category(),
                              "RenameObject cancelled");
    } catch (...) {
      operation->finish(false, std::current_exception());
    }
  }

  void finish(bool renamed, std::exception_ptr error = {}) noexcept {
    session_.reset();
    request_.reset();
    const Complete complete = complete_;
    void* const context = context_;
    complete_ = nullptr;
    complete(context, renamed, std::move(error));
  }

  void submit(AsyncHttpRequest arguments) {
    request_ = std::make_unique<AsyncS3Request>(
        state_, reactor_, std::move(arguments), received, this);
    if (!request_->start()) {
      throw std::system_error(errno, std::generic_category(),
                              "submit asynchronous RenameObject");
    }
  }

  void advance() noexcept {
    try {
      if (cancelled_) {
        throw std::system_error(ECANCELED, std::generic_category(),
                                "RenameObject cancelled");
      }
      if (!AsyncExpressSession::ready(state_)) {
        await_session(RESUME_ADVANCE);
        return;
      }
      const int support =
          state_.rename_object_support.load(std::memory_order_acquire);
      if (support < 0) {
        finish(false);
      } else if (support > 0) {
        submit_authorized(SUBMIT_RENAME);
      } else {
        submit_authorized(SUBMIT_PROBE);
      }
    } catch (...) {
      finish(false, std::current_exception());
    }
  }

  void await_session(SessionAction action) {
    session_action_ = action;
    session_ = std::make_unique<AsyncExpressSession>(
        state_, reactor_, session_ready, this);
    if (!session_->start()) {
      throw std::system_error(errno, std::generic_category(),
                              "submit S3 Express session refresh");
    }
  }

  void submit_authorized(SessionAction action) {
    if (!AsyncExpressSession::ready(state_)) {
      await_session(action);
      return;
    }
    try {
      switch (action) {
        case RESUME_ADVANCE:
          advance();
          return;
        case SUBMIT_PROBE:
          submit_probe();
          return;
        case SUBMIT_PROBE_DELETE:
          submit_probe_delete();
          return;
        case SUBMIT_RENAME:
          submit_rename();
          return;
      }
    } catch (const std::system_error& error) {
      if (error.code().value() == EAGAIN) {
        await_session(action);
        return;
      }
      throw;
    }
  }

  static void session_ready(void* context,
                            std::exception_ptr error) noexcept {
    auto* operation = static_cast<AsyncNativeRename*>(context);
    operation->session_.reset();
    if (error) {
      operation->finish(false, std::move(error));
      return;
    }
    if (operation->cancelled_) {
      try {
        throw std::system_error(ECANCELED, std::generic_category(),
                                "RenameObject cancelled");
      } catch (...) {
        operation->finish(false, std::current_exception());
      }
      return;
    }
    try {
      operation->submit_authorized(operation->session_action_);
    } catch (...) {
      operation->finish(false, std::current_exception());
    }
  }

  void submit_probe() {
    const std::string source = object_request_path(state_, key_);
    const std::string probe_name =
        ".ngs3fs-rename-probe-" + std::to_string(::getpid()) + '-' +
        std::to_string(fuse_monotonic_ns());
    probe_destination_ = destination_path(source, probe_name);
    const std::string missing_source = probe_destination_ + ".missing";
    AsyncHttpRequest arguments;
    arguments.method = "PUT";
    arguments.path = probe_destination_ + "?renameObject";
    arguments.headers = {
        Header{"x-amz-rename-source", missing_source},
        Header{"if-none-match", "*"},
        Header{"x-amz-client-token", rename_client_token()},
    };
    HeaderList authorization = authorization_headers(
        state_, "PUT", sso_view(arguments.path), arguments.headers,
        kEmptyPayloadSha256);
    arguments.headers.insert(
        arguments.headers.end(),
        std::make_move_iterator(authorization.begin()),
        std::make_move_iterator(authorization.end()));
    stage_ = PROBE;
    submit(std::move(arguments));
  }

  void submit_probe_delete() {
    AsyncHttpRequest arguments;
    arguments.method = "DELETE";
    arguments.path = probe_destination_;
    const HeaderList authorization = authorization_headers(
        state_, "DELETE", sso_view(arguments.path), {}, kEmptyPayloadSha256);
    arguments.headers.assign(authorization.begin(), authorization.end());
    stage_ = DELETE_PROBE;
    submit(std::move(arguments));
  }

  void submit_rename() {
    const std::string source = object_request_path(state_, key_);
    AsyncHttpRequest arguments;
    arguments.method = "PUT";
    arguments.path = destination_ + "?renameObject";
    arguments.headers = {
        Header{"x-amz-rename-source", request_path_without_query(source)},
        Header{"x-amz-client-token", rename_client_token()},
    };
    if (!etag_.empty()) {
      arguments.headers.push_back(
          Header{"x-amz-rename-source-if-match", etag_});
    }
    if (no_replace_) {
      arguments.headers.push_back(Header{"if-none-match", "*"});
    }
    HeaderList authorization = authorization_headers(
        state_, "PUT", sso_view(arguments.path), arguments.headers,
        kEmptyPayloadSha256);
    arguments.headers.insert(
        arguments.headers.end(),
        std::make_move_iterator(authorization.begin()),
        std::make_move_iterator(authorization.end()));
    stage_ = RENAME;
    submit(std::move(arguments));
  }

  static void received(void* context, Response&& response,
                       std::exception_ptr error) noexcept {
    auto* operation = static_cast<AsyncNativeRename*>(context);
    Response value = std::move(response);
    operation->request_.reset();
    try {
      if (error) std::rethrow_exception(error);
      if ((value.status == 401 || value.status == 403) &&
          operation->state_.config.directory_bucket &&
          operation->session_attempt_++ < 3) {
        invalidate_express_session(operation->state_);
        switch (operation->stage_) {
          case PROBE:
            operation->submit_authorized(SUBMIT_PROBE);
            return;
          case DELETE_PROBE:
            operation->submit_authorized(SUBMIT_PROBE_DELETE);
            return;
          case RENAME:
            operation->submit_authorized(SUBMIT_RENAME);
            return;
        }
      }
      operation->session_attempt_ = 0;
      if (operation->cancelled_) {
        throw std::system_error(ECANCELED, std::generic_category(),
                                "RenameObject cancelled");
      }
      switch (operation->stage_) {
        case PROBE:
          operation->probe_received(value);
          return;
        case DELETE_PROBE:
          if (value.status != 200 && value.status != 204 &&
              value.status != 404) {
            throw_s3_response(
                value, "DeleteObject RenameObject capability probe");
          }
          operation->state_.rename_object_support.store(
              -1, std::memory_order_release);
          operation->finish(false);
          return;
        case RENAME:
          operation->rename_received(value);
          return;
      }
    } catch (...) {
      operation->finish(false, std::current_exception());
    }
  }

  void probe_received(const Response& response) {
    if (response.status == 200 || response.status == 201 ||
        response.status == 204) {
      submit_authorized(SUBMIT_PROBE_DELETE);
      return;
    }
    if (rename_object_source_missing(response)) {
      state_.rename_object_support.store(1, std::memory_order_release);
      submit_authorized(SUBMIT_RENAME);
      return;
    }
    if (rename_object_unsupported(response)) {
      state_.rename_object_support.store(-1, std::memory_order_release);
      finish(false);
      return;
    }
    throw_s3_response(response, "RenameObject capability probe");
  }

  void rename_received(const Response& response) {
    if (response.status == 200) {
      state_.rename_object_support.store(1, std::memory_order_release);
      finish(true);
      return;
    }
    if (response.status == 412) {
      throw std::system_error(
          no_replace_ ? EEXIST : ESTALE, std::generic_category(),
          no_replace_ ? "RenameObject destination exists"
                      : "RenameObject source changed");
    }
    if (!rename_object_unsupported(response)) {
      throw_s3_response(response, "RenameObject");
    }
    state_.rename_object_support.store(-1, std::memory_order_release);
    finish(false);
  }

  State& state_;
  FuseReactor& reactor_;
  std::string key_;
  std::string etag_;
  std::string destination_;
  std::string probe_destination_;
  bool no_replace_;
  Complete complete_;
  void* context_;
  std::unique_ptr<AsyncExpressSession> session_;
  std::unique_ptr<AsyncS3Request> request_;
  FuseReactor::ReactorTask task_;
  unsigned session_attempt_ = 0;
  bool started_ = false;
  bool cancelled_ = false;
};

class AsyncRemoteRename {
 public:
  using Complete = void (*)(void*, std::exception_ptr) noexcept;

  AsyncRemoteRename(State& state, FuseReactor& reactor,
                    std::string key, uint64_t size, std::string etag,
                    std::string version_id, std::string destination,
                    bool no_replace, Complete complete, void* context)
      : state_(state), reactor_(reactor), key_(std::move(key)), size_(size),
        etag_(std::move(etag)), version_id_(std::move(version_id)),
        destination_(std::move(destination)), no_replace_(no_replace),
        complete_(complete), context_(context),
        source_(object_request_path(state_, key_)) {
    native_ = std::make_unique<AsyncNativeRename>(
        state_, reactor_, key_, etag_, destination_, no_replace_,
        native_complete, this);
  }

  bool start() noexcept {
    if (started_ || complete_ == nullptr) {
      errno = EINVAL;
      return false;
    }
    started_ = true;
    if (!native_->start()) {
      native_.reset();
      started_ = false;
      return false;
    }
    return true;
  }

  void cancel() noexcept {
    cancelled_ = true;
    if (native_) native_->cancel();
    if (session_) session_->cancel();
    if (request_) request_->cancel();
  }

 private:
  enum Stage {
    COPY_OBJECT,
    PUT_MARKER,
    DELETE_SOURCE,
    CREATE_MULTIPART,
    COPY_PART,
    COMPLETE_MULTIPART,
    ABORT_MULTIPART,
  } stage_ = COPY_OBJECT;

  static constexpr uint64_t kMaximumSingleCopySize =
      5ULL * 1024ULL * 1024ULL * 1024ULL;
  static constexpr uint64_t kCopyPartSize =
      1024ULL * 1024ULL * 1024ULL;

  void finish(std::exception_ptr error = {}) noexcept {
    native_.reset();
    session_.reset();
    request_.reset();
    const Complete complete = complete_;
    void* const context = context_;
    complete_ = nullptr;
    complete(context, std::move(error));
  }

  void submit(AsyncHttpRequest arguments) {
    request_ = std::make_unique<AsyncS3Request>(
        state_, reactor_, std::move(arguments), received, this);
    if (!request_->start()) {
      throw std::system_error(errno, std::generic_category(),
                              "submit asynchronous remote rename");
    }
  }

  static bool needs_express_session(Stage stage) noexcept {
    switch (stage) {
      case PUT_MARKER:
      case DELETE_SOURCE:
      case CREATE_MULTIPART:
      case COMPLETE_MULTIPART:
      case ABORT_MULTIPART:
        return true;
      case COPY_OBJECT:
      case COPY_PART:
        return false;
    }
    return false;
  }

  void submit_stage(Stage stage) {
    switch (stage) {
      case COPY_OBJECT:
        submit_copy_object();
        return;
      case PUT_MARKER:
        submit_empty_marker();
        return;
      case DELETE_SOURCE:
        submit_delete_source();
        return;
      case CREATE_MULTIPART:
        submit_create_multipart();
        return;
      case COPY_PART:
        submit_copy_part();
        return;
      case COMPLETE_MULTIPART:
        submit_complete_multipart();
        return;
      case ABORT_MULTIPART:
        submit_abort_multipart();
        return;
    }
  }

  void await_session(Stage stage) {
    resume_stage_ = stage;
    session_ = std::make_unique<AsyncExpressSession>(
        state_, reactor_, session_ready, this);
    if (!session_->start()) {
      throw std::system_error(errno, std::generic_category(),
                              "submit S3 Express session refresh");
    }
  }

  void schedule(Stage stage) {
    if (cancelled_ && stage != ABORT_MULTIPART) {
      throw std::system_error(ECANCELED, std::generic_category(),
                              "remote rename cancelled");
    }
    if (needs_express_session(stage) &&
        !AsyncExpressSession::ready(state_)) {
      await_session(stage);
      return;
    }
    try {
      submit_stage(stage);
    } catch (const CredentialRefreshPending&) {
      await_session(stage);
      return;
    } catch (const std::system_error& error) {
      if (needs_express_session(stage) && error.code().value() == EAGAIN) {
        await_session(stage);
        return;
      }
      throw;
    }
  }

  static void session_ready(void* context,
                            std::exception_ptr error) noexcept {
    auto* operation = static_cast<AsyncRemoteRename*>(context);
    operation->session_.reset();
    if (error) {
      if (operation->resume_stage_ == ABORT_MULTIPART &&
          operation->pending_error_) {
        operation->warn_abort_failure(std::move(error));
      } else {
        operation->fail_or_abort(std::move(error));
      }
      return;
    }
    try {
      operation->schedule(operation->resume_stage_);
    } catch (...) {
      operation->fail_or_abort(std::current_exception());
    }
  }

  static void native_complete(void* context, bool renamed,
                              std::exception_ptr error) noexcept {
    auto* operation = static_cast<AsyncRemoteRename*>(context);
    operation->native_.reset();
    if (error) {
      operation->finish(std::move(error));
      return;
    }
    if (renamed) {
      operation->finish();
      return;
    }
    try {
      operation->start_fallback();
    } catch (...) {
      operation->finish(std::current_exception());
    }
  }

  void start_fallback() {
    if (cancelled_) {
      throw std::system_error(ECANCELED, std::generic_category(),
                              "remote rename cancelled");
    }
    if (state_.config.bucket.empty()) {
      throw std::system_error(EOPNOTSUPP, std::generic_category(),
                              "CopyObject fallback requires --bucket");
    }
    if (size_ > kMaximumSingleCopySize) {
      const uint64_t part_count =
          (size_ + kCopyPartSize - 1) / kCopyPartSize;
      if (part_count > kMaximumMultipartParts) {
        throw std::system_error(EFBIG, std::generic_category(),
                                "multipart copy exceeds 10,000 parts");
      }
      schedule(CREATE_MULTIPART);
    } else {
      schedule(COPY_OBJECT);
    }
  }

  std::string copy_source() const {
    return make_copy_source(
        state_.config.bucket, source_, version_id_,
        authority_uses_virtual_bucket(
            state_.config.authority, state_.config.bucket));
  }

  void submit_copy_object() {
    AsyncHttpRequest arguments;
    arguments.method = "PUT";
    arguments.path = destination_;
    arguments.headers = {Header{"x-amz-copy-source", copy_source()}};
    if (!etag_.empty()) {
      arguments.headers.push_back(
          Header{"x-amz-copy-source-if-match", etag_});
    }
    if (no_replace_) {
      arguments.headers.push_back(Header{"if-none-match", "*"});
    }
    HeaderList authorization = base_authorization_headers(
        state_, "PUT", sso_view(arguments.path), arguments.headers,
        kEmptyPayloadSha256);
    arguments.headers.insert(
        arguments.headers.end(),
        std::make_move_iterator(authorization.begin()),
        std::make_move_iterator(authorization.end()));
    stage_ = COPY_OBJECT;
    submit(std::move(arguments));
  }

  void submit_empty_marker() {
    AsyncHttpRequest arguments;
    arguments.method = "PUT";
    arguments.path = destination_;
    const HeaderList authorization = authorization_headers(
        state_, "PUT", sso_view(arguments.path), {}, kEmptyPayloadSha256);
    arguments.headers.assign(authorization.begin(), authorization.end());
    stage_ = PUT_MARKER;
    submit(std::move(arguments));
  }

  void submit_delete_source() {
    AsyncHttpRequest arguments;
    arguments.method = "DELETE";
    arguments.path = source_;
    if (!etag_.empty()) {
      arguments.headers.push_back(Header{"if-match", etag_});
    }
    HeaderList authorization = authorization_headers(
        state_, "DELETE", sso_view(arguments.path), arguments.headers,
        kEmptyPayloadSha256);
    arguments.headers.insert(
        arguments.headers.end(),
        std::make_move_iterator(authorization.begin()),
        std::make_move_iterator(authorization.end()));
    stage_ = DELETE_SOURCE;
    submit(std::move(arguments));
  }

  void submit_create_multipart() {
    AsyncHttpRequest arguments;
    arguments.method = "POST";
    arguments.path = query_path(destination_, "uploads=");
    const HeaderList authorization = authorization_headers(
        state_, "POST", sso_view(arguments.path), {}, kEmptyPayloadSha256);
    arguments.headers.assign(authorization.begin(), authorization.end());
    stage_ = CREATE_MULTIPART;
    submit(std::move(arguments));
  }

  void submit_copy_part() {
    const uint64_t begin = part_index_ * kCopyPartSize;
    const uint64_t end = std::min(size_, begin + kCopyPartSize) - 1;
    AsyncHttpRequest arguments;
    arguments.method = "PUT";
    arguments.path = query_path(
        destination_, "partNumber=" + std::to_string(part_index_ + 1) +
            "&uploadId=" + uri_encode(upload_id_, false));
    arguments.headers = {
        Header{"x-amz-copy-source", copy_source()},
        Header{"x-amz-copy-source-range",
               "bytes=" + std::to_string(begin) + '-' +
                   std::to_string(end)},
    };
    if (!etag_.empty()) {
      arguments.headers.push_back(
          Header{"x-amz-copy-source-if-match", etag_});
    }
    HeaderList authorization = base_authorization_headers(
        state_, "PUT", sso_view(arguments.path), arguments.headers,
        kEmptyPayloadSha256);
    arguments.headers.insert(
        arguments.headers.end(),
        std::make_move_iterator(authorization.begin()),
        std::make_move_iterator(authorization.end()));
    stage_ = COPY_PART;
    submit(std::move(arguments));
  }

  void submit_complete_multipart() {
    std::string body;
    body.reserve(64 + part_etags_.size() * 96);
    body += "<CompleteMultipartUpload>";
    for (size_t i = 0; i < part_etags_.size(); ++i) {
      body += "<Part><PartNumber>" + std::to_string(i + 1) +
              "</PartNumber><ETag>" + part_etags_[i] +
              "</ETag></Part>";
    }
    body += "</CompleteMultipartUpload>";

    AsyncHttpRequest arguments;
    arguments.method = "POST";
    arguments.path = query_path(
        destination_, "uploadId=" + uri_encode(upload_id_, false));
    arguments.headers = {Header{"content-type", "application/xml"}};
    if (no_replace_) {
      arguments.headers.push_back(Header{"if-none-match", "*"});
    }
    append_authorization(arguments.headers, state_, "POST",
                         sso_view(arguments.path), kUnsignedPayload);
    arguments.upload = true;
    arguments.body.resize(body.size());
    memcpy(arguments.body.data(), body.data(), body.size());
    stage_ = COMPLETE_MULTIPART;
    submit(std::move(arguments));
  }

  void submit_abort_multipart() {
    AsyncHttpRequest arguments;
    arguments.method = "DELETE";
    arguments.path = query_path(
        destination_, "uploadId=" + uri_encode(upload_id_, false));
    const HeaderList authorization = authorization_headers(
        state_, "DELETE", sso_view(arguments.path), {}, kEmptyPayloadSha256);
    arguments.headers.assign(authorization.begin(), authorization.end());
    stage_ = ABORT_MULTIPART;
    submit(std::move(arguments));
  }

  static void received(void* context, Response&& response,
                       std::exception_ptr error) noexcept {
    auto* operation = static_cast<AsyncRemoteRename*>(context);
    Response value = std::move(response);
    operation->request_.reset();
    if (!error && (value.status == 401 || value.status == 403) &&
        operation->state_.config.directory_bucket &&
        needs_express_session(operation->stage_) &&
        operation->session_attempt_++ < 3) {
      invalidate_express_session(operation->state_);
      try {
        operation->schedule(operation->stage_);
      } catch (...) {
        if (operation->stage_ == ABORT_MULTIPART &&
            operation->pending_error_) {
          operation->warn_abort_failure(std::current_exception());
        } else {
          operation->fail_or_abort(std::current_exception());
        }
      }
      return;
    }
    if (!error) operation->session_attempt_ = 0;
    if (operation->stage_ == ABORT_MULTIPART) {
      if (error) {
        try {
          std::rethrow_exception(error);
        } catch (const std::exception& failure) {
          fprintf(stderr, "warning: failed to abort multipart copy: %s\n",
                  failure.what());
        } catch (...) {
          fprintf(stderr, "warning: failed to abort multipart copy\n");
        }
      }
      operation->upload_id_.clear();
      operation->finish(std::move(operation->pending_error_));
      return;
    }
    if (error) {
      if (operation->stage_ == CREATE_MULTIPART) {
        fprintf(stderr,
                "error: multipart-copy CreateMultipartUpload outcome unknown; "
                "lifecycle cleanup may be required: %s\n",
                operation->destination_.c_str());
        operation->finish(std::move(error));
      } else if (operation->stage_ == COMPLETE_MULTIPART) {
        fprintf(stderr,
                "error: multipart-copy completion outcome unknown: %s\n",
                operation->destination_.c_str());
        operation->finish(std::move(error));
      } else {
        operation->fail_or_abort(std::move(error));
      }
      return;
    }
    try {
      if (operation->cancelled_) {
        throw std::system_error(ECANCELED, std::generic_category(),
                                "remote rename cancelled");
      }
      operation->response_received(value);
    } catch (...) {
      operation->fail_or_abort(std::current_exception());
    }
  }

  void fail_or_abort(std::exception_ptr error) noexcept {
    if (upload_id_.empty()) {
      finish(std::move(error));
      return;
    }
    pending_error_ = std::move(error);
    try {
      schedule(ABORT_MULTIPART);
    } catch (const std::exception& failure) {
      fprintf(stderr, "warning: failed to abort multipart copy: %s\n",
              failure.what());
      upload_id_.clear();
      finish(std::move(pending_error_));
    } catch (...) {
      fprintf(stderr, "warning: failed to abort multipart copy\n");
      upload_id_.clear();
      finish(std::move(pending_error_));
    }
  }

  void warn_abort_failure(std::exception_ptr error) noexcept {
    try {
      if (error) std::rethrow_exception(error);
    } catch (const std::exception& failure) {
      fprintf(stderr, "warning: failed to abort multipart copy: %s\n",
              failure.what());
    } catch (...) {
      fprintf(stderr, "warning: failed to abort multipart copy\n");
    }
    upload_id_.clear();
    finish(std::move(pending_error_));
  }

  void response_received(const Response& response) {
    switch (stage_) {
      case COPY_OBJECT:
        copy_received(response);
        return;
      case PUT_MARKER:
        require_s3_success(response, "PutObject rename marker fallback");
        schedule(DELETE_SOURCE);
        return;
      case DELETE_SOURCE:
        if (response.status == 412) {
          throw std::system_error(
              ESTALE, std::generic_category(),
              "CopyObject succeeded but the source changed before DeleteObject");
        }
        if (response.status != 200 && response.status != 204) {
          throw_s3_response(response, "CopyObject succeeded but DeleteObject");
        }
        finish();
        return;
      case CREATE_MULTIPART:
        create_received(response);
        return;
      case COPY_PART:
        part_received(response);
        return;
      case COMPLETE_MULTIPART:
        complete_received(response);
        return;
      case ABORT_MULTIPART:
        return;
    }
  }

  void copy_received(const Response& response) {
    if (response.status == 412) {
      throw std::system_error(
          no_replace_ ? EEXIST : ESTALE, std::generic_category(),
          no_replace_ ? "CopyObject destination exists"
                      : "CopyObject source changed");
    }
    if (response.status != 200) {
      const bool marker_copy_unsupported =
          size_ == 0 && key_.ends_with('/') &&
          (response.status == 400 || response.status == 405 ||
           response.status == 501);
      if (marker_copy_unsupported) {
        schedule(PUT_MARKER);
        return;
      }
      throw_s3_response(response, "CopyObject rename fallback");
    }
    require_s3_success(response, "CopyObject rename fallback");
    S3Xml xml(response_xml(response), "CopyObject rename fallback");
    const tinyxml2::XMLElement& root = xml.result_root("CopyObjectResult");
    if (xml.required_text(root, "ETag").empty()) {
      throw std::runtime_error("CopyObject response omitted ETag");
    }
    schedule(DELETE_SOURCE);
  }

  void create_received(const Response& response) {
    require_s3_success(response, "multipart-copy CreateMultipartUpload");
    S3Xml xml(response_xml(response), "multipart-copy CreateMultipartUpload");
    const tinyxml2::XMLElement& root =
        xml.result_root("InitiateMultipartUploadResult");
    upload_id_ = xml.required_text(root, "UploadId");
    if (upload_id_.empty()) {
      throw std::runtime_error(
          "multipart-copy CreateMultipartUpload omitted UploadId");
    }
    part_etags_.reserve(size_t(
        (size_ + kCopyPartSize - 1) / kCopyPartSize));
    schedule(COPY_PART);
  }

  void part_received(const Response& response) {
    require_s3_success(response, "UploadPartCopy");
    S3Xml xml(response_xml(response), "UploadPartCopy");
    const tinyxml2::XMLElement& root = xml.result_root("CopyPartResult");
    std::string etag = xml.required_text(root, "ETag");
    if (etag.empty()) {
      throw std::runtime_error("UploadPartCopy omitted ETag");
    }
    part_etags_.push_back(std::move(etag));
    ++part_index_;
    if (part_index_ * kCopyPartSize < size_) {
      schedule(COPY_PART);
    } else {
      schedule(COMPLETE_MULTIPART);
    }
  }

  void complete_received(const Response& response) {
    if (response.status == 412 && no_replace_) {
      throw std::system_error(
          EEXIST, std::generic_category(),
          "multipart-copy destination exists");
    }
    require_s3_success(response, "multipart-copy completion");
    S3Xml xml(response_xml(response), "multipart-copy completion");
    xml.result_root("CompleteMultipartUploadResult");
    upload_id_.clear();
    schedule(DELETE_SOURCE);
  }

  State& state_;
  FuseReactor& reactor_;
  std::string key_;
  uint64_t size_;
  std::string etag_;
  std::string version_id_;
  std::string destination_;
  bool no_replace_;
  Complete complete_;
  void* context_;
  std::string source_;
  std::unique_ptr<AsyncNativeRename> native_;
  std::unique_ptr<AsyncExpressSession> session_;
  std::unique_ptr<AsyncS3Request> request_;
  std::string upload_id_;
  std::vector<std::string> part_etags_;
  std::exception_ptr pending_error_;
  uint64_t part_index_ = 0;
  unsigned session_attempt_ = 0;
  Stage resume_stage_ = COPY_OBJECT;
  bool started_ = false;
  bool cancelled_ = false;
};

void ngs3fs_init(void* userdata, fuse_conn_info* connection) {
  auto& state = *static_cast<State*>(userdata);
  connection->no_interrupt = 1;
  unsigned int desired = FUSE_CAP_ASYNC_READ | FUSE_CAP_ATOMIC_O_TRUNC;
  if ((connection->capable & FUSE_CAP_EXPLICIT_INVAL_DATA) != 0) {
    desired |= FUSE_CAP_EXPLICIT_INVAL_DATA;
    connection->want &= ~unsigned(FUSE_CAP_AUTO_INVAL_DATA);
  }
  if (state.splice_available) {
    desired |= FUSE_CAP_SPLICE_READ | FUSE_CAP_SPLICE_WRITE |
               FUSE_CAP_SPLICE_MOVE;
  }
  connection->want &= ~unsigned(FUSE_CAP_WRITEBACK_CACHE);
  connection->want |= connection->capable & desired;
  state.atomic_o_trunc =
      (connection->want & FUSE_CAP_ATOMIC_O_TRUNC) != 0;
  connection->max_readahead = kKernelReadAheadSize;
  connection->max_read = static_cast<unsigned int>(std::min<size_t>(
      state.config.maximum_read_size,
      std::numeric_limits<unsigned int>::max()));
  connection->max_write = static_cast<unsigned int>(std::min<size_t>(
      state.config.maximum_write_size,
      std::numeric_limits<unsigned int>::max()));
}

void forget_inode(fuse_ino_t inode, uint64_t count) noexcept {
  if (inode == FUSE_ROOT_ID) {
    return;
  }
  InodeBase* item = reinterpret_cast<InodeBase*>(uintptr_t(inode));
  if (item == nullptr) {
    return;
  }
  uint32_t value = item->nlookup.load(std::memory_order_relaxed);
  for (;;) {
    const uint32_t next = count >= value ? 0 : value - uint32_t(count);
    if (item->nlookup.compare_exchange_weak(
            value, next, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
      return;
    }
  }
}

void ngs3fs_forget(fuse_req_t request, fuse_ino_t inode,
                   uint64_t count) {
  State& state = state_from(request);
  forget_inode(inode, count);
  sweep_retired_items(state);
  fuse_reply_none(request);
}

void ngs3fs_forget_multi(fuse_req_t request, size_t count,
                         fuse_forget_data* forgets) {
  State& state = state_from(request);
  for (size_t i = 0; i < count; ++i) {
    forget_inode(forgets[i].ino, forgets[i].nlookup);
  }
  sweep_retired_items(state);
  fuse_reply_none(request);
}

void reply_lookup_result(fuse_req_t request, fuse_ino_t parent, const char* name) {
  if (name == nullptr || !valid_fuse_component(name)) {
    fuse_reply_err(request, ENOENT);
    return;
  }
  try {
    State& state = state_from(request);
    InodeBase& directory = inode_item(state, parent);
    if (!directory.directory()) {
      throw std::system_error(ENOTDIR, std::generic_category(), "lookup");
    }
    const double timeout = remaining_directory_timeout(directory);
    fuse_ino_t inode = 0;
    fuse_entry_param entry{};
    {
      Directory& children = directory.dir_children();
      std::shared_lock guard(children.mutex);
      const auto i = children.find(
          terark::fstring(name, ptrdiff_t(strlen(name))));
      InodeBase* item = i == children.end() ? nullptr : i->second;
      if (item != nullptr && !item->detached()) {
        retain_inode_count(item->nlookup, "inode lookup count");
        inode            = item_inode(item);
        entry.ino        = inode;
        entry.generation = 1;
        fill_inode_stat(state, inode, *item, entry.attr);
      }
    }
    if (inode == 0) {
      fuse_entry_param negative{};
      negative.entry_timeout = timeout;
      fuse_reply_entry(request, &negative);
      return;
    }

    entry.attr_timeout  = timeout;
    entry.entry_timeout = timeout;
    if (fuse_reply_entry(request, &entry) != 0) {
      forget_inode(inode, 1);
      sweep_retired_items(state);
    }
  } catch (...) {
    reply_callback_error(request);
  }
}

struct AsyncLookup {
  State& state;
  FuseReactor& reactor;
  fuse_req_t request;
  fuse_ino_t inode;
  ssostr<248> name;
  InodePin pin;
  DirectoryContinuation waiter;

  AsyncLookup(State& s, FuseReactor& r, fuse_req_t req,
               fuse_ino_t ino, const char* component)
      : state(s), reactor(r), request(req), inode(ino), name(component) {
    InodeBase& item = inode_item(state, inode);
    retain_inode_count(item.open_count, "lookup directory pin");
    pin = InodePin(state, item);
    waiter.complete = completed;
    waiter.context = this;
  }

  static void completed(void* context, std::exception_ptr error) noexcept {
    std::unique_ptr<AsyncLookup> task(static_cast<AsyncLookup*>(context));
    try {
      if (error) std::rethrow_exception(error);
      reply_lookup_result(task->request, task->inode, task->name.c_str());
    } catch (...) {
      reply_callback_error(task->request);
    }
  }

  void start() noexcept {
    try {
      if (!await_directory_refresh(state, reactor, inode, waiter)) completed(this, {});
    } catch (...) {
      completed(this, std::current_exception());
    }
  }
};

void ngs3fs_lookup(fuse_req_t request, fuse_ino_t parent, const char* name) {
  if (name == nullptr || !valid_fuse_component(name)) {
    fuse_reply_err(request, ENOENT);
    return;
  }
  try {
    State& state = state_from(request);
    InodeBase& item = inode_item(state, parent);
    if (!item.directory()) {
      throw std::system_error(ENOTDIR, std::generic_category(), "lookup");
    }
    cache_touch(static_cast<InodeDir&>(item));
    if (FuseReactor* reactor = current_fuse_reactor()) {
      if (item.expire.load(std::memory_order_acquire) <= fuse_monotonic_ns()) {
        (new AsyncLookup(state, *reactor, request, parent, name))->start();
        return;
      }
    } else {
      refresh_directory(state, parent);
    }
    reply_lookup_result(request, parent, name);
  } catch (...) {
    reply_callback_error(request);
  }
}

void ngs3fs_getattr(fuse_req_t request, fuse_ino_t inode,
                    fuse_file_info* file) {
  try {
    State& state = state_from(request);
    struct stat status{};
    InodeBase& item = inode_reference(state, inode);
    const bool directory = item.directory();
    fill_inode_stat(state, inode, item, status);
    OpenHandle* handle = directory ? nullptr : handle_optional(file);
    if (handle != nullptr) {
      std::lock_guard handle_guard(handle->mutex);
      status.st_size = off_t(handle->size);
    }
    fuse_reply_attr(request, &status, remaining_inode_timeout(state, item));
  } catch (...) {
    reply_callback_error(request);
  }
}

struct AsyncLocalCacheWork {
  using Work = void (*)(void*);
  using Complete = void (*)(void*, std::exception_ptr) noexcept;
  FuseReactor::ReactorTask ticket;
  FuseReactor* reactor = nullptr;
  Work work = nullptr;
  Complete complete = nullptr;
  void* context = nullptr;
  std::exception_ptr failure;

  void start(State& state, FuseReactor& owner, Work run,
               Complete done, void* value) {
    reactor = &owner;
    work = run;
    complete = done;
    context = value;
    failure = {};
    ticket = {completed, completed, this};
    if (!owner.reserve_completion(&ticket)) {
      throw std::system_error(errno, std::generic_category(), "reserve local cache work");
    }
    try {
      state.uploads->submit(value, [this] {
        try {
          IoExecutorScope local_only(nullptr, 0);
          work(context);
        } catch (...) { failure = std::current_exception(); }
        reactor->complete(&ticket);
      });
    } catch (...) {
      failure = std::current_exception();
      owner.complete(&ticket);
    }
  }

  static void completed(void* value) noexcept {
    auto* task = static_cast<AsyncLocalCacheWork*>(value);
    task->complete(task->context, std::move(task->failure));
  }
};

struct AsyncCacheRetirement {
  State* state = nullptr;
  FuseReactor* reactor = nullptr;
  std::shared_ptr<CacheEntry> entry;
  FuseReactor::ReactorTask ticket;
  AsyncIoRequest wait;
  ssostr<248> key;
  ssostr<64> etag;
  ssostr<64> version;
  CacheIdentity identity{};
  uint64_t notification = 0;
  int wait_fd = -1;
  bool reuse = false;
  bool preserve = false;
  bool ready_result = false;
  std::exception_ptr failure;
  void (*complete)(void*, std::exception_ptr) noexcept = nullptr;
  void* context = nullptr;

  bool ready(State& s, FuseReactor& r, std::string_view name,
               const CacheIdentity* value = nullptr, bool keep = false) {
    if (ready_result) {
      ready_result = false;
      if (sso_view(key) == name && reuse == (value != nullptr) && preserve == keep &&
          (value == nullptr || (sso_view(etag) == value->etag &&
              sso_view(version) == value->version_id &&
              identity.size == value->size && identity.mtime == value->mtime))) {
        return true;
      }
    }
    state = &s;
    reactor = &r;
    key = name;
    reuse = value != nullptr;
    preserve = keep;
    etag = value ? value->etag : std::string_view{};
    version = value ? value->version_id : std::string_view{};
    identity = CacheIdentity{
        .key = sso_view(key), .etag = sso_view(etag),
        .version_id = sso_view(version),
        .size = value ? value->size : 0, .mtime = value ? value->mtime : 0,
    };
    failure = {};
    schedule();
    return false;
  }

  void schedule() {
    ticket = {completed, completed, this};
    if (!reactor->reserve_completion(&ticket)) {
      throw std::system_error(errno, std::generic_category(), "reserve cache retirement");
    }
    try {
      state->uploads->submit(this, [this] {
        {
          IoExecutorScope local_only(nullptr, 0);
          work();
        }
        reactor->complete(&ticket);
      });
    } catch (...) {
      failure = std::current_exception();
      // Only allocation/admission failure can take this short local cleanup
      // path. No generation wait, filesystem operation or network runs here.
      if (entry) {
        fprintf(stderr, "warning: cache retirement worker unavailable; "
                        "unregistering its waiter inline\n");
        entry->end_async_wait();
        entry.reset();
        wait_fd = -1;
      }
      reactor->complete(&ticket);
    }
  }

  void work() noexcept {
    try {
      if (entry) {
        entry->end_async_wait();
        entry.reset();
        wait_fd = -1;
      }
      if (failure) return;
      for (;;) {
        entry = state->local_cache->retiring_entry(
            sso_view(key), reuse ? &identity : nullptr, preserve);
        if (!entry) return;
        wait_fd = entry->begin_retire_wait();
        if (wait_fd >= 0) return;
        entry.reset();
      }
    } catch (...) {
      failure = std::current_exception();
      entry.reset();
      wait_fd = -1;
    }
  }

  static void completed(void* context) noexcept {
    auto* task = static_cast<AsyncCacheRetirement*>(context);
    if (task->failure || !task->entry) {
      task->ready_result = !task->failure;
      task->complete(task->context, std::move(task->failure));
      return;
    }
    task->wait = {};
    task->wait.kind = AsyncIoRequest::READ;
    task->wait.fd = task->wait_fd;
    task->wait.data = &task->notification;
    task->wait.length = sizeof(task->notification);
    task->wait.timeout_ms = 0;
    task->wait.complete = available;
    task->wait.context = task;
    if (task->reactor->submit(task->wait)) return;
    available(task, -errno);
  }

  static void available(void* context, ssize_t result) noexcept {
    auto* task = static_cast<AsyncCacheRetirement*>(context);
    try {
      if (result < 0) {
        try {
          throw std::system_error(-int(result), std::generic_category(),
                                  "cache retirement wait");
        } catch (...) { task->failure = std::current_exception(); }
      }
      task->schedule();
    } catch (...) {
      // An admitted continuation normally may always reserve local cleanup.
      // Keep the failure path terminal if that invariant cannot be honored.
      task->failure = std::current_exception();
      if (task->entry) {
        task->entry->end_async_wait();
        task->entry.reset();
      }
      task->complete(task->context, std::move(task->failure));
    }
  }
};

struct AsyncTruncate {
  State& state;
  FuseReactor& reactor;
  fuse_req_t request;
  InodePin pin;
  OpenHandle handle;
  std::unique_ptr<PathMutationGuard> mutation;
  std::unique_ptr<AsyncSignedS3Request> remote;
  Response committed;
  ChecksumValue checksum;
  std::exception_ptr failure;
  unsigned phase = 0;

  AsyncTruncate(State& s, FuseReactor& r, fuse_req_t req, fuse_ino_t inode)
      : state(s), reactor(r), request(req) {
    InodeBase& item = inode_item(state, inode);
    if (!item.regular()) throw std::system_error(EISDIR, std::generic_category(), "truncate");
    retain_inode_count(item.open_count, "truncate pin");
    pin = InodePin(state, item);
    handle.inode = inode;
    handle.item = static_cast<InodeFile*>(&item);
    handle.object_path = object_request_path(state, item_key(state, item));
    mutation = std::make_unique<PathMutationGuard>(
        state, handle.object_path, false, std::string_view{}, false, "truncate");
    if (object_request_path(state, item_key(state, item)) != handle.object_path) {
      throw std::system_error(EAGAIN, std::generic_category(), "truncate path changed");
    }
  }

  void send(const char* method, unsigned next) {
    AsyncHttpRequest args;
    args.method = method;
    args.path = handle.object_path;
    if (next == 2) {
      args.headers.push_back(Header{"content-type", "application/octet-stream"});
      if (!handle.etag.empty()) args.headers.push_back(Header{"if-match", handle.etag});
      if (checksum_has_digest(state.config.checksum)) checksum = retained_checksum(state.config.checksum, nullptr);
      append_upload_checksum(args.headers, state.config.checksum, checksum);
    }
    remote = std::make_unique<AsyncSignedS3Request>(state, reactor, std::move(args), received, this);
    phase = next;
    if (!remote->start()) throw std::system_error(errno, std::generic_category(), "submit truncate");
  }

  void finish(const ObjectMetadata* metadata) {
    publish_written_metadata(handle, committed, metadata);
    if (!state.atomic_o_trunc) update_truncate_pending(*pin, true);
    struct stat status{};
    fill_inode_stat(state, handle.inode, *pin, status);
    status.st_size = 0;
    fuse_reply_attr(request, &status, 0.0);
  }

  static void received(void* context, Response&& response, std::exception_ptr error) noexcept {
    std::unique_ptr<AsyncTruncate> task(static_cast<AsyncTruncate*>(context));
    const bool ambiguous = task->remote->ambiguous();
    Response value = std::move(response);
    task->remote.reset();
    try {
      if (task->phase == 2 && (error || (ambiguous && value.status == 412))) {
        task->failure = error;
        if (!task->failure) {
          task->failure = std::make_exception_ptr(std::system_error(
              EIO, std::generic_category(), "PutObject commit outcome unknown"));
        }
        task->send("HEAD", 4);
        task.release();
        return;
      }
      if (error) std::rethrow_exception(error);
      if (task->phase == 1) {
        task->handle.etag = decode_head_response(value).etag;
        task->send("PUT", 2);
        task.release();
      } else if (task->phase == 2) {
        require_s3_success(value, "PutObject");
        verify_upload_checksum(value, task->state.config.checksum, task->checksum, "PutObject");
        task->committed = std::move(value);
        if (task->committed.headers.find("last-modified") == task->committed.headers.end()) {
          task->send("HEAD", 3);
          task.release();
        } else {
          task->finish(nullptr);
        }
      } else {
        const ObjectMetadata metadata = decode_head_response(value);
        if (task->phase == 4) {
          if (metadata.size != 0 || (!task->handle.etag.empty() && metadata.etag == task->handle.etag)) {
            std::rethrow_exception(task->failure);
          }
          task->committed = std::move(value);
          fprintf(stderr, "warning: recovered an ambiguous S3 truncate with HeadObject: %s\n",
                  task->handle.object_path.c_str());
        }
        task->finish(&metadata);
      }
    } catch (...) {
      reply_callback_error(task->request);
    }
  }
};

void ngs3fs_setattr(fuse_req_t request, fuse_ino_t inode,
                    struct stat* attributes, int to_set,
                    fuse_file_info* file) {
  try {
    State& state = state_from(request);
    OpenHandle* handle = handle_optional(file);
    uint64_t item_size;
    InodeFile* file_item;
    std::string object_path;
    InodeBase& base = inode_item(state, inode);
    if (!base.regular()) {
      throw std::system_error(EISDIR, std::generic_category(), "setattr");
    }
    file_item = &static_cast<InodeFile&>(base);
    item_size = file_item->fsize.load(std::memory_order_relaxed);
    if ((to_set & FUSE_SET_ATTR_SIZE) != 0 && handle == nullptr) {
      object_path = object_request_path(state, item_key(state, *file_item));
    }
    constexpr int supported_changes = FUSE_SET_ATTR_SIZE |
        FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME |
        FUSE_SET_ATTR_ATIME_NOW | FUSE_SET_ATTR_MTIME_NOW |
        FUSE_SET_ATTR_CTIME | FUSE_SET_ATTR_TIMES_SET |
        FUSE_SET_ATTR_FORCE |
        FUSE_SET_ATTR_KILL_SUID | FUSE_SET_ATTR_KILL_SGID |
        FUSE_SET_ATTR_FILE | FUSE_SET_ATTR_KILL_PRIV | FUSE_SET_ATTR_OPEN |
        FUSE_SET_ATTR_TOUCH;
    if ((to_set & ~supported_changes) != 0) {
      throw std::system_error(EOPNOTSUPP, std::generic_category(),
                              "unsupported setattr mask " +
                                  std::to_string(to_set));
    }

    uint64_t handle_size = item_size;
    if ((to_set & FUSE_SET_ATTR_SIZE) != 0) {
      if (attributes->st_size < 0) {
        throw std::system_error(EACCES, std::generic_category(), "truncate");
      }
      const uint64_t size =
          static_cast<uint64_t>(attributes->st_size);
      if (handle == nullptr) {
        if (size != 0) {
          throw std::system_error(EOPNOTSUPP, std::generic_category(),
                                  "only truncation to zero is supported");
        }
        if (FuseReactor* reactor = current_fuse_reactor()) {
          auto task = std::make_unique<AsyncTruncate>(state, *reactor, request, inode);
          task->send("HEAD", 1);
          task.release();
          return;
        }
        PathMutationGuard mutation(
            state, object_path, false, {}, false, "truncate");
        InodeBase& current = inode_item(state, inode);
        if (object_request_path(state, item_key(state, current)) !=
            object_path) {
          throw std::system_error(
              EAGAIN, std::generic_category(), "truncate path changed");
        }
        ObjectMetadata metadata = head_object(state, object_path);
        OpenHandle truncated;
        truncated.inode         = inode;
        truncated.item          = file_item;
        truncated.object_path   = object_path;
        truncated.etag          = std::move(metadata.etag);
        const Response response = put_object(state, truncated, nullptr);
        update_written_metadata(state, truncated, response);
        if (!state.atomic_o_trunc) {
          update_truncate_pending(inode_item(state, inode), true);
        }
        handle_size = 0;
      } else if (!handle->writable) {
        throw std::system_error(EACCES, std::generic_category(), "truncate");
      } else {
        std::lock_guard handle_guard(handle->mutex);
        if (size != 0 && size != handle->size) {
          throw std::system_error(EOPNOTSUPP, std::generic_category(),
                                  "only truncation to zero is supported");
        }
        if (size == 0 && handle->size != 0) {
          throw std::system_error(
              EOPNOTSUPP, std::generic_category(),
              "cannot rewind a streaming S3 write");
        }
        handle_size = handle->size;
      }
    } else if (handle != nullptr && handle->writable) {
      std::lock_guard handle_guard(handle->mutex);
      handle_size = handle->size;
    }

    struct stat status{};
    fill_inode_stat(state, inode, inode_item(state, inode), status);
    if ((to_set & FUSE_SET_ATTR_SIZE) != 0) {
      status.st_size = static_cast<off_t>(handle_size);
    } else if (handle != nullptr && handle->writable) {
      status.st_size = static_cast<off_t>(handle_size);
    }
    fuse_reply_attr(request, &status, 0.0);
  } catch (...) {
    reply_callback_error(request);
  }
}

void abandon_created_inode(State& state, fuse_ino_t parent,
                             fuse_ino_t inode) noexcept;

struct AsyncOpen {
  State& state;
  FuseReactor& reactor;
  fuse_req_t request;
  fuse_ino_t inode;
  fuse_file_info file;
  std::unique_ptr<OpenHandle> handle;
  std::shared_lock<ReactorSharedMutex> identity;
  std::unique_ptr<AsyncSignedS3Request> head;
  AsyncCacheRetirement retirement;
  FuseReactor::ReactorTask cache_ticket;
  std::shared_ptr<CacheEntry> cache_result;
  std::exception_ptr cache_failure;
  AsyncIoRequest wait;
  UniqueFd retry_timer;
  uint64_t notification = 0;
  uint64_t registration_deadline = 0;
  bool registered = false;
  bool truncate_pending = false;
  std::optional<InodeWriteState> write_started;
  bool keep_cache = false;
  bool generation_conflict = false;
  bool write_prefetch_drained = false;
  bool cache_initialized = false;
  bool cache_retry = false;
  fuse_ino_t created_parent = 0;
  double created_timeout = 0;

  AsyncOpen(State& s, FuseReactor& r, fuse_req_t req, fuse_ino_t ino,
             const fuse_file_info& info,
             std::unique_ptr<OpenHandle> prepared = {})
      : state(s), reactor(r), request(req), inode(ino), file(info),
        handle(prepared ? std::move(prepared) : std::make_unique<OpenHandle>()),
        identity(handle->identity_mutex, std::defer_lock) {
    handle->writable = (file.flags & O_ACCMODE) == O_WRONLY;
    retirement.complete = retired;
    retirement.context = this;
  }

  ~AsyncOpen() {
    if (identity.owns_lock()) identity.unlock();
    if (handle) {
      if (write_started) cancel_inode_write(*handle->item, *write_started);
      if (handle->current_reservation) {
        release_part_budget(state);
        handle->current_reservation = false;
      }
      if (created_parent != 0) abandon_created_inode(state, created_parent, inode);
      if (registered) unregister_open_handle(state, handle->object_path, *handle);
    }
  }

  static void available(void* context, ssize_t result) noexcept {
    auto* task = static_cast<AsyncOpen*>(context);
    task->handle->identity_mutex.end_async_wait();
    if (result < 0) {
      std::unique_ptr<AsyncOpen> owner(task);
      fuse_reply_err(task->request, -int(result));
      return;
    }
    task->resume();
  }

  static void retry_registration(void* context, ssize_t result) noexcept {
    auto* task = static_cast<AsyncOpen*>(context);
    if (result < 0) {
      std::unique_ptr<AsyncOpen> owner(task);
      fuse_reply_err(task->request, -int(result));
      return;
    }
    task->resume();
  }

  bool register_handle() {
    try {
      InodeBase& item = inode_item(state, inode);
      if (item.directory()) {
        throw std::system_error(EISDIR, std::generic_category(), "open");
      }
      truncate_pending = item.truncate_pending();
      register_open_handle(state, inode, *handle, false);
      registered = true;
      return true;
    } catch (const std::system_error& error) {
      if (error.code().value() != EAGAIN) throw;
      const uint64_t now = fuse_monotonic_ns();
      if (registration_deadline == 0) registration_deadline = now + 100'000'000;
      if (now >= registration_deadline) {
        throw std::system_error(EBUSY, std::generic_category(),
                                "object already open with conflicting access");
      }
      if (!retry_timer) {
        retry_timer.reset(::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC));
        if (!retry_timer) {
          throw std::system_error(errno, std::generic_category(), "timerfd(open)");
        }
      }
      itimerspec timer{};
      timer.it_value.tv_nsec = 1'000'000;
      if (::timerfd_settime(retry_timer.get(), 0, &timer, nullptr) != 0) {
        throw std::system_error(errno, std::generic_category(), "timerfd_settime(open)");
      }
      wait = {};
      wait.kind = AsyncIoRequest::READ;
      wait.fd = retry_timer.get();
      wait.data = &notification;
      wait.length = sizeof(notification);
      wait.complete = retry_registration;
      wait.context = this;
      if (!reactor.submit(wait)) {
        throw std::system_error(errno, std::generic_category(), "submit open wait");
      }
      return false;
    }
  }

  void resume() noexcept {
    std::unique_ptr<AsyncOpen> owner(this);
    try {
      if (!registered && !register_handle()) {
        owner.release();
        return;
      }
      if (!identity.owns_lock() && !identity.try_lock()) {
        const int fd = handle->identity_mutex.begin_async_wait();
        if (!identity.try_lock()) {
          wait = {};
          wait.kind = AsyncIoRequest::READ;
          wait.fd = fd;
          wait.data = &notification;
          wait.length = sizeof(notification);
          wait.timeout_ms = 0;
          wait.complete = available;
          wait.context = this;
          if (reactor.submit(wait)) {
            owner.release();
            return;
          }
          const int error = errno;
          handle->identity_mutex.end_async_wait();
          throw std::system_error(error, std::generic_category(), "submit open identity wait");
        }
        handle->identity_mutex.end_async_wait();
      }
      if (!handle->recovery_read && (!handle->writable || !handle->create_exclusive)) {
        AsyncHttpRequest args;
        args.method = "HEAD";
        args.path = handle->object_path;
        head = std::make_unique<AsyncSignedS3Request>(
            state, reactor, std::move(args), received, this);
        if (!head->start()) {
          throw std::system_error(errno, std::generic_category(), "submit HeadObject");
        }
        owner.release();
        return;
      }
      if (!finish()) owner.release();
    } catch (...) {
      reply_callback_error(request);
    }
  }

  static void received(void* context, Response&& response,
                        std::exception_ptr error) noexcept {
    std::unique_ptr<AsyncOpen> task(static_cast<AsyncOpen*>(context));
    try {
      task->head.reset();
      if (error) std::rethrow_exception(error);
      task->keep_cache = publish_open_metadata(
          task->state, *task->handle, decode_head_response(response));
      if (!task->finish()) task.release();
    } catch (const OpenGenerationConflict& conflict) {
      try {
        task->generation_conflict = conflict.old_readers;
        task->invalidate_conflict();
        task.release();
      } catch (...) { reply_callback_error(task->request); }
    } catch (...) {
      reply_callback_error(task->request);
    }
  }

  static void invalidated(void* context, int result) noexcept {
    std::unique_ptr<AsyncOpen> task(static_cast<AsyncOpen*>(context));
    if (result != 0 && result != -ENOENT) {
      fprintf(stderr, "warning: stale page-cache invalidation failed: path=%s: %s\n",
              task->handle->object_path.c_str(), strerror(-result));
    }
    if (result != 0 && result != -ENOENT) {
      fuse_reply_err(task->request, -result);
    } else if (task->generation_conflict) {
      fuse_reply_err(task->request, EBUSY);
    } else {
      try {
        task->write_prefetch_drained = task->handle->writable;
        if (!task->finish()) task.release();
      } catch (...) { reply_callback_error(task->request); }
    }
  }

  void invalidate_conflict() {
    // A local notification can wait for locked folios whose READ still needs
    // a storage worker. Await its io-wq completion without occupying that pool.
    if (!async_page_cache_fence(state, reactor, inode, 0, 0, invalidated, this)) {
      throw std::system_error(errno, std::generic_category(),
                              "submit open generation invalidation");
    }
  }

  static void write_drained(void* context, int result) noexcept {
    std::unique_ptr<AsyncOpen> task(static_cast<AsyncOpen*>(context));
    try {
      if (result != 0 && result != -ENOENT) {
        throw std::system_error(-result, std::generic_category(), "drain prefetch before write open");
      }
      task->write_prefetch_drained = true;
      if (!task->finish()) task.release();
    } catch (...) { reply_callback_error(task->request); }
  }

  static void retired(void* context, std::exception_ptr error) noexcept {
    std::unique_ptr<AsyncOpen> task(static_cast<AsyncOpen*>(context));
    try {
      if (error) std::rethrow_exception(error);
      if (!task->finish()) task.release();
    } catch (...) { reply_callback_error(task->request); }
  }

  static void initialized(void* context) noexcept {
    std::unique_ptr<AsyncOpen> task(static_cast<AsyncOpen*>(context));
    try {
      if (task->cache_failure) std::rethrow_exception(task->cache_failure);
      if (!task->cache_retry) {
        task->handle->cache_entry = std::move(task->cache_result);
        task->cache_initialized = true;
      }
      if (!task->finish()) task.release();
    } catch (...) { reply_callback_error(task->request); }
  }

  void initialize_cache(const CacheIdentity& cache_identity) {
    cache_ticket = {initialized, initialized, this};
    if (!reactor.reserve_completion(&cache_ticket)) {
      throw std::system_error(errno, std::generic_category(), "reserve cache initialization");
    }
    cache_retry = false;
    cache_failure = {};
    try {
      // identity remains locked and the registered handle owns these identity
      // strings until the terminal owner callback; only the result is shared.
      state.uploads->submit(handle.get(), [this, cache_identity] {
        try {
          IoExecutorScope local_only(nullptr, 0);
          cache_result = handle->writable
              ? state.local_cache->create_writer(cache_identity,
                    std::min(state.config.part_size * kMaximumMultipartParts,
                             kMaximumObjectSize), false)
              : state.local_cache->open(cache_identity, false);
        } catch (const CacheRetirementPending&) {
          // The generation may change between the preliminary gate and the
          // actual key-locked operation. Never wait for it in the worker pool.
          cache_retry = true;
        } catch (...) { cache_failure = std::current_exception(); }
        reactor.complete(&cache_ticket);
      });
    } catch (...) {
      cache_failure = std::current_exception();
      reactor.complete(&cache_ticket);
    }
  }

  bool finish() {
    const bool writable = handle->writable;
    if (handle->unlinked.load(std::memory_order_acquire) ||
        handle->stale.load(std::memory_order_acquire)) {
      throw std::system_error(ESTALE, std::generic_category(), "open identity changed");
    }
    if (writable && handle->size != 0 &&
        (file.flags & O_TRUNC) == 0 && !truncate_pending) {
      throw std::system_error(EOPNOTSUPP, std::generic_category(),
                              "existing non-empty write open requires O_TRUNC");
    }
    if (writable && handle->size != 0 && !write_prefetch_drained) {
      {
        // A closed reader's STORE can outlive its handle. Close admission by
        // advancing the epoch, then drain old STORE before exposing O_TRUNC.
        InodeMetadataGuard guard(*handle->item);
        constexpr uint64_t refreshing = 1ULL << 63;
        constexpr uint64_t mask = refreshing - 1;
        uint64_t epoch = handle->item->generation_epoch.load(std::memory_order_relaxed) & mask;
        epoch = epoch == mask ? 1 : epoch + 1;
        handle->item->generation_epoch.store(epoch | refreshing, std::memory_order_release);
        handle->generation_epoch = epoch;
        handle->item->set_page_cache_valid(false);
      }
      // O_TRUNC itself discards the old cache. Do not expire attributes with
      // an extra INVAL notification here; only drain pre-truncation STORE.
      if (!async_page_cache_fence(state, reactor, inode, 0, 0, write_drained, this, false)) {
        throw std::system_error(errno, std::generic_category(), "submit write-open fence");
      }
      return false;
    }
    if (!cache_initialized && state.local_cache && (writable || !handle->recovery_read)) {
      const CacheIdentity cache_identity{
          .key = handle->key, .etag = handle->etag,
          .version_id = handle->version_id, .size = handle->size,
          .mtime = created_parent != 0 ? 0 : handle->item->mtime.load(std::memory_order_relaxed),
      };
      if (cache_retry && !retirement.ready(state, reactor, handle->key,
                              writable ? nullptr : &cache_identity)) return false;
      initialize_cache(cache_identity);
      return false;
    }
    if (cache_initialized) {
      if (!writable && !handle->cache_entry) warn_cache_bypass(state, handle->object_path);
    }
    if (!writable) handle->reader = make_file_reader(state, *handle);
    if (writable) {
      handle->item->set_page_cache_valid(false);
      keep_cache = false;
      if (state.local_cache) {
        handle->write_id = handle->cache_entry->write_id();
      } else {
        if (!reserve_part_budget(state, false)) {
          throw std::system_error(EAGAIN, std::generic_category(), "pinned write budget exhausted");
        }
        handle->current_reservation = true;
      }
      handle->size = 0;
      handle->stream_offset = 0;
      handle->writer = make_file_writer(state, *handle);
      write_started = begin_inode_write(*handle->item);
    }
    identity.unlock();
    file.fh = reinterpret_cast<uint64_t>(handle.get());
    file.direct_io = 0;
    file.keep_cache = keep_cache ? 1 : 0;
    file.nonseekable = writable ? 1 : 0;
    file.noflush = writable ? 0 : 1;
    int result;
    if (created_parent != 0) {
      fuse_entry_param entry{};
      entry.ino = inode;
      entry.generation = 1;
      entry.attr_timeout = created_timeout;
      entry.entry_timeout = created_timeout;
      fill_inode_stat(state, inode, inode_item(state, inode), entry.attr);
      result = fuse_reply_create(request, &entry, &file);
    } else {
      result = fuse_reply_open(request, &file);
    }
    if (result == 0) handle.release();
    else file.fh = 0;
    return true;
  }
};

void ngs3fs_open(fuse_req_t request, fuse_ino_t inode,
                 fuse_file_info* file) {
  State& state = state_from(request);
  const int access_mode = file->flags & O_ACCMODE;
  if (access_mode == O_RDWR) {
    fuse_reply_err(request, EOPNOTSUPP);
    return;
  }
  if (access_mode != O_RDONLY && access_mode != O_WRONLY) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  if ((file->flags & O_DIRECT) != 0) {
    fuse_reply_err(request, EOPNOTSUPP);
    return;
  }
  const bool writable = access_mode == O_WRONLY;
  if (writable && (file->flags & (O_SYNC | O_DSYNC)) != 0) {
    fuse_reply_err(request, EOPNOTSUPP);
    return;
  }
  if (FuseReactor* reactor = current_fuse_reactor()) {
    try {
      auto* task = new AsyncOpen(state, *reactor, request, inode, *file);
      task->resume();
    } catch (...) {
      reply_callback_error(request);
    }
    return;
  }
  bool registered      = false;
  bool budget_reserved = false;
  bool keep_cache      = false;
  std::unique_ptr<OpenHandle> handle;
  std::string registered_path;
  std::optional<InodeWriteState> write_started;
  try {
    handle = std::make_unique<OpenHandle>();
    handle->writable = writable;
    InodeBase& item = inode_item(state, inode);
    if (item.directory()) {
      throw std::system_error(EISDIR, std::generic_category(), "open");
    }
    const bool truncate_pending = item.truncate_pending();
    register_open_handle(state, inode, *handle);
    registered = true;
    std::shared_lock identity_guard(handle->identity_mutex);
    registered_path = handle->object_path;
    if (!handle->recovery_read &&
        (!writable || !handle->create_exclusive)) {
      keep_cache = refresh_open_metadata(state, *handle);
    }
    if (writable && handle->size != 0 &&
        (file->flags & O_TRUNC) == 0 && !truncate_pending) {
      throw std::system_error(
          EOPNOTSUPP, std::generic_category(),
          "existing non-empty write open requires O_TRUNC");
    }

    if (writable && handle->size != 0) {
      {
        InodeMetadataGuard guard(*handle->item);
        constexpr uint64_t refreshing = 1ULL << 63;
        constexpr uint64_t mask = refreshing - 1;
        uint64_t epoch = handle->item->generation_epoch.load(std::memory_order_relaxed) & mask;
        epoch = epoch == mask ? 1 : epoch + 1;
        handle->item->generation_epoch.store(epoch | refreshing, std::memory_order_release);
        handle->generation_epoch = epoch;
        handle->item->set_page_cache_valid(false);
      }
      // Close old publication admission, then drain admitted STOREs before
      // exposing O_TRUNC. The kernel truncation already removes old pages.
      std::unique_lock fence(state.prefetch_publication_mutex);
    }

    if (!writable && state.local_cache && !handle->recovery_read) {
      handle->cache_entry = state.local_cache->open(CacheIdentity{
          .key = handle->key,
          .etag = handle->etag,
          .version_id = handle->version_id,
          .size = handle->size,
          .mtime = handle->item->mtime.load(std::memory_order_relaxed),
      });
      if (!handle->cache_entry) {
        warn_cache_bypass(state, handle->object_path);
      }
    }
    if (!writable) {
      handle->reader = make_file_reader(state, *handle);
    }

    if (writable) {
      handle->item->set_page_cache_valid(false);
      keep_cache = false;
      if (state.local_cache) {
        const uint64_t maximum_size = std::min(
            state.config.part_size * kMaximumMultipartParts,
            kMaximumObjectSize);
        handle->cache_entry = state.local_cache->create_writer(
            CacheIdentity{
                .key = handle->key,
                .etag = handle->etag,
                .version_id = handle->version_id,
                .size = handle->size,
                .mtime = handle->item->mtime.load(
                    std::memory_order_relaxed),
            },
            maximum_size);
        handle->write_id = handle->cache_entry->write_id();
      } else {
        if (!reserve_part_budget(state, false)) {
          throw std::system_error(EAGAIN, std::generic_category(),
                                  "pinned write budget exhausted");
        }
        budget_reserved = true;
        handle->current_reservation = true;
      }
      handle->size = 0;
      handle->stream_offset = 0;
      handle->writer = make_file_writer(state, *handle);
      write_started = begin_inode_write(*handle->item);
    }

    identity_guard.unlock();
    file->fh = reinterpret_cast<uint64_t>(handle.release());
    budget_reserved = false;
    // All handles stay buffered. O_RDWR is rejected above, so Linux rejects
    // MAP_SHARED writable mappings without requiring direct I/O.
    file->direct_io   = 0;
    file->keep_cache  = keep_cache ? 1 : 0;
    file->nonseekable = writable ? 1 : 0;
    file->noflush     = writable ? 0 : 1;
    if (fuse_reply_open(request, file) != 0) {
      std::unique_ptr<OpenHandle> failed_handle(handle_optional(file));
      file->fh = 0;
      if (write_started) cancel_inode_write(*failed_handle->item, *write_started);
      write_started.reset();
      if (failed_handle->current_reservation) {
        release_part_budget(state);
        failed_handle->current_reservation = false;
      }
      std::string failed_path;
      {
        std::shared_lock failed_identity(failed_handle->identity_mutex);
        failed_path = failed_handle->object_path;
      }
      unregister_open_handle(state, failed_path, *failed_handle);
      registered = false;
    }
  } catch (...) {
    if (budget_reserved) {
      release_part_budget(state);
    }
    if (write_started) cancel_inode_write(*handle->item, *write_started);
    if (registered) {
      unregister_open_handle(state, registered_path, *handle);
    }
    reply_callback_error(request);
  }
}

void abandon_created_inode(State& state, fuse_ino_t parent,
                           fuse_ino_t inode) noexcept {
  bool forgotten = false;
  try {
    InodeBase& item = inode_reference(state, inode);
    // Abandon this exact inode, not its name. detach_cached_item validates the
    // parent slot pointer under children.mutex, so a concurrently installed
    // replacement is untouched and no cross-network mutation lock is needed.
    forget_inode(inode, 1);
    forgotten = true;
    if (!item.detached()) {
      remove_item(state, item);
    } else {
      sweep_retired_items(state);
    }
  } catch (...) {
    invalidate_directory(state, parent);
    if (!forgotten) {
      forget_inode(inode, 1);
    }
    sweep_retired_items(state);
  }
}

void start_directory_action(fuse_req_t request, fuse_ino_t parent,
                              const char* name, fuse_file_info* file, int kind);

void create_cached_file(fuse_req_t request, fuse_ino_t parent, const char* name,
                         fuse_file_info* file,
                         std::unique_lock<IoMutex>* held = nullptr,
                         bool* cache_retry = nullptr) {
  if (cache_retry != nullptr) *cache_retry = false;
  const bool prepared = held != nullptr;
  if (name == nullptr || !valid_fuse_component(name)) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  const int access_mode = file->flags & O_ACCMODE;
  if (access_mode != O_WRONLY || (file->flags & O_DIRECT) != 0) {
    fuse_reply_err(request, EOPNOTSUPP);
    return;
  }
  if ((file->flags & (O_SYNC | O_DSYNC)) != 0) {
    fuse_reply_err(request, EOPNOTSUPP);
    return;
  }

  State& state = state_from(request);
  bool registered      = false;
  bool budget_reserved = false;
  fuse_ino_t inode     = 0;
  std::unique_ptr<OpenHandle> handle;
  std::string path;
  try {
    InodeBase& directory = inode_item(state, parent);
    if (!directory.directory()) {
      throw std::system_error(ENOTDIR, std::generic_category(), "create");
    }
    handle = std::make_unique<OpenHandle>();
    handle->writable = true;
    handle->create_exclusive = true;
    double timeout;
    {
      std::unique_lock directory_guard(
          directory.dir_children().mutation_mutex, std::defer_lock);
      if (!prepared) {
        directory_guard.lock();
        refresh_directory(state, parent);
      }
      ListedChild child;
      child.name = name;
      inode = install_item(
          state, parent, std::move(child), 0, true, true, true);
      inode_item(state, inode).set_fsize(0);
      register_open_handle(state, inode, *handle, !prepared);
      path       = handle->object_path;
      registered = true;
      timeout    = remaining_directory_timeout(directory);
    }
    if (held != nullptr) held->unlock();
    if (FuseReactor* reactor = current_fuse_reactor();
        prepared && reactor != nullptr && state.local_cache) {
      auto task = std::make_unique<AsyncOpen>(
          state, *reactor, request, inode, *file, std::move(handle));
      task->registered = true;
      task->created_parent = parent;
      task->created_timeout = timeout;
      registered = false;
      inode = 0;
      task.release()->resume();
      return;
    }
    if (state.local_cache) {
      const uint64_t maximum_size = std::min(
          state.config.part_size * kMaximumMultipartParts,
          kMaximumObjectSize);
      handle->cache_entry = state.local_cache->create_writer(
          CacheIdentity{
              .key = handle->key,
              .etag = {},
              .version_id = {},
              .size = 0,
              .mtime = 0,
          },
          maximum_size);
      handle->write_id = handle->cache_entry->write_id();
    } else {
      if (!reserve_part_budget(state, false)) {
        throw std::system_error(EAGAIN, std::generic_category(),
                                "pinned write budget exhausted");
      }
      budget_reserved             = true;
      handle->current_reservation = true;
    }
    handle->writer = make_file_writer(state, *handle);
    begin_inode_write(*handle->item);

    fuse_entry_param entry{};
    entry.ino           = inode;
    entry.generation    = 1;
    entry.attr_timeout  = timeout;
    entry.entry_timeout = timeout;
    fill_inode_stat(state, inode, inode_item(state, inode), entry.attr);
    file->fh          = reinterpret_cast<uint64_t>(handle.release());
    file->direct_io   = 0;
    file->keep_cache  = 0;
    file->nonseekable = 1;
    budget_reserved   = false;
    if (fuse_reply_create(request, &entry, file) != 0) {
      std::unique_ptr<OpenHandle> failed(handle_optional(file));
      file->fh = 0;
      if (failed->current_reservation) {
        release_part_budget(state);
        failed->current_reservation = false;
      }
      abandon_created_inode(state, parent, inode);
      unregister_open_handle(state, failed->object_path, *failed);
      registered = false;
    }
  } catch (...) {
    const std::exception_ptr failure = std::current_exception();
    if (held != nullptr && held->owns_lock()) held->unlock();
    if (budget_reserved) {
      release_part_budget(state);
    }
    if (inode != 0) {
      abandon_created_inode(state, parent, inode);
    }
    if (registered) {
      unregister_open_handle(state, path, *handle);
    }
    if (cache_retry != nullptr) {
      try { std::rethrow_exception(failure); }
      catch (const CacheRetirementPending&) { *cache_retry = true; return; }
      catch (...) {}
    }
    reply_callback_error(request);
  }
}

void ngs3fs_create(fuse_req_t request, fuse_ino_t parent, const char* name,
                    mode_t, fuse_file_info* file) {
  if (current_fuse_reactor() != nullptr && name != nullptr &&
      valid_fuse_component(name) && file != nullptr &&
      (file->flags & O_ACCMODE) == O_WRONLY &&
      (file->flags & (O_DIRECT | O_SYNC | O_DSYNC)) == 0) {
    start_directory_action(request, parent, name, file, 0);
  } else if (file != nullptr) {
    create_cached_file(request, parent, name, file);
  } else {
    fuse_reply_err(request, EINVAL);
  }
}

int reply_pinned_cached_range(fuse_req_t request, CacheEntry& entry,
                              uint64_t offset, size_t length) {
  fuse_bufvec buffers{};
  buffers.count        = 1;
  buffers.buf[0].size  = length;
  buffers.buf[0].flags = fuse_buf_flags(
      FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK | FUSE_BUF_FD_RETRY);
  buffers.buf[0].fd    = entry.data_fd();
  buffers.buf[0].pos   = off_t(offset);
  const int result = fuse_reply_data(
      request, &buffers, FUSE_BUF_SPLICE_MOVE);
  entry.unpin(offset, length);
  return result;
}

int reply_cached_range(fuse_req_t request, CacheEntry& entry,
                       uint64_t offset, size_t length) {
  entry.pin(offset, length);
  entry.touch(offset, length);
  return reply_pinned_cached_range(request, entry, offset, length);
}

bool content_range_matches(const Response& response,
                           uint64_t offset, size_t length,
                           uint64_t object_size) noexcept {
  if (response.status != 206) {
    return response.status == 200 && offset == 0 &&
        uint64_t(length) == object_size;
  }
  if (response.content_range.empty()) {
    return false;
  }
  return s3_content_range_matches(
      sso_view(response.content_range), offset, length, object_size);
}

[[noreturn]] void throw_inconsistent_range_response(
    const Response& response, uint64_t offset, size_t length,
    uint64_t object_size, const char* operation) {
  if (response.status != 200 && response.status != 206) {
    throw_s3_response(response, operation);
  }
  std::string message = std::string(operation) +
      " returned an inconsistent range status=" +
      std::to_string(response.status) + " expected_offset=" +
      std::to_string(offset) + " expected_bytes=" +
      std::to_string(length) + " expected_total=" +
      std::to_string(object_size) + " body_bytes=" +
      std::to_string(response.body_bytes);
  if (!response.content_range.empty()) {
    append_s3_diagnostic(message, "content_range",
                         sso_view(response.content_range));
  }
  throw std::runtime_error(message);
}

class CacheReadSink final : public RangeFileSink {
 public:
  CacheReadSink(CacheEntry& entry, const CacheFetchClaim& claim,
                fuse_req_t request,
                uint64_t wanted_offset, size_t wanted_length)
      : RangeFileSink(entry.data_fd(), claim.offset),
        entry_(entry),
        claim_(claim),
        request_(request),
        wanted_offset_(wanted_offset),
        wanted_length_(wanted_length) {}

  void progress(const Response& response, bool complete) override {
    if (!content_range_matches(response, claim_.offset, claim_.length,
                               entry_.size())) {
      return;
    }
    if (response.body_bytes > claim_.length) {
      throw std::runtime_error("cache GET exceeded its claimed range");
    }
    entry_.publish_clean(
        claim_, published_, response.body_bytes,
        complete && response.body_bytes == claim_.length);
    published_ = std::max(published_, response.body_bytes);
    if (!replied_ && entry_.pin_clean(wanted_offset_, wanted_length_)) {
      replied_ = true;
      result_ = reply_pinned_cached_range(
          request_, entry_, wanted_offset_, wanted_length_);
    }
  }

  [[nodiscard]] bool replied() const noexcept { return replied_; }
  [[nodiscard]] int result() const noexcept { return result_; }

 private:
  CacheEntry& entry_;
  CacheFetchClaim claim_;
  fuse_req_t request_;
  uint64_t wanted_offset_;
  size_t wanted_length_;
  size_t published_ = 0;
  bool replied_ = false;
  int result_   = 0;
};

class CacheRetrySink final : public RangeFileSink {
 public:
  CacheRetrySink(CacheEntry& entry, uint64_t offset) noexcept
      : RangeFileSink(entry.data_fd(), offset) {}

  void progress(const Response&, bool) override {}
};

struct PrefetchContinuation {
  PrefetchContinuation* next = nullptr;
  FuseReactor* reactor = nullptr;
  FuseReactor::ReactorTask task;
  size_t wanted = 0;
};

std::shared_ptr<ReadAheadStoragePool::Storage> retain_prefetch_storage(
    std::shared_ptr<ReadAheadStoragePool> pool,
    ReadAheadStoragePool::Storage storage) {
  return std::shared_ptr<ReadAheadStoragePool::Storage>(
      new ReadAheadStoragePool::Storage(std::move(storage)),
      [pool = std::move(pool)](ReadAheadStoragePool::Storage* value) {
        pool->release(std::move(*value));
        delete value;
      });
}

struct UncachedPrefetch {
  explicit UncachedPrefetch(
      std::shared_ptr<ReadAheadStoragePool> storage_pool,
      ReadAheadStoragePool::Storage acquired,
      uint64_t begin, size_t count)
      : pool(std::move(storage_pool)),
        storage(retain_prefetch_storage(pool, std::move(acquired))),
        offset(begin), length(count), sink(*this) {}

  std::shared_ptr<ReadAheadStoragePool> pool;
  std::shared_ptr<InodeFile> item_pin;
  std::shared_ptr<ReadAheadStoragePool::Storage> storage;
  uint64_t offset;
  size_t length;
  size_t produced = 0;
  std::exception_ptr error;
  std::mutex mutex;
  std::condition_variable condition;
  std::atomic<bool> cancelled{false};
  bool range_valid = false;
  bool complete = false;
  bool checksum_retrying = false;
  bool checksum_bad = false;
  size_t read_pins = 0;
  bool retire_when_idle = false;
  bool checksum_hold = false;
  size_t retired_prefix = 0;
  size_t published_prefix = 0;
  size_t active_replies = 0;
  PrefetchContinuation* reply_drained = nullptr;
  PrefetchContinuation* continuations = nullptr;
  std::function<void()> legacy_progress;

  bool pin(uint64_t begin, size_t count) noexcept {
    std::lock_guard guard(mutex);
    if (!storage || begin < offset + retired_prefix || count > length ||
        begin - offset > length - count) return false;
    ++read_pins;
    return true;
  }

  void retire(bool unpin = false) noexcept {
    std::shared_ptr<ReadAheadStoragePool::Storage> released;
    {
      std::lock_guard guard(mutex);
      if (unpin) {
        assert(read_pins != 0);
        --read_pins;
      }
      const bool published_all = !checksum_hold && published_prefix == length;
      if ((retire_when_idle || published_all) && complete &&
          !checksum_retrying && read_pins == 0) {
        // The last STORE needs only final truncation. Punching its suffix
        // immediately before truncating repeats the retirement syscall path.
        released = std::move(storage);
      } else if (storage && read_pins == 0 && !checksum_hold && !checksum_retrying) {
        const size_t page = size_t(::sysconf(_SC_PAGESIZE));
        const size_t end = std::min(published_prefix, produced) / page * page;
        if (end > retired_prefix) {
          // No admitted READ refers to this prefix. STORE advances its prefix
          // only after its actual completion; downloading writes later offsets.
          if (storage->discard(retired_prefix, end - retired_prefix)) {
            retired_prefix = end;
          }
        }
      }
    }
    // Do not truncate or unmap while holding the range-selection mutex.
  }

  void publication_finished() noexcept {
    {
      std::lock_guard guard(mutex);
      retire_when_idle = true;
    }
    retire();
  }

  void reply_finished() noexcept {
    PrefetchContinuation* ready = nullptr;
    {
      std::lock_guard guard(mutex);
      assert(active_replies != 0);
      if (--active_replies == 0) ready = std::exchange(reply_drained, nullptr);
    }
    if (ready) ready->reactor->complete(&ready->task);
  }

  bool subscribe(PrefetchContinuation& waiter) noexcept {
    std::lock_guard guard(mutex);
    if (!checksum_retrying &&
        (produced >= waiter.wanted || complete || error != nullptr)) {
      return false;
    }
    waiter.next = continuations;
    continuations = &waiter;
    return true;
  }

  void notify_waiters() {
    PrefetchContinuation* ready = nullptr;
    {
      std::lock_guard guard(mutex);
      PrefetchContinuation** p = &continuations;
      while (*p != nullptr) {
        PrefetchContinuation* waiter = *p;
        if (!checksum_retrying &&
            (produced >= waiter->wanted || complete || error != nullptr)) {
          *p = waiter->next;
          waiter->next = ready;
          ready = waiter;
        } else {
          p = &waiter->next;
        }
      }
    }
    while (ready != nullptr) {
      PrefetchContinuation* waiter = ready;
      ready = ready->next;
      waiter->next = nullptr;
      waiter->reactor->complete(&waiter->task);
    }
    condition.notify_all();
  }

  class Sink final : public RangeFileSink {
   public:
    explicit Sink(UncachedPrefetch& prefetch) noexcept
        : RangeFileSink(prefetch.storage->fd.get(), 0, false),
          prefetch_(prefetch) {}

    [[nodiscard]] bool cancelled() const noexcept override {
      return prefetch_.cancelled.load(std::memory_order_acquire);
    }

    void progress(const Response& response, bool complete) override {
      if (cancelled()) {
        return;
      }
      if (!publishing_) {
        return;
      }
      publish(response, complete);
    }

    void start_publishing(const Response& response) {
      publishing_ = true;
      publish(response, false);
    }

   private:
    void publish(const Response& response, bool complete) {
      (void)complete;
      {
        std::lock_guard guard(prefetch_.mutex);
        prefetch_.produced = response.body_bytes;
      }
      prefetch_.notify_waiters();
      if (prefetch_.legacy_progress) prefetch_.legacy_progress();
    }

    UncachedPrefetch& prefetch_;
    bool publishing_ = false;
  };

  Sink sink;
};

void warn_prefetch_budget(State& state, fuse_ino_t inode) {
  const time_t now = wall_time_seconds();
  time_t previous = state.prefetch_warning_time.load(std::memory_order_relaxed);
  if (now - previous < 60 || !state.prefetch_warning_time.compare_exchange_strong(previous, now)) return;
  const auto usage = state.prefetch_budget.snapshot();
  fprintf(stderr, "warning: prefetch memory budget exhausted: inode=%llu used=%zu peak=%zu; shrinking speculative reads or waiting for demand capacity\n",
          (unsigned long long)inode, usage.used, usage.peak);
}

std::shared_ptr<UncachedPrefetch> allocate_prefetch(
    State& state, fuse_ino_t inode, uint64_t offset, size_t wanted,
    uint64_t size, size_t preferred) {
  const size_t page = state.page_size;
  const auto rounded = [page](size_t n) { return (n + page - 1) / page * page; };
  const size_t limit = PrefetchBudget::file_limit(size,
      state.config.max_prefetch_window_size, state.config.max_file_prefetch_memory, page);
  size_t length = size_t(std::min<uint64_t>(size - offset, preferred));
  PrefetchBudget::Reservation charge;
  if (length > wanted) {
    charge = state.prefetch_budget.try_reserve(inode, limit, rounded(length), rounded(wanted), false);
    if (charge) {
      length = std::min(length, charge.bytes());
    }
  }
  if (!charge) {
    length = wanted;
    charge = state.prefetch_budget.try_reserve(inode, limit, rounded(length), rounded(length), true);
  }
  if (!charge) {
    warn_prefetch_budget(state, inode);
    return {};
  }
  if (length < preferred && length < size - offset) warn_prefetch_budget(state, inode);
  if (charge.bytes() > rounded(length)) charge.split(charge.bytes() - rounded(length)).reset();
  auto p = std::make_shared<UncachedPrefetch>(state.read_ahead_pool,
      state.read_ahead_pool->acquire(length, std::move(charge)), offset, length);
  auto* item = static_cast<InodeFile*>(&inode_item(state, inode));
  retain_inode_count(item->open_count, "pin prefetch inode identity");
  p->item_pin = std::shared_ptr<InodeFile>(item, [](InodeFile* value) {
    release_inode_count(value->open_count);
  });
  p->checksum_hold = state.config.verify_read_checksum && offset == 0 && length == size;
  return p;
}

// Owns only immutable identity and an inode pin, not an open handle. Closing
// the last handle can cancel a tail without waiting for a publication owner
// that itself would keep that handle open.
class AsyncPrefetchPublisher : public std::enable_shared_from_this<AsyncPrefetchPublisher> {
 public:
  AsyncPrefetchPublisher(State& state, FuseReactor& reactor,
                         const OpenHandle& handle,
                         std::shared_ptr<UncachedPrefetch> prefetch,
                         size_t demand)
      : state_(state), reactor_(reactor), prefetch_(std::move(prefetch)),
        inode_(handle.inode), epoch_(handle.generation_epoch),
        size_(handle.size), path_(handle.object_path.c_str()), next_(demand) {
    prefetch_->published_prefix = demand;
    item_ = prefetch_->item_pin;
  }

  void pause() noexcept { paused_ = true; ++revision_; }
  void stop() noexcept {
    pause();
    prefetch_->publication_finished();
  }
  void begin_retry() {
    pause();
    {
      std::lock_guard guard(prefetch_->mutex);
      prefetch_->retire_when_idle = false;
    }
    retry_ready_ = false;
    retry_invalidated_ = false;
    auto context = std::make_unique<std::shared_ptr<AsyncPrefetchPublisher>>(shared_from_this());
    if (!async_page_cache_fence(
            state_, reactor_, inode_, off_t(prefetch_->offset), off_t(prefetch_->length),
            invalidated_retry, context.get())) {
      throw std::system_error(errno, std::generic_category(), "submit prefetch retry invalidation");
    }
    context.release();
  }
  void retry_complete() noexcept {
    next_ = 0;
    retry_ready_ = true;
    if (retry_invalidated_) {
      paused_ = false;
      progress();
    }
  }
  void progress() noexcept {
    if (busy_ || paused_ || failed_) return;
    try {
      size_t available;
      {
        std::lock_guard guard(prefetch_->mutex);
        if (prefetch_->error || prefetch_->checksum_bad || prefetch_->checksum_hold ||
            prefetch_->checksum_retrying ||
            prefetch_->cancelled.load(std::memory_order_acquire)) return;
        available = prefetch_->produced;
        if (!prefetch_->complete || prefetch_->offset + available != size_) {
          available -= available % state_.page_size;
        }
      }
      if (available <= next_) {
        if (next_ == prefetch_->length) prefetch_->publication_finished();
        return;
      }
      const size_t length = std::min(available - next_, kPreferredIoSize);
      auto batch = std::make_unique<Batch>(shared_from_this(), next_, length);
      busy_ = true;
      batch.release()->start();
    } catch (...) { fail(ENOMEM); }
  }

 private:
  static void invalidated_retry(void* context, int result) noexcept {
    std::unique_ptr<std::shared_ptr<AsyncPrefetchPublisher>> hold(
        static_cast<std::shared_ptr<AsyncPrefetchPublisher>*>(context));
    auto& owner = **hold;
    if (result != 0 && result != -ENOENT) {
      owner.fail(-result);
      return;
    }
    owner.retry_invalidated_ = true;
    if (owner.retry_ready_) {
      owner.paused_ = false;
      owner.progress();
    }
  }

  struct Batch {
    std::shared_ptr<AsyncPrefetchPublisher> owner;
    std::shared_ptr<ReadAheadStoragePool::Storage> storage;
    std::shared_lock<ReactorSharedMutex> fence;
    AsyncIoRequest wait;
    size_t offset;
    size_t length;
    size_t revision;
    uint64_t notification = 0;
    bool waiting = false;

    Batch(std::shared_ptr<AsyncPrefetchPublisher> value, size_t off, size_t len)
        : owner(std::move(value)), storage(owner->prefetch_->storage),
          fence(owner->state_.prefetch_publication_mutex, std::defer_lock),
          offset(off), length(len), revision(owner->revision_) {}
    ~Batch() {
      if (waiting) owner->state_.prefetch_publication_mutex.end_async_wait();
    }
    static void available(void* context, ssize_t result) noexcept {
      auto* self = static_cast<Batch*>(context);
      if (result < 0) finish(self, int(result));
      else self->start();
    }
    static void finish(void* context, int result) noexcept {
      std::unique_ptr<Batch> batch(static_cast<Batch*>(context));
      auto owner = batch->owner;
      const size_t end = batch->offset + batch->length;
      const bool obsolete = batch->revision != owner->revision_;
      batch.reset(); // Release fence/source ownership before any next STORE.
      owner->busy_ = false;
      if (result != 0) {
        if (result == -ECANCELED) owner->progress();
        else if (result == -ESTALE) owner->stop();
        else owner->fail(-result);
        return;
      }
      if (!obsolete) {
        owner->next_ = end;
        {
          std::lock_guard guard(owner->prefetch_->mutex);
          owner->prefetch_->published_prefix = end;
        }
        owner->prefetch_->retire();
      }
      owner->progress();
    }
    void start() noexcept {
      try {
        if (!fence.try_lock()) {
          if (!waiting) {
            wait.fd = owner->state_.prefetch_publication_mutex.begin_async_wait();
            waiting = true;
          }
          if (!fence.try_lock()) {
            wait.kind     = AsyncIoRequest::READ;
            wait.data     = &notification;
            wait.length   = sizeof(notification);
            wait.complete = available;
            wait.context  = this;
            if (owner->reactor_.submit(wait)) return;
            finish(this, -errno);
            return;
          }
        }
        if (waiting) {
          owner->state_.prefetch_publication_mutex.end_async_wait();
          waiting = false;
        }
        if (revision != owner->revision_ || owner->paused_ ||
            owner->prefetch_->cancelled.load(std::memory_order_acquire)) {
          finish(this, -ECANCELED);
          return;
        }
        const uint64_t epoch = owner->item_->generation_epoch.load(std::memory_order_acquire)
            & ((1ULL << 63) - 1);
        if (epoch != owner->epoch_) {
          finish(this, -ESTALE);
          return;
        }
        if (!owner->reactor_.notify_store(
                owner->inode_, off_t(owner->prefetch_->offset + offset),
                storage->fd.get(), off_t(offset), length,
                finish, this)) {
          finish(this, -errno);
        }
      } catch (...) { finish(this, -EIO); }
    }
  };

  void fail(int error) noexcept {
    failed_ = true;
    prefetch_->publication_finished();
    fprintf(stderr, "warning: prefetch STORE failed: path=%s: %s\n",
            path_.c_str(), strerror(error));
  }
  State& state_;
  FuseReactor& reactor_;
  std::shared_ptr<UncachedPrefetch> prefetch_;
  std::shared_ptr<InodeFile> item_;
  fuse_ino_t inode_;
  uint64_t epoch_;
  uint64_t size_;
  ssostr<248> path_;
  size_t next_;
  size_t revision_ = 0;
  bool busy_   = false;
  bool paused_ = false;
  bool failed_ = false;
  bool retry_ready_       = false;
  bool retry_invalidated_ = false;
};

bool wait_for_prefetch(UncachedPrefetch& prefetch, size_t wanted) {
  assert(current_fuse_reactor() == nullptr);
  std::unique_lock guard(prefetch.mutex);
  prefetch.condition.wait(guard, [&] {
    return prefetch.produced >= wanted || prefetch.complete ||
        prefetch.error != nullptr;
  });
  return prefetch.produced >= wanted;
}

bool wait_for_prefetch_complete(UncachedPrefetch& prefetch) {
  assert(current_fuse_reactor() == nullptr);
  std::unique_lock guard(prefetch.mutex);
  prefetch.condition.wait(guard, [&] {
    return prefetch.complete || prefetch.error != nullptr;
  });
  return prefetch.complete && prefetch.error == nullptr;
}

class LegacyPrefetchPublisher : public std::enable_shared_from_this<LegacyPrefetchPublisher> {
 public:
  LegacyPrefetchPublisher(State& s, const OpenHandle& h,
                          std::shared_ptr<UncachedPrefetch> p, size_t demand)
      : state_(s), prefetch_(std::move(p)), inode_(h.inode), epoch_(h.generation_epoch), next_(demand) {
    item_ = prefetch_->item_pin;
    prefetch_->published_prefix = demand;
  }
  void progress() noexcept {
    try {
      std::lock_guard guard(mutex_);
      if (busy_ || failed_) return;
      {
        std::lock_guard lock(prefetch_->mutex);
        size_t available = prefetch_->produced / state_.page_size * state_.page_size;
        if (prefetch_->complete) available = prefetch_->produced;
        if (available <= next_ && !prefetch_->complete) return;
      }
      busy_ = true;
      std::lock_guard queue_guard(state_.legacy_publication_mutex);
      if (!state_.publications) throw std::system_error(ENOTCONN, std::generic_category());
      state_.publications->submit(this, [self = shared_from_this()] { self->run(); });
    } catch (...) {
      fail(EIO);
    }
  }
 private:
  void fail(int error) noexcept {
    { std::lock_guard guard(mutex_); failed_ = true; busy_ = false; }
    fprintf(stderr, "warning: legacy prefetch STORE failed: inode=%llu: %s\n",
            (unsigned long long)inode_, strerror(error));
    prefetch_->publication_finished();
  }
  void run() noexcept {
    try {
      size_t length;
      bool complete;
      std::shared_ptr<ReadAheadStoragePool::Storage> storage;
      {
        std::lock_guard guard(prefetch_->mutex);
        complete = prefetch_->complete;
        size_t available = prefetch_->produced / state_.page_size * state_.page_size;
        if (complete) available = prefetch_->produced;
        length = std::min(available - std::min(next_, available), kPreferredIoSize);
        storage = prefetch_->storage;
      }
      if (length != 0) {
        std::shared_lock fence(state_.prefetch_publication_mutex);
        if (prefetch_->cancelled.load(std::memory_order_acquire) ||
            (item_->generation_epoch.load(std::memory_order_acquire) & ((1ULL << 63) - 1)) != epoch_) {
          fail(ESTALE);
          return;
        }
        fuse_session* session;
        { std::lock_guard guard(state_.session_mutex); session = state_.session; }
        if (!session) { fail(ENOTCONN); return; }
        fuse_bufvec buffers{};
        buffers.count = 1;
        buffers.buf[0].size = length;
        buffers.buf[0].flags = fuse_buf_flags(FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK | FUSE_BUF_FD_RETRY);
        buffers.buf[0].fd = storage->fd.get();
        buffers.buf[0].pos = off_t(next_);
        // Dedicated notification worker: never occupy the download/checksum
        // workers, nor keep session/inode locks while STORE waits on a READ.
        const int result = fuse_lowlevel_notify_store(session, inode_,
            off_t(prefetch_->offset + next_), &buffers, FUSE_BUF_SPLICE_MOVE);
        if (result != 0) { fail(-result); return; }
        next_ += length;
        { std::lock_guard guard(prefetch_->mutex); prefetch_->published_prefix = next_; }
      }
      storage.reset();
      prefetch_->retire();
      { std::lock_guard guard(mutex_); busy_ = false; }
      if (complete && (next_ >= prefetch_->length || length == 0)) {
        prefetch_->publication_finished();
      } else progress();
    } catch (...) { fail(EIO); }
  }
  State& state_;
  std::shared_ptr<UncachedPrefetch> prefetch_;
  std::shared_ptr<InodeFile> item_;
  fuse_ino_t inode_;
  uint64_t epoch_;
  size_t next_;
  std::mutex mutex_;
  bool busy_ = false;
  bool failed_ = false;
};

struct PrefetchTailTask {
  std::shared_ptr<UncachedPrefetch> prefetch;
  bool collect_stats;
  std::atomic<uint64_t>* remote_reads;
  std::atomic<uint64_t>* remote_read_bytes;
  HttpPool::Lease lease;
  std::unique_ptr<RangeDownload> download;
  std::shared_ptr<LegacyPrefetchPublisher> publisher;
};

void run_prefetch_tail(void* context) noexcept {
  std::unique_ptr<PrefetchTailTask> task(
      static_cast<PrefetchTailTask*>(context));
  (void)task->lease;
  try {
    Response finished = task->download->finish();
    const bool valid = task->prefetch->range_valid &&
        finished.body_bytes == task->prefetch->length;
    if (task->collect_stats && valid) {
      task->remote_reads->fetch_add(1, std::memory_order_relaxed);
      task->remote_read_bytes->fetch_add(
          finished.body_bytes, std::memory_order_relaxed);
    }
    {
      std::lock_guard guard(task->prefetch->mutex);
      task->prefetch->produced = finished.body_bytes;
      task->prefetch->complete = true;
    }
  } catch (...) {
    std::lock_guard guard(task->prefetch->mutex);
    task->prefetch->error = std::current_exception();
    task->prefetch->complete = true;
  }
  task->prefetch->notify_waiters();
  if (task->publisher) task->publisher->progress();
  else task->prefetch->publication_finished();
}

class UncachedFileReader final : public FileReader {
 public:
  ~UncachedFileReader() override;

  void read(State& state, OpenHandle& handle, fuse_req_t request,
            fuse_ino_t inode, size_t size, off_t offset) override;

  bool try_prefetch(State& state, OpenHandle& handle,
                    fuse_req_t request, uint64_t offset,
                    size_t wanted, uint64_t object_size);

  std::shared_ptr<UncachedPrefetch> select_async(
      State& state, fuse_ino_t inode, uint64_t offset, size_t wanted, uint64_t object_size,
      bool& created);

 private:
  void join() noexcept;

  std::shared_ptr<UncachedPrefetch> prefetch_;
  std::deque<std::shared_ptr<UncachedPrefetch>> retained_;
  std::thread thread_;
  std::mutex mutex_;
  size_t retained_bytes_ = 0;
  size_t next_window_ = 1024U * 1024U;
};

enum CacheChecksumResult {
  CACHE_CHECKSUM_UNAVAILABLE,
  CACHE_CHECKSUM_VALID,
  CACHE_CHECKSUM_INVALID,
};

CacheChecksumResult verify_cached_checksum(
    const Response& response, CacheEntry& entry, uint64_t offset,
    size_t length, ChecksumAlgorithm preferred) {
  ChecksumAlgorithm algorithm = CHECKSUM_NONE;
  std::string_view expected;
  if (!read_checksum_from_response(response, preferred,
                                   algorithm, expected)) {
    return CACHE_CHECKSUM_UNAVAILABLE;
  }
  DataChecksum checksum(algorithm);
  std::array<std::byte, kPreferredIoSize> bytes;
  size_t consumed = 0;
  while (consumed != length) {
    const size_t count = std::min(bytes.size(), length - consumed);
    ssize_t result;
    do {
      result = ::pread(entry.data_fd(), bytes.data(), count,
                       off_t(offset + consumed));
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
      throw std::system_error(errno, std::generic_category(),
                              "pread(cache checksum)");
    }
    if (result == 0) {
      throw std::runtime_error("cache data ended during checksum");
    }
    checksum.update(std::span(bytes).first(size_t(result)));
    consumed += size_t(result);
  }
  const ChecksumValue actual = checksum.finish();
  if (algorithm == CHECKSUM_CRC64XZ) {
    uint64_t received = 0;
    return parse_unsigned(expected, received) &&
                   received == actual.integer
               ? CACHE_CHECKSUM_VALID : CACHE_CHECKSUM_INVALID;
  }
  return expected == sso_view(actual.base64)
             ? CACHE_CHECKSUM_VALID : CACHE_CHECKSUM_INVALID;
}

bool verify_cached_part(CacheEntry& entry,
                        const CacheChecksumClaim& claim) {
  DataChecksum checksum(ChecksumAlgorithm(claim.algorithm));
  std::array<std::byte, kPreferredIoSize> bytes;
  uint64_t consumed = 0;
  while (consumed != claim.size) {
    const size_t count = size_t(std::min<uint64_t>(
        bytes.size(), claim.size - consumed));
    ssize_t result;
    do {
      result = ::pread(entry.data_fd(), bytes.data(), count,
                       off_t(claim.offset + consumed));
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
      throw std::system_error(errno, std::generic_category(),
                              "pread(cache part checksum)");
    }
    if (result == 0) {
      throw std::runtime_error("cache part ended during checksum");
    }
    checksum.update(std::span(bytes).first(size_t(result)));
    consumed += size_t(result);
  }
  return sso_view(checksum.finish().base64) == claim.value;
}

bool part_checksum(const S3Xml& xml,
                   const tinyxml2::XMLElement& part,
                   ChecksumAlgorithm preferred,
                   ChecksumAlgorithm& algorithm,
                   std::string& value) {
  const auto read = [&](ChecksumAlgorithm candidate) {
    value = xml.optional_text(part, checksum_xml_name(candidate));
    const std::string_view trimmed = trim_xml_space(value);
    if (trimmed.data() != value.data() || trimmed.size() != value.size()) {
      value = std::string(trimmed);
    }
    return !value.empty();
  };
  constexpr std::array algorithms{
      CHECKSUM_CRC64NVME, CHECKSUM_CRC32C, CHECKSUM_CRC32,
      CHECKSUM_SHA256, CHECKSUM_SHA1, CHECKSUM_XXHASH128,
      CHECKSUM_XXHASH3, CHECKSUM_XXHASH64, CHECKSUM_SHA512,
      CHECKSUM_MD5,
  };
  if (checksum_is_s3(preferred)) {
    if (read(preferred)) {
      algorithm = preferred;
      return true;
    }
  }
  for (ChecksumAlgorithm candidate : algorithms) {
    if (candidate == preferred) {
      continue;
    }
    if (read(candidate)) {
      algorithm = candidate;
      return true;
    }
  }
  return false;
}

struct ChecksumManifestState {
  std::vector<CacheChecksumPart> parts;
  unsigned expected_number = 1;
  unsigned marker = 0;
  uint64_t offset = 0;
};

bool parse_checksum_manifest_page(
    ChecksumManifestState& state, const Response& response,
    uint64_t object_size, ChecksumAlgorithm preferred) {
  if (response.status != 200) {
    throw_s3_response(response, "GetObjectAttributes");
  }
  S3Xml xml(response_xml(response), "GetObjectAttributes");
  const tinyxml2::XMLElement& root =
      xml.result_root("GetObjectAttributesOutput");
  const tinyxml2::XMLElement& parts =
      xml.required_child(root, "ObjectParts");
  for (const tinyxml2::XMLElement* part = parts.FirstChildElement();
       part != nullptr; part = part->NextSiblingElement()) {
    if (!S3Xml::named(*part, "Part")) {
      continue;
    }
    uint64_t number = 0;
    uint64_t size = 0;
    const std::string number_text =
        xml.required_text(*part, "PartNumber");
    const std::string size_text = xml.required_text(*part, "Size");
    if (!parse_unsigned(trim_xml_space(number_text), number) ||
        number != state.expected_number ||
        number > kMaximumMultipartParts ||
        !parse_unsigned(trim_xml_space(size_text), size) || size == 0 ||
        state.offset > object_size || size > object_size - state.offset) {
      throw std::runtime_error(
          "GetObjectAttributes returned an invalid part sequence");
    }
    ChecksumAlgorithm algorithm = CHECKSUM_NONE;
    std::string value;
    if (!part_checksum(xml, *part, preferred, algorithm, value)) {
      throw std::runtime_error(
          "GetObjectAttributes omitted a usable part checksum");
    }
    state.parts.push_back(CacheChecksumPart{
        .offset    = state.offset,
        .size      = size,
        .algorithm = uint32_t(algorithm),
        .value     = std::move(value),
    });
    state.offset += size;
    ++state.expected_number;
  }
  const bool truncated = xml.required_bool(parts, "IsTruncated");
  if (!truncated) {
    if (state.offset != object_size) {
      throw std::runtime_error(
          "GetObjectAttributes part sizes do not match the object");
    }
    return true;
  }
  uint64_t next = 0;
  const std::string next_text =
      xml.required_text(parts, "NextPartNumberMarker");
  if (!parse_unsigned(trim_xml_space(next_text), next) ||
      next <= state.marker || next != state.expected_number - 1 ||
      next > kMaximumMultipartParts) {
    throw std::runtime_error(
        "GetObjectAttributes returned invalid part pagination");
  }
  state.marker = unsigned(next);
  return false;
}

std::vector<CacheChecksumPart> get_checksum_manifest(
    State& state, const OpenHandle& handle) {
  ChecksumManifestState manifest;
  for (;;) {
    std::string path = query_path(handle.object_path, "attributes");
    if (!handle.version_id.empty()) {
      path += "&versionId=" + uri_encode(handle.version_id, false);
    }
    std::array<Header, 3> signed_headers;
    size_t signed_count = 0;
    signed_headers[signed_count++] =
        Header{"x-amz-max-parts", "1000"};
    signed_headers[signed_count++] =
        Header{"x-amz-object-attributes", "ObjectParts,Checksum"};
    std::string marker_text;
    if (manifest.marker != 0) {
      marker_text = std::to_string(manifest.marker);
      signed_headers[signed_count++] =
          Header{"x-amz-part-number-marker", marker_text};
    }
    HeaderList headers = authorization_headers(
        state, "GET", path,
        std::span(signed_headers.data(), signed_count),
        kEmptyPayloadSha256);
    for (size_t i = 0; i < signed_count; ++i) {
      headers.push_back(signed_headers[i]);
    }
    HttpPool::Lease lease = state.http->acquire();
    const Response response = request_with_retries([&] {
      return lease->request_no_body(
          "GET", path, std::span<const Header>(headers),
          kMaximumListResponseSize);
    }, "GetObjectAttributes");
    if (parse_checksum_manifest_page(
            manifest, response, handle.size, state.config.checksum)) {
      return std::move(manifest.parts);
    }
  }
}

void ensure_checksum_manifest(State& state, OpenHandle& handle) {
  CacheEntry& entry = *handle.cache_entry;
  if (!entry.begin_checksum_manifest()) {
    return;
  }
  try {
    if (!checksum_is_s3(state.config.checksum)) {
      throw std::runtime_error(
          "selected checksum has no S3 ObjectParts representation");
    }
    entry.finish_checksum_manifest(get_checksum_manifest(state, handle));
  } catch (const std::exception& error) {
    entry.checksum_manifest_unavailable();
    fprintf(stderr,
            "warning: multipart read checksum unavailable; continuing "
            "without part verification: path=%s: %s\n",
            handle.object_path.c_str(), error.what());
  } catch (...) {
    entry.checksum_manifest_unavailable();
    fprintf(stderr,
            "warning: multipart read checksum unavailable; continuing "
            "without part verification: path=%s\n",
            handle.object_path.c_str());
  }
}

bool service_cached_checksums(State& state, OpenHandle& handle,
                              uint64_t offset, size_t length,
                              HttpPool::Lease* existing = nullptr) {
  CacheEntry& entry = *handle.cache_entry;
  for (;;) {
    const CacheChecksumClaim claim = entry.claim_checksum(offset, length);
    if (claim.action == CACHE_CHECKSUM_NONE) {
      return true;
    }
    if (claim.action == CACHE_CHECKSUM_BAD) {
      return false;
    }
    if (claim.action == CACHE_CHECKSUM_WAIT) {
      entry.wait_for_checksum(offset, length);
      continue;
    }

    bool valid = false;
    try {
      valid = verify_cached_part(entry, claim);
      if (valid) {
        entry.finish_checksum(claim, true);
        continue;
      }
      entry.checksum_mismatch(claim);
      HttpPool::Lease acquired;
      if (existing == nullptr || !*existing) {
        acquired = state.http->acquire();
      }
      HttpClient& client = existing != nullptr && *existing
          ? existing->client() : acquired.client();
      WorkerState& worker = worker_state(state);
      const AuthorizedRangeRequest range = make_range_request(
          state, handle, worker, claim.offset, size_t(claim.size));
      CacheRetrySink sink(entry, claim.offset);
      const Response response = client.get_range_to_fd(
          range.path(), claim.offset, size_t(claim.size), sink,
          range.headers, true, false);
      const CacheFetchClaim fetched{
          claim.offset, claim.epoch, 0, size_t(claim.size)};
      valid = content_range_matches(response, fetched.offset, fetched.length,
                                    handle.size) &&
          response.body_bytes == fetched.length &&
          verify_cached_part(entry, claim);
    } catch (...) {
      entry.finish_checksum(claim, false);
      throw;
    }
    entry.finish_checksum(claim, valid);
    if (!valid) {
      return false;
    }
  }
}

struct ReadChecksumContext {
  State& state;
  FuseReactor& reactor;
  OpenHandle& handle;
  OpenRequestGuard active;
  std::shared_ptr<CacheEntry> entry;
  ssostr<248> path;
  ssostr<64> etag;
  ssostr<64> version;
  std::string cache_key;
  uint64_t cache_epoch = 0;
  uint64_t size;

  ReadChecksumContext(State& s, FuseReactor& r, OpenHandle& h,
                        const OpenRequestGuard& guard)
      : state(s), reactor(r), handle(h), active(guard), entry(h.cache_entry),
        path(h.object_path), etag(h.etag), version(h.version_id),
        size(h.size) {}

  void refresh_identity() {
    constexpr uint64_t closing = 1ULL << 63;
    const bool handle_closing =
        (handle.request_state.load(std::memory_order_acquire) & closing) != 0;
    if (handle.stale.load(std::memory_order_acquire) ||
        handle.cache_entry != entry) {
      throw std::system_error(ESTALE, std::generic_category(),
                              "stale read checksum identity");
    }
    if (handle_closing && entry) {
      const CacheIdentitySnapshot snapshot = entry->identity_snapshot();
      if (entry->stale()) {
        throw std::system_error(ESTALE, std::generic_category(),
                                "stale closing checksum identity");
      }
      cache_key = snapshot.key;
      cache_epoch = snapshot.epoch;
      const std::string current_path = object_request_path(state, snapshot.key);
      path.assign(current_path.data(), current_path.size());
      etag.assign(snapshot.etag.data(), snapshot.etag.size());
      version.assign(snapshot.version_id.data(), snapshot.version_id.size());
      size = snapshot.size;
      return;
    }
    if (handle_closing) return;
    if (handle.generation_epoch != 0 &&
        (handle.item->generation_epoch.load(std::memory_order_acquire) &
         ((1ULL << 63) - 1)) != handle.generation_epoch) {
      throw std::system_error(ESTALE, std::generic_category(),
                              "stale read checksum generation");
    }
    cache_key = handle.key;
    cache_epoch = entry ? entry->epoch() : 0;
    path.assign(handle.object_path.data(), handle.object_path.size());
    etag.assign(handle.etag.data(), handle.etag.size());
    version.assign(handle.version_id.data(), handle.version_id.size());
    size = handle.size;
  }

  bool closing_cache_identity_changed() const {
    constexpr uint64_t closing = 1ULL << 63;
    if (!entry ||
        (handle.request_state.load(std::memory_order_acquire) & closing) == 0) {
      return false;
    }
    const CacheIdentitySnapshot snapshot = entry->identity_snapshot();
    return snapshot.epoch != cache_epoch || snapshot.key != cache_key;
  }
};

struct AsyncReadIdentityGate {
  using Complete = void (*)(void*, std::exception_ptr) noexcept;

  explicit AsyncReadIdentityGate(ReadChecksumContext& value)
      : context(value), identity(value.handle.identity_mutex, std::defer_lock) {}

  bool acquire(Complete callback, void* value) {
    complete = callback;
    owner = value;
    if (identity.try_lock()) {
      context.refresh_identity();
      return true;
    }
    ReactorSharedMutex& mutex = *identity.mutex();
    const int fd = mutex.begin_async_wait();
    if (identity.try_lock()) {
      mutex.end_async_wait();
      context.refresh_identity();
      return true;
    }
    wait = {};
    wait.kind       = AsyncIoRequest::READ;
    wait.fd         = fd;
    wait.data       = &notification;
    wait.length     = sizeof(notification);
    wait.timeout_ms = 0;
    wait.complete   = available;
    wait.context    = this;
    if (context.reactor.submit(wait)) return false;
    const int error = errno;
    mutex.end_async_wait();
    throw std::system_error(error, std::generic_category(),
                            "submit read identity wait");
  }

  void unlock() noexcept {
    if (identity.owns_lock()) identity.unlock();
  }

  static void available(void* value, ssize_t result) noexcept {
    auto* self = static_cast<AsyncReadIdentityGate*>(value);
    self->identity.mutex()->end_async_wait();
    std::exception_ptr error;
    try {
      if (result < 0) {
        throw std::system_error(-int(result), std::generic_category(),
                                "read identity wait");
      }
      if (!self->acquire(self->complete, self->owner)) return;
    } catch (...) {
      error = std::current_exception();
    }
    self->complete(self->owner, error);
  }

  ReadChecksumContext& context;
  std::shared_lock<ReactorSharedMutex> identity;
  AsyncIoRequest wait;
  uint64_t notification = 0;
  Complete complete = nullptr;
  void* owner = nullptr;
};

AsyncHttpRequest checksum_retry_request(
    const ReadChecksumContext& context, uint64_t offset, size_t length,
    RangeFileSink& sink) {
  AsyncHttpRequest args;
  args.method.assign("GET");
  args.path = context.path;
  if (!context.version.empty()) {
    const std::string path = query_path(sso_view(context.path),
        "versionId=" + uri_encode(sso_view(context.version), false));
    args.path.assign(path.data(), path.size());
  }
  char range[64];
  const int count = snprintf(range, sizeof(range), "bytes=%" PRIu64 "-%" PRIu64,
                             offset, offset + length - 1);
  if (count < 0 || size_t(count) >= sizeof(range)) {
    throw std::runtime_error("checksum retry range overflow");
  }
  std::array<Header, 3> signed_headers;
  size_t n = 0;
  signed_headers[n++] = {"range", std::string_view(range, size_t(count))};
  if (context.version.empty() && !context.etag.empty()) {
    signed_headers[n++] = {"if-match", sso_view(context.etag)};
  }
  if (offset == 0 && length == context.size &&
      context.state.config.checksum_service != CHECKSUM_SERVICE_OSS &&
      context.state.config.checksum_service != CHECKSUM_SERVICE_GCS) {
    signed_headers[n++] = {"x-amz-checksum-mode", "ENABLED"};
  }
  args.headers.assign(signed_headers.begin(), signed_headers.begin() + n);
  args.range       = true;
  args.offset      = offset;
  args.length      = length;
  args.destination = &sink;
  args.capture_headers = true;
  return args;
}

void start_cached_checksums(const ReadChecksumContext& context,
                            uint64_t offset, size_t length) noexcept;

struct AsyncCacheChecksum {
  ReadChecksumContext context;
  AsyncReadIdentityGate identity;
  CacheChecksumClaim part;
  CacheFetchClaim whole;
  Response response;
  std::unique_ptr<CacheRetrySink> sink;
  std::unique_ptr<AsyncSignedS3Request> http;
  FuseReactor::ReactorTask completion;
  AsyncIoRequest wait;
  uint64_t notification = 0;
  std::exception_ptr error;
  bool retrying = false;
  bool valid = false;

  AsyncCacheChecksum(const ReadChecksumContext& ctx, CacheChecksumClaim claim)
      : context(ctx), identity(context), part(std::move(claim)) {}
  AsyncCacheChecksum(const ReadChecksumContext& ctx, CacheFetchClaim claim,
                      Response result)
      : context(ctx), identity(context), whole(claim), response(std::move(result)) {}

  uint64_t offset() const { return whole ? whole.offset : part.offset; }
  size_t length() const { return whole ? whole.length : size_t(part.size); }

  void finish(bool accepted, bool obsolete_identity = false) noexcept {
    if (whole) {
      if (obsolete_identity) context.entry->fail_fetch(whole);
      else if (!accepted || retrying) context.entry->finish_retry(whole, accepted);
      else context.entry->finish_fetch(whole);
    } else {
      if (obsolete_identity) context.entry->abandon_checksum(part);
      else context.entry->finish_checksum(part, accepted);
    }
    if (!accepted && !obsolete_identity) {
      fprintf(stderr, "error: asynchronous read checksum failed: path=%s "
              "offset=%" PRIu64 " bytes=%zu\n",
              context.path.c_str(), offset(), length());
      queue_page_invalidation(context.state, context.handle,
                              off_t(offset()), off_t(length()));
    }
    delete this;
  }

  static void verified(void* value) noexcept {
    std::unique_ptr<AsyncCacheChecksum> self(
        static_cast<AsyncCacheChecksum*>(value));
    if (self->error || self->valid || self->retrying) {
      const bool accepted = !self->error && self->valid;
      self.release()->finish(accepted);
      return;
    }
    try {
      self->retrying = true;
      if (self->whole) self->context.entry->begin_retry(self->whole);
      else self->context.entry->checksum_mismatch(self->part);
      // Already completed FUSE reads cannot be recalled. Invalidation ensures
      // later faults wait for the retry or observe its failure.
      queue_page_invalidation(self->context.state, self->context.handle,
                               off_t(self->offset()), off_t(self->length()));
      if (!self->identity.acquire(identity_ready, self.get())) {
        self.release();
        return;
      }
      self.release()->start_retry();
    } catch (...) {
      self.release()->finish(false);
    }
  }

  static void identity_ready(void* value, std::exception_ptr error) noexcept {
    auto* self = static_cast<AsyncCacheChecksum*>(value);
    if (error) {
      self->finish(false);
      return;
    }
    self->start_retry();
  }

  void start_retry() noexcept {
    try {
      sink = std::make_unique<CacheRetrySink>(*context.entry, offset());
      auto args = checksum_retry_request(context, offset(), length(), *sink);
      http = std::make_unique<AsyncSignedS3Request>(
          context.state, context.reactor, std::move(args), received, this, 1);
      if (!http->start()) {
        throw std::system_error(errno, std::generic_category(),
                                "submit checksum retry");
      }
    } catch (...) {
      identity.unlock();
      finish(false);
    }
  }

  static void received(void* value, Response&& result,
                         std::exception_ptr failure) noexcept {
    auto* self = static_cast<AsyncCacheChecksum*>(value);
    self->http.reset();
    self->identity.unlock();
    const bool rejected = failure ||
        !content_range_matches(result, self->offset(), self->length(),
                               self->context.size) ||
        result.body_bytes != self->length();
    bool obsolete_identity = false;
    if (rejected) {
      try {
        obsolete_identity = self->context.closing_cache_identity_changed();
      } catch (...) {
        obsolete_identity = true;
      }
      fprintf(stderr, "error: checksum retry response rejected: path=%s status=%d "
              "offset=%" PRIu64 " expected=%zu received=%zu transport_error=%d\n",
              self->context.path.c_str(), result.status, self->offset(), self->length(),
              size_t(result.body_bytes), int(bool(failure)));
      if (failure) {
        try { std::rethrow_exception(failure); }
        catch (const std::exception& error) { fprintf(stderr, "checksum retry: %s\n", error.what()); }
        catch (...) {}
      }
      self->finish(false, obsolete_identity);
      return;
    }
    self->response = std::move(result);
    self->verify();
  }

  void verify() noexcept {
    completion = {verified, nullptr, this};
    if (!context.reactor.reserve_completion(&completion)) {
      finish(false);
      return;
    }
    try {
      context.state.uploads->submit(&context.handle, [this] {
        try {
          IoExecutorScope local_only(nullptr, 0);
          if (whole) {
            const auto result = verify_cached_checksum(
                response, *context.entry, whole.offset, whole.length,
                context.state.config.checksum);
            valid = result == CACHE_CHECKSUM_VALID ||
                (!retrying && result == CACHE_CHECKSUM_UNAVAILABLE);
          } else {
            valid = verify_cached_part(*context.entry, part);
          }
        } catch (...) {
          error = std::current_exception();
        }
        context.reactor.complete(&completion);
      });
    } catch (...) {
      error = std::current_exception();
      context.reactor.complete(&completion);
    }
  }

  static void manifest_ready(void* value, ssize_t result) noexcept {
    auto* self = static_cast<AsyncCacheChecksum*>(value);
    self->context.entry->end_async_wait();
    if (result < 0) self->finish(false);
    else self->start();
  }

  void start() noexcept {
    try {
      if (whole) {
        const int fd = context.entry->begin_manifest_wait();
        if (fd >= 0) {
          wait.kind       = AsyncIoRequest::READ;
          wait.fd         = fd;
          wait.data       = &notification;
          wait.length     = sizeof(notification);
          wait.timeout_ms = 0;
          wait.complete   = manifest_ready;
          wait.context    = this;
          if (context.reactor.submit(wait)) return;
          context.entry->end_async_wait();
          throw std::system_error(errno, std::generic_category(), "submit manifest wait");
        }
        if (context.entry->checksum_manifest_available()) {
          context.entry->finish_fetch(whole);
          start_cached_checksums(context, whole.offset, whole.length);
          delete this;
          return;
        }
      }
      verify();
    } catch (...) {
      finish(false);
    }
  }
};

void start_cached_checksums(const ReadChecksumContext& context,
                            uint64_t offset, size_t length) noexcept {
  for (;;) {
    CacheChecksumClaim claim;
    try {
      claim = context.entry->claim_checksum(offset, length, true);
      if (claim.action != CACHE_CHECKSUM_VERIFY) return;
      auto* task = new AsyncCacheChecksum(context, claim);
      task->start();
    } catch (...) {
      if (claim.action == CACHE_CHECKSUM_VERIFY) {
        context.entry->finish_checksum(claim, false);
        queue_page_invalidation(context.state, context.handle,
                                 off_t(claim.offset), off_t(claim.size));
      }
      fprintf(stderr, "warning: cannot start asynchronous part checksum: path=%s\n",
              context.path.c_str());
      return;
    }
  }
}

struct AsyncChecksumManifest {
  ReadChecksumContext context;
  AsyncReadIdentityGate identity;
  ChecksumManifestState manifest;
  std::unique_ptr<AsyncSignedS3Request> http;

  explicit AsyncChecksumManifest(const ReadChecksumContext& ctx)
      : context(ctx), identity(context) {}

  void unavailable() noexcept {
    context.entry->checksum_manifest_unavailable();
    fprintf(stderr, "warning: multipart read checksum unavailable: path=%s; "
            "whole-object checksum remains best effort\n", context.path.c_str());
    delete this;
  }

  static void received(void* value, Response&& result,
                         std::exception_ptr error) noexcept {
    auto* self = static_cast<AsyncChecksumManifest*>(value);
    self->http.reset();
    self->identity.unlock();
    try {
      if (error) std::rethrow_exception(error);
      if (parse_checksum_manifest_page(self->manifest, result, self->context.size,
                                       self->context.state.config.checksum)) {
        self->context.entry->finish_checksum_manifest(std::move(self->manifest.parts));
        start_cached_checksums(self->context, 0, size_t(self->context.size));
        delete self;
      } else {
        self->start();
      }
    } catch (...) {
      self->unavailable();
    }
  }

  void start() noexcept {
    try {
      if (!checksum_is_s3(context.state.config.checksum)) {
        unavailable();
        return;
      }
      if (!identity.acquire(identity_ready, this)) return;
      start_request();
    } catch (...) {
      identity.unlock();
      unavailable();
    }
  }

  static void identity_ready(void* value, std::exception_ptr error) noexcept {
    auto* self = static_cast<AsyncChecksumManifest*>(value);
    if (error) {
      self->unavailable();
      return;
    }
    self->start_request();
  }

  void start_request() noexcept {
    try {
      std::string path = query_path(sso_view(context.path), "attributes");
      if (!context.version.empty()) {
        path += "&versionId=" + uri_encode(sso_view(context.version), false);
      }
      std::array<Header, 3> signed_headers;
      size_t n = 0;
      signed_headers[n++] = {"x-amz-max-parts", "1000"};
      signed_headers[n++] = {"x-amz-object-attributes", "ObjectParts,Checksum"};
      if (manifest.marker != 0) {
        signed_headers[n++] = {"x-amz-part-number-marker", std::to_string(manifest.marker)};
      }
      AsyncHttpRequest args;
      args.method.assign("GET");
      args.path.assign(path.data(), path.size());
      args.headers.assign(signed_headers.begin(), signed_headers.begin() + n);
      args.max_response_body = kMaximumListResponseSize;
      http = std::make_unique<AsyncSignedS3Request>(context.state, context.reactor,
          std::move(args), received, this);
      if (!http->start()) {
        throw std::system_error(errno, std::generic_category(), "submit checksum manifest");
      }
    } catch (...) {
      identity.unlock();
      unavailable();
    }
  }
};

void ensure_async_checksum_manifest(const ReadChecksumContext& context) {
  if (!context.entry->begin_checksum_manifest(false)) return;
  try {
    auto* task = new AsyncChecksumManifest(context);
    task->start();
  } catch (...) {
    context.entry->checksum_manifest_unavailable();
    throw;
  }
}

bool cache_response_matches(const Response& response,
                            const CacheFetchClaim& claim,
                            uint64_t object_size) noexcept {
  return content_range_matches(response, claim.offset, claim.length,
                               object_size) &&
      response.body_bytes == claim.length;
}

size_t cache_fetch_expansion(State& state, OpenHandle& handle,
                             uint64_t offset, size_t wanted) {
  constexpr size_t initial = 1024U * 1024U;
  std::lock_guard guard(handle.mutex);
  if (!handle.cache_read_seen) {
    handle.cache_read_window = initial;
    handle.cache_read_seen = true;
  } else if (offset == handle.cache_last_read_end) {
    const size_t remaining =
        state.config.max_prefetch_window_size - handle.cache_read_window;
    handle.cache_read_window +=
        std::min(handle.cache_read_window, remaining);
  } else {
    handle.cache_read_window = initial;
  }
  handle.cache_last_read_end = offset + wanted;
  return std::max(wanted, handle.cache_read_window);
}

bool read_cached(State& state, OpenHandle& handle,
                 fuse_req_t request, uint64_t offset, size_t wanted) {
  CacheEntry& entry = *handle.cache_entry;
  if (state.config.verify_read_checksum && entry.stale()) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "stale cached S3 generation");
  }
  if (state.config.verify_read_checksum) {
    ensure_checksum_manifest(state, handle);
  }
  const size_t expansion = cache_fetch_expansion(state, handle, offset, wanted);
  for (;;) {
    if (state.config.verify_read_checksum &&
        !service_cached_checksums(state, handle, offset, wanted)) {
      throw std::system_error(EIO, std::generic_category(),
                              "cached S3 part checksum mismatch");
    }
    if (entry.pin_clean(offset, wanted)) {
      const int result = reply_pinned_cached_range(
          request, entry, offset, wanted);
      if (result != 0) {
        fprintf(stderr, "fuse_reply_data(cache hit) failed: %s\n",
                strerror(-result));
      }
      return true;
    }
    if (entry.range_bad(offset, wanted)) {
      throw std::system_error(EIO, std::generic_category(),
                              "cached S3 checksum mismatch");
    }
    HttpPool::Lease lease;
    CacheFetchClaim claim;
    {
      size_t selected_expansion = wanted;
      if (expansion > wanted) {
        lease = state.http->try_acquire_bulk();
        if (lease) {
          selected_expansion = expansion;
        }
      }
      claim = entry.claim_fetch(offset, wanted, selected_expansion);
      if (!claim) {
        entry.wait_for_range(offset, wanted);
        continue;
      }

      try {
        if (!entry.prepare_read(claim.offset, claim.length)) {
          entry.fail_fetch(claim);
          return false;
        }
      } catch (...) {
        entry.fail_fetch(claim);
        throw;
      }
    }

    CacheReadSink sink(entry, claim, request, offset, wanted);
    Response response;
    try {
      WorkerState& worker = worker_state(state);
      if (!lease) {
        lease = state.http->acquire();
      }
      HttpClient& client = lease.client();
      for (unsigned attempt = 0; ; ++attempt) {
        const AuthorizedRangeRequest range = make_range_request(
            state, handle, worker, claim.offset, claim.length);
        response = client.get_range_to_fd(
            range.path(), claim.offset, claim.length, sink,
            range.headers, true,
            state.config.report_metrics);
        bool retry = retryable_response(response);
        if (response.status == 403 && !state.config.directory_bucket &&
            !range.range_signed) {
          int expected = 0;
          const bool selected_signed =
              state.range_signing_mode.compare_exchange_strong(
                  expected, 2, std::memory_order_relaxed);
          retry = selected_signed || expected == 2;
          if (selected_signed) {
            fprintf(stderr,
                    "warning: S3 endpoint rejected an unsigned Range "
                    "header; signing Range for this mount\n");
          }
          worker.authorization_valid = false;
        } else if ((response.status == 401 || response.status == 403) &&
                   state.config.directory_bucket) {
          invalidate_express_session(state);
          retry = true;
        }
        if (!retry || attempt == 3) {
          break;
        }
        if (sink.replied()) {
          throw std::logic_error(
              "retryable S3 error arrived after cached data was published");
        }
        fprintf(stderr,
                "warning: retrying cached GetObject after S3 response "
                "status=%d attempt=%u\n",
                response.status, attempt + 1);
        if (!retry_after_delay(response)) {
          retry_delay(attempt);
        }
      }
      if (!cache_response_matches(response, claim, handle.size)) {
        throw_inconsistent_range_response(
            response, claim.offset, claim.length, handle.size,
            "cached GetObject");
      }

      const bool checksum_scope_matches =
          claim.offset == 0 && uint64_t(claim.length) == handle.size;
      const CacheChecksumResult checked =
          state.config.verify_read_checksum && checksum_scope_matches &&
              !entry.checksum_manifest_available()
              ? verify_cached_checksum(response, entry, claim.offset,
                                       claim.length, state.config.checksum)
              : CACHE_CHECKSUM_UNAVAILABLE;
      if (checked == CACHE_CHECKSUM_INVALID) {
        entry.begin_retry(claim);
        CacheRetrySink retry_sink(entry, claim.offset);
        const AuthorizedRangeRequest retry_range = make_range_request(
            state, handle, worker, claim.offset, claim.length);
        const Response retried = client.get_range_to_fd(
            retry_range.path(), claim.offset, claim.length, retry_sink,
            retry_range.headers, true, false);
        const bool retry_valid = cache_response_matches(
            retried, claim, handle.size) &&
            verify_cached_checksum(retried, entry, claim.offset,
                                   claim.length,
                                   state.config.checksum) ==
                CACHE_CHECKSUM_VALID;
        entry.finish_retry(claim, retry_valid);
        if (!retry_valid) {
          if (sink.replied()) {
            fprintf(stderr,
                    "error: cached S3 checksum retry failed: path=%s "
                    "offset=%" PRIu64 " bytes=%zu\n",
                    handle.object_path.c_str(), claim.offset, claim.length);
            queue_page_invalidation(
                state, handle, off_t(claim.offset),
                off_t(claim.length));
            return true;
          }
          throw std::system_error(EIO, std::generic_category(),
                                  "cached S3 checksum retry failed");
        }
      } else {
        entry.finish_fetch(claim);
      }
      if (state.config.verify_read_checksum &&
          !service_cached_checksums(state, handle, claim.offset,
                                    claim.length, &lease)) {
        if (sink.replied()) {
          fprintf(stderr,
                  "error: cached S3 part checksum retry failed: path=%s "
                  "offset=%" PRIu64 " bytes=%zu\n",
                  handle.object_path.c_str(), claim.offset, claim.length);
          queue_page_invalidation(
              state, handle, off_t(claim.offset),
              off_t(claim.length));
          return true;
        }
        throw std::system_error(EIO, std::generic_category(),
                                "cached S3 part checksum retry failed");
      }
    } catch (...) {
      entry.fail_fetch(claim);
      if (sink.replied()) {
        if (entry.range_bad(claim.offset, claim.length)) {
          queue_page_invalidation(
              state, handle, off_t(claim.offset),
              off_t(claim.length));
        }
        try {
          throw;
        } catch (const std::exception& error) {
          fprintf(stderr,
                  "warning: cache fill tail failed after FUSE reply: "
                  "path=%s offset=%" PRIu64 " bytes=%zu: %s\n",
                  handle.object_path.c_str(), claim.offset, claim.length,
                  error.what());
        } catch (...) {
          fprintf(stderr,
                  "warning: cache fill tail failed after FUSE reply: "
                  "path=%s offset=%" PRIu64 " bytes=%zu\n",
                  handle.object_path.c_str(), claim.offset, claim.length);
        }
        return true;
      }
      throw;
    }
    if (sink.replied()) {
      if (sink.result() != 0) {
        fprintf(stderr, "fuse_reply_data(cache fill) failed: %s\n",
                strerror(-sink.result()));
      }
      return true;
    }
  }
}

void read_open_file(State& state, OpenHandle& handle, bool use_cache,
                    UncachedFileReader* uncached,
                    fuse_req_t request, fuse_ino_t inode, size_t size,
                    off_t offset) {
  if (offset < 0) {
    fuse_reply_err(request, EINVAL);
    return;
  }

  try {
    std::shared_lock identity_guard(handle.identity_mutex);
    if (handle.stale.load(std::memory_order_acquire)) {
      throw std::system_error(ESTALE, std::generic_category(),
                              "stale open file");
    }
    if (!handle.recovery_read && handle.generation_epoch != 0 &&
        (handle.item->generation_epoch.load(std::memory_order_acquire) &
         ((1ULL << 63) - 1)) !=
            handle.generation_epoch) {
      handle.stale.store(true, std::memory_order_release);
      throw std::system_error(ESTALE, std::generic_category(),
                              "stale open file generation");
    }
    if (handle.inode != inode) {
      throw std::system_error(EBADF, std::generic_category(), "read inode");
    }
    const uint64_t unsigned_offset = static_cast<uint64_t>(offset);

    if (handle.writable) {
      throw std::system_error(EBADF, std::generic_category(), "read");
    }

    const uint64_t object_size = handle.size;
    if (unsigned_offset >= object_size || size == 0) {
      fuse_reply_buf(request, nullptr, 0);
      return;
    }
    const size_t wanted = static_cast<size_t>(
        std::min<uint64_t>(size, object_size - unsigned_offset));
    warn_random_read(state, handle.object_path, object_size,
                     unsigned_offset, size);
    const bool report_metrics = state.config.report_metrics;
    const uint64_t fuse_start_ns =
        report_metrics ? fuse_monotonic_ns() : 0;
    const uint64_t cpu_start_ns =
        report_metrics ? fuse_thread_cpu_ns() : 0;

    if (handle.recovery_read && handle.cache_entry) {
      const int result = reply_cached_range(
          request, *handle.cache_entry, unsigned_offset, wanted);
      if (result != 0) {
        fprintf(stderr, "fuse_reply_data(cache recovery) failed: %s\n",
                strerror(-result));
      }
      return;
    }
    if (use_cache && handle.cache_entry != nullptr &&
        read_cached(state, handle, request, unsigned_offset, wanted)) {
      return;
    }
    if (uncached != nullptr && !state.config.verify_read_checksum &&
        uncached->try_prefetch(
            state, handle, request, unsigned_offset, wanted, object_size)) {
      return;
    }

    WorkerState& worker = worker_state(state);
    HttpPool::Lease lease = state.http->acquire();
    HttpClient& client = lease.client();
    Pipe pipe           = std::move(worker.transport_pipe);
    if (pipe.capacity() < state.config.maximum_read_size) {
      pipe = Pipe::create(state.config.maximum_read_size);
    }
    if (pipe.capacity() < wanted) {
      throw std::runtime_error("FUSE read is larger than pipe capacity");
    }
    Response response;
    unsigned checksum_retries = 0;
    bool checksum_terminal = false;
    for (unsigned attempt = 0; ; ++attempt) {
      try {
        const auto range_request =
            make_range_request(state, handle, worker,
                               unsigned_offset, wanted);
        response = client.get_range(
            range_request.path(), unsigned_offset, wanted, pipe,
            range_request.headers, range_request.verify_checksum,
            report_metrics);
        if (response.status == 403 && !state.config.directory_bucket &&
            !range_request.range_signed && attempt != 3) {
          int expected = 0;
          const bool selected_signed =
              state.range_signing_mode.compare_exchange_strong(
                  expected, 2, std::memory_order_relaxed);
          const bool retry_signed = selected_signed || expected == 2;
          if (retry_signed) {
            if (selected_signed) {
              fprintf(stderr,
                      "warning: S3 endpoint rejected an unsigned Range "
                      "header; signing Range for this mount\n");
            }
            if (response.requires_consume) {
              client.consume(response);
            }
            worker.authorization_valid = false;
            pipe = Pipe::create(state.config.maximum_read_size);
            continue;
          }
        }
        if ((response.status == 401 || response.status == 403) &&
            state.config.directory_bucket && attempt != 3) {
          if (response.requires_consume) {
            client.consume(response);
          }
          invalidate_express_session(state);
          pipe = Pipe::create(state.config.maximum_read_size);
          retry_delay(attempt);
          continue;
        }
        if (retryable_response(response) && attempt != 3) {
          if (response.requires_consume) {
            client.consume(response);
          }
          pipe = Pipe::create(state.config.maximum_read_size);
          retry_delay(attempt);
          continue;
        }
        if (!range_request.range_signed &&
            (response.status == 200 || response.status == 206)) {
          int expected = 0;
          state.range_signing_mode.compare_exchange_strong(
              expected, 1, std::memory_order_relaxed);
        }
        if (state.config.verify_read_checksum && unsigned_offset == 0 &&
            uint64_t(wanted) == object_size && response.body_bytes == wanted &&
            content_range_matches(response, unsigned_offset, wanted, object_size)) {
          try {
            verify_read_checksum(response, pipe, wanted, state.config.checksum);
          } catch (...) {
            if (response.requires_consume) client.consume(response);
            if (checksum_retries++ != 0) {
              checksum_terminal = true;
              handle.read_checksum_bad.store(true, std::memory_order_release);
              throw;
            }
            pipe = Pipe::create(state.config.maximum_read_size);
            continue;
          }
        }
        break;
      } catch (...) {
        if (attempt == 3 || checksum_terminal) {
          throw;
        }
        pipe = Pipe::create(state.config.maximum_read_size);
        retry_delay(attempt);
      }
    }
    ResponseCreditGuard credit(client, response);
    if (response.status == 412) {
      throw std::system_error(ESTALE, std::generic_category(),
                              "S3 object changed during open");
    }
    const bool valid_range = content_range_matches(
        response, unsigned_offset, wanted, object_size);
    if (!valid_range ||
        response.body_bytes != wanted) {
      throw_inconsistent_range_response(
          response, unsigned_offset, wanted, object_size, "GetObject");
    }

    fuse_bufvec buffers{};
    buffers.count = 1;
    buffers.buf[0].size = wanted;
    buffers.buf[0].flags = static_cast<fuse_buf_flags>(
        FUSE_BUF_IS_FD | FUSE_BUF_FD_RETRY);
    buffers.buf[0].fd = pipe.read_fd();
    const int reply_result =
        fuse_reply_data(request, &buffers, FUSE_BUF_SPLICE_MOVE);
    if (report_metrics && response.wire_start_ns != 0 &&
        response.wire_last_data_ns >= response.wire_start_ns) {
      const uint64_t page_cache_complete_ns = fuse_monotonic_ns();
      const uint64_t cpu_complete_ns = fuse_thread_cpu_ns();
      const uint64_t total_ns = page_cache_complete_ns - fuse_start_ns;
      const uint64_t transport_span_ns =
          response.wire_last_data_ns - response.wire_start_ns;
      const uint64_t residual_ns =
          total_ns > transport_span_ns ? total_ns - transport_span_ns : 0;
      std::lock_guard metrics_guard(state.metrics_mutex);
      fprintf(stderr,
              "{\"event\":\"read\",\"offset\":%" PRIu64
              ",\"bytes\":%zu,\"fetched_bytes\":%zu"
              ",\"cache_hit\":false,\"total_ns\":%" PRIu64
              ",\"transport_span_ns\":%" PRIu64
              ",\"residual_ns\":%" PRIu64 ",\"cpu_ns\":%" PRIu64
              ",\"external_bytes\":%zu,\"fallback_bytes\":%zu"
              ",\"transport_splice_calls\":%zu}\n",
              unsigned_offset, wanted, wanted, total_ns, transport_span_ns,
              residual_ns, cpu_complete_ns - cpu_start_ns,
              response.externally_spliced_bytes,
              response.fallback_copied_bytes,
              response.transport_splice_calls);
    }

    // Return transport flow-control credit only after libfuse has drained the
    // pipe into /dev/fuse. HTTP/1.1 has no application-managed credit, so its
    // consume operation is intentionally a no-op.
    if (reply_result != 0) {
      fprintf(stderr, "fuse_reply_data failed: %s\n",
              strerror(-reply_result));
    }
    try {
      credit.consume();
    } catch (const std::exception& error) {
      // The FUSE request was already answered. Do not attempt a second reply.
      fprintf(stderr, "failed to return transport receive credit: %s\n",
              error.what());
    }
    if (reply_result == 0) {
      if (state.config.stats_interval_seconds != 0) {
        state.remote_reads.fetch_add(1, std::memory_order_relaxed);
        state.remote_read_bytes.fetch_add(wanted, std::memory_order_relaxed);
      }
      worker.transport_pipe = std::move(pipe);
    }
  } catch (...) {
    reply_callback_error(request);
    if (use_cache && handle.cache_entry && offset >= 0) {
      const uint64_t begin = uint64_t(offset);
      const uint64_t object_size = handle.size;
      const size_t wanted = begin >= object_size ? 0 : size_t(
          std::min<uint64_t>(size, object_size - begin));
      if (wanted != 0 && handle.cache_entry->range_bad(begin, wanted)) {
        queue_page_invalidation(
            state, handle, offset, off_t(wanted));
      }
    }
  }
}

UncachedFileReader::~UncachedFileReader() {
  if (prefetch_) {
    prefetch_->cancelled.store(true, std::memory_order_release);
  }
  for (const auto& prefetch : retained_) {
    prefetch->cancelled.store(true, std::memory_order_release);
  }
  join();
}

void UncachedFileReader::join() noexcept {
  if (thread_.joinable()) {
    thread_.join();
  }
}

bool UncachedFileReader::try_prefetch(
    State& state, OpenHandle& handle, fuse_req_t request,
    uint64_t offset, size_t wanted, uint64_t object_size) {
  std::unique_lock reader_guard(mutex_);
  const auto contains = [&](UncachedPrefetch& value) {
    return value.pin(offset, wanted);
  };
  std::shared_ptr<UncachedPrefetch> selected;
  struct ReadPin {
    std::shared_ptr<UncachedPrefetch> value;
    ~ReadPin() { if (value) value->retire(true); }
  } pin;
  std::shared_ptr<LegacyPrefetchPublisher> publisher;
  std::unique_ptr<RangeDownload> started_download;
  HttpPool::Lease started_lease;
  if (prefetch_ && contains(*prefetch_)) {
    selected = prefetch_;
  } else {
    for (auto i = retained_.rbegin(); i != retained_.rend(); ++i) {
      if (contains(**i)) {
        selected = *i;
        break;
      }
    }
  }
  if (!selected) {
    bool start = prefetch_ == nullptr;
    if (prefetch_) {
      const std::shared_ptr<UncachedPrefetch> pending = prefetch_;
      const uint64_t end = pending->offset + pending->length;
      if (offset == end) {
        reader_guard.unlock();
        wait_for_prefetch_complete(*pending);
        reader_guard.lock();
        if (prefetch_ != pending) {
          return false;
        }
      }
      bool ready;
      {
        std::lock_guard guard(prefetch_->mutex);
        ready = prefetch_->complete || prefetch_->error != nullptr;
      }
      start = ready;
    }

    if (!start) {
      return false;
    }

    if (prefetch_) {
      join();
      const uint64_t end = prefetch_->offset + prefetch_->length;
      bool valid;
      {
        std::lock_guard guard(prefetch_->mutex);
        valid = prefetch_->error == nullptr && prefetch_->complete &&
            prefetch_->produced == prefetch_->length &&
            prefetch_->range_valid;
      }
      if (valid && offset == end) {
        const size_t maximum = state.config.max_prefetch_window_size;
        const size_t growth  = maximum - next_window_;
        next_window_ += std::min(next_window_, growth);
      } else if (valid) {
        retained_bytes_ += prefetch_->length;
        retained_.push_back(std::move(prefetch_));
        next_window_ = 1024U * 1024U;
      } else {
        next_window_ = 1024U * 1024U;
      }
      prefetch_.reset();
    }

    const uint64_t remaining = object_size - offset;
    const size_t length = size_t(std::min<uint64_t>(
        remaining, std::max(wanted, next_window_)));
    if (length <= wanted) {
      return false;
    }

    const size_t maximum = state.config.max_prefetch_window_size;
    while (!retained_.empty() &&
           (length >= maximum || retained_bytes_ > maximum - length)) {
      retained_bytes_ -= retained_.front()->length;
      retained_.pop_front();
    }

    started_lease = state.http->try_acquire_bulk();
    if (!started_lease) {
      return false;
    }

    WorkerState& worker = worker_state(state);
    selected = allocate_prefetch(state, handle.inode, offset, wanted, object_size, length);
    if (!selected || selected->length <= wanted) return false;
    if (!selected->pin(offset, wanted)) abort();
    pin.value = selected;
    const AuthorizedRangeRequest range = make_range_request(
        state, handle, worker, offset, selected->length);
    if (offset % state.page_size == 0 && wanted % state.page_size == 0) {
      publisher = std::make_shared<LegacyPrefetchPublisher>(state, handle, selected, wanted);
      selected->legacy_progress = [weak = std::weak_ptr(publisher)] {
        if (auto value = weak.lock()) value->progress();
      };
    }
    prefetch_ = selected;
    reader_guard.unlock();
    try {
      started_download = started_lease->begin_range_to_fd(
          range.path(), selected->offset, selected->length, selected->sink,
          range.headers, true, false);
      const size_t received = started_download->receive_at_least(wanted);
      const bool range_valid = content_range_matches(
          started_download->response(), selected->offset,
          selected->length, object_size);
      if (received < wanted || !range_valid) {
        {
          std::lock_guard guard(selected->mutex);
          selected->error = std::make_exception_ptr(
              std::runtime_error("invalid read-ahead response"));
          selected->complete = true;
        }
        selected->notify_waiters();
        reader_guard.lock();
        if (prefetch_ == selected) {
          prefetch_.reset();
        }
        reader_guard.unlock();
        return false;
      }
      selected->range_valid = true;
      selected->sink.start_publishing(started_download->response());
    } catch (...) {
      {
        std::lock_guard guard(selected->mutex);
        selected->error = std::current_exception();
        selected->complete = true;
      }
      selected->notify_waiters();
      reader_guard.lock();
      if (prefetch_ == selected) {
        prefetch_.reset();
      }
      reader_guard.unlock();
      return false;
    }
    reader_guard.lock();
  }

  if (!pin.value) pin.value = selected;
  const size_t relative = size_t(offset - selected->offset);
  if (!started_download) {
    reader_guard.unlock();
    if (!wait_for_prefetch(*selected, relative + wanted)) {
      return false;
    }
  }
  if (!selected->range_valid) {
    return false;
  }

  if (started_download) {
    auto tail = std::make_unique<PrefetchTailTask>(PrefetchTailTask{
        selected,
        state.config.stats_interval_seconds != 0,
        &state.remote_reads, &state.remote_read_bytes,
        std::move(started_lease), std::move(started_download), std::move(publisher)});
    try {
      PrefetchTailTask* submitted = tail.release();
      try {
        thread_ = std::thread(run_prefetch_tail, submitted);
      } catch (...) {
        tail.reset(submitted);
        throw;
      }
    } catch (...) {
      {
        std::lock_guard guard(selected->mutex);
        selected->error = std::current_exception();
        selected->complete = true;
      }
      selected->notify_waiters();
    }
  }
  int result;
  if (wanted < 2 * state.page_size) {
    result = fuse_reply_buf(
        request,
        static_cast<const char*>(selected->storage->mapping) + relative,
        wanted);
  } else {
    fuse_bufvec buffers{};
    buffers.count        = 1;
    buffers.buf[0].size  = wanted;
    buffers.buf[0].flags = fuse_buf_flags(
        FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK | FUSE_BUF_FD_RETRY);
    buffers.buf[0].fd    = selected->storage->fd.get();
    buffers.buf[0].pos   = off_t(relative);
    result = fuse_reply_data(
        request, &buffers, FUSE_BUF_SPLICE_MOVE);
  }
  if (result != 0) {
    fprintf(stderr, "fuse_reply_data(read-ahead) failed: %s\n",
            strerror(-result));
  }
  return true;
}

void UncachedFileReader::read(
    State& state, OpenHandle& handle, fuse_req_t request,
    fuse_ino_t inode, size_t size, off_t offset) {
  read_open_file(state, handle, false, this,
                 request, inode, size, offset);
}

std::shared_ptr<UncachedPrefetch> UncachedFileReader::select_async(
    State& state, fuse_ino_t inode, uint64_t offset, size_t wanted, uint64_t object_size,
    bool& created) {
  std::lock_guard guard(mutex_);
  created = false;
  const auto contains = [&](UncachedPrefetch& p) {
    return p.pin(offset, wanted);
  };
  if (prefetch_ && contains(*prefetch_)) {
    return prefetch_;
  }
  for (auto i = retained_.rbegin(); i != retained_.rend(); ++i) {
    if (contains(**i)) {
      return *i;
    }
  }
  const size_t maximum = state.config.max_prefetch_window_size;
  if (prefetch_ && offset != prefetch_->offset + prefetch_->length) {
    bool pending;
    {
      std::lock_guard progress_guard(prefetch_->mutex);
      pending = !prefetch_->complete && !prefetch_->error;
    }
    if (pending) {
      // A disjoint demand must not cancel the useful in-flight window or
      // expand another speculative window. Keep its bytes for overlap reuse.
      auto selected = allocate_prefetch(state, inode, offset, wanted, object_size, wanted);
      if (!selected) return {};
      retained_.push_back(selected);
      retained_bytes_ += wanted;
      while (retained_.size() > 1 && retained_bytes_ > maximum) {
        retained_bytes_ -= retained_.front()->length;
        retained_.pop_front();
      }
      created = true;
      if (!selected->pin(offset, wanted)) abort();
      return selected;
    }
  }
  size_t next = next_window_;
  if (prefetch_) {
    if (offset == prefetch_->offset + prefetch_->length) {
      next += std::min(next, maximum - next);
    } else {
      next = 1024U * 1024U;
    }
  }
  const size_t length = size_t(std::min<uint64_t>(
      object_size - offset, std::max(wanted, next)));
  auto selected = allocate_prefetch(state, inode, offset, wanted, object_size, length);
  if (!selected) return {};
  next_window_ = next;
  if (prefetch_) {
    retained_.push_back(prefetch_);
    retained_bytes_ += prefetch_->length;
  }
  while (!retained_.empty() &&
         (length >= maximum || retained_bytes_ > maximum - length)) {
    retained_bytes_ -= retained_.front()->length;
    retained_.pop_front();
  }
  prefetch_ = selected;
  created = true;
  if (!selected->pin(offset, wanted)) abort();
  return selected;
}

class CachedFileReader final : public FileReader {
 public:
  void read(State& state, OpenHandle& handle, fuse_req_t request,
            fuse_ino_t inode, size_t size, off_t offset) override {
    read_open_file(state, handle, true, nullptr,
                   request, inode, size, offset);
  }
};

class UncachedFileWriter final : public FileWriter {
 public:
  void write(State& state, OpenHandle& handle, fuse_req_t request,
             fuse_ino_t inode, fuse_bufvec* input, off_t offset) override;
  void write_async(State&, OpenHandle&, fuse_req_t, fuse_ino_t,
                    fuse_bufvec*, off_t, FuseReactor&) override;
  void flush(State& state, OpenHandle& handle) override;
  void fsync(State& state, OpenHandle& handle, bool data_only) override;
  void release(State& state, OpenHandle& handle) noexcept override;
};

class CachedFileWriter final : public FileWriter {
 public:
  void write(State& state, OpenHandle& handle, fuse_req_t request,
             fuse_ino_t inode, fuse_bufvec* input, off_t offset) override;
  void write_async(State&, OpenHandle&, fuse_req_t, fuse_ino_t,
                    fuse_bufvec*, off_t, FuseReactor&) override;
  void flush(State& state, OpenHandle& handle) override;
  void fsync(State& state, OpenHandle& handle, bool data_only) override;
  void release(State& state, OpenHandle& handle) noexcept override;
};

std::shared_ptr<FileReader> make_file_reader(State& state,
                                             OpenHandle& handle) {
  if (state.local_cache) {
    return std::make_shared<CachedFileReader>();
  }
  std::lock_guard guard(state.open_files_mutex);
  auto found = state.open_files.find(handle.object_path);
  if (found == state.open_files.end()) {
    throw std::system_error(EBADF, std::generic_category(), "unregistered read handle");
  }
  auto& opened = found->second;
  if (opened.reader_inode == handle.inode && opened.reader_epoch == handle.generation_epoch) {
    if (auto reader = opened.uncached_reader.lock()) return reader;
  }
  auto reader = std::make_shared<UncachedFileReader>();
  opened.uncached_reader = reader;
  opened.reader_inode    = handle.inode;
  opened.reader_epoch    = handle.generation_epoch;
  return reader;
}

std::unique_ptr<FileWriter> make_file_writer(State& state,
                                              OpenHandle&) {
  if (state.local_cache) {
    return std::make_unique<CachedFileWriter>();
  }
  return std::make_unique<UncachedFileWriter>();
}

struct AsyncRangeTransfer {
  struct RetrySink final : RangeFileSink {
    UncachedPrefetch& prefetch;
    RetrySink(int fd, UncachedPrefetch& value)
        : RangeFileSink(fd, 0, false), prefetch(value) {}
    void progress(const Response&, bool) override {}
    bool cancelled() const noexcept override {
      return prefetch.cancelled.load(std::memory_order_acquire);
    }
  };
  State& state;
  FuseReactor& reactor;
  std::shared_ptr<UncachedPrefetch> prefetch;
  uint64_t object_size;
  AsyncHttpRequest arguments;
  std::unique_ptr<AsyncS3Request> http;
  std::unique_ptr<AsyncSignedS3Request> retry_http;
  std::unique_ptr<ReadChecksumContext> checksums;
  std::unique_ptr<AsyncReadIdentityGate> retry_identity;
  std::shared_ptr<AsyncPrefetchPublisher> publisher;
  std::shared_ptr<ReadAheadStoragePool::Storage> retry_storage;
  std::unique_ptr<RangeFileSink> retry_sink;
  Response checksum_response;
  FuseReactor::ReactorTask checksum_completion;
  PrefetchContinuation retry_continuation;
  std::exception_ptr checksum_error;
  bool retried = false;
  bool checksum_valid = false;
  bool checksum_available = false;

  struct Sink final : RangeFileSink {
    AsyncRangeTransfer& owner;
    explicit Sink(AsyncRangeTransfer& value)
        : RangeFileSink(value.prefetch->storage->fd.get(), 0, false),
          owner(value) {}
    bool cancelled() const noexcept override {
      return owner.prefetch->cancelled.load(std::memory_order_acquire);
    }
    void progress(const Response& response, bool complete) override {
      if (cancelled()) return;
      if (owner.retried) return;
      if (!owner.prefetch->range_valid) {
        if (!content_range_matches(response, owner.prefetch->offset,
                                   owner.prefetch->length,
                                   owner.object_size)) {
          throw_inconsistent_range_response(
              response, owner.prefetch->offset, owner.prefetch->length,
              owner.object_size, "asynchronous GetObject");
        }
        {
          std::lock_guard guard(owner.prefetch->mutex);
          owner.prefetch->range_valid = true;
        }
        owner.prefetch->sink.start_publishing(response);
      } else {
        owner.prefetch->sink.progress(response, complete);
      }
      if (owner.publisher) owner.publisher->progress();
    }
  } sink;

  AsyncRangeTransfer(State& s, FuseReactor& r,
                      std::shared_ptr<UncachedPrefetch> p, uint64_t size,
                      AsyncHttpRequest args)
      : state(s), reactor(r), prefetch(std::move(p)), object_size(size),
        arguments(std::move(args)), sink(*this) {
    arguments.destination = &sink;
  }

  static void received(void* context, Response&& response,
                        std::exception_ptr error) noexcept {
    std::unique_ptr<AsyncRangeTransfer> task(
        static_cast<AsyncRangeTransfer*>(context));
    task->http.reset();
    task->retry_http.reset();
    if (task->retry_identity) task->retry_identity->unlock();
    try {
      if (!error && (!content_range_matches(response, task->prefetch->offset,
                                            task->prefetch->length, task->object_size) ||
                    response.body_bytes != task->prefetch->length)) {
        throw std::system_error(EIO, std::generic_category(),
                                "incomplete asynchronous GetObject");
      }
      if (!error && task->state.config.stats_interval_seconds != 0) {
        task->state.remote_reads.fetch_add(1, std::memory_order_relaxed);
        task->state.remote_read_bytes.fetch_add(
            response.body_bytes, std::memory_order_relaxed);
      }
    } catch (...) {
      error = std::current_exception();
    }
    if (task->checksums && !error) {
      task->checksum_response = std::move(response);
      task.release()->verify();
      return;
    }
    if (task->checksums && task->retried && error) {
      task->checksums->handle.read_checksum_bad.store(true, std::memory_order_release);
      queue_page_invalidation(task->state, task->checksums->handle,
                              off_t(task->prefetch->offset), off_t(task->prefetch->length));
    }
    {
      std::lock_guard guard(task->prefetch->mutex);
      task->prefetch->error = error;
      task->prefetch->complete = true;
      task->prefetch->checksum_retrying = false;
      task->prefetch->checksum_bad = task->retried && error != nullptr;
    }
    task->prefetch->notify_waiters();
    if (error && task->publisher) task->publisher->stop();
    else if (task->publisher) task->publisher->progress();
    else task->prefetch->publication_finished();
    task->prefetch->retire();
  }

  static void verified(void* value) noexcept {
    std::unique_ptr<AsyncRangeTransfer> task(static_cast<AsyncRangeTransfer*>(value));
    if (!task->checksum_error && !task->checksum_valid && !task->retried) {
      try {
        task->retried = true;
        {
          std::lock_guard guard(task->prefetch->mutex);
          task->prefetch->checksum_retrying = true;
          task->prefetch->complete = false;
        }
        if (task->publisher) {
          task->publisher->begin_retry();
        } else {
          queue_page_invalidation(task->state, task->checksums->handle,
                                   off_t(task->prefetch->offset), off_t(task->prefetch->length));
        }
        task->retry_identity = std::make_unique<AsyncReadIdentityGate>(*task->checksums);
        if (!task->retry_identity->acquire(identity_ready, task.get())) {
          task.release();
          return;
        }
        task.release()->start_retry();
        return;
      } catch (...) {
        task->checksum_error = std::current_exception();
      }
    }
    const bool valid = !task->checksum_error && task->checksum_valid;
    if (valid && !task->checksum_available && task->publisher) {
      task->publisher->stop();
      task->publisher.reset();
      fprintf(stderr, "warning: prefetch checksum unavailable; withholding proactive STORE: path=%s\n",
              task->checksums->path.c_str());
    }
    if (!valid) {
      if (task->publisher) task->publisher->stop();
      task->checksums->handle.read_checksum_bad.store(true, std::memory_order_release);
      queue_page_invalidation(task->state, task->checksums->handle,
                               off_t(task->prefetch->offset), off_t(task->prefetch->length));
      fprintf(stderr, "error: asynchronous uncached checksum retry failed: path=%s\n",
              task->checksums->path.c_str());
    }
    {
      std::lock_guard guard(task->prefetch->mutex);
      if (valid && task->retry_storage) {
        task->prefetch->storage = task->retry_storage;
        task->prefetch->published_prefix = 0;
        task->prefetch->retired_prefix = 0;
      }
      task->prefetch->complete = true;
      task->prefetch->checksum_retrying = false;
      task->prefetch->checksum_bad = !valid;
      task->prefetch->checksum_hold = false;
    }
    task->prefetch->notify_waiters();
    if (valid && task->publisher) {
      if (task->retried) task->publisher->retry_complete();
      else task->publisher->progress();
    } else if (!task->publisher) task->prefetch->publication_finished();
    task->prefetch->retire();
  }

  static void identity_ready(void* value, std::exception_ptr error) noexcept {
    auto* self = static_cast<AsyncRangeTransfer*>(value);
    if (error) {
      self->checksum_error = error;
      verified(self);
      return;
    }
    self->start_retry();
  }

  void start_retry() noexcept {
    try {
      {
        // Unverified units are never STOREd. Pending READs already wait on
        // checksum_retrying; only actual replies can still use the old bytes.
        std::lock_guard guard(prefetch->mutex);
        if (prefetch->active_replies != 0) {
          retry_continuation.reactor = &reactor;
          retry_continuation.task = {retry_after_replies, retry_cancelled, this};
          if (!reactor.reserve_completion(&retry_continuation.task)) throw std::system_error(errno, std::generic_category(), "reserve retry reply drain");
          prefetch->reply_drained = &retry_continuation;
          return;
        }
        retry_storage = prefetch->storage;
      }
      retry_sink = std::make_unique<RetrySink>(retry_storage->fd.get(), *prefetch);
      auto args = checksum_retry_request(
          *checksums, prefetch->offset, prefetch->length, *retry_sink);
      args.capture_headers = true;
      retry_http = std::make_unique<AsyncSignedS3Request>(
          state, reactor, std::move(args), received, this, 1);
      if (!retry_http->start()) {
        throw std::system_error(errno, std::generic_category(),
                                "submit uncached checksum retry");
      }
    } catch (...) {
      retry_identity->unlock();
      checksum_error = std::current_exception();
      verified(this);
    }
  }

  static void retry_after_replies(void* context) noexcept {
    static_cast<AsyncRangeTransfer*>(context)->start_retry();
  }
  static void retry_cancelled(void* context) noexcept {
    auto* task = static_cast<AsyncRangeTransfer*>(context);
    task->checksum_error = std::make_exception_ptr(std::system_error(ECANCELED, std::generic_category()));
    verified(task);
  }

  void verify() noexcept {
    checksum_completion = {verified, nullptr, this};
    if (!reactor.reserve_completion(&checksum_completion)) {
      retried = true;
      verified(this);
      return;
    }
    try {
      state.uploads->submit(&checksums->handle, [this] {
        try {
          IoExecutorScope local_only(nullptr, 0);
          ChecksumAlgorithm algorithm = CHECKSUM_NONE;
          std::string_view expected;
          if (!read_checksum_from_response(checksum_response, state.config.checksum,
                                            algorithm, expected)) {
            checksum_valid = !retried;
          } else {
            checksum_available = true;
            DataChecksum checksum(algorithm);
            const auto& storage = retried ? retry_storage : prefetch->storage;
            checksum.update(std::span(
                static_cast<const std::byte*>(storage->mapping), prefetch->length));
            const auto actual = checksum.finish();
            uint64_t value = 0;
            checksum_valid = algorithm == CHECKSUM_CRC64XZ
                ? parse_unsigned(expected, value) && value == actual.integer
                : expected == sso_view(actual.base64);
          }
        } catch (...) {
          checksum_error = std::current_exception();
        }
        reactor.complete(&checksum_completion);
      });
    } catch (...) {
      checksum_error = std::current_exception();
      reactor.complete(&checksum_completion);
    }
  }

  void start() noexcept {
    try {
      bool accepted;
      if (state.config.directory_bucket) {
        retry_http = std::make_unique<AsyncSignedS3Request>(
            state, reactor, std::move(arguments), received, this);
        accepted = retry_http->start();
      } else {
        http = std::make_unique<AsyncS3Request>(
            state, reactor, std::move(arguments), received, this);
        accepted = http->start();
      }
      if (!accepted) {
        throw std::system_error(errno, std::generic_category(),
                                "submit asynchronous GetObject");
      }
    } catch (...) {
      received(this, Response{}, std::current_exception());
    }
  }
};

struct AsyncReadTask {
  AsyncReadTask(State& state_value, OpenHandle& handle_value,
                fuse_req_t request_value, fuse_ino_t inode_value,
                size_t size_value, off_t offset_value)
      : state(&state_value), handle(&handle_value),
        request(request_value), inode(inode_value), size(size_value),
        offset(offset_value), active(handle_value),
        identity(handle_value.identity_mutex, std::defer_lock) {}
  ~AsyncReadTask() {
    if (prefetch_pinned) prefetch->retire(true);
  }

  State* state;
  OpenHandle* handle;
  fuse_req_t request;
  fuse_ino_t inode;
  size_t size;
  off_t offset;
  OpenRequestGuard active;
  std::shared_lock<ReactorSharedMutex> identity;
  FuseReactor* reactor = nullptr;
  std::shared_ptr<UncachedPrefetch> prefetch;
  bool prefetch_pinned = false;
  PrefetchContinuation continuation;
  AsyncIoRequest wait;
  uint64_t notification = 0;
  size_t wanted = 0;
  size_t expansion = 0;
  CacheFetchClaim cache_claim;
  std::unique_ptr<CacheReadSink> cache_sink;
  std::unique_ptr<AsyncS3Request> cache_http;
  std::unique_ptr<AsyncSignedS3Request> cache_signed_http;
  std::exception_ptr prepare_error;
  bool cache_prepared = false;
  std::unique_ptr<ReadChecksumContext> checksums;
  AsyncCredentialWait credential_wait;
  bool credential_cached = false;

  void credential_failed(std::exception_ptr error) noexcept {
    if (credential_cached) {
      cache_received(this, Response{}, error);
      return;
    }
    {
      std::lock_guard guard(prefetch->mutex);
      prefetch->error = error;
      prefetch->complete = true;
    }
    prefetch->publication_finished();
    prefetch->notify_waiters();
    try { std::rethrow_exception(error); }
    catch (...) { reply_callback_error(request); }
    delete this;
  }

  static void credentials_ready(void* context, ssize_t result) noexcept {
    auto* task = static_cast<AsyncReadTask*>(context);
    try {
      if (result < 0) {
        throw std::system_error(int(-result), std::generic_category(),
                                "wait for read credentials");
      }
      if (task->credential_cached) cache_prepared_ready(task);
      else task->start_uncached_transfer();
    } catch (...) {
      task->credential_failed(std::current_exception());
    }
  }

  void credentials_pending(bool cached) noexcept {
    credential_cached = cached;
    try {
      if (credential_wait.start(*state, *reactor, credentials_ready, this)) return;
      throw std::system_error(errno, std::generic_category(),
                              "submit read credential wait");
    } catch (...) {
      credential_failed(std::current_exception());
    }
  }

  static void cancel(void* context) noexcept {
    std::unique_ptr<AsyncReadTask> task(static_cast<AsyncReadTask*>(context));
    if (task->cache_claim) {
      task->handle->cache_entry->fail_fetch(task->cache_claim);
    }
    if (!task->cache_sink || !task->cache_sink->replied()) {
      fuse_reply_err(task->request, EIO);
    }
  }

  static void cache_available(void* context, ssize_t result) noexcept {
    auto* task = static_cast<AsyncReadTask*>(context);
    task->handle->cache_entry->end_async_wait();
    if (result < 0) {
      fuse_reply_err(task->request, -int(result));
      delete task;
      return;
    }
    task->read_cache();
  }

  static void cache_received(void* context, Response&& response,
                              std::exception_ptr error) noexcept {
    std::unique_ptr<AsyncReadTask> task(static_cast<AsyncReadTask*>(context));
    CacheEntry& entry = *task->handle->cache_entry;
    try {
      if (error) std::rethrow_exception(error);
      if (!cache_response_matches(response, task->cache_claim, task->handle->size)) {
        throw_inconsistent_range_response(
            response, task->cache_claim.offset, task->cache_claim.length,
            task->handle->size, "asynchronous cached GetObject");
      }
      const CacheFetchClaim fetched = task->cache_claim;
      if (task->checksums && fetched.offset == 0 &&
          fetched.length == task->handle->size &&
          !entry.checksum_manifest_available()) {
        auto* check = new AsyncCacheChecksum(
            *task->checksums, fetched, std::move(response));
        task->cache_claim = {};
        check->start();
      } else {
        entry.finish_fetch(fetched);
        if (task->checksums) {
          start_cached_checksums(*task->checksums, fetched.offset, fetched.length);
        }
      }
      task->cache_claim = {};
      if (task->state->config.stats_interval_seconds != 0) {
        task->state->remote_reads.fetch_add(1, std::memory_order_relaxed);
        task->state->remote_read_bytes.fetch_add(
            fetched.length, std::memory_order_relaxed);
      }
      if (task->cache_sink->replied()) return;
      task->cache_http.reset();
      task->cache_signed_http.reset();
      task->cache_sink.reset();
      task.release()->read_cache();
    } catch (...) {
      entry.fail_fetch(task->cache_claim);
      if (!task->cache_sink || !task->cache_sink->replied()) {
        reply_callback_error(task->request);
      } else {
        fprintf(stderr, "warning: cache fill failed after asynchronous read reply\n");
      }
    }
  }

  static void cache_prepared_ready(void* context) noexcept {
    auto* task = static_cast<AsyncReadTask*>(context);
    try {
      if (task->prepare_error) std::rethrow_exception(task->prepare_error);
      if (!task->cache_prepared) {
        task->handle->cache_entry->fail_fetch(task->cache_claim);
        task->cache_claim = {};
        task->read_uncached();
        return;
      }
      WorkerState& worker = worker_state(*task->state);
      const auto range = make_range_request(
          *task->state, *task->handle, worker,
          task->cache_claim.offset, task->cache_claim.length,
          !task->state->config.directory_bucket);
      task->cache_sink = std::make_unique<CacheReadSink>(
          *task->handle->cache_entry, task->cache_claim, task->request,
          uint64_t(task->offset), task->wanted);
      AsyncHttpRequest args;
      args.method.assign("GET");
      args.path.assign(range.path().data(), range.path().size());
      args.headers.assign(range.headers.begin(), range.headers.end());
      args.destination       = task->cache_sink.get();
      args.range             = true;
      args.offset            = task->cache_claim.offset;
      args.length            = task->cache_claim.length;
      args.measure_transport = task->state->config.report_metrics;
      args.capture_headers   = task->state->config.verify_read_checksum;
      bool accepted;
      if (task->state->config.directory_bucket) {
        task->cache_signed_http = std::make_unique<AsyncSignedS3Request>(
            *task->state, *task->reactor, std::move(args), cache_received, task);
        accepted = task->cache_signed_http->start();
      } else {
        task->cache_http = std::make_unique<AsyncS3Request>(
            *task->state, *task->reactor, std::move(args), cache_received, task);
        accepted = task->cache_http->start();
      }
      if (!accepted) {
        throw std::system_error(errno, std::generic_category(),
                                "submit asynchronous cached GetObject");
      }
    } catch (const CredentialRefreshPending&) {
      task->credentials_pending(true);
    } catch (...) {
      cache_received(task, Response{}, std::current_exception());
    }
  }

  void read_cache() noexcept {
    try {
      CacheEntry& entry = *handle->cache_entry;
      if (state->config.verify_read_checksum && !checksums) {
        checksums = std::make_unique<ReadChecksumContext>(
            *state, *reactor, *handle, active);
        ensure_async_checksum_manifest(*checksums);
      }
      if (expansion == 0) {
        expansion = cache_fetch_expansion(*state, *handle, uint64_t(offset), wanted);
      }
      for (;;) {
        if (checksums) {
          if (entry.stale()) {
            throw std::system_error(ESTALE, std::generic_category(), "stale checksum generation");
          }
          if (entry.checksum_failed(uint64_t(offset), wanted)) {
            throw std::system_error(EIO, std::generic_category(), "cached part checksum mismatch");
          }
          const int fd = entry.begin_checksum_wait(uint64_t(offset), wanted);
          if (fd >= 0) {
            wait.kind       = AsyncIoRequest::READ;
            wait.fd         = fd;
            wait.data       = &notification;
            wait.length     = sizeof(notification);
            wait.timeout_ms = 0;
            wait.complete   = cache_available;
            wait.context    = this;
            if (reactor->submit(wait)) return;
            entry.end_async_wait();
            throw std::system_error(errno, std::generic_category(), "submit checksum wait");
          }
          start_cached_checksums(*checksums, uint64_t(offset), wanted);
        }
        if (entry.pin_clean(uint64_t(offset), wanted)) {
          const int result = reply_pinned_cached_range(
              request, entry, uint64_t(offset), wanted);
          if (result != 0) {
            fprintf(stderr, "asynchronous cache hit reply failed: %s\n",
                    strerror(-result));
          }
          delete this;
          return;
        }
        if (entry.range_bad(uint64_t(offset), wanted)) {
          throw std::system_error(EIO, std::generic_category(), "poisoned cache range");
        }
        cache_claim = entry.claim_fetch(uint64_t(offset), wanted, expansion);
        if (cache_claim) break;
        const int fd = entry.begin_async_wait(uint64_t(offset), wanted);
        if (fd < 0) continue;
        wait.kind       = AsyncIoRequest::READ;
        wait.fd         = fd;
        wait.data       = &notification;
        wait.length     = sizeof(notification);
        wait.timeout_ms = 0;
        wait.complete   = cache_available;
        wait.context    = this;
        if (reactor->submit(wait)) return;
        entry.end_async_wait();
        throw std::system_error(errno, std::generic_category(), "submit cache wait");
      }
      // Allocation/eviction can write to local storage. Only its completion
      // comes back here; HTTP parsing and socket I/O never enter this worker.
      continuation.task = {cache_prepared_ready, cancel, this};
      if (!reactor->reserve_completion(&continuation.task)) {
        throw std::system_error(errno, std::generic_category(), "reserve cache completion");
      }
      try {
        state->uploads->submit(handle, [this] {
          try {
            IoExecutorScope local_only(nullptr, 0);
            cache_prepared = handle->cache_entry->prepare_read(
                cache_claim.offset, cache_claim.length);
          } catch (...) {
            prepare_error = std::current_exception();
          }
          reactor->complete(&continuation.task);
        });
      } catch (...) {
        prepare_error = std::current_exception();
        reactor->complete(&continuation.task);
      }
    } catch (...) {
      if (cache_claim) handle->cache_entry->fail_fetch(cache_claim);
      reply_callback_error(request);
      delete this;
    }
  }

  static void available(void* context, ssize_t result) noexcept {
    auto* task = static_cast<AsyncReadTask*>(context);
    task->handle->identity_mutex.end_async_wait();
    if (result < 0) {
      fuse_reply_err(task->request, -int(result));
      delete task;
      return;
    }
    task->start();
  }

  static void ready(void* context) noexcept {
    std::unique_ptr<AsyncReadTask> task(static_cast<AsyncReadTask*>(context));
    try {
      bool retrying;
      const size_t relative = size_t(uint64_t(task->offset) -
                                     task->prefetch->offset);
      struct ReplySource {
        UncachedPrefetch* prefetch = nullptr;
        std::shared_ptr<ReadAheadStoragePool::Storage> storage;
        ~ReplySource() {
          storage.reset();
          if (prefetch) prefetch->reply_finished();
        }
      } source;
      auto& storage = source.storage;
      {
        std::lock_guard guard(task->prefetch->mutex);
        retrying = task->prefetch->checksum_retrying;
        if (task->prefetch->checksum_bad) {
          throw std::system_error(EIO, std::generic_category(), "poisoned read checksum");
        }
        if (!retrying && (task->prefetch->produced < relative + task->wanted ||
                          !task->prefetch->range_valid)) {
          if (task->prefetch->error != nullptr) {
            std::rethrow_exception(task->prefetch->error);
          }
          throw std::system_error(EIO, std::generic_category(),
                                  "incomplete asynchronous read");
        }
        if (!retrying) {
          storage = task->prefetch->storage;
          ++task->prefetch->active_replies;
          source.prefetch = task->prefetch.get();
        }
      }
      if (retrying) {
        task.release()->wait_prefetch();
        return;
      }
      if (task->handle->stale.load(std::memory_order_acquire)) {
        throw std::system_error(ESTALE, std::generic_category(),
                                "stale asynchronous read");
      }
      int result;
      if (task->wanted < 2 * task->state->page_size) {
        result = fuse_reply_buf(
            task->request,
            static_cast<const char*>(storage->mapping) + relative,
            task->wanted);
      } else {
        fuse_bufvec buffers{};
        buffers.count        = 1;
        buffers.buf[0].size  = task->wanted;
        buffers.buf[0].flags = fuse_buf_flags(
            FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK | FUSE_BUF_FD_RETRY);
        buffers.buf[0].fd    = storage->fd.get();
        buffers.buf[0].pos   = off_t(relative);
        result = fuse_reply_data(
            task->request, &buffers, FUSE_BUF_SPLICE_MOVE);
      }
      if (result != 0) {
        fprintf(stderr, "asynchronous read reply failed: %s\n", strerror(-result));
      }
    } catch (...) {
      reply_callback_error(task->request);
    }
  }

  void start() noexcept {
    try {
      if (!identity.try_lock()) {
        const int fd = handle->identity_mutex.begin_async_wait();
        if (!identity.try_lock()) {
          wait.kind       = AsyncIoRequest::READ;
          wait.fd         = fd;
          wait.data       = &notification;
          wait.length     = sizeof(notification);
          wait.timeout_ms = 0;
          wait.complete   = available;
          wait.context    = this;
          if (reactor->submit(wait)) return;
          handle->identity_mutex.end_async_wait();
          throw std::system_error(errno, std::generic_category(),
                                  "submit identity wait");
        }
        handle->identity_mutex.end_async_wait();
      }
      if (offset < 0) {
        throw std::system_error(EINVAL, std::generic_category(), "read offset");
      }
      if (handle->inode != inode || handle->writable) {
        throw std::system_error(EBADF, std::generic_category(), "read handle");
      }
      if (handle->read_checksum_bad.load(std::memory_order_acquire)) {
        throw std::system_error(EIO, std::generic_category(), "read checksum mismatch");
      }
      if (handle->stale.load(std::memory_order_acquire) ||
          (handle->generation_epoch != 0 &&
           (handle->item->generation_epoch.load(std::memory_order_acquire) &
            ((1ULL << 63) - 1)) != handle->generation_epoch)) {
        throw std::system_error(ESTALE, std::generic_category(), "read generation");
      }
      const uint64_t begin = uint64_t(offset);
      if (begin >= handle->size || size == 0) {
        fuse_reply_buf(request, nullptr, 0);
        delete this;
        return;
      }
      wanted = size_t(std::min<uint64_t>(size, handle->size - begin));
      warn_random_read(*state, handle->object_path, handle->size, begin, size);
      if (handle->recovery_read && handle->cache_entry) {
        reply_cached_range(request, *handle->cache_entry, begin, wanted);
        delete this;
        return;
      }
      if (handle->cache_entry) {
        read_cache();
        return;
      }
      read_uncached();
    } catch (...) {
      reply_callback_error(request);
      delete this;
    }
  }

  void read_uncached() noexcept {
    try {
      const uint64_t begin = uint64_t(offset);
      const auto budget = state->prefetch_budget.snapshot();
      if (budget.stopped) throw std::system_error(ECANCELED, std::generic_category(), "prefetch budget stopped");
      bool created;
      if (state->local_cache) {
        prefetch = allocate_prefetch(*state, inode, begin, wanted, handle->size, wanted);
        if (prefetch && !prefetch->pin(begin, wanted)) abort();
        created = true;
      } else {
        prefetch = static_cast<UncachedFileReader*>(handle->reader.get())->select_async(
            *state, inode, begin, wanted, handle->size, created);
      }
      if (!prefetch) {
        const int fd = state->prefetch_budget.begin_async_wait(budget.revision);
        if (fd < 0) {
          continuation.task = {budget_ready, cancel, this};
          if (!reactor->reserve_completion(&continuation.task)) throw std::system_error(errno, std::generic_category(), "budget retry");
          reactor->complete(&continuation.task);
          return;
        }
        wait = {};
        wait.kind = AsyncIoRequest::READ;
        wait.fd = fd;
        wait.data = &notification;
        wait.length = sizeof(notification);
        wait.complete = budget_available;
        wait.context = this;
        if (reactor->submit(wait)) return;
        state->prefetch_budget.end_async_wait();
        throw std::system_error(errno, std::generic_category(), "submit prefetch budget wait");
      }
      prefetch_pinned = true;
      if (created) {
        start_uncached_transfer();
        return;
      }
      wait_prefetch();
    } catch (...) {
      reply_callback_error(request);
      delete this;
    }
  }

  static void budget_ready(void* context) noexcept {
    static_cast<AsyncReadTask*>(context)->read_uncached();
  }
  static void budget_available(void* context, ssize_t result) noexcept {
    auto* task = static_cast<AsyncReadTask*>(context);
    task->state->prefetch_budget.end_async_wait();
    if (result < 0 && result != -EAGAIN) {
      fuse_reply_err(task->request, -int(result));
      delete task;
    } else task->read_uncached();
  }

  void start_uncached_transfer() noexcept {
    try {
      WorkerState& worker = worker_state(*state);
      const auto range = make_range_request(
          *state, *handle, worker, prefetch->offset, prefetch->length,
          !state->config.directory_bucket);
      AsyncHttpRequest args;
      args.method.assign("GET");
      args.path.assign(range.path().data(), range.path().size());
      args.headers.assign(range.headers.begin(), range.headers.end());
      args.range             = true;
      args.offset            = prefetch->offset;
      args.length            = prefetch->length;
      args.measure_transport = state->config.report_metrics;
      args.capture_headers   = state->config.verify_read_checksum;
      auto transfer = std::make_unique<AsyncRangeTransfer>(
          *state, *reactor, prefetch, handle->size, std::move(args));
      if (!state->local_cache && prefetch->length > wanted &&
          (!state->config.verify_read_checksum || prefetch->checksum_hold) &&
          prefetch->offset % state->page_size == 0 && wanted % state->page_size == 0) {
        transfer->publisher = std::make_shared<AsyncPrefetchPublisher>(
            *state, *reactor, *handle, prefetch, wanted);
      }
      if (state->config.verify_read_checksum && prefetch->offset == 0 &&
          prefetch->length == handle->size) {
        transfer->checksums = std::make_unique<ReadChecksumContext>(
            *state, *reactor, *handle, active);
      }
      transfer.release()->start();
      wait_prefetch();
    } catch (const CredentialRefreshPending&) {
      credentials_pending(false);
    } catch (...) {
      credential_cached = false;
      credential_failed(std::current_exception());
    }
  }

  void wait_prefetch() noexcept {
    continuation.reactor = reactor;
    continuation.task    = {ready, cancel, this};
    continuation.wanted  = size_t(uint64_t(offset) - prefetch->offset) + wanted;
    if (!reactor->reserve_completion(&continuation.task)) {
      fuse_reply_err(request, errno);
      delete this;
      return;
    }
    if (!prefetch->subscribe(continuation)) reactor->complete(&continuation.task);
  }
};

void ngs3fs_read(fuse_req_t request, fuse_ino_t inode, size_t size,
                 off_t offset, fuse_file_info* file) {
  try {
    State& state = state_from(request);
    OpenHandle& handle = handle_required(file);
    if (!handle.reader) {
      throw std::system_error(EBADF, std::generic_category(),
                              "missing file reader");
    }
    FuseReactor* reactor = current_fuse_reactor();
    if (reactor != nullptr) {
      auto task = std::make_unique<AsyncReadTask>(
          state, handle, request, inode, size, offset);
      task->reactor = reactor;
      task.release()->start();
      return;
    }
    OpenRequestGuard active(handle);
    handle.reader->read(state, handle, request, inode, size, offset);
  } catch (...) {
    reply_callback_error(request);
  }
}

void UncachedFileWriter::write(State& state, OpenHandle& handle,
                               fuse_req_t request, fuse_ino_t inode,
                               fuse_bufvec* input, off_t offset) {
  if (offset < 0) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  if (input == nullptr) {
    fuse_reply_err(request, EINVAL);
    return;
  }

  try {
    if (handle.inode != inode) {
      throw std::system_error(EBADF, std::generic_category(), "write inode");
    }
    if (!handle.writable) {
      throw std::system_error(EBADF, std::generic_category(), "write_buf");
    }
    if (handle.unlinked.load(std::memory_order_acquire)) {
      throw std::system_error(ESTALE, std::generic_category(),
                              "write unlinked file");
    }
    const size_t length = fuse_buf_size(input);
    if (length == 0) {
      fuse_reply_write(request, 0);
      return;
    }
    const uint64_t unsigned_offset = static_cast<uint64_t>(offset);
    const uint64_t end = unsigned_offset + length;
    if (end < unsigned_offset) {
      throw std::system_error(EFBIG, std::generic_category(),
                              "write offset overflow");
    }
    const uint64_t maximum_size = std::min(
        state.config.part_size * kMaximumMultipartParts,
        kMaximumObjectSize);
    if (end > maximum_size) {
      throw std::system_error(EFBIG, std::generic_category(),
                              "configured S3 multipart limit exceeded");
    }

    std::unique_lock handle_guard(handle.mutex);
    if (handle.write_state == WRITE_FAILED) {
      throw std::system_error(handle.write_error,
                              std::generic_category(),
                              "write handle failed");
    }
    if (handle.write_state != WRITE_OPEN) {
      throw std::system_error(EIO, std::generic_category(),
                              "write after flush");
    }
    while (handle.write_in_progress) {
      wait_handle(handle, handle_guard);
    }
    if (handle.unlinked.load(std::memory_order_acquire)) {
      throw std::system_error(ESTALE, std::generic_category(),
                              "write unlinked file");
    }
    if (handle.write_state == WRITE_FAILED) {
      throw std::system_error(handle.write_error,
                              std::generic_category(),
                              "write handle failed");
    }
    if (handle.write_state != WRITE_OPEN) {
      throw std::system_error(EIO, std::generic_category(),
                              "write after flush");
    }
    if (unsigned_offset != handle.stream_offset) {
      throw std::system_error(ESPIPE, std::generic_category(),
                              "non-sequential write offset");
    }
    handle.write_in_progress = true;
    if (!handle.part_limit_warned &&
        end > state.config.part_size * 9000ULL) {
      fprintf(stderr,
              "warning: sequential write is approaching the configured "
              "10,000-part limit: bytes=%" PRIu64 " maximum=%" PRIu64
              "\n",
              end, maximum_size);
      handle.part_limit_warned = true;
    }

    size_t remaining = length;
    size_t copied_bytes = 0;
    try {
      while (remaining != 0) {
        if (input->idx >= input->count) {
          throw std::runtime_error("short FUSE write buffer");
        }
        fuse_buf& buf = input->buf[input->idx];
        if (input->off > buf.size) {
          throw std::runtime_error("invalid FUSE write buffer offset");
        }
        const size_t available = buf.size - input->off;
        if (available == 0) {
          ++input->idx;
          input->off = 0;
          continue;
        }
        ensure_current_part(state, handle, handle_guard);
        const size_t part_room = size_t(
            state.config.part_size - handle.current_part->bytes);
        const size_t count = std::min({remaining, available, part_room});
        if ((buf.flags & FUSE_BUF_IS_FD) != 0) {
          const bool seek = (buf.flags & FUSE_BUF_FD_SEEK) != 0;
          if (seek && buf.pos < 0) {
            throw std::system_error(EINVAL, std::generic_category(),
                                    "negative FUSE buffer offset");
          }
          uint64_t position = seek ? uint64_t(buf.pos) + input->off : 0;
          if (seek && position < uint64_t(buf.pos)) {
            throw std::system_error(EOVERFLOW, std::generic_category(),
                                    "FUSE buffer offset overflow");
          }
          retain_fd(state, *handle.current_part, buf.fd, position, count,
                    seek);
        } else {
          const auto* data = static_cast<const std::byte*>(buf.mem);
          if (data == nullptr) {
            throw std::system_error(EFAULT, std::generic_category(),
                                    "null memory-backed FUSE buffer");
          }
          retain_bytes(*handle.current_part,
                       std::span(data + input->off, count));
          copied_bytes += count;
        }
        input->off += count;
        remaining -= count;
        if (input->off == buf.size) {
          ++input->idx;
          input->off = 0;
        }
        submit_current_part(state, handle);
      }
    } catch (const std::system_error& error) {
      handle.write_in_progress = false;
      fail_write(handle, error.code().value());
      throw;
    } catch (...) {
      handle.write_in_progress = false;
      fail_write(handle, EIO);
      throw;
    }
    if (copied_bytes != 0) {
      record_memory_fallback(state, copied_bytes);
    }
    handle.stream_offset     = end;
    handle.size              = end;
    handle.item->set_fsize(end);
    handle.write_in_progress = false;
    notify_handle(handle);
    if (state.config.stats_interval_seconds != 0) {
      state.fuse_writes.fetch_add(1, std::memory_order_relaxed);
      state.fuse_write_bytes.fetch_add(length, std::memory_order_relaxed);
    }
    fuse_reply_write(request, length);
  } catch (...) {
    reply_callback_error(request);
  }
}

void pwrite_cache_exact(int fd, uint64_t offset,
                        std::span<const std::byte> bytes) {
  size_t written = 0;
  while (written != bytes.size()) {
    const ssize_t result = io_pwrite(
        fd, bytes.data() + written, bytes.size() - written,
        off_t(offset + written), 0, true);
    if (result > 0) {
      written += size_t(result);
    } else if (result < 0 && errno == EINTR) {
      continue;
    } else if (result == 0) {
      throw std::system_error(EIO, std::generic_category(),
                              "short cache pwrite");
    } else {
      throw std::system_error(errno, std::generic_category(),
                              "pwrite(cache data)");
    }
  }
}

size_t copy_fd_to_cache(int source_fd, uint64_t& source_offset,
                        bool seek, int cache_fd, uint64_t cache_offset,
                        size_t length) {
  std::array<std::byte, 64U * 1024U> bytes;
  const size_t wanted = std::min(length, bytes.size());
  for (;;) {
    const ssize_t result = seek
        ? io_pread(source_fd, bytes.data(), wanted, off_t(source_offset))
        : io_read(source_fd, bytes.data(), wanted);
    if (result > 0) {
      pwrite_cache_exact(cache_fd, cache_offset,
                         std::span(bytes).first(size_t(result)));
      if (seek) {
        source_offset += uint64_t(result);
      }
      return size_t(result);
    }
    if (result == 0) {
      throw std::system_error(ECONNRESET, std::generic_category(),
                              "FUSE write source reached EOF");
    }
    if (errno != EINTR) {
      throw std::system_error(errno, std::generic_category(),
                              "read(FUSE cache write fallback)");
    }
  }
}

size_t write_fuse_buffers_to_cache(State& state, fuse_bufvec& input,
                                   Pipe& pipe, int cache_fd,
                                   uint64_t cache_offset, size_t length) {
  size_t remaining = length;
  size_t copied    = 0;
  bool fd_fallback = false;
  while (remaining != 0) {
    if (input.idx >= input.count) {
      throw std::runtime_error("short FUSE write buffer");
    }
    fuse_buf& buf = input.buf[input.idx];
    if (input.off > buf.size) {
      throw std::runtime_error("invalid FUSE write buffer offset");
    }
    const size_t available = buf.size - input.off;
    if (available == 0) {
      ++input.idx;
      input.off = 0;
      continue;
    }
    const size_t count = std::min(remaining, available);
    if ((buf.flags & FUSE_BUF_IS_FD) == 0) {
      const auto* data = static_cast<const std::byte*>(buf.mem);
      if (data == nullptr) {
        throw std::system_error(EFAULT, std::generic_category(),
                                "null memory-backed FUSE buffer");
      }
      pwrite_cache_exact(cache_fd, cache_offset,
                         std::span(data + input.off, count));
      copied += count;
    } else {
      if (pipe.capacity() == 0) {
        pipe = Pipe::create(kPreferredIoSize);
      }
      const bool seek = (buf.flags & FUSE_BUF_FD_SEEK) != 0;
      if (seek && buf.pos < 0) {
        throw std::system_error(EINVAL, std::generic_category(),
                                "negative FUSE buffer offset");
      }
      uint64_t source_offset = seek ? uint64_t(buf.pos) + input.off : 0;
      if (seek && source_offset < uint64_t(buf.pos)) {
        throw std::system_error(EOVERFLOW, std::generic_category(),
                                "FUSE buffer offset overflow");
      }
      size_t moved = 0;
      if (!fd_fallback) {
        try {
          uint64_t* position = seek ? &source_offset : nullptr;
          moved = splice_some(buf.fd, position, pipe.write_fd(), count,
                              SPLICE_F_MOVE);
          if (moved == 0) {
            throw std::system_error(EAGAIN, std::generic_category(),
                                    "FUSE write source has no data");
          }
        } catch (const std::system_error& error) {
          const int value = error.code().value();
          if (value != EPERM && value != ENOSYS && value != EINVAL &&
              value != EOPNOTSUPP && value != EXDEV) {
            throw;
          }
          fd_fallback = true;
        }
      }
      if (moved != 0) {
        uint64_t destination = cache_offset;
        try {
          splice_to_fd_exact(pipe.read_fd(), cache_fd, destination,
                             moved, SPLICE_F_MOVE);
        } catch (const std::system_error& error) {
          const int value = error.code().value();
          if (value != EPERM && value != ENOSYS && value != EINVAL &&
              value != EOPNOTSUPP && value != EXDEV) {
            throw;
          }
          if (destination < cache_offset ||
              destination - cache_offset > moved) {
            throw std::runtime_error(
                "invalid cache splice destination offset");
          }
          size_t pipe_remaining = moved - size_t(destination - cache_offset);
          uint64_t unused_offset = 0;
          while (pipe_remaining != 0) {
            const size_t count = copy_fd_to_cache(
                pipe.read_fd(), unused_offset, false, cache_fd,
                destination, pipe_remaining);
            destination    += count;
            pipe_remaining -= count;
            copied         += count;
          }
          fd_fallback = true;
        }
      } else if (fd_fallback) {
        moved = copy_fd_to_cache(buf.fd, source_offset, seek, cache_fd,
                                 cache_offset, count);
        copied += moved;
      }
      if (moved != count) {
        input.off  += moved;
        cache_offset += moved;
        remaining -= moved;
        continue;
      }
    }
    input.off    += count;
    cache_offset += count;
    remaining   -= count;
    if (input.off == buf.size) {
      ++input.idx;
      input.off = 0;
    }
  }
  if (copied != 0) {
    record_memory_fallback(state, copied);
  }
  return copied;
}

ChecksumValue checksum_file_range(ChecksumAlgorithm algorithm, int fd,
                                  uint64_t offset, size_t length) {
  if (!checksum_has_digest(algorithm)) {
    return {};
  }
  DataChecksum checksum(algorithm);
  std::array<std::byte, kPreferredIoSize> bytes;
  size_t consumed = 0;
  while (consumed != length) {
    const size_t count = std::min(bytes.size(), length - consumed);
    ssize_t result;
    do {
      result = ::pread(fd, bytes.data(), count, off_t(offset + consumed));
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
      throw std::system_error(errno, std::generic_category(),
                              "pread(cache upload checksum)");
    }
    if (result == 0) {
      throw std::system_error(EIO, std::generic_category(),
                              "short cache upload checksum input");
    }
    checksum.update(std::span(bytes).first(size_t(result)));
    consumed += size_t(result);
  }
  return checksum.finish();
}

std::string upload_cached_part(State& state, OpenHandle& handle,
                               unsigned number, uint64_t offset,
                               size_t length, ChecksumValue& checksum) {
  ensure_multipart(state, handle);
  checksum = checksum_file_range(
      state.config.checksum, handle.cache_entry->data_fd(), offset, length);
  std::string upload_id;
  {
    std::lock_guard guard(handle.mutex);
    upload_id = handle.upload_id;
  }
  std::string query = "partNumber=" + std::to_string(number);
  query += "&uploadId=" + uri_encode(upload_id, false);
  const std::string path = query_path(handle.object_path, query);
  const Response response = request_with_retries([&] {
    std::vector<Header> headers;
    append_upload_checksum(headers, state.config.checksum, checksum);
    append_authorization(headers, state, "PUT", path, kUnsignedPayload);
    HttpPool::Lease client = state.http->acquire_bulk();
    return client->put_from_fd(path, headers,
                               handle.cache_entry->data_fd(),
                               offset, length);
  }, "UploadPart(cache)");
  require_s3_success(response, "UploadPart(cache)");
  verify_upload_checksum(response, state.config.checksum,
                         checksum, "UploadPart(cache)");
  const auto etag = response.headers.find("etag");
  if (etag == response.headers.end() || etag->second.empty()) {
    throw std::runtime_error("UploadPart(cache) response omitted ETag");
  }
  return std::string(etag->second.data(), etag->second.size());
}

void upload_cached_part_job(State& state, OpenHandle& handle,
                            unsigned number, uint64_t offset,
                            size_t length) noexcept {
  std::string etag;
  ChecksumValue checksum;
  int error = 0;
  try {
    {
      std::lock_guard guard(handle.mutex);
      if (handle.unlinked.load(std::memory_order_acquire)) {
        throw std::system_error(ESTALE, std::generic_category(),
                                "cached write was unlinked");
      }
      if (handle.write_state == WRITE_FAILED) {
        throw std::system_error(handle.write_error,
                                std::generic_category(),
                                "cached write handle failed");
      }
    }
    etag = upload_cached_part(state, handle, number, offset, length,
                              checksum);
  } catch (const std::system_error& exception) {
    error = exception.code().value();
  } catch (...) {
    error = EIO;
  }
  std::lock_guard guard(handle.mutex);
  if (handle.unlinked.load(std::memory_order_acquire)) {
    error = ESTALE;
  }
  if (error == 0) {
    handle.part_etags[number - 1] = std::move(etag);
    if (checksum_multipart_type(state.config.checksum) == "COMPOSITE") {
      handle.part_checksums[number - 1] = std::move(checksum.base64);
    }
    if (state.config.checksum == CHECKSUM_CRC64NVME ||
        state.config.checksum == CHECKSUM_CRC64XZ) {
      handle.part_checksum_values[number - 1] = checksum.integer;
      handle.part_sizes[number - 1] = length;
    }
  } else {
    fail_write(handle, error);
  }
  --handle.pending_parts;
  notify_handle(handle);
}

void submit_cached_part(State& state, OpenHandle& handle,
                        uint64_t offset, size_t length,
                        const OpenRequestGuard* parent = nullptr) {
  const unsigned number = handle.next_part_number;
  if (number > kMaximumMultipartParts) {
    throw std::system_error(EFBIG, std::generic_category(),
                            "S3 multipart part limit exceeded");
  }
  handle.part_etags.resize(number);
  if (checksum_multipart_type(state.config.checksum) == "COMPOSITE") {
    handle.part_checksums.resize(number);
  }
  if (state.config.checksum == CHECKSUM_CRC64NVME ||
      state.config.checksum == CHECKSUM_CRC64XZ) {
    handle.part_checksum_values.resize(number);
    handle.part_sizes.resize(number);
  }
  handle.next_part_number   = number + 1;
  handle.multipart_required = true;
  ++handle.pending_parts;
  try {
    if (FuseReactor* reactor = current_fuse_reactor();
        reactor != nullptr && !state.config.tls) {
      submit_async_part(state, handle, *reactor, {}, number, offset, length, parent);
    } else {
      state.uploads->submit_upload(
          &handle, [&state, &handle, number, offset, length] {
            upload_cached_part_job(state, handle, number, offset, length);
          });
    }
  } catch (...) {
    --handle.pending_parts;
    handle.next_part_number = number;
    handle.part_etags.resize(number - 1);
    handle.part_checksums.resize(std::min<size_t>(
        handle.part_checksums.size(), number - 1));
    handle.part_checksum_values.resize(std::min<size_t>(
        handle.part_checksum_values.size(), number - 1));
    handle.part_sizes.resize(std::min<size_t>(
        handle.part_sizes.size(), number - 1));
    throw;
  }
}

void submit_ready_cached_parts(State& state, OpenHandle& handle,
                               bool include_final,
                               const OpenRequestGuard* parent = nullptr) {
  const uint64_t part_size = state.config.part_size;
  for (;;) {
    const uint64_t offset =
        uint64_t(handle.next_part_number - 1) * part_size;
    if (offset >= handle.stream_offset) {
      return;
    }
    const uint64_t available = handle.stream_offset - offset;
    if (!include_final && available < part_size) {
      return;
    }
    const size_t length = size_t(std::min<uint64_t>(available, part_size));
    submit_cached_part(state, handle, offset, length, parent);
  }
}

void CachedFileWriter::write(State& state, OpenHandle& handle,
                             fuse_req_t request, fuse_ino_t inode,
                             fuse_bufvec* input, off_t offset) {
  if (offset < 0 || input == nullptr) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  try {
    if (handle.inode != inode || !handle.writable || !handle.cache_entry) {
      throw std::system_error(EBADF, std::generic_category(),
                              "cached write handle");
    }
    if (handle.unlinked.load(std::memory_order_acquire)) {
      throw std::system_error(ESTALE, std::generic_category(),
                              "write unlinked file");
    }
    const size_t length = fuse_buf_size(input);
    if (length == 0) {
      fuse_reply_write(request, 0);
      return;
    }
    const uint64_t start = uint64_t(offset);
    if (start > UINT64_MAX - length) {
      throw std::system_error(EFBIG, std::generic_category(),
                              "write offset overflow");
    }
    const uint64_t end = start + length;
    const uint64_t maximum_size = std::min(
        state.config.part_size * kMaximumMultipartParts,
        kMaximumObjectSize);
    if (end > maximum_size) {
      throw std::system_error(EFBIG, std::generic_category(),
                              "configured S3 multipart limit exceeded");
    }

    std::unique_lock guard(handle.mutex);
    while (handle.write_in_progress) {
      wait_handle(handle, guard);
    }
    if (handle.unlinked.load(std::memory_order_acquire)) {
      throw std::system_error(ESTALE, std::generic_category(),
                              "write unlinked file");
    }
    if (handle.write_state == WRITE_FAILED) {
      throw std::system_error(handle.write_error,
                              std::generic_category(),
                              "cached write handle failed");
    }
    if (handle.write_state != WRITE_OPEN) {
      if (!handle.write_after_flush_warned) {
        fprintf(stderr,
                "error: write after first flush is unsupported: path=%s\n",
                handle.object_path.c_str());
        handle.write_after_flush_warned = true;
      }
      throw std::system_error(EIO, std::generic_category(),
                              "write after flush");
    }
    if (start != handle.stream_offset) {
      throw std::system_error(ESPIPE, std::generic_category(),
                              "non-sequential write offset");
    }
    handle.write_in_progress = true;
    try {
      handle.cache_entry->prepare_write(start, length);
      write_fuse_buffers_to_cache(
          state, *input, handle.cache_write_pipe,
          handle.cache_entry->data_fd(), start, length);
      remember_partial_write_pages(
          handle, start, end, handle.cache_entry->page_size());
      handle.cache_entry->publish_dirty(start, length, end);
      handle.stream_offset = end;
      handle.size          = end;
      handle.item->set_fsize(end);
      submit_ready_cached_parts(state, handle, false);
    } catch (const std::system_error& error) {
      handle.write_in_progress = false;
      handle.cache_entry->isolate_write();
      fail_write(handle, error.code().value());
      throw;
    } catch (...) {
      handle.write_in_progress = false;
      handle.cache_entry->isolate_write();
      fail_write(handle, EIO);
      throw;
    }
    handle.write_in_progress = false;
    notify_handle(handle);
    if (state.config.stats_interval_seconds != 0) {
      state.fuse_writes.fetch_add(1, std::memory_order_relaxed);
      state.fuse_write_bytes.fetch_add(length, std::memory_order_relaxed);
    }
    fuse_reply_write(request, length);
  } catch (...) {
    reply_callback_error(request);
  }
}

Response put_cached_object(State& state, OpenHandle& handle) {
  const size_t length = size_t(handle.stream_offset);
  const ChecksumValue checksum = checksum_file_range(
      state.config.checksum, handle.cache_entry->data_fd(), 0, length);
  bool ambiguous = false;
  try {
    Response response = request_with_retries([&] {
      std::vector<Header> headers{
          Header{"content-type", "application/octet-stream"},
      };
      headers.push_back(Header{
          "x-amz-meta-ngs3fs-write-id", handle.write_id});
      if (!handle.etag.empty()) {
        headers.push_back(Header{"if-match", handle.etag});
      } else if (handle.create_exclusive) {
        headers.push_back(Header{"if-none-match", "*"});
      }
      append_upload_checksum(headers, state.config.checksum, checksum);
      append_authorization(headers, state, "PUT", handle.object_path,
                           length == 0 ? kEmptyPayloadSha256
                                       : kUnsignedPayload);
      HttpPool::Lease client = state.http->acquire_bulk();
      if (length == 0) {
        client->begin_upload("PUT", handle.object_path, headers, 0);
        return client->finish_upload();
      }
      return client->put_from_fd(handle.object_path, headers,
                                 handle.cache_entry->data_fd(), 0, length);
    }, "PutObject(cache)", &ambiguous);
    if (ambiguous && response.status == 412) {
      Response recovered;
      if (recover_write_commit(state, handle, recovered)) {
        return recovered;
      }
      throw std::system_error(EIO, std::generic_category(),
                              "cached PutObject outcome unknown");
    }
    if (response.status == 412 && handle.create_exclusive) {
      throw std::system_error(EEXIST, std::generic_category(),
                              "PutObject destination exists");
    }
    require_s3_success(response, "PutObject(cache)");
    verify_upload_checksum(response, state.config.checksum,
                           checksum, "PutObject(cache)");
    return response;
  } catch (...) {
    if (ambiguous) {
      Response recovered;
      if (recover_write_commit(state, handle, recovered)) {
        return recovered;
      }
      fprintf(stderr, "error: cached PutObject outcome unknown: %s\n",
              handle.object_path.c_str());
      throw std::system_error(EIO, std::generic_category(),
                              "cached PutObject outcome unknown");
    }
    throw;
  }
}

void commit_cached_write(OpenHandle& handle) {
  InodeFile& item = *handle.item;
  handle.cache_entry->commit_write(CacheIdentity{
      .key = handle.key,
      .etag = handle.etag,
      .version_id = handle.version_id,
      .size = handle.stream_offset,
      .mtime = item.mtime.load(std::memory_order_relaxed),
  });
}

void CachedFileWriter::flush(State& state, OpenHandle& handle) {
  std::unique_lock guard(handle.mutex);
  while (handle.write_in_progress) {
    wait_handle(handle, guard);
  }
  while (handle.write_state == WRITE_SEALING) {
    wait_handle(handle, guard);
  }
  if (handle.unlinked.load(std::memory_order_acquire)) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "flush unlinked file");
  }
  if (handle.write_state == WRITE_SEALED) {
    return;
  }
  if (handle.write_state == WRITE_FAILED) {
    throw std::system_error(handle.write_error, std::generic_category(),
                            "cached write handle failed");
  }
  handle.write_state = WRITE_SEALING;
  try {
    handle.cache_entry->begin_write();
    if (handle.stream_offset <= state.config.part_size &&
        !handle.multipart_required) {
      guard.unlock();
      const Response response = put_cached_object(state, handle);
      handle.page_cache_store_failed |=
          !store_cached_partial_pages(state, handle);
      update_written_metadata(state, handle, response);
      commit_cached_write(handle);
      guard.lock();
    } else {
      submit_ready_cached_parts(state, handle, true);
      while (handle.pending_parts != 0 &&
             handle.write_state != WRITE_FAILED) {
        wait_handle(handle, guard);
      }
      if (handle.write_state == WRITE_FAILED) {
        throw std::system_error(handle.write_error,
                                std::generic_category(),
                                "cached multipart upload failed");
      }
      for (size_t i = 0; i < handle.part_etags.size(); ++i) {
        if (handle.part_etags[i].empty()) {
          throw std::system_error(EIO, std::generic_category(),
                                  "cached multipart part has no ETag");
        }
      }
      guard.unlock();
      complete_multipart(state, handle);
      // The writer remains registered until this flush finishes, so no new
      // opener can observe metadata before partial pages are restored.
      handle.page_cache_store_failed |=
          !store_cached_partial_pages(state, handle);
      if (handle.page_cache_store_failed) {
        handle.item->set_page_cache_valid(false);
      }
      commit_cached_write(handle);
      guard.lock();
    }
    handle.write_state = WRITE_SEALED;
    notify_handle(handle);
  } catch (const std::system_error& error) {
    if (!guard.owns_lock()) {
      guard.lock();
    }
    fail_write(handle, error.code().value());
    throw;
  } catch (...) {
    if (!guard.owns_lock()) {
      guard.lock();
    }
    fail_write(handle, EIO);
    throw;
  }

  const bool unregister = handle.registered;
  handle.registered = false;
  guard.unlock();
  if (unregister) {
    unregister_open_handle(state, handle.object_path, handle, false);
  }
}

void CachedFileWriter::fsync(State&, OpenHandle& handle, bool) {
  std::unique_lock guard(handle.mutex);
  while (handle.write_in_progress) {
    wait_handle(handle, guard);
  }
  if (handle.unlinked.load(std::memory_order_acquire)) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "fsync unlinked file");
  }
  CacheEntry& entry = *handle.cache_entry;
  guard.unlock();
  entry.sync_write();
  guard.lock();
  if (handle.write_state == WRITE_FAILED) {
    throw std::system_error(handle.write_error,
                            std::generic_category(),
                            "cached write handle failed");
  }
}

void UncachedFileWriter::flush(State& state, OpenHandle& handle) {
  if (handle.unlinked.load(std::memory_order_acquire)) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "flush unlinked file");
  }
  seal_write(state, handle);
  bool unregister = false;
  {
    std::lock_guard handle_guard(handle.mutex);
    unregister = handle.registered;
    handle.registered = false;
  }
  if (unregister) {
    unregister_open_handle(state, handle.object_path, handle, false);
  }
}

void UncachedFileWriter::fsync(State&, OpenHandle& handle, bool) {
  std::unique_lock handle_guard(handle.mutex);
  while (handle.write_in_progress) {
    wait_handle(handle, handle_guard);
  }
  if (handle.unlinked.load(std::memory_order_acquire)) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "fsync unlinked file");
  }
  if (handle.write_state == WRITE_FAILED) {
    throw std::system_error(handle.write_error,
                            std::generic_category(),
                            "write handle failed");
  }
}

struct CachedRemotePart {
  std::string etag;
  uint64_t size = 0;
  unsigned number = 0;
};

std::vector<CachedRemotePart> list_cached_parts(
    State& state, const OpenHandle& handle) {
  std::vector<CachedRemotePart> parts;
  unsigned marker = 0;
  for (;;) {
    std::string query = "uploadId=" + uri_encode(handle.upload_id, false);
    query += "&max-parts=1000";
    if (marker != 0) {
      query += "&part-number-marker=" + std::to_string(marker);
    }
    const std::string path = query_path(handle.object_path, query);
    const Response response = request_with_retries([&] {
      const HeaderList headers = authorization_headers(
          state, "GET", path, {}, kEmptyPayloadSha256);
      HttpPool::Lease client = state.http->acquire();
      return client->request_no_body(
          "GET", path, headers, kMaximumListResponseSize);
    }, "ListParts(cache recovery)");
    require_s3_success(response, "ListParts(cache recovery)");
    S3Xml xml(response_xml(response), "ListParts(cache recovery)");
    const tinyxml2::XMLElement& root =
        xml.result_root("ListPartsResult");
    unsigned last = marker;
    for (const tinyxml2::XMLElement* child = root.FirstChildElement();
         child != nullptr; child = child->NextSiblingElement()) {
      if (!S3Xml::named(*child, "Part")) {
        continue;
      }
      uint64_t number = 0;
      uint64_t length = 0;
      const std::string number_text = xml.required_text(
          *child, "PartNumber");
      const std::string length_text = xml.required_text(*child, "Size");
      if (!parse_unsigned(trim_xml_space(number_text), number) ||
          number == 0 || number > kMaximumMultipartParts ||
          !parse_unsigned(trim_xml_space(length_text), length)) {
        throw std::runtime_error(
            "ListParts(cache recovery) returned an invalid part");
      }
      if (number <= last) {
        throw std::runtime_error(
            "ListParts(cache recovery) returned unordered parts");
      }
      parts.push_back(CachedRemotePart{
          .etag = xml.required_text(*child, "ETag"),
          .size = length,
          .number = unsigned(number),
      });
      last = unsigned(number);
    }
    const bool truncated = xml.required_bool(root, "IsTruncated");
    if (!truncated) {
      return parts;
    }
    uint64_t next = 0;
    const std::string next_text = xml.required_text(
        root, "NextPartNumberMarker");
    if (!parse_unsigned(trim_xml_space(next_text), next) ||
        next <= marker || next > kMaximumMultipartParts) {
      throw std::runtime_error(
          "ListParts(cache recovery) returned an invalid marker");
    }
    marker = unsigned(next);
  }
}

void recover_cached_write(State& state,
                          const std::shared_ptr<CacheEntry>& entry,
                          const std::stop_token& stop) {
  RequestStopScope request_stop(stop);
  OpenHandle handle;
  InodeFile item;
  handle.writable      = true;
  handle.item          = &item;
  handle.key           = entry->key();
  handle.object_path   = object_request_path(state, handle.key);
  handle.etag          = entry->etag();
  handle.version_id    = entry->version_id();
  handle.upload_id     = entry->upload_id();
  handle.write_id      = entry->write_id();
  handle.cache_entry   = entry;
  handle.stream_offset = entry->written_end();
  handle.size          = handle.stream_offset;
  item.set_fsize(handle.size);
  fprintf(stderr, "warning: recovering cached write: path=%s bytes=%" PRIu64
                  "\n",
          handle.object_path.c_str(), handle.stream_offset);

  if (handle.stream_offset <= state.config.part_size &&
      handle.upload_id.empty()) {
    (void)put_cached_object(state, handle);
  } else {
    if (handle.upload_id.empty()) {
      ensure_multipart(state, handle);
    }
    const std::vector<CachedRemotePart> remote =
        list_cached_parts(state, handle);
    const size_t count = size_t(
        (handle.stream_offset - 1) / state.config.part_size + 1);
    handle.part_etags.resize(count);
    for (const CachedRemotePart& part : remote) {
      if (part.number > count) {
        throw std::runtime_error(
            "ListParts(cache recovery) returned an excess part");
      }
      const uint64_t offset =
          uint64_t(part.number - 1) * state.config.part_size;
      const size_t expected = size_t(std::min<uint64_t>(
          state.config.part_size, handle.stream_offset - offset));
      if (part.size != expected || part.etag.empty()) {
        throw std::runtime_error(
            "ListParts(cache recovery) disagrees with local data");
      }
      handle.part_etags[part.number - 1] = part.etag;
    }
    if (checksum_multipart_type(state.config.checksum) == "COMPOSITE") {
      handle.part_checksums.resize(count);
    }
    if (state.config.checksum == CHECKSUM_CRC64NVME ||
        state.config.checksum == CHECKSUM_CRC64XZ) {
      handle.part_checksum_values.resize(count);
      handle.part_sizes.resize(count);
    }
    for (size_t i = 0; i < count; ++i) {
      check_request_stop();
      const unsigned number = unsigned(i + 1);
      const uint64_t offset = uint64_t(i) * state.config.part_size;
      const size_t length = size_t(std::min<uint64_t>(
          state.config.part_size, handle.stream_offset - offset));
      ChecksumValue checksum;
      if (handle.part_etags[i].empty()) {
        handle.part_etags[i] = upload_cached_part(
            state, handle, number, offset, length, checksum);
      } else {
        checksum = checksum_file_range(
            state.config.checksum, entry->data_fd(), offset, length);
      }
      if (checksum_multipart_type(state.config.checksum) == "COMPOSITE") {
        handle.part_checksums[i] = std::move(checksum.base64);
      }
      if (state.config.checksum == CHECKSUM_CRC64NVME ||
          state.config.checksum == CHECKSUM_CRC64XZ) {
        handle.part_checksum_values[i] = checksum.integer;
        handle.part_sizes[i] = length;
      }
    }
    check_request_stop();
    complete_multipart(state, handle);
  }

  check_request_stop();
  const ObjectMetadata metadata = head_object(state, handle.object_path);
  if (metadata.size != handle.stream_offset) {
    throw std::runtime_error(
        "recovered S3 object has the wrong size");
  }
  entry->commit_write(CacheIdentity{
      .key = handle.key,
      .etag = metadata.etag,
      .version_id = metadata.version_id,
      .size = metadata.size,
      .mtime = metadata.mtime,
  });
}

class AsyncPendingDelete {
 public:
  using Complete = void (*)(void*, std::exception_ptr) noexcept;
  AsyncPendingDelete(State& state, FuseReactor& reactor, std::string path,
                       Complete complete, void* context)
      : state_(state), reactor_(reactor), path_(std::move(path)),
        complete_(complete), context_(context) {}

  bool start() noexcept {
    completion_ = {begin, nullptr, this};
    if (!reactor_.reserve_completion(&completion_)) return false;
    reactor_.complete(&completion_);
    return true;
  }

  void cancel() noexcept {
    cancelled_ = true;
    if (http_) http_->cancel();
    else if (rename_) rename_->cancel();
    else if (session_) session_->cancel();
    else if (wait_.pending()) reactor_.cancel(wait_);
  }

 private:
  enum Phase { HEAD_HIDDEN, HEAD_VISIBLE, DELETE_HIDDEN } phase_ = HEAD_HIDDEN;
  State& state_;
  FuseReactor& reactor_;
  std::string path_;
  std::string destination_;
  State::PendingDelete operation_;
  OpenHandleIdentityLocks readers_;
  size_t locked_ = 0;
  std::string hidden_etag_;
  Complete complete_;
  void* context_;
  std::unique_ptr<AsyncS3Request> http_;
  std::unique_ptr<AsyncNativeRename> rename_;
  std::unique_ptr<AsyncExpressSession> session_;
  FuseReactor::ReactorTask completion_;
  AsyncIoRequest wait_;
  uint64_t notification_ = 0;
  std::exception_ptr local_error_;
  bool owned_ = false;
  bool cancelled_ = false;

  void finish(std::exception_ptr error = {}) noexcept {
    if (owned_) {
      std::lock_guard guard(state_.open_files_mutex);
      const auto pending = state_.pending_deletes.find(path_);
      if (pending != state_.pending_deletes.end() &&
          pending->second.key == operation_.key) {
        if (error) pending->second.deleting = false;
        else state_.pending_deletes.erase(pending);
      }
    }
    if (error) {
      fprintf(stderr, "error: pending S3 delete retained for recovery: path=%s\n",
              path_.c_str());
    }
    readers_.identities.clear();
    readers_.pins.clear();
    const auto callback = complete_;
    void* const value = context_;
    callback(value, error);
  }

  void check_cancelled() const {
    if (cancelled_) {
      throw std::system_error(ECANCELED, std::generic_category(), "pending S3 delete cancelled");
    }
  }

  static void begin(void* value) noexcept {
    auto* self = static_cast<AsyncPendingDelete*>(value);
    try {
      self->check_cancelled();
      {
        std::lock_guard guard(self->state_.open_files_mutex);
        const auto pending = self->state_.pending_deletes.find(self->path_);
        if (pending != self->state_.pending_deletes.end() && !pending->second.deleting &&
            (pending->second.rollback || !self->state_.open_files.contains(self->path_))) {
          self->operation_ = pending->second;
          pending->second.deleting = true;
          self->owned_ = true;
          if (self->operation_.rollback) {
            const auto opened = self->state_.open_files.find(self->path_);
            if (opened != self->state_.open_files.end()) {
              if (opened->second.writer) {
                throw std::logic_error("pending restore has an open writer");
              }
              for (OpenHandle* handle : opened->second.handles) {
                if (handle == nullptr || handle->writable) {
                  throw std::logic_error("invalid pending restore reader");
                }
                try {
                  self->readers_.pins.push_back(std::make_unique<OpenRequestGuard>(*handle));
                } catch (const std::system_error& error) {
                  if (error.code().value() == EBADF) continue;
                  throw;
                }
                self->readers_.handles.push_back(handle);
              }
            }
          }
        }
      }
      if (!self->owned_) {
        self->finish();
        return;
      }
      if (!self->operation_.rollback) {
        self->phase_ = DELETE_HIDDEN;
        self->send();
        return;
      }
      self->destination_ = object_request_path(self->state_, self->operation_.restore_key);
      self->readers_.identities.reserve(self->readers_.handles.size());
      for (OpenHandle* handle : self->readers_.handles) {
        self->readers_.identities.emplace_back(handle->identity_mutex, std::defer_lock);
      }
      self->lock_readers();
    } catch (...) {
      self->finish(std::current_exception());
    }
  }

  static void unlocked(void* value, ssize_t result) noexcept {
    auto* self = static_cast<AsyncPendingDelete*>(value);
    self->readers_.handles[self->locked_]->identity_mutex.end_async_wait();
    if (result < 0) {
      try {
        throw std::system_error(-int(result), std::generic_category(), "pending restore identity wait");
      } catch (...) { self->finish(std::current_exception()); }
    } else self->lock_readers();
  }

  void lock_readers() noexcept {
    try {
      check_cancelled();
      for (; locked_ < readers_.handles.size(); ++locked_) {
        auto& lock = readers_.identities[locked_];
        auto& mutex = readers_.handles[locked_]->identity_mutex;
        if (lock.try_lock()) continue;
        const int fd = mutex.begin_async_wait();
        if (lock.try_lock()) {
          mutex.end_async_wait();
          continue;
        }
        wait_.kind       = AsyncIoRequest::READ;
        wait_.fd         = fd;
        wait_.data       = &notification_;
        wait_.length     = sizeof(notification_);
        wait_.timeout_ms = 0;
        wait_.complete   = unlocked;
        wait_.context    = this;
        if (reactor_.submit(wait_)) return;
        mutex.end_async_wait();
        throw std::system_error(errno, std::generic_category(), "submit pending restore wait");
      }
      send();
    } catch (...) { finish(std::current_exception()); }
  }

  static void authenticated(void* value, std::exception_ptr error) noexcept {
    auto* self = static_cast<AsyncPendingDelete*>(value);
    self->session_.reset();
    if (error) self->finish(error);
    else self->send();
  }

  void send() noexcept {
    try {
      check_cancelled();
      if (!AsyncExpressSession::ready(state_)) {
        session_ = std::make_unique<AsyncExpressSession>(
            state_, reactor_, authenticated, this);
        if (session_->start()) return;
        throw std::system_error(errno, std::generic_category(), "submit restore authentication");
      }
      AsyncHttpRequest args;
      const std::string_view method = phase_ == DELETE_HIDDEN ? "DELETE" : "HEAD";
      const std::string_view path = phase_ == HEAD_VISIBLE ? destination_ : path_;
      args.method.assign(method.data(), method.size());
      args.path.assign(path.data(), path.size());
      if (phase_ == DELETE_HIDDEN && !hidden_etag_.empty()) {
        args.headers.push_back({"if-match", hidden_etag_});
      }
      append_authorization(args.headers, state_, method, path, kEmptyPayloadSha256);
      args.capture_headers = true;
      http_ = std::make_unique<AsyncS3Request>(
          state_, reactor_, std::move(args), received, this);
      if (!http_->start()) {
        throw std::system_error(errno, std::generic_category(), "submit pending delete request");
      }
    } catch (...) { finish(std::current_exception()); }
  }

  static void received(void* value, Response&& response,
                         std::exception_ptr error) noexcept {
    auto* self = static_cast<AsyncPendingDelete*>(value);
    try {
      if (error) std::rethrow_exception(error);
      self->check_cancelled();
      self->http_.reset();
      if (self->phase_ == DELETE_HIDDEN) {
        if (response.status != 200 && response.status != 204 && response.status != 404) {
          throw_s3_response(response, "pending DeleteObject");
        }
        self->cleanup();
      } else if (self->phase_ == HEAD_HIDDEN) {
        if (response.status == 404) { self->cleanup(); return; }
        self->hidden_etag_ = decode_head_response(response).etag;
        self->phase_ = HEAD_VISIBLE;
        self->send();
      } else if (response.status == 404) {
        for (OpenHandle* handle : self->readers_.handles) {
          handle->object_path.reserve(self->destination_.size());
          handle->key.reserve(self->operation_.restore_key.size());
        }
        self->rename_ = std::make_unique<AsyncNativeRename>(
            self->state_, self->reactor_, self->operation_.key, self->hidden_etag_,
            self->destination_, true, restored, self);
        if (!self->rename_->start()) {
          throw std::system_error(errno, std::generic_category(), "submit pending rename restore");
        }
      } else {
        const auto metadata = decode_head_response(response);
        if (self->operation_.replacement_etag.empty() ||
            metadata.etag != self->operation_.replacement_etag) {
          throw std::system_error(EBUSY, std::generic_category(), "ambiguous pending rename target");
        }
        self->phase_ = DELETE_HIDDEN;
        self->send();
      }
    } catch (...) { self->finish(std::current_exception()); }
  }

  static void restored(void* value, bool accepted, std::exception_ptr error) noexcept {
    auto* self = static_cast<AsyncPendingDelete*>(value);
    try {
      if (error) std::rethrow_exception(error);
      if (!accepted) {
        throw std::system_error(EOPNOTSUPP, std::generic_category(), "pending native rename restore");
      }
      {
        std::lock_guard guard(self->state_.open_files_mutex);
        const auto opened = self->state_.open_files.find(self->path_);
        if (!reader_snapshot_matches(
                opened == self->state_.open_files.end() ? std::span<OpenHandle* const>{}
                                                       : opened->second.handles,
                self->readers_.handles)) {
          throw std::logic_error("pending restore reader registry changed");
        }
        if (opened != self->state_.open_files.end()) {
          for (OpenHandle* handle : opened->second.handles) {
            handle->object_path.assign(self->destination_);
            handle->key.assign(self->operation_.restore_key);
          }
          auto node = self->state_.open_files.extract(opened);
          node.key().assign(self->destination_);
          self->state_.open_files.insert(std::move(node));
        }
      }
      self->cleanup();
    } catch (...) { self->finish(std::current_exception()); }
  }

  static void cleaned(void* value) noexcept {
    auto* self = static_cast<AsyncPendingDelete*>(value);
    self->finish(self->local_error_);
  }

  void cleanup() noexcept {
    completion_ = {cleaned, nullptr, this};
    if (!reactor_.reserve_completion(&completion_)) {
      try { throw std::system_error(errno, std::generic_category(), "reserve pending delete cleanup"); }
      catch (...) { finish(std::current_exception()); }
      return;
    }
    try {
      state_.uploads->submit(this, [this] {
        try {
          IoExecutorScope local_only(nullptr, 0);
          state_.local_cache->finish_pending_delete(operation_.key);
        } catch (...) { local_error_ = std::current_exception(); }
        reactor_.complete(&completion_);
      });
    } catch (...) {
      local_error_ = std::current_exception();
      reactor_.complete(&completion_);
    }
  }
};

void finish_pending_delete(State& state, std::string_view path) noexcept {
  State::PendingDelete operation;
  {
    std::lock_guard guard(state.open_files_mutex);
    const auto pending = state.pending_deletes.find(path);
    if (pending == state.pending_deletes.end() ||
        pending->second.deleting) {
      return;
    }
    if (!pending->second.rollback && state.open_files.contains(path)) {
      return;
    }
    pending->second.deleting = true;
    operation = pending->second;
  }

  try {
    bool restored = false;
    OpenHandleIdentityLocks reader_locks;
    if (operation.rollback) {
      reader_locks = lock_open_handle_identities(
          state, path, "pending rename restore");
      std::optional<ObjectMetadata> hidden;
      try {
        hidden = head_object(state, path);
      } catch (const std::system_error& error) {
        if (error.code().value() != ENOENT) {
          throw;
        }
      }
      if (hidden) {
        const std::string destination = object_request_path(
            state, operation.restore_key);
        std::optional<ObjectMetadata> visible;
        try {
          visible = head_object(state, destination);
        } catch (const std::system_error& error) {
          if (error.code().value() != ENOENT) {
            throw;
          }
        }
        if (!visible) {
          for (OpenHandle* handle : reader_locks.handles) {
            handle->object_path.reserve(destination.size());
            handle->key.reserve(operation.restore_key.size());
          }
          if (!try_native_rename_object(
                  state, operation.key, hidden->etag, destination, true)) {
            throw std::system_error(
                EOPNOTSUPP, std::generic_category(),
                "restore interrupted RenameObject destination");
          }
          restored = true;
        } else if (!operation.replacement_etag.empty() &&
                   visible->etag == operation.replacement_etag) {
          delete_object(state, operation.key, hidden->etag);
        } else {
          throw std::system_error(
              EBUSY, std::generic_category(),
              "cannot identify interrupted RenameObject destination");
        }
      }
    } else {
      delete_object(state, operation.key, {});
    }
    std::lock_guard guard(state.open_files_mutex);
    if (restored) {
      auto opened = state.open_files.find(path);
      if (opened == state.open_files.end()) {
        if (!reader_locks.handles.empty()) {
          throw std::logic_error(
              "pending rename reader registry disappeared");
        }
      } else {
        if (opened->second.handles != reader_locks.handles) {
          throw std::logic_error(
              "pending rename reader registry changed");
        }
        const std::string destination = object_request_path(
            state, operation.restore_key);
        for (OpenHandle* handle : opened->second.handles) {
          if (handle == nullptr) {
            continue;
          }
          handle->object_path.assign(destination);
          handle->key.assign(operation.restore_key);
        }
        auto node = state.open_files.extract(opened);
        node.key().assign(destination);
        state.open_files.insert(std::move(node));
      }
    }
    state.local_cache->finish_pending_delete(operation.key);
    const auto pending = state.pending_deletes.find(path);
    if (pending != state.pending_deletes.end() &&
        pending->second.key == operation.key) {
      state.pending_deletes.erase(pending);
    }
    fprintf(stderr,
            "warning: %s interrupted S3 rename state: path=%.*s\n",
            restored ? "restored" : "removed",
            int(path.size()), path.data());
  } catch (const std::exception& error) {
    std::lock_guard guard(state.open_files_mutex);
    const auto pending = state.pending_deletes.find(path);
    if (pending != state.pending_deletes.end() &&
        pending->second.key == operation.key) {
      pending->second.deleting = false;
    }
    fprintf(stderr,
            "error: pending S3 delete retained for recovery: path=%.*s: "
            "%s\n",
            int(path.size()), path.data(), error.what());
  } catch (...) {
    std::lock_guard guard(state.open_files_mutex);
    const auto pending = state.pending_deletes.find(path);
    if (pending != state.pending_deletes.end() &&
        pending->second.key == operation.key) {
      pending->second.deleting = false;
    }
    fprintf(stderr,
            "error: pending S3 delete retained for recovery: path=%.*s\n",
            int(path.size()), path.data());
  }
}

void cache_recovery_loop(std::stop_token stop, State* state) noexcept {
  std::vector<std::string> pending_paths;
  {
    std::lock_guard guard(state->open_files_mutex);
    pending_paths.reserve(state->pending_deletes.size());
    for (const auto& [path, pending] : state->pending_deletes) {
      (void)pending;
      pending_paths.push_back(path);
    }
  }
  for (const std::string& path : pending_paths) {
    if (stop.stop_requested()) {
      return;
    }
    finish_pending_delete(*state, path);
  }
  std::vector<std::shared_ptr<CacheEntry>> recovery_entries;
  {
    std::lock_guard guard(state->open_files_mutex);
    recovery_entries = state->recovery_entries;
  }
  for (const std::shared_ptr<CacheEntry>& entry : recovery_entries) {
    if (stop.stop_requested()) {
      return;
    }
    std::string path = state->config.bucket_path;
    path.push_back('/');
    path += uri_encode(entry->key(), true);
    try {
      recover_cached_write(*state, entry, stop);
      {
        std::lock_guard guard(state->open_files_mutex);
        state->recovery_paths.erase(path);
        std::erase(state->recovery_entries, entry);
      }
      state->open_files_condition.notify_all();
      fprintf(stderr, "warning: cached write recovery completed: path=%s\n",
              path.c_str());
    } catch (const std::exception& error) {
      if (stop.stop_requested()) {
        return;
      }
      fprintf(stderr,
              "error: cached write recovery isolated: path=%s: %s\n",
              path.c_str(), error.what());
    } catch (...) {
      if (stop.stop_requested()) {
        return;
      }
      fprintf(stderr,
              "error: cached write recovery isolated: path=%s\n",
              path.c_str());
    }
  }
}

struct AsyncWriteRequest {
  AsyncWriteRequest(State& s, OpenHandle& h, FuseReactor& r,
                     fuse_req_t req, fuse_ino_t ino,
                     const fuse_bufvec& source, off_t off)
      : state(s), handle(h), reactor(r), request(req), inode(ino),
        input(source), offset(off), active(h) {
    if (source.count > 1) {
      const size_t bytes = offsetof(fuse_bufvec, buf) + source.count * sizeof(fuse_buf);
      descriptor = std::make_unique<std::byte[]>(bytes);
      memcpy(descriptor.get(), &source, bytes);
    }
    length = fuse_buf_size(&source);
    remaining = length;
  }
  virtual ~AsyncWriteRequest() = default;
  virtual void process() noexcept = 0;

  State& state;
  OpenHandle& handle;
  FuseReactor& reactor;
  fuse_req_t request;
  fuse_ino_t inode;
  fuse_bufvec input;
  off_t offset;
  OpenRequestGuard active;
  std::unique_ptr<std::byte[]> descriptor;
  void* input_owner = nullptr;
  size_t length = 0;
  size_t remaining = 0;
  uint64_t end = 0;
  bool owns_write = false;
  bool handle_wait = false;
  AsyncIoRequest io;
  uint64_t notification = 0;

  fuse_bufvec& buffers() noexcept {
    return descriptor ? *reinterpret_cast<fuse_bufvec*>(descriptor.get()) : input;
  }

  void finish(int error) noexcept {
    std::unique_ptr<AsyncWriteRequest> self(this);
    if (owns_write) {
      std::lock_guard guard(handle.mutex);
      if (error == 0 && handle.write_state == WRITE_FAILED) error = handle.write_error;
      if (error == 0) {
        handle.stream_offset = end;
        handle.size = end;
        handle.item->set_fsize(end);
      } else {
        fail_write(handle, error);
      }
      handle.write_in_progress = false;
      notify_handle(handle);
    }
    if (error != 0) {
      fuse_reply_err(request, error);
    } else {
      if (state.config.stats_interval_seconds != 0) {
        state.fuse_writes.fetch_add(1, std::memory_order_relaxed);
        state.fuse_write_bytes.fetch_add(length, std::memory_order_relaxed);
      }
      fuse_reply_write(request, length);
    }
    if (input_owner != nullptr) {
      reactor.release_input(input_owner, remaining == 0);
      input_owner = nullptr;
    }
  }

  static void available(void* context, ssize_t result) noexcept {
    auto* self = static_cast<AsyncWriteRequest*>(context);
    self->handle.reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
    self->handle_wait = false;
    if (result < 0) self->finish(-int(result));
    else self->start();
  }

  void start() noexcept {
    try {
      if (offset < 0 || handle.inode != inode || !handle.writable) {
        throw std::system_error(offset < 0 ? EINVAL : EBADF,
                                std::generic_category(), "asynchronous write");
      }
      if (length == 0) { finish(0); return; }
      if (uint64_t(offset) > UINT64_MAX - length) {
        throw std::system_error(EFBIG, std::generic_category(), "write offset overflow");
      }
      end = uint64_t(offset) + length;
      if (end > std::min(state.config.part_size * kMaximumMultipartParts,
                         kMaximumObjectSize)) {
        throw std::system_error(EFBIG, std::generic_category(), "multipart size limit");
      }
      {
        std::lock_guard guard(handle.mutex);
        if (handle.unlinked.load()) {
          throw std::system_error(ESTALE, std::generic_category(), "write unlinked file");
        }
        if (handle.write_state != WRITE_OPEN) {
          throw std::system_error(handle.write_state == WRITE_FAILED
              ? handle.write_error : EIO, std::generic_category(), "write after seal");
        }
        if (handle.write_in_progress) {
          handle.reactor_waiters.fetch_add(1, std::memory_order_acq_rel);
          handle_wait = true;
        } else {
          if (uint64_t(offset) != handle.stream_offset) {
            throw std::system_error(ESPIPE, std::generic_category(), "non-sequential write");
          }
          handle.write_in_progress = true;
          owns_write = true;
        }
      }
      if (handle_wait) {
        io = {};
        io.kind       = AsyncIoRequest::READ;
        io.fd         = handle.state_event.get();
        io.data       = &notification;
        io.length     = sizeof(notification);
        io.timeout_ms = 0; // Serialize local writes, not a network request.
        io.complete   = available;
        io.context    = this;
        if (reactor.submit(io)) return;
        handle.reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
        handle_wait = false;
        throw std::system_error(errno, std::generic_category(), "submit write admission");
      }
      process();
    } catch (...) { finish(AsyncPartUpload::error_code(std::current_exception())); }
  }

  void consume(size_t size) {
    fuse_bufvec& input = buffers();
    input.off += size;
    remaining -= size;
    if (input.off == input.buf[input.idx].size) {
      ++input.idx;
      input.off = 0;
    }
  }
};

struct AsyncUncachedWrite final : AsyncWriteRequest {
  using AsyncWriteRequest::AsyncWriteRequest;

  enum Stage { BUDGET, SPLICE, COPY_READ, PIPE_WRITE } stage = SPLICE;
  std::unique_ptr<std::byte[]> copy;
  bool budget_wait = false;
  bool copying_fd = false;
  bool memory_write = false;
  size_t copied_bytes = 0;

  void submit_io(AsyncIoRequest::Kind kind, int fd, void* data, size_t size,
                 int output = -1, off_t position = -1, unsigned flags = 0) {
    io = {};
    io.kind         = kind;
    io.fd           = fd;
    io.data         = data;
    io.length       = size;
    io.output_fd    = output;
    io.input_offset = position;
    // Budget admission and local FUSE-pipe transfers have no S3 deadline.
    // Shutdown still cancels their CQEs before retiring the input/handle.
    io.timeout_ms   = 0;
    io.flags        = flags;
    io.complete     = received;
    io.context      = this;
    if (!reactor.submit(io)) {
      throw std::system_error(errno, std::generic_category(), "submit retained write");
    }
  }

  bool reserve() {
    if (handle.current_reservation) return true;
    if (reserve_part_budget(state, false)) {
      handle.current_reservation = true;
      return true;
    }
    {
      std::lock_guard guard(state.budget_mutex);
      if (!state.budget_event) {
        state.budget_event.reset(::eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE));
        if (!state.budget_event) {
          throw std::system_error(errno, std::generic_category(), "write budget event");
        }
      }
      state.budget_reactor_waiters.fetch_add(1, std::memory_order_acq_rel);
      budget_wait = true;
    }
    if (reserve_part_budget(state, false)) {
      state.budget_reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
      budget_wait = false;
      handle.current_reservation = true;
      return true;
    }
    stage = BUDGET;
    try {
      submit_io(AsyncIoRequest::READ, state.budget_event.get(), &notification,
                 sizeof(notification));
    } catch (...) {
      state.budget_reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
      budget_wait = false;
      throw;
    }
    return false;
  }

  void process() noexcept override {
    try {
      if (remaining == 0) {
        if (copied_bytes != 0) record_memory_fallback(state, copied_bytes);
        finish(0);
        return;
      }
      {
        std::lock_guard guard(handle.mutex);
        if (handle.write_state == WRITE_FAILED || handle.unlinked.load()) {
          throw std::system_error(handle.unlinked.load() ? ESTALE : handle.write_error,
                                  std::generic_category(), "retained write failed");
        }
      }
      if (!reserve()) return;
      if (!handle.current_part) handle.current_part = std::make_unique<RetainedPart>();
      fuse_bufvec& input = buffers();
      while (input.idx < input.count && input.off == input.buf[input.idx].size) {
        ++input.idx;
        input.off = 0;
      }
      if (input.idx == input.count || input.off > input.buf[input.idx].size) {
        throw std::runtime_error("short asynchronous FUSE write buffer");
      }
      const fuse_buf& buffer = input.buf[input.idx];
      const bool fd = (buffer.flags & FUSE_BUF_IS_FD) != 0;
      memory_write = !fd || copying_fd;
      RetainedPart& part = *handle.current_part;
      PipeSegment& segment = writable_segment(
          part, memory_write ? PIPE_INPUT_MEMORY : PIPE_INPUT_FD);
      const size_t count = std::min({remaining, buffer.size - input.off,
          size_t(state.config.part_size - part.bytes),
          segment.pipe.capacity() - segment.bytes});
      if (count == 0) throw std::logic_error("empty retained write transfer");
      if (!fd) {
        if (buffer.mem == nullptr) {
          throw std::system_error(EFAULT, std::generic_category(), "null FUSE write memory");
        }
        stage = PIPE_WRITE;
        submit_io(AsyncIoRequest::WRITE, segment.pipe.write_fd(),
                   static_cast<std::byte*>(buffer.mem) + input.off, count);
      } else {
        const bool seek = (buffer.flags & FUSE_BUF_FD_SEEK) != 0;
        if (seek && (buffer.pos < 0 ||
            uint64_t(buffer.pos) > uint64_t(INT64_MAX) - input.off)) {
          throw std::system_error(EOVERFLOW, std::generic_category(), "FUSE buffer offset");
        }
        const off_t position = seek ? buffer.pos + off_t(input.off) : -1;
        if (copying_fd) {
          if (!copy) copy = std::make_unique<std::byte[]>(64U * 1024U);
          stage = COPY_READ;
          submit_io(seek ? AsyncIoRequest::PREAD : AsyncIoRequest::READ,
                     buffer.fd, copy.get(), std::min<size_t>(count, 64U * 1024U),
                     -1, position);
        } else {
          stage = SPLICE;
          submit_io(AsyncIoRequest::SPLICE, buffer.fd, nullptr, count,
                     segment.pipe.write_fd(), position,
                     SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
        }
      }
    } catch (...) { finish(AsyncPartUpload::error_code(std::current_exception())); }
  }

  static void received(void* context, ssize_t result) noexcept {
    auto* self = static_cast<AsyncUncachedWrite*>(context);
    try {
      if (self->budget_wait) {
        self->state.budget_reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
        self->budget_wait = false;
      }
      if (self->stage == BUDGET) {
        if (result < 0) self->finish(-int(result));
        else self->process();
        return;
      }
      if (self->stage == SPLICE && result == -EAGAIN &&
          self->handle.current_part->segments.back().bytes != 0) {
        self->handle.current_part->segments.back().pipe.close_write_end();
        self->process();
        return;
      }
      if (self->stage == SPLICE && result < 0 &&
          (result == -EINVAL || result == -EPERM || result == -ENOSYS ||
           result == -EOPNOTSUPP || result == -EXDEV)) {
        self->copying_fd = true;
        self->process();
        return;
      }
      if (result <= 0) {
        throw std::system_error(result < 0 ? -int(result) : EIO,
                                std::generic_category(), "retained FUSE write");
      }
      const size_t count = size_t(result);
      if (self->stage == COPY_READ) {
        self->stage = PIPE_WRITE;
        self->io = {};
        self->io.kind = AsyncIoRequest::WRITE;
        self->io.fd = self->handle.current_part->segments.back().pipe.write_fd();
        self->io.data = self->copy.get();
        self->io.length = count;
        self->io.exact = true;
        self->io.timeout_ms = 0;
        self->io.complete = received;
        self->io.context = self;
        if (!self->reactor.submit(self->io)) {
          throw std::system_error(errno, std::generic_category(), "copy retained FUSE write");
        }
        return;
      }
      if (self->io.exact && count != self->io.length) {
        throw std::system_error(EIO, std::generic_category(), "short retained pipe write");
      }
      auto& part = *self->handle.current_part;
      part.segments.back().bytes += count;
      part.bytes += count;
      if (self->memory_write) self->copied_bytes += count;
      self->consume(count);
      {
        std::lock_guard guard(self->handle.mutex);
        submit_current_part(self->state, self->handle, &self->active);
      }
      self->process();
    } catch (...) { self->finish(AsyncPartUpload::error_code(std::current_exception())); }
  }
};

struct AsyncCachedWrite final : AsyncWriteRequest {
  using AsyncWriteRequest::AsyncWriteRequest;
  FuseReactor::ReactorTask completion;
  std::exception_ptr worker_error;

  static void ready(void* context) noexcept {
    auto* self = static_cast<AsyncCachedWrite*>(context);
    if (self->worker_error) {
      self->handle.cache_entry->isolate_write();
      self->finish(AsyncPartUpload::error_code(self->worker_error));
      return;
    }
    try {
      {
        std::lock_guard guard(self->handle.mutex);
        remember_partial_write_pages(self->handle, uint64_t(self->offset),
            self->end, self->handle.cache_entry->page_size());
        self->handle.stream_offset = self->end;
        self->handle.size = self->end;
        submit_ready_cached_parts(self->state, self->handle, false, &self->active);
      }
      self->remaining = 0;
      self->finish(0);
    } catch (...) { self->finish(AsyncPartUpload::error_code(std::current_exception())); }
  }

  void process() noexcept override {
    completion = {ready, ready, this};
    if (!reactor.reserve_completion(&completion)) {
      finish(ECANCELED);
      return;
    }
    try {
      state.uploads->submit(&handle, [this] {
        try {
          IoExecutorScope local_only(nullptr, 0);
          handle.cache_entry->prepare_write(uint64_t(offset), length);
          write_fuse_buffers_to_cache(state, buffers(), handle.cache_write_pipe,
              handle.cache_entry->data_fd(), uint64_t(offset), length);
          handle.cache_entry->publish_dirty(uint64_t(offset), length, end);
        } catch (...) { worker_error = std::current_exception(); }
        reactor.complete(&completion);
      });
    } catch (...) {
      worker_error = std::current_exception();
      reactor.complete(&completion);
    }
  }
};

template<class WriteTask>
void start_async_write(State& state, OpenHandle& handle, FuseReactor& reactor,
                        fuse_req_t request, fuse_ino_t inode,
                        fuse_bufvec* input, off_t offset) {
  if (input == nullptr || input->count == 0) {
    throw std::system_error(EINVAL, std::generic_category(), "invalid FUSE write buffers");
  }
  auto task = std::make_unique<WriteTask>(state, handle, reactor,
                                         request, inode, *input, offset);
  task->input_owner = reactor.retain_input();
  if (task->input_owner == nullptr) {
    throw std::system_error(errno == 0 ? EIO : errno,
                            std::generic_category(), "retain FUSE write input");
  }
  task.release()->start();
}

void UncachedFileWriter::write_async(State& state, OpenHandle& handle,
                                     fuse_req_t request, fuse_ino_t inode,
                                     fuse_bufvec* input, off_t offset,
                                     FuseReactor& reactor) {
  start_async_write<AsyncUncachedWrite>(
      state, handle, reactor, request, inode, input, offset);
}

void CachedFileWriter::write_async(State& state, OpenHandle& handle,
                                   fuse_req_t request, fuse_ino_t inode,
                                   fuse_bufvec* input, off_t offset,
                                   FuseReactor& reactor) {
  start_async_write<AsyncCachedWrite>(
      state, handle, reactor, request, inode, input, offset);
}

void ngs3fs_write_buf(fuse_req_t request, fuse_ino_t inode,
                      fuse_bufvec* input, off_t offset,
                      fuse_file_info* file) {
  try {
    State& state = state_from(request);
    OpenHandle& handle = handle_required(file);
    if (!handle.writer) {
      throw std::system_error(EBADF, std::generic_category(),
                              "missing file writer");
    }
    FuseReactor* reactor = current_fuse_reactor();
    if (reactor != nullptr && !state.config.tls) {
      handle.writer->write_async(state, handle, request, inode,
                                  input, offset, *reactor);
      return;
    }
    handle.writer->write(state, handle, request, inode, input, offset);
  } catch (...) {
    reply_callback_error(request);
  }
}

struct AsyncFlushRequest {
  AsyncFlushRequest(State& s, OpenHandle& h, FuseReactor& r, fuse_req_t req)
      : state(s), handle(h), reactor(r), request(req), active(h) {}

  State& state;
  OpenHandle& handle;
  FuseReactor& reactor;
  fuse_req_t request;
  OpenRequestGuard active;
  std::shared_ptr<RetainedPart> part;
  std::vector<Pipe> replay;
  ChecksumValue checksum;
  ssostr<248> body;
  ssostr<64> body_etag;
  Response committed;
  std::optional<ObjectMetadata> metadata;
  std::unique_ptr<AsyncSignedS3Request> http;
  FuseReactor::ReactorTask completion;
  AsyncIoRequest wait;
  UniqueFd timer;
  uint64_t notification = 0;
  std::exception_ptr worker_error;
  bool owns_seal = false;
  bool parts_prepared = false;
  bool waiting = false;
  bool reserved = false;
  bool admitted = false;
  bool prepared = false;
  bool ambiguous = false;
  bool recovering = false;
  unsigned attempt = 0;

  void finish(int error) noexcept {
    std::unique_ptr<AsyncFlushRequest> self(this);
    http.reset();
    replay.clear();
    part.reset();
    if (reserved) release_part_budget(state);
    if (admitted) state.uploads->finish_upload();
    bool unregister = false;
    {
      std::lock_guard guard(handle.mutex);
      if (owns_seal) {
        if (handle.unlinked.load()) error = ESTALE;
        if (error == 0 && handle.write_state == WRITE_FAILED) error = handle.write_error;
        if (error == 0) {
          handle.write_state = WRITE_SEALED;
          unregister = handle.registered;
          handle.registered = false;
        } else {
          fail_write(handle, error);
        }
        notify_handle(handle);
      }
    }
    if (unregister) unregister_open_handle(state, handle.object_path, handle, false);
    fuse_reply_err(request, error);
  }

  static void awakened(void* context, ssize_t result) noexcept {
    auto* self = static_cast<AsyncFlushRequest*>(context);
    self->handle.reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
    self->waiting = false;
    if (result < 0) self->finish(-int(result));
    else self->start();
  }

  void start() noexcept {
    try {
      {
        std::lock_guard guard(handle.mutex);
        if (handle.unlinked.load()) {
          throw std::system_error(ESTALE, std::generic_category(), "flush unlinked file");
        }
        if (handle.write_state == WRITE_FAILED) {
          throw std::system_error(handle.write_error, std::generic_category(), "flush failed write");
        }
        if (handle.write_state == WRITE_SEALED) { finish_after_unlock = true; }
        else {
          if (handle.write_state == WRITE_OPEN) {
            handle.write_state = WRITE_SEALING;
            owns_seal = true;
          }
          if (!owns_seal || handle.write_in_progress) {
            handle.reactor_waiters.fetch_add(1, std::memory_order_acq_rel);
            waiting = true;
          } else {
            if (!parts_prepared) {
              if (handle.cache_entry) {
                if (handle.stream_offset > state.config.part_size || handle.multipart_required) {
                  submit_ready_cached_parts(state, handle, true, &active);
                }
              } else if (!handle.multipart_required) {
                part.reset(handle.current_part.release());
                if (part && !part->segments.empty()) part->segments.back().pipe.close_write_end();
                reserved = handle.current_reservation;
                handle.current_reservation = false;
              } else if (handle.current_part && handle.current_part->bytes != 0) {
                std::shared_ptr<RetainedPart> tail(handle.current_part.release());
                if (!tail->segments.empty()) tail->segments.back().pipe.close_write_end();
                handle.current_reservation = false;
                try { submit_part(state, handle, std::move(tail), &active); }
                catch (...) { release_part_budget(state); throw; }
              } else if (handle.current_reservation) {
                handle.current_reservation = false;
                release_part_budget(state);
              }
              parts_prepared = true;
            }
            if (handle.pending_parts != 0) {
              handle.reactor_waiters.fetch_add(1, std::memory_order_acq_rel);
              waiting = true;
            }
          }
        }
      }
      if (finish_after_unlock) { finish(0); return; }
      if (waiting) {
        wait = {};
        wait.kind = AsyncIoRequest::READ;
        wait.fd = handle.state_event.get();
        wait.data = &notification;
        wait.length = sizeof(notification);
        wait.timeout_ms = 0; // Pending uploads each own their network deadline.
        wait.complete = awakened;
        wait.context = this;
        if (reactor.submit(wait)) return;
        handle.reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
        waiting = false;
        throw std::system_error(errno, std::generic_category(), "wait for write seal");
      }
      // Admission covers the final PUT/Complete until local publication finishes.
      completion = {prepared_ready, prepared_ready, this};
      if (!reactor.reserve_completion(&completion)) {
        finish(ECANCELED);
        return;
      }
      try {
        state.uploads->submit_upload(&handle, [this] {
          admitted = true;
          prepare();
          reactor.complete(&completion);
        }, true);
      } catch (...) {
        worker_error = std::current_exception();
        reactor.complete(&completion);
      }
    } catch (...) { finish(AsyncPartUpload::error_code(std::current_exception())); }
  }

  bool finish_after_unlock = false;

  void prepare() noexcept {
    try {
      IoExecutorScope local_only(nullptr, 0);
      if (!prepared) {
        if (handle.cache_entry) handle.cache_entry->begin_write();
        if (handle.multipart_required) {
          for (const auto& etag : handle.part_etags) {
            if (etag.empty()) throw std::runtime_error("multipart commit has a missing ETag");
          }
          if (checksum_multipart_type(state.config.checksum) == "COMPOSITE") {
            if (handle.part_checksums.size() != handle.part_etags.size()) {
              throw std::runtime_error("multipart commit has missing checksums");
            }
            for (const auto& value : handle.part_checksums) {
              if (value.empty()) throw std::runtime_error("multipart part has no checksum");
            }
          }
          body = complete_body(handle, state.config.checksum);
          if (state.config.checksum == CHECKSUM_CRC64NVME ||
              state.config.checksum == CHECKSUM_CRC64XZ) {
            checksum = combined_multipart_checksum(handle, state.config.checksum);
          }
        } else if (checksum_has_digest(state.config.checksum)) {
          checksum = handle.cache_entry
              ? checksum_file_range(state.config.checksum,
                    handle.cache_entry->data_fd(), 0, size_t(handle.stream_offset))
              : retained_checksum(state.config.checksum, part.get());
        }
        prepared = true;
      }
      replay.clear();
      if (part) {
        replay.reserve(part->segments.size());
        for (const auto& segment : part->segments) {
          if (segment.bytes == 0) continue;
          Pipe clone = Pipe::create(segment.pipe.capacity());
          if (clone.capacity() < segment.bytes) {
            throw std::system_error(ENOBUFS, std::generic_category(), "write replay capacity");
          }
          tee_exact(segment.pipe.read_fd(), clone.write_fd(), segment.bytes, 0);
          clone.close_write_end();
          replay.push_back(std::move(clone));
        }
      }
    } catch (...) { worker_error = std::current_exception(); }
  }

  static void prepared_ready(void* context) noexcept {
    auto* self = static_cast<AsyncFlushRequest*>(context);
    if (self->worker_error) {
      self->finish(AsyncPartUpload::error_code(self->worker_error));
      return;
    }
    self->send();
  }

  void send() noexcept {
    try {
      AsyncHttpRequest args;
      args.upload = true;
      if (handle.multipart_required) {
        args.method = "POST";
        args.path = query_path(handle.object_path,
            "uploadId=" + uri_encode(handle.upload_id, false));
        args.headers.push_back({"content-type", "application/xml"});
        const auto* bytes = reinterpret_cast<const std::byte*>(body.data());
        args.body.assign(bytes, bytes + body.size());
        if (state.config.checksum == CHECKSUM_CRC64NVME) {
          args.headers.push_back({checksum_header_name(state.config.checksum), checksum.base64});
          args.headers.push_back({"x-amz-mp-object-size", std::to_string(handle.stream_offset)});
        }
      } else {
        args.method = "PUT";
        args.path = handle.object_path;
        args.headers.push_back({"content-type", "application/octet-stream"});
        if (!handle.write_id.empty()) {
          args.headers.push_back({"x-amz-meta-ngs3fs-write-id", handle.write_id});
        }
        append_upload_checksum(args.headers, state.config.checksum, checksum);
        if (handle.cache_entry) {
          args.source_fd = handle.cache_entry->data_fd();
          args.source_length = size_t(handle.stream_offset);
        } else if (part) {
          size_t i = 0;
          args.source_segments.reserve(replay.size());
          for (const auto& segment : part->segments) {
            if (segment.bytes == 0) continue;
            args.source_segments.push_back({replay[i++].read_fd(), 0, segment.bytes, false});
          }
        }
      }
      if (!handle.etag.empty()) args.headers.push_back({"if-match", handle.etag});
      else if (handle.create_exclusive) args.headers.push_back({"if-none-match", "*"});
      http = std::make_unique<AsyncSignedS3Request>(
          state, reactor, std::move(args), received, this, 1,
          !handle.multipart_required && handle.stream_offset == 0
              ? kEmptyPayloadSha256 : kUnsignedPayload);
      if (!http->start()) throw std::system_error(errno, std::generic_category(), "submit write commit");
    } catch (...) { received(this, Response{}, std::current_exception()); }
  }

  static void retry_ready(void* context, ssize_t result) noexcept {
    auto* self = static_cast<AsyncFlushRequest*>(context);
    if (result < 0) { self->finish(-int(result)); return; }
    self->completion = {prepared_ready, prepared_ready, self};
    if (!self->reactor.reserve_completion(&self->completion)) {
      self->finish(ECANCELED);
      return;
    }
    try {
      self->state.uploads->submit(&self->handle, [self] {
        self->prepare();
        self->reactor.complete(&self->completion);
      });
    } catch (...) {
      self->worker_error = std::current_exception();
      self->reactor.complete(&self->completion);
    }
  }

  void retry(const Response& response) {
    const uint64_t ms = async_retry_milliseconds(response, attempt++);
    timer.reset(::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC));
    if (!timer) throw std::system_error(errno, std::generic_category(), "write retry timer");
    itimerspec spec{};
    spec.it_value.tv_sec = time_t(ms / 1000);
    spec.it_value.tv_nsec = long((ms % 1000) * 1'000'000);
    if (::timerfd_settime(timer.get(), 0, &spec, nullptr) != 0) {
      throw std::system_error(errno, std::generic_category(), "arm write retry");
    }
    wait = {};
    wait.kind = AsyncIoRequest::READ;
    wait.fd = timer.get();
    wait.data = &notification;
    wait.length = sizeof(notification);
    wait.complete = retry_ready;
    wait.context = this;
    if (!reactor.submit(wait)) throw std::system_error(errno, std::generic_category(), "submit write retry");
  }

  static void received(void* context, Response&& response, std::exception_ptr error) noexcept {
    auto* self = static_cast<AsyncFlushRequest*>(context);
    self->http.reset();
    try {
      if (!error && self->state.config.directory_bucket &&
          (response.status == 401 || response.status == 403) && self->attempt < 3) {
        invalidate_express_session(self->state);
        self->retry(response);
        return;
      }
      if (AsyncPartUpload::error_code(error) == ECANCELED) {
        self->finish(ECANCELED);
        return;
      }
      if (error || retryable_response(response)) {
        self->ambiguous = true;
        if (self->attempt < 3) { self->retry(response); return; }
        self->head(true);
        return;
      }
      if (self->ambiguous) {
        S3ErrorInfo embedded;
        if (response.status < 200 || response.status >= 300 ||
            (!response.body.empty() && parse_s3_error(response_xml(response), embedded))) {
          self->head(true);
          return;
        }
      }
      if (response.status == 412 && self->handle.create_exclusive) {
        throw std::system_error(EEXIST, std::generic_category(), "write destination exists");
      }
      if (response.status == 409 && self->handle.multipart_required) {
        throw std::system_error(ESTALE, std::generic_category(), "multipart commit conflict");
      }
      require_s3_success(response, self->handle.multipart_required
          ? "CompleteMultipartUpload" : "PutObject");
      if (self->handle.multipart_required) {
        S3Xml xml(response_xml(response), "CompleteMultipartUpload");
        const auto& root = xml.result_root("CompleteMultipartUploadResult");
        self->body_etag = xml.optional_text(root, "ETag");
        if (self->state.config.checksum == CHECKSUM_CRC64NVME) {
          if (xml.required_text(root, checksum_xml_name(self->state.config.checksum)) !=
              sso_view(self->checksum.base64)) {
            throw std::runtime_error("multipart commit checksum mismatch");
          }
        } else if (self->state.config.checksum == CHECKSUM_CRC64XZ) {
          verify_upload_checksum(response, self->state.config.checksum,
                                 self->checksum, "CompleteMultipartUpload");
        }
      } else {
        verify_upload_checksum(response, self->state.config.checksum,
                               self->checksum, "PutObject");
      }
      self->committed = std::move(response);
      if (self->committed.headers.find("last-modified") == self->committed.headers.end()) {
        self->head(false);
      } else {
        self->commit_local();
      }
    } catch (...) { self->finish(AsyncPartUpload::error_code(std::current_exception())); }
  }

  void head(bool recover) noexcept {
    recovering = recover;
    try {
      AsyncHttpRequest args;
      args.method = "HEAD";
      args.path = handle.object_path;
      http = std::make_unique<AsyncSignedS3Request>(state, reactor, std::move(args), headed, this);
      if (!http->start()) throw std::system_error(errno, std::generic_category(), "submit write metadata");
    } catch (...) { headed(this, Response{}, std::current_exception()); }
  }

  static void headed(void* context, Response&& response, std::exception_ptr error) noexcept {
    auto* self = static_cast<AsyncFlushRequest*>(context);
    self->http.reset();
    try {
      if (error) std::rethrow_exception(error);
      self->metadata = decode_head_response(response);
      const auto& metadata = *self->metadata;
      if (metadata.size != self->handle.stream_offset ||
          (self->recovering &&
           ((!self->handle.write_id.empty() && metadata.write_id != self->handle.write_id) ||
            (self->handle.write_id.empty() && !self->handle.etag.empty() &&
             metadata.etag == self->handle.etag)))) {
        throw std::system_error(EIO, std::generic_category(), "write commit outcome unknown");
      }
      if (self->recovering) {
        self->committed = std::move(response);
        fprintf(stderr, "warning: recovered ambiguous S3 write commit: %s\n",
                self->handle.object_path.c_str());
      }
      self->commit_local();
    } catch (...) {
      if (self->recovering) {
        fprintf(stderr, "error: S3 write commit outcome unknown: %s\n",
                self->handle.object_path.c_str());
      }
      self->finish(AsyncPartUpload::error_code(std::current_exception()));
    }
  }

  static void committed_local(void* context) noexcept {
    auto* self = static_cast<AsyncFlushRequest*>(context);
    if (!self->worker_error) {
      std::lock_guard guard(self->handle.mutex);
      self->handle.upload_id.clear();
      self->handle.part_etags.clear();
      self->handle.part_checksums.clear();
      self->handle.part_checksum_values.clear();
      self->handle.part_sizes.clear();
    }
    self->finish(AsyncPartUpload::error_code(self->worker_error));
  }

  void commit_local() noexcept {
    completion = {committed_local, committed_local, this};
    if (!reactor.reserve_completion(&completion)) { finish(ECANCELED); return; }
    try {
      state.uploads->submit(&handle, [this] {
        try {
          IoExecutorScope local_only(nullptr, 0);
          if (handle.cache_entry) {
            handle.page_cache_store_failed |= !store_cached_partial_pages(state, handle);
          } else if (part) {
            handle.page_cache_store_failed |= !store_page_cache(state, handle, *part, 0);
          }
          publish_written_metadata(handle, committed, metadata ? &*metadata : nullptr,
                                    sso_view(body_etag));
          if (handle.cache_entry) commit_cached_write(handle);
        } catch (...) { worker_error = std::current_exception(); }
        reactor.complete(&completion);
      });
    } catch (...) {
      worker_error = std::current_exception();
      reactor.complete(&completion);
    }
  }
};

struct AsyncFsyncRequest {
  AsyncFsyncRequest(State& s, OpenHandle& h, FuseReactor& r, fuse_req_t req)
      : state(s), handle(h), reactor(r), request(req), active(h) {}
  State& state;
  OpenHandle& handle;
  FuseReactor& reactor;
  fuse_req_t request;
  OpenRequestGuard active;
  AsyncIoRequest wait;
  FuseReactor::ReactorTask completion;
  uint64_t notification = 0;
  std::exception_ptr error;

  void finish(int value) noexcept {
    std::unique_ptr<AsyncFsyncRequest> self(this);
    fuse_reply_err(request, value);
  }

  static void awakened(void* context, ssize_t result) noexcept {
    auto* self = static_cast<AsyncFsyncRequest*>(context);
    self->handle.reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
    if (result < 0) self->finish(-int(result));
    else self->start();
  }

  static void ready(void* context) noexcept {
    auto* self = static_cast<AsyncFsyncRequest*>(context);
    int value = AsyncPartUpload::error_code(self->error);
    {
      std::lock_guard guard(self->handle.mutex);
      if (self->handle.unlinked.load()) value = ESTALE;
      else if (self->handle.write_state == WRITE_FAILED) value = self->handle.write_error;
    }
    self->finish(value);
  }

  void start() noexcept {
    try {
      bool waiting = false;
      {
        std::lock_guard guard(handle.mutex);
        if (handle.unlinked.load()) {
          throw std::system_error(ESTALE, std::generic_category(), "fsync unlinked file");
        }
        if (handle.write_state == WRITE_FAILED) {
          throw std::system_error(handle.write_error, std::generic_category(), "fsync failed write");
        }
        if (handle.write_in_progress) {
          waiting = true;
          handle.reactor_waiters.fetch_add(1, std::memory_order_acq_rel);
        }
      }
      if (waiting) {
        wait = {};
        wait.kind = AsyncIoRequest::READ;
        wait.fd = handle.state_event.get();
        wait.data = &notification;
        wait.length = sizeof(notification);
        wait.timeout_ms = 0; // Wait for local writes without a network timeout.
        wait.complete = awakened;
        wait.context = this;
        if (reactor.submit(wait)) return;
        handle.reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
        throw std::system_error(errno, std::generic_category(), "wait for fsync");
      }
      if (!handle.cache_entry) { finish(0); return; }
      completion = {ready, ready, this};
      if (!reactor.reserve_completion(&completion)) { finish(ECANCELED); return; }
      try {
        state.uploads->submit(&handle, [this] {
          try {
            IoExecutorScope local_only(nullptr, 0);
            handle.cache_entry->sync_write();
          } catch (...) { error = std::current_exception(); }
          reactor.complete(&completion);
        });
      } catch (...) {
        error = std::current_exception();
        reactor.complete(&completion);
      }
    } catch (...) { finish(AsyncPartUpload::error_code(std::current_exception())); }
  }
};

void ngs3fs_flush(fuse_req_t request, fuse_ino_t,
                  fuse_file_info* file) {
  try {
    OpenHandle& handle = handle_required(file);
    if (handle.writer) {
      State& state = state_from(request);
      if (FuseReactor* reactor = current_fuse_reactor();
          reactor != nullptr && !state.config.tls) {
        auto task = std::make_unique<AsyncFlushRequest>(state, handle, *reactor, request);
        task.release()->start();
        return;
      }
      handle.writer->flush(state, handle);
    }
    fuse_reply_err(request, 0);
  } catch (...) {
    reply_callback_error(request);
  }
}

void ngs3fs_fsync(fuse_req_t request, fuse_ino_t, int data_only,
                  fuse_file_info* file) {
  try {
    OpenHandle& handle = handle_required(file);
    if (handle.writer) {
      State& state = state_from(request);
      if (FuseReactor* reactor = current_fuse_reactor();
          reactor != nullptr && !state.config.tls) {
        auto task = std::make_unique<AsyncFsyncRequest>(state, handle, *reactor, request);
        task.release()->start();
        return;
      }
      handle.writer->fsync(state, handle, data_only != 0);
    }
    fuse_reply_err(request, 0);
  } catch (...) {
    reply_callback_error(request);
  }
}

void discard_pending_inode(State& state, OpenHandle& handle) noexcept {
  InodeFile* item = handle.item;
  if (item == nullptr || item->detached() || !item->pending()) {
    return;
  }
  InodeBase* parent = item->parent();
  if (parent == nullptr || !parent->directory() || parent->detached()) {
    return;
  }
  try {
    std::lock_guard guard(parent->dir_children().mutation_mutex);
    if (!item->detached() && item->pending()) {
      remove_item(state, *item);
    }
  } catch (...) {
  }
}

void release_write_local_no_network(State& state, OpenHandle& handle) noexcept;

void UncachedFileWriter::release(State& state,
                                 OpenHandle& handle) noexcept {
  try {
    std::unique_lock guard(handle.mutex);
    while (handle.write_in_progress || handle.write_state == WRITE_SEALING) {
      wait_handle(handle, guard);
    }
    if (handle.write_state == WRITE_OPEN) fail_write(handle, EIO);
    while (handle.pending_parts != 0) wait_handle(handle, guard);
    // Only the legacy engine calls this blocking wrapper.
    abort_multipart(state, handle);
  } catch (...) {
    fprintf(stderr, "warning: legacy write release failed: path=%s\n",
            handle.object_path.c_str());
  }
  release_write_local_no_network(state, handle);
}

void CachedFileWriter::release(State& state,
                               OpenHandle& handle) noexcept {
  try {
    std::unique_lock guard(handle.mutex);
    while (handle.write_in_progress || handle.write_state == WRITE_SEALING) {
      wait_handle(handle, guard);
    }
    while (handle.pending_parts != 0) wait_handle(handle, guard);
    const bool unlinked = handle.unlinked.load(std::memory_order_acquire);
    guard.unlock();
    if (unlinked) abort_multipart(state, handle);
  } catch (...) {
    fprintf(stderr, "warning: legacy cached write release failed: path=%s\n",
            handle.object_path.c_str());
  }
  release_write_local_no_network(state, handle);
}

// Last-reference retirement only. Never waits for an admitted write and never
// performs S3 I/O, including the allocation/shutdown fallback of async release.
void release_write_local_no_network(State& state, OpenHandle& handle) noexcept {
  bool registered = false;
  bool release_budget = false;
  bool preserve = false;
  bool discard_pending = false;
  const bool unlinked = handle.unlinked.load(std::memory_order_acquire);
  {
    std::lock_guard guard(handle.mutex);
    registered = handle.registered;
    handle.registered = false;
    release_budget = handle.current_reservation;
    handle.current_reservation = false;
    handle.current_part.reset();
    preserve = handle.cache_entry && !unlinked &&
        handle.cache_entry->dirty() && handle.write_state != WRITE_SEALED;
    discard_pending = handle.write_state != WRITE_SEALED && !preserve;
    if (handle.write_state == WRITE_OPEN || handle.write_state == WRITE_SEALING) {
      fail_write(handle, EIO);
    }
    if (!preserve && !handle.upload_id.empty()) {
      fprintf(stderr, "warning: multipart upload not aborted during local-only "
                      "release; lifecycle cleanup may be required: path=%s "
                      "upload_id=%s\n", handle.object_path.c_str(),
              handle.upload_id.c_str());
      handle.upload_id.clear();
    }
  }
  if (preserve) {
    fprintf(stderr, "warning: cached write preserved for recovery: path=%s "
                    "bytes=%" PRIu64 "\n",
            handle.object_path.c_str(), handle.stream_offset);
    try {
      std::lock_guard guard(state.open_files_mutex);
      state.recovery_paths.insert(handle.object_path);
    } catch (...) {
      fprintf(stderr, "warning: recovery registry allocation failed; cached "
                      "write remains on disk: path=%s\n",
              handle.object_path.c_str());
    }
  } else if (unlinked && handle.cache_entry) {
    try { handle.cache_entry->discard_write(); }
    catch (...) {
      fprintf(stderr, "warning: local cache cleanup failed for unlinked "
                      "write handle: path=%s\n", handle.object_path.c_str());
    }
  }
  // Removing a pending dentry must precede dropping the last inode open count.
  if (!preserve && handle.write_state != WRITE_SEALED && handle.writer &&
      handle.item && !handle.item->detached()) {
    update_truncate_pending(*handle.item, false);
    handle.item->parent()->expire.store(0, std::memory_order_release);
    handle.item->set_page_cache_valid(false);
  }
  if (discard_pending) discard_pending_inode(state, handle);
  if (registered) unregister_open_handle(state, handle.object_path, handle);
  else release_open_inode(state, handle);
  if (release_budget) release_part_budget(state);
}

struct AsyncWriteRelease {
  using Complete = void (*)(void*, int) noexcept;
  State& state;
  OpenHandle& handle;
  FuseReactor& reactor;
  Complete complete;
  void* context;
  FuseReactor::ReactorTask ticket;
  std::unique_ptr<AsyncSignedS3Request> http;
  int error = 0;

  static void done(void* context) noexcept {
    auto* self = static_cast<AsyncWriteRelease*>(context);
    const auto callback = self->complete;
    void* target = self->context;
    const int error = self->error;
    delete self;
    callback(target, error);
  }

  void cleanup() noexcept {
    try {
      state.uploads->submit(&handle, [this] {
        {
          IoExecutorScope local_only(nullptr, 0);
          release_write_local_no_network(state, handle);
        }
        reactor.complete(&ticket);
      });
    } catch (...) {
      fprintf(stderr, "warning: local release worker unavailable; completing "
                      "local-only handle cleanup inline: path=%s\n",
              handle.object_path.c_str());
      {
        IoExecutorScope local_only(nullptr, 0);
        release_write_local_no_network(state, handle);
      }
      reactor.complete(&ticket);
    }
  }

  static void aborted(void* context, Response&& response,
                       std::exception_ptr error) noexcept {
    auto* self = static_cast<AsyncWriteRelease*>(context);
    self->http.reset();
    if (error || (response.status != 204 && response.status != 200 &&
                  response.status != 404)) {
      fprintf(stderr, "warning: failed to abort multipart upload on release; "
                      "lifecycle cleanup may be required: path=%s status=%d\n",
              self->handle.object_path.c_str(), response.status);
    }
    self->handle.upload_id.clear();
    self->cleanup();
  }

  void start() noexcept {
    const bool preserve = handle.cache_entry && !handle.unlinked.load() &&
        handle.cache_entry->dirty() && handle.write_state != WRITE_SEALED;
    if (handle.upload_id.empty() || preserve) {
      cleanup();
      return;
    }
    try {
      AsyncHttpRequest args;
      args.method = "DELETE";
      args.path = query_path(handle.object_path,
          "uploadId=" + uri_encode(handle.upload_id, false));
      http = std::make_unique<AsyncSignedS3Request>(state, reactor,
          std::move(args), aborted, this);
      if (!http->start()) throw std::system_error(errno, std::generic_category(), "abort multipart");
    } catch (...) { aborted(this, Response{}, std::current_exception()); }
  }
};

// The caller owns the retiring handle through completion. False means no work
// was admitted and no callback runs. Completion is terminal: local registry and
// inode cleanup is finished even if worker admission needs a rare inline fallback.
bool start_async_write_release(State& state, OpenHandle& handle,
                                FuseReactor& reactor,
                                AsyncWriteRelease::Complete complete,
                                void* context) noexcept {
  {
    std::lock_guard guard(handle.mutex);
    if (handle.pending_parts != 0 || handle.write_in_progress ||
        handle.write_state == WRITE_SEALING) {
      fprintf(stderr, "error: non-idle write handle at last-reference retirement: "
                      "path=%s pending_parts=%zu write_in_progress=%d state=%d\n",
              handle.object_path.c_str(), size_t(handle.pending_parts),
              int(handle.write_in_progress), int(handle.write_state));
      assert(handle.pending_parts == 0 && !handle.write_in_progress &&
             handle.write_state != WRITE_SEALING);
      errno = EIO;
      return false;
    }
  }
  try {
    auto task = std::make_unique<AsyncWriteRelease>(
        AsyncWriteRelease{state, handle, reactor, complete, context, {}, {}});
    task->ticket = {AsyncWriteRelease::done, AsyncWriteRelease::done, task.get()};
    if (!reactor.reserve_completion(&task->ticket)) return false;
    task.release()->start();
    return true;
  } catch (...) {
    errno = ENOMEM;
    return false;
  }
}

struct ReleaseTask {
  State* state;
  OpenHandle* handle;
  fuse_req_t request;
  FuseReactor* reactor;
  FuseReactor::ReactorTask task;
  std::unique_ptr<AsyncPendingDelete> pending;
  int result = 0;

  static void completed(void* context, std::exception_ptr = {}) noexcept {
    std::unique_ptr<ReleaseTask> task(static_cast<ReleaseTask*>(context));
    std::unique_ptr<OpenHandle> handle(task->handle);
    fuse_reply_err(task->request, task->result);
  }

  void finish_pending() noexcept {
    try {
      bool found;
      {
        std::lock_guard guard(state->open_files_mutex);
        found = state->pending_deletes.contains(handle->object_path);
      }
      if (found) {
        pending = std::make_unique<AsyncPendingDelete>(
            *state, *reactor, handle->object_path, completed, this);
        if (pending->start()) return;
        fprintf(stderr, "warning: pending delete cleanup not admitted: path=%s\n",
                handle->object_path.c_str());
      }
    } catch (...) {
      fprintf(stderr, "warning: pending delete retained after handle close: path=%s\n",
              handle->object_path.c_str());
    }
    completed(this);
  }

  static void write_released(void* context, int error) noexcept {
    auto* task = static_cast<ReleaseTask*>(context);
    if (error != 0) {
      task->result = error;
      fprintf(stderr, "warning: write release cleanup: path=%s: %s\n",
              task->handle->object_path.c_str(), strerror(error));
    }
    task->finish_pending();
  }

  static void finish(void* context) noexcept {
    auto* release = static_cast<ReleaseTask*>(context);
    if (release->reactor != nullptr) {
      if (release->handle->writer) {
        if (start_async_write_release(*release->state, *release->handle,
                *release->reactor, write_released, release)) return;
        // Last-reference retirement cannot race an admitted writer. Failure
        // here is an internal invariant failure, not a successful close.
        fprintf(stderr, "error: cannot retire admitted write handle: path=%s: %s\n",
                release->handle->object_path.c_str(), strerror(errno));
        release->result = EIO;
        release_write_local_no_network(*release->state, *release->handle);
        release->finish_pending();
        return;
      }
      release_open_inode(*release->state, *release->handle);
      release->finish_pending();
      return;
    }
    std::unique_ptr<ReleaseTask> task(static_cast<ReleaseTask*>(context));
    std::unique_ptr<OpenHandle> handle(task->handle);
    try {
      if (handle->writer) {
        handle->writer->release(*task->state, *handle);
      } else {
        release_open_inode(*task->state, *handle);
      }
      finish_pending_delete(*task->state, handle->object_path);
      fuse_reply_err(task->request, 0);
    } catch (...) {
      reply_callback_error(task->request);
    }
  }

  static void ready(void* context) noexcept {
    auto* task = static_cast<ReleaseTask*>(context);
    if (task->reactor != nullptr) {
      task->reactor->complete(&task->task);
      return;
    }
    finish(task);
  }
};

void ngs3fs_release(fuse_req_t request, fuse_ino_t,
                    fuse_file_info* file) {
  try {
    State& state = state_from(request);
    OpenHandle& handle = handle_required(file);
    auto task = std::make_unique<ReleaseTask>();
    task->state   = &state;
    task->handle  = &handle;
    task->request = request;
    task->reactor = current_fuse_reactor();
    task->task    = {ReleaseTask::finish, ReleaseTask::finish, task.get()};
    if (task->reactor != nullptr && !task->reactor->reserve_completion(&task->task)) {
      throw std::system_error(errno, std::generic_category(), "reserve release completion");
    }
    handle.release_ready   = ReleaseTask::ready;
    handle.release_context = task.get();
    constexpr uint64_t closing = 1ULL << 63;
    handle.request_state.fetch_or(closing, std::memory_order_acq_rel);
    file->fh = 0;
    if (!handle.writable && handle.registered) {
      // The application has closed the handle. Internal read/checksum tasks
      // retain its lifetime, but must not prohibit a new application writer.
      handle.registered = false;
      unregister_open_handle(state, {}, handle, false);
    }
    task.release();
    drop_open_request(handle);
  } catch (...) {
    reply_callback_error(request);
  }
}

void ngs3fs_opendir(fuse_req_t request, fuse_ino_t inode,
                    fuse_file_info* file) {
  if (file == nullptr) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  try {
    State& state = state_from(request);
    InodeBase& item = inode_item(state, inode);
    if (!item.directory()) {
      throw std::system_error(
          ENOTDIR, std::generic_category(), "opendir");
    }
    auto handle = std::make_unique<DirHandle>();
    handle->item = static_cast<InodeDir*>(&item);
    // The kernel's lookup reference already pins item. An overlapping rmdir
    // may detach it after this check, just as for a native open directory.
    if (item.detached()) {
      throw std::system_error(ESTALE, std::generic_category(), "opendir");
    }
    retain_inode_count(item.open_count, "directory open count");
    cache_touch(*handle->item);
    file->fh = reinterpret_cast<uint64_t>(handle.release());
    if (fuse_reply_open(request, file) != 0) {
      std::unique_ptr<DirHandle> failed(
          reinterpret_cast<DirHandle*>(uintptr_t(file->fh)));
      release_inode_count(failed->item->open_count);
      file->fh = 0;
      sweep_retired_items(state);
    }
  } catch (...) {
    reply_callback_error(request);
  }
}

void reply_directory_entries(fuse_req_t request, fuse_ino_t inode, size_t size,
                               off_t offset, DirHandle& handle) {
  try {
    State& state = state_from(request);
    InodeDir* directory = handle.item;
    cache_touch(*directory);

    thread_local std::vector<char> output;
    output.resize(size);
    size_t used = 0;

    auto append = [&](const char* name, fuse_ino_t entry_inode,
                      mode_t type, off_t next) {
      struct stat status{};
      status.st_ino  = entry_inode;
      status.st_mode = type;
      const size_t entry_size = fuse_add_direntry(
          request, nullptr, 0, name, &status, next);
      if (entry_size > output.size() - used) {
        return false;
      }
      fuse_add_direntry(request, output.data() + used,
                        output.size() - used, name, &status, next);
      used += entry_size;
      return true;
    };

    const fuse_ino_t parent_inode = item_inode(state, directory->parent());

    bool full = offset == 0 && !append(".", inode, S_IFDIR, 1);
    if (!full && offset <= 1) {
      full = !append("..", parent_inode, S_IFDIR, 2);
    }
    if (!full) {
      size_t slot = offset <= 2 ? 0 : size_t(offset - 2);
      char name[NAME_MAX + 1];
      for (;;) {
        fuse_ino_t child_inode;
        mode_t type;
        off_t next;
        {
          const Directory& children = directory->children;
          std::shared_lock guard(children.mutex);
          while (slot < children.end_i() && children.is_deleted(slot)) {
            ++slot;
          }
          if (slot >= children.end_i()) {
            break;
          }
          InodeBase* child = children.val(slot);
          const terark::fstring key = children.key(slot);
          memcpy(name, key.data(), key.size());
          name[key.size()] = '\0';
          child_inode = item_inode(child);
          type        = child->directory() ? S_IFDIR : S_IFREG;
          next        = off_t(slot + 3);
          ++slot;
        }
        if (!append(name, child_inode, type, next)) {
          break;
        }
      }
    }
    fuse_reply_buf(request, output.data(), used);
  } catch (...) {
    reply_callback_error(request);
  }
}

struct AsyncReaddir {
  State& state;
  FuseReactor& reactor;
  fuse_req_t request;
  fuse_ino_t inode;
  size_t size;
  off_t offset;
  DirHandle& handle;
  InodePin pin;
  DirectoryContinuation waiter;

  AsyncReaddir(State& s, FuseReactor& r, fuse_req_t req, fuse_ino_t ino,
                 size_t bytes, off_t off, DirHandle& dir)
      : state(s), reactor(r), request(req), inode(ino), size(bytes),
        offset(off), handle(dir) {
    retain_inode_count(handle.item->open_count, "readdir pin");
    pin = InodePin(state, *handle.item);
    waiter.complete = completed;
    waiter.context = this;
  }

  static void completed(void* context, std::exception_ptr error) noexcept {
    std::unique_ptr<AsyncReaddir> task(static_cast<AsyncReaddir*>(context));
    task->handle.initialized.store(error ? 0 : 2, std::memory_order_release);
    notify_dir_handle(task->handle);
    try {
      if (error) std::rethrow_exception(error);
      reply_directory_entries(task->request, task->inode, task->size,
                                task->offset, task->handle);
    } catch (...) {
      reply_callback_error(task->request);
    }
  }

  void start() noexcept {
    try {
      handle.initialized.store(1, std::memory_order_release);
      if (!await_directory_refresh(state, reactor, inode, waiter)) completed(this, {});
    } catch (...) {
      completed(this, std::current_exception());
    }
  }
};

void ngs3fs_readdir(fuse_req_t request, fuse_ino_t inode, size_t size,
                    off_t offset, fuse_file_info* file) {
  if (offset < 0) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  try {
    State& state = state_from(request);
    if (file == nullptr || file->fh == 0) {
      throw std::system_error(EBADF, std::generic_category(), "readdir");
    }
    auto& handle = *reinterpret_cast<DirHandle*>(uintptr_t(file->fh));
    InodeBase& item = inode_reference(state, inode);
    if (!item.directory() || handle.item != &item) {
      throw std::system_error(ENOTDIR, std::generic_category(), "readdir");
    }
    if (FuseReactor* reactor = current_fuse_reactor()) {
      if (handle.initialized.load(std::memory_order_acquire) != 2 && !item.detached()) {
        (new AsyncReaddir(state, *reactor, request, inode, size, offset, handle))->start();
        return;
      }
    } else {
      uint8_t initialized = handle.initialized.load(std::memory_order_acquire);
      while (initialized != 2) {
        if (initialized == 0 && handle.initialized.compare_exchange_weak(
                initialized, 1, std::memory_order_acquire, std::memory_order_acquire)) {
          try {
            if (!item.detached()) refresh_directory(state, inode);
            handle.initialized.store(2, std::memory_order_release);
            notify_dir_handle(handle);
            break;
          } catch (...) {
            handle.initialized.store(0, std::memory_order_release);
            notify_dir_handle(handle);
            throw;
          }
        }
        if (initialized == 1) wait_dir_handle(handle);
        initialized = handle.initialized.load(std::memory_order_acquire);
      }
    }
    reply_directory_entries(request, inode, size, offset, handle);
  } catch (...) {
    reply_callback_error(request);
  }
}

void ngs3fs_releasedir(fuse_req_t request, fuse_ino_t inode,
                       fuse_file_info* file) {
  try {
    if (file == nullptr || file->fh == 0) {
      throw std::system_error(
          EBADF, std::generic_category(), "releasedir");
    }
    State& state = state_from(request);
    std::unique_ptr<DirHandle> handle(
        reinterpret_cast<DirHandle*>(uintptr_t(file->fh)));
    file->fh = 0;
    InodeDir* item = handle->item;
    if (item == nullptr ||
        item->open_count.load(std::memory_order_relaxed) == 0) {
      throw std::system_error(
          EBADF, std::generic_category(), "releasedir inode");
    }
    InodeBase* expected = inode == FUSE_ROOT_ID
                              ? static_cast<InodeBase*>(state.root_item.get())
                              : reinterpret_cast<InodeBase*>(uintptr_t(inode));
    if (item != expected) {
      throw std::system_error(
          EBADF, std::generic_category(), "releasedir inode");
    }
    release_inode_count(item->open_count);
    state.cache_condition.notify_all();
    sweep_retired_items(state);
    fuse_reply_err(request, 0);
  } catch (...) {
    reply_callback_error(request);
  }
}

void delete_object(State& state, std::string_view key,
                   std::string_view etag) {
  const std::string path = object_request_path(state, key);
  std::vector<Header> headers;
  if (!etag.empty()) {
    headers.push_back(Header{"if-match", etag});
  }
  const Response response = request_with_retries([&] {
    std::vector<Header> request_headers = headers;
    append_authorization(request_headers, state, "DELETE", path,
                         kEmptyPayloadSha256);
    HttpPool::Lease client = state.http->acquire();
    return client->request_no_body("DELETE", path, request_headers);
  }, "DeleteObject");
  if (response.status == 412) {
    throw std::system_error(ESTALE, std::generic_category(), "DeleteObject");
  }
  if (response.status != 200 && response.status != 204 &&
      response.status != 404) {
    throw_s3_response(response, "DeleteObject");
  }
}

std::string pending_delete_key(State& state) {
  std::string key = state.config.prefix;
  key.append(kInternalDeleteDirectory);
  key.push_back('/');
  key += rename_client_token();
  return key;
}

bool hide_open_readers(State& state, std::string_view key,
                       std::string_view path,
                       std::string_view restore_key = {},
                       std::string_view replacement_etag = {},
                       std::string* hidden_key_result = nullptr,
                       std::string* hidden_path_result = nullptr) {
  {
    std::lock_guard guard(state.open_files_mutex);
    const auto opened = state.open_files.find(path);
    if (opened == state.open_files.end() || opened->second.readers == 0) {
      return false;
    }
  }

  const ObjectMetadata metadata = head_object(state, path);
  if (!native_rename_supported(state, path)) {
    return false;
  }
  std::string hidden_key  = pending_delete_key(state);
  if (hidden_key.size() > 1024) {
    return false;
  }
  std::string hidden_path = object_request_path(state, hidden_key);
  state.local_cache->create_pending_delete(
      hidden_key, restore_key, replacement_etag);

  bool readers_gone = false;
  OpenHandleIdentityLocks reader_locks;
  try {
    reader_locks = lock_open_handle_identities(
        state, path, "native unlink");
    readers_gone = reader_locks.handles.empty();
    for (OpenHandle* handle : reader_locks.handles) {
      handle->object_path.reserve(hidden_path.size());
      handle->key.reserve(hidden_key.size());
    }
    if (!readers_gone) {
      std::lock_guard guard(state.open_files_mutex);
      const auto opened = state.open_files.find(path);
      readers_gone = opened == state.open_files.end();
      if (!reader_snapshot_matches(
              readers_gone ? std::span<OpenHandle* const>{} : opened->second.handles,
              reader_locks.handles)) {
        throw std::logic_error(
            "reader registry changed during native unlink");
      }
      const bool inserted = readers_gone || state.pending_deletes.try_emplace(
          hidden_path, State::PendingDelete{
              .key = hidden_key,
              .restore_key = std::string(restore_key),
              .replacement_etag = std::string(replacement_etag),
              .rollback = !restore_key.empty(),
              .deleting = true,
          }).second;
      if (!inserted) {
        throw std::logic_error("pending-delete key collision");
      }
    }
    if (readers_gone) {
      state.local_cache->finish_pending_delete(hidden_key);
      return false;
    }
  } catch (...) {
    state.local_cache->finish_pending_delete(hidden_key);
    throw;
  }

  bool renamed = false;
  try {
    renamed = try_native_rename_object(
        state, key, metadata.etag, hidden_path, true);
  } catch (...) {
    const std::exception_ptr failure = std::current_exception();
    bool known_absent = false;
    try {
      const ObjectMetadata hidden = head_object(state, hidden_path);
      renamed = hidden.size == metadata.size &&
          (metadata.etag.empty() || hidden.etag == metadata.etag);
    } catch (const std::system_error& error) {
      known_absent = error.code().value() == ENOENT;
    } catch (...) {
    }
    if (!renamed) {
      reader_locks.identities.clear();
      if (known_absent) {
        state.local_cache->finish_pending_delete(hidden_key);
        std::lock_guard guard(state.open_files_mutex);
        state.pending_deletes.erase(hidden_path);
      } else {
        {
          std::lock_guard guard(state.open_files_mutex);
          const auto pending = state.pending_deletes.find(hidden_path);
          if (pending != state.pending_deletes.end()) {
            pending->second.deleting = false;
          }
        }
        finish_pending_delete(state, hidden_path);
      }
      std::rethrow_exception(failure);
    }
  }
  if (!renamed) {
    state.local_cache->finish_pending_delete(hidden_key);
    std::lock_guard guard(state.open_files_mutex);
    state.pending_deletes.erase(hidden_path);
    return false;
  }

  {
    std::lock_guard guard(state.open_files_mutex);
    const auto opened = state.open_files.find(path);
    readers_gone = opened == state.open_files.end();
    if (!reader_snapshot_matches(
            readers_gone ? std::span<OpenHandle* const>{} : opened->second.handles,
            reader_locks.handles)) {
      throw std::logic_error("reader registry changed during native rename");
    }
    if (!readers_gone) {
      for (OpenHandle* handle : opened->second.handles) {
        handle->object_path.assign(hidden_path);
        handle->key.assign(hidden_key);
      }
      auto node = state.open_files.extract(opened);
      node.key().assign(hidden_path);
      state.open_files.insert(std::move(node));
    }
    if (restore_key.empty()) {
      const auto pending = state.pending_deletes.find(hidden_path);
      if (pending != state.pending_deletes.end()) {
        pending->second.deleting = false;
      }
    }
  }

  if (!state.local_cache->remove(key, true)) {
    fprintf(stderr,
            "warning: native unlink preserved remote readers but could not "
            "detach their local cache generation: path=%.*s\n",
            int(path.size()), path.data());
  }
  if (hidden_key_result != nullptr) {
    *hidden_key_result = std::move(hidden_key);
  }
  if (hidden_path_result != nullptr) {
    *hidden_path_result = hidden_path;
  }
  if (readers_gone && restore_key.empty()) {
    reader_locks.identities.clear();
    finish_pending_delete(state, hidden_path);
  }
  return true;
}

struct AsyncReaderLocks {
  OpenHandleIdentityLocks locks;
  FuseReactor* reactor = nullptr;
  AsyncIoRequest wait;
  uint64_t notification = 0;
  size_t index = 0;
  void (*complete)(void*, std::exception_ptr) noexcept = nullptr;
  void* context = nullptr;

  void snapshot(State& state, FuseReactor& owner, std::string_view path) {
    reactor = &owner;
    std::lock_guard guard(state.open_files_mutex);
    const auto opened = state.open_files.find(path);
    if (opened == state.open_files.end()) return;
    if (opened->second.writer || opened->second.handles.size() != opened->second.readers) {
      throw std::logic_error("invalid reader registry during namespace mutation");
    }
    locks.handles.reserve(opened->second.handles.size());
    locks.pins.reserve(opened->second.handles.size());
    for (OpenHandle* handle : opened->second.handles) {
      if (handle == nullptr || handle->writable) throw std::logic_error("invalid mutation reader");
      try {
        locks.pins.push_back(std::make_unique<OpenRequestGuard>(*handle));
        locks.handles.push_back(handle);
      } catch (const std::system_error& error) {
        if (error.code().value() != EBADF) throw;
      }
    }
    locks.identities.reserve(locks.handles.size());
    for (OpenHandle* handle : locks.handles) {
      locks.identities.emplace_back(handle->identity_mutex, std::defer_lock);
    }
  }

  bool acquire() {
    for (; index < locks.identities.size(); ++index) {
      auto& lock = locks.identities[index];
      if (lock.try_lock()) continue;
      ReactorSharedMutex& mutex = *lock.mutex();
      const int fd = mutex.begin_async_wait();
      if (lock.try_lock()) {
        mutex.end_async_wait();
        continue;
      }
      wait = {};
      wait.kind = AsyncIoRequest::READ;
      wait.fd = fd;
      wait.data = &notification;
      wait.length = sizeof(notification);
      wait.timeout_ms = 0;
      wait.complete = available;
      wait.context = this;
      if (reactor->submit(wait)) return false;
      const int error = errno;
      mutex.end_async_wait();
      throw std::system_error(error, std::generic_category(), "submit mutation identity wait");
    }
    return true;
  }

  static void available(void* context, ssize_t result) noexcept {
    auto* task = static_cast<AsyncReaderLocks*>(context);
    task->locks.identities[task->index].mutex()->end_async_wait();
    try {
      if (result < 0) throw std::system_error(-int(result), std::generic_category(), "mutation identity wait");
      if (!task->acquire()) return;
    } catch (...) {
      task->complete(task->context, std::current_exception());
      return;
    }
    task->complete(task->context, {});
  }
};

class AsyncHideReaders {
 public:
  using Complete = void (*)(void*, bool, std::exception_ptr) noexcept;
  AsyncHideReaders(State& state, FuseReactor& reactor, std::string key, std::string path,
                     std::string restore, std::string replacement, Complete complete, void* context)
      : state_(state), reactor_(reactor), key_(std::move(key)), path_(std::move(path)),
        restore_(std::move(restore)), replacement_(std::move(replacement)),
        complete_(complete), context_(context) {}

  bool start() noexcept {
    task_ = {begin, nullptr, this};
    return reactor_.post(&task_);
  }

  std::string take_hidden_key() noexcept { return std::move(hidden_key_); }
  std::string take_hidden_path() noexcept { return std::move(hidden_path_); }

 private:
  State& state_;
  FuseReactor& reactor_;
  std::string key_, path_, restore_, replacement_, hidden_key_, hidden_path_;
  ObjectMetadata metadata_;
  AsyncReaderLocks readers_;
  std::unique_ptr<AsyncSignedS3Request> head_;
  std::unique_ptr<AsyncNativeRename> rename_;
  std::unique_ptr<AsyncPendingDelete> cleanup_;
  AsyncCacheRetirement retirement_;
  AsyncLocalCacheWork local_;
  FuseReactor::ReactorTask task_;
  Complete complete_;
  void* context_;
  std::exception_ptr failure_;
  bool recovering_ = false;
  bool marker_ = false;
  bool pending_ = false;
  bool hidden_ = false;
  bool readers_gone_ = false;
  bool cache_retry_ = false;
  bool cache_removed_ = false;

  void finish(bool hidden, std::exception_ptr error = {}) noexcept {
    readers_.locks.identities.clear();
    readers_.locks.pins.clear();
    complete_(context_, hidden, std::move(error));
  }

  void finish_discard_marker() noexcept {
    if (pending_) {
      std::lock_guard guard(state_.open_files_mutex);
      state_.pending_deletes.erase(hidden_path_);
      pending_ = false;
    }
    finish(false, failure_);
  }

  static void remove_marker(void* value) {
    auto* task = static_cast<AsyncHideReaders*>(value);
    task->state_.local_cache->finish_pending_delete(task->hidden_key_);
  }

  static void marker_removed(void* value, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncHideReaders*>(value);
    if (error) {
      fprintf(stderr, "warning: native unlink marker cleanup could not run: path=%s\n",
              task->hidden_path_.c_str());
      if (!task->failure_) task->failure_ = std::move(error);
    }
    task->marker_ = false;
    task->finish_discard_marker();
  }

  void discard_marker(std::exception_ptr error = {}) noexcept {
    failure_ = std::move(error);
    if (!marker_) { finish_discard_marker(); return; }
    try {
      local_.start(state_, reactor_, remove_marker, marker_removed, this);
    } catch (...) {
      // Keep the marker for mount-time recovery if local cleanup cannot be
      // admitted; no filesystem operation is allowed in this owner fallback.
      marker_removed(this, std::current_exception());
    }
  }

  static void begin(void* context) noexcept {
    auto* task = static_cast<AsyncHideReaders*>(context);
    try {
      bool any = false;
      {
        std::lock_guard guard(task->state_.open_files_mutex);
        const auto opened = task->state_.open_files.find(task->path_);
        any = opened != task->state_.open_files.end() && opened->second.readers != 0;
      }
      if (!any) { task->finish(false); return; }
      task->head(task->path_);
    } catch (...) { task->finish(false, std::current_exception()); }
  }

  void head(std::string_view path) {
    AsyncHttpRequest args;
    args.method = "HEAD";
    args.path.assign(path.data(), path.size());
    head_ = std::make_unique<AsyncSignedS3Request>(state_, reactor_, std::move(args), received, this);
    if (!head_->start()) throw std::system_error(errno, std::generic_category(), "submit native unlink HEAD");
  }

  static void received(void* context, Response&& response, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncHideReaders*>(context);
    task->head_.reset();
    try {
      if (task->recovering_) {
        if (!error && response.status == 404) {
          task->discard_marker(task->failure_);
          return;
        }
        if (!error) {
          const ObjectMetadata metadata = decode_head_response(response);
          if (metadata.size == task->metadata_.size &&
              (task->metadata_.etag.empty() || metadata.etag == task->metadata_.etag)) {
            task->publish();
            return;
          }
        }
        task->recover_pending();
        return;
      }
      if (error) std::rethrow_exception(error);
      task->metadata_ = decode_head_response(response);
      task->hidden_key_ = pending_delete_key(task->state_);
      if (task->hidden_key_.size() > 1024) { task->finish(false); return; }
      task->hidden_path_ = object_request_path(task->state_, task->hidden_key_);
      task->readers_.complete = locked;
      task->readers_.context = task;
      task->readers_.snapshot(task->state_, task->reactor_, task->path_);
      if (task->readers_.acquire()) task->rename();
    } catch (...) { task->finish(false, std::current_exception()); }
  }

  static void locked(void* context, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncHideReaders*>(context);
    try {
      if (error) std::rethrow_exception(error);
      task->rename();
    } catch (...) { task->finish(false, std::current_exception()); }
  }

  void rename() {
    if (readers_.locks.handles.empty()) { finish(false); return; }
    for (OpenHandle* handle : readers_.locks.handles) {
      handle->object_path.reserve(hidden_path_.size());
      handle->key.reserve(hidden_key_.size());
    }
    local_.start(state_, reactor_, create_marker, marker_created, this);
  }

  static void create_marker(void* value) {
    auto* task = static_cast<AsyncHideReaders*>(value);
    task->state_.local_cache->create_pending_delete(
        task->hidden_key_, task->restore_, task->replacement_);
  }

  static void marker_created(void* value, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncHideReaders*>(value);
    if (error) { task->finish(false, std::move(error)); return; }
    task->marker_ = true;
    task->rename_marked();
  }

  void rename_marked() noexcept {
    try {
      bool any;
      {
        std::lock_guard guard(state_.open_files_mutex);
        const auto opened = state_.open_files.find(path_);
        any = opened != state_.open_files.end();
        if (!reader_snapshot_matches(any ? std::span<OpenHandle* const>(opened->second.handles) :
                                           std::span<OpenHandle* const>{}, readers_.locks.handles)) {
          throw std::logic_error("reader registry changed during native unlink");
        }
        if (any) {
          pending_ = state_.pending_deletes.try_emplace(hidden_path_, State::PendingDelete{
              .key = hidden_key_, .restore_key = restore_, .replacement_etag = replacement_,
              .rollback = !restore_.empty(), .deleting = true}).second;
          if (!pending_) throw std::logic_error("pending-delete key collision");
        }
      }
      if (!any) { discard_marker(); return; }
      rename_ = std::make_unique<AsyncNativeRename>(
          state_, reactor_, key_, metadata_.etag, hidden_path_, true, renamed, this);
      if (!rename_->start()) throw std::system_error(errno, std::generic_category(), "submit native unlink");
    } catch (...) {
      discard_marker(std::current_exception());
    }
  }

  static void renamed(void* context, bool renamed, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncHideReaders*>(context);
    task->rename_.reset();
    try {
      if (error) {
        task->failure_ = error;
        task->recovering_ = true;
        try { task->head(task->hidden_path_); }
        catch (...) { task->recover_pending(); }
      } else if (!renamed) {
        task->discard_marker();
      } else task->publish();
    } catch (...) { task->finish(false, std::current_exception()); }
  }

  void recover_pending() {
    readers_.locks.identities.clear();
    {
      std::lock_guard guard(state_.open_files_mutex);
      const auto pending = state_.pending_deletes.find(hidden_path_);
      if (pending != state_.pending_deletes.end()) pending->second.deleting = false;
    }
    cleanup_ = std::make_unique<AsyncPendingDelete>(state_, reactor_, hidden_path_, cleaned, this);
    if (!cleanup_->start()) finish(hidden_, failure_ ? failure_ : std::make_exception_ptr(
        std::system_error(errno, std::generic_category(), "submit pending unlink cleanup")));
  }

  static void cleaned(void* context, std::exception_ptr) noexcept {
    auto* task = static_cast<AsyncHideReaders*>(context);
    task->cleanup_.reset();
    // Pending-delete failures retain the marker for explicit recovery; the
    // original namespace operation keeps its established best-effort result.
    task->finish(task->hidden_, task->failure_);
  }

  static void cache_retired(void* context, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncHideReaders*>(context);
    try {
      if (error) std::rethrow_exception(error);
      task->finish_cache();
    } catch (...) { task->finish(task->hidden_, std::current_exception()); }
  }

  void finish_cache() {
    retirement_.complete = cache_retired;
    retirement_.context = this;
    if (cache_retry_ && !retirement_.ready(state_, reactor_, key_, nullptr, true)) return;
    cache_retry_ = false;
    local_.start(state_, reactor_, remove_cache, cache_removed, this);
  }

  static void remove_cache(void* value) {
    auto* task = static_cast<AsyncHideReaders*>(value);
    task->cache_removed_ = task->state_.local_cache->remove(
        task->key_, true, &task->cache_retry_, false);
  }

  static void cache_removed(void* value, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncHideReaders*>(value);
    try {
      if (error) std::rethrow_exception(error);
      if (task->cache_retry_) { task->finish_cache(); return; }
      if (!task->cache_removed_) {
        fprintf(stderr, "warning: native unlink could not detach local cache generation: path=%s\n",
                task->path_.c_str());
      }
      if (task->readers_gone_ && task->restore_.empty()) task->recover_pending();
      else task->finish(true);
    } catch (...) {
      task->finish(task->hidden_, std::current_exception());
    }
  }

  void publish() {
    bool gone;
    {
      std::lock_guard guard(state_.open_files_mutex);
      const auto opened = state_.open_files.find(path_);
      gone = opened == state_.open_files.end();
      if (!reader_snapshot_matches(gone ? std::span<OpenHandle* const>{} :
                                         std::span<OpenHandle* const>(opened->second.handles), readers_.locks.handles)) {
        throw std::logic_error("reader registry changed during native rename");
      }
      if (!gone) {
        for (OpenHandle* handle : opened->second.handles) {
          if (std::find(readers_.locks.handles.begin(), readers_.locks.handles.end(), handle) ==
              readers_.locks.handles.end()) continue;
          handle->object_path.assign(hidden_path_);
          handle->key.assign(hidden_key_);
        }
        auto node = state_.open_files.extract(opened);
        node.key().assign(hidden_path_);
        state_.open_files.insert(std::move(node));
      }
      if (restore_.empty()) {
        const auto pending = state_.pending_deletes.find(hidden_path_);
        if (pending != state_.pending_deletes.end()) pending->second.deleting = false;
      }
    }
    hidden_ = true;
    readers_gone_ = gone;
    failure_ = {};
    finish_cache();
  }
};

void remove_item(State& state, InodeBase& item) {
  if (detach_cached_item(state, item)) {
    sweep_retired_items(state);
  }
}

struct AsyncUnlink {
  State& state;
  FuseReactor& reactor;
  fuse_req_t request;
  fuse_ino_t parent;
  std::string name, key, path;
  InodePin directory, item;
  std::unique_lock<IoMutex> directory_lock;
  std::unique_ptr<PathMutationGuard> mutation;
  std::unique_ptr<AsyncHideReaders> hide;
  std::unique_ptr<AsyncSignedS3Request> remote;
  DirectoryContinuation listing;
  AsyncCacheRetirement retirement;
  AsyncLocalCacheWork local;
  AsyncIoRequest wait;
  uint64_t notification = 0;
  OpenHandle* writer = nullptr;
  std::unique_ptr<OpenRequestGuard> writer_pin;
  unsigned phase = 0;
  bool waiting_writer = false;
  bool hidden = false;
  bool preserve_readers = false;
  bool writer_marker = false;
  bool marker_created = false;
  bool writer_reserved = false;
  bool delete_committed = false;
  bool cache_retry = false;
  bool cache_preserved = false;
  bool cache_qualified = false;
  std::vector<std::shared_ptr<CacheEntry>> reader_entries;
  std::exception_ptr failure;

  AsyncUnlink(State& s, FuseReactor& r, fuse_req_t req, fuse_ino_t p, const char* n)
      : state(s), reactor(r), request(req), parent(p), name(n) {
    InodeBase& base = inode_item(state, parent);
    if (!base.directory()) throw std::system_error(ENOTDIR, std::generic_category(), "unlink");
    retain_inode_count(base.open_count, "unlink parent pin");
    directory = InodePin(state, base);
    directory_lock = std::unique_lock<IoMutex>(base.dir_children().mutation_mutex, std::defer_lock);
    listing.complete = resumed;
    listing.context = this;
    retirement.complete = resumed;
    retirement.context = this;
  }

  ~AsyncUnlink() {
    if (writer_reserved) {
      { std::lock_guard guard(writer->mutex); writer->write_in_progress = false; }
      notify_handle(*writer);
    }
  }

  void finish_failure() noexcept {
    std::unique_ptr<AsyncUnlink> owner(this);
    if (writer_marker && !delete_committed) {
      std::lock_guard guard(state.open_files_mutex);
      state.pending_deletes.erase(path);
    } else if (writer_marker) {
      std::lock_guard guard(state.open_files_mutex);
      const auto pending = state.pending_deletes.find(path);
      if (pending != state.pending_deletes.end()) pending->second.deleting = false;
    }
    invalidate_directory(state, parent);
    try { std::rethrow_exception(failure); }
    catch (...) { reply_callback_error(request); }
  }

  static void remove_marker(void* value) {
    auto* task = static_cast<AsyncUnlink*>(value);
    if (task->marker_created && !task->delete_committed) {
      task->state.local_cache->finish_pending_delete(task->key);
    }
    task->reader_entries.clear();
  }

  static void failed_marker_removed(void* value, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncUnlink*>(value);
    if (error) {
      fprintf(stderr, "warning: failed unlink marker retained for recovery: path=%s\n",
              task->path.c_str());
    }
    task->marker_created = false;
    task->finish_failure();
  }

  void fail(std::exception_ptr error) noexcept {
    failure = std::move(error);
    if ((marker_created && !delete_committed) || !reader_entries.empty()) {
      try {
        local.start(state, reactor, remove_marker, failed_marker_removed, this);
      } catch (...) { failed_marker_removed(this, std::current_exception()); }
      return;
    }
    finish_failure();
  }

  static void create_marker(void* value) {
    auto* task = static_cast<AsyncUnlink*>(value);
    if (task->writer->cache_entry->dirty()) {
      task->state.local_cache->create_pending_delete(task->key);
      task->marker_created = true;
    }
  }

  static void discard_write(void* value) {
    auto* task = static_cast<AsyncUnlink*>(value);
    task->writer->cache_entry->discard_write();
  }

  static void remove_cache(void* value) {
    auto* task = static_cast<AsyncUnlink*>(value);
    if (!task->cache_qualified) {
      task->preserve_readers = cached_readers_fully_clean(std::move(task->reader_entries));
      task->cache_qualified = true;
    }
    task->cache_preserved = task->state.local_cache->remove(
        task->key, task->preserve_readers, &task->cache_retry, false);
  }

  static void resumed(void* context, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncUnlink*>(context);
    if (error) task->fail(std::move(error));
    else task->advance();
  }

  static void unlocked(void* context, ssize_t result) noexcept {
    auto* task = static_cast<AsyncUnlink*>(context);
    if (task->waiting_writer) task->writer->reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
    else task->directory_lock.mutex()->end_async_wait();
    if (result < 0) task->fail(std::make_exception_ptr(
        std::system_error(-int(result), std::generic_category(), "unlink state wait")));
    else task->advance();
  }

  void wait_on(int fd) {
    wait = {};
    wait.kind = AsyncIoRequest::READ;
    wait.fd = fd;
    wait.data = &notification;
    wait.length = sizeof(notification);
    wait.timeout_ms = 0;
    wait.complete = unlocked;
    wait.context = this;
    if (!reactor.submit(wait)) {
      const int error = errno;
      if (waiting_writer) writer->reactor_waiters.fetch_sub(1, std::memory_order_acq_rel);
      else directory_lock.mutex()->end_async_wait();
      throw std::system_error(error, std::generic_category(), "submit unlink state wait");
    }
  }

  static void hidden_readers(void* context, bool hidden, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncUnlink*>(context);
    task->hidden = hidden;
    task->hide.reset();
    resumed(context, std::move(error));
  }

  static void received(void* context, Response&& response, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncUnlink*>(context);
    task->remote.reset();
    try {
      if (error) std::rethrow_exception(error);
      if (response.status != 200 && response.status != 204 && response.status != 404) {
        throw_s3_response(response, "DeleteObject");
      }
      task->delete_committed = true;
    } catch (...) { task->fail(std::current_exception()); return; }
    task->advance();
  }

  void advance() noexcept {
    std::unique_ptr<AsyncUnlink> owner(this);
    try {
      for (;;) switch (phase) {
        case 0: {
          if (!directory_lock.try_lock()) {
            const int fd = directory_lock.mutex()->begin_async_wait();
            if (!directory_lock.try_lock()) {
              wait_on(fd);
              owner.release();
              return;
            }
            directory_lock.mutex()->end_async_wait();
          }
          phase = 1;
          if (await_directory_refresh(state, reactor, parent, listing)) {
            owner.release(); return;
          }
          break;
        }
        case 1:
          item = pin_cached_child(state, *directory, name);
          if (!item) throw std::system_error(ENOENT, std::generic_category(), "unlink");
          if (item->directory()) throw std::system_error(EISDIR, std::generic_category(), "unlink");
          key = item_key(state, *item);
          path = object_request_path(state, key);
          mutation = std::make_unique<PathMutationGuard>(state, path, false,
              std::string_view{}, false, "unlink", true, true);
          phase = 2;
          if (state.local_cache) {
            hide = std::make_unique<AsyncHideReaders>(state, reactor, key, path,
                std::string{}, std::string{}, hidden_readers, this);
            if (!hide->start()) throw std::system_error(errno, std::generic_category(), "submit unlink reader preservation");
            owner.release(); return;
          }
          break;
        case 2: {
          if (hidden) { phase = 5; break; }
          {
            std::lock_guard guard(state.open_files_mutex);
            const auto opened = state.open_files.find(path);
            if (opened != state.open_files.end()) {
              if (state.local_cache && opened->second.readers != 0) {
                reader_entries = cached_reader_entries(opened->second.handles);
              }
              if (opened->second.writer) {
                for (OpenHandle* handle : opened->second.handles) {
                  if (handle == nullptr || !handle->writable) continue;
                  try { writer_pin = std::make_unique<OpenRequestGuard>(*handle); writer = handle; }
                  catch (const std::system_error& error) { if (error.code().value() != EBADF) throw; }
                  break;
                }
              }
            }
          }
          phase = 3;
          break;
        }
        case 3:
          if (writer != nullptr) {
            std::unique_lock guard(writer->mutex);
            if (writer->write_in_progress || writer->write_state == WRITE_SEALING) {
              writer->reactor_waiters.fetch_add(1, std::memory_order_acq_rel);
              waiting_writer = true;
              guard.unlock();
              wait_on(writer->state_event.get());
              owner.release(); return;
            }
            writer->write_in_progress = true;
            writer_reserved = true;
            guard.unlock();
            if (state.local_cache && writer->cache_entry) {
              phase = 30;
              local.start(state, reactor, create_marker, resumed, this);
              owner.release(); return;
            }
          }
          phase = 31;
          break;
        case 30: {
          if (marker_created) {
            std::lock_guard open_guard(state.open_files_mutex);
            writer_marker = state.pending_deletes.try_emplace(path, State::PendingDelete{
                .key = key, .restore_key = {}, .replacement_etag = {}, .rollback = false, .deleting = true}).second;
            if (!writer_marker) throw std::logic_error("pending writer unlink collision");
          }
          phase = 31;
          break;
        }
        case 31:
          {
            AsyncHttpRequest args;
            args.method = "DELETE";
            args.path = path;
            remote = std::make_unique<AsyncSignedS3Request>(state, reactor, std::move(args), received, this);
            phase = 4;
            if (!remote->start()) throw std::system_error(errno, std::generic_category(), "submit unlink DeleteObject");
            owner.release(); return;
          }
        case 4:
          if (writer != nullptr) {
            {
              std::lock_guard guard(writer->mutex);
              writer->unlinked.store(true, std::memory_order_release);
              writer->stale.store(true, std::memory_order_release);
              if (writer->write_state != WRITE_SEALED) fail_write(*writer, ESTALE);
            }
            if (writer->cache_entry) {
              phase = 40;
              local.start(state, reactor, discard_write, resumed, this);
              owner.release(); return;
            }
          }
          phase = 40;
          break;
        case 40:
          if (writer_marker) {
            std::lock_guard guard(state.open_files_mutex);
            const auto pending = state.pending_deletes.find(path);
            if (pending != state.pending_deletes.end()) pending->second.deleting = false;
            writer_marker = false;
          }
          phase = 5;
          break;
        case 5: {
          cache_preserved = hidden;
          if (!hidden && state.local_cache) {
            if (cache_retry && !retirement.ready(state, reactor, key, nullptr, preserve_readers)) {
              owner.release(); return;
            }
            cache_retry = false;
            phase = 50;
            local.start(state, reactor, remove_cache, resumed, this);
            owner.release(); return;
          }
          phase = 50;
          break;
        }
        case 50: {
          if (cache_retry) { phase = 5; break; }
          if (!cache_preserved) {
            std::lock_guard guard(state.open_files_mutex);
            const auto opened = state.open_files.find(path);
            if (opened != state.open_files.end()) {
              for (OpenHandle* handle : opened->second.handles) {
                if (handle != nullptr && !handle->writable) handle->stale.store(true, std::memory_order_release);
              }
            }
          }
          if (!item->detached()) remove_item(state, *item);
          fuse_reply_err(request, 0);
          return;
        }
        default: abort();
      }
    } catch (...) {
      owner.release();
      fail(std::current_exception());
    }
  }
};

void ngs3fs_unlink(fuse_req_t request, fuse_ino_t parent,
                   const char* name) {
  if (name == nullptr || !valid_fuse_component(name)) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  State& state = state_from(request);
  try {
    if (FuseReactor* reactor = current_fuse_reactor()) {
      auto task = std::make_unique<AsyncUnlink>(state, *reactor, request, parent, name);
      task.release()->advance();
      return;
    }
    InodeBase& directory = inode_item(state, parent);
    if (!directory.directory()) {
      throw std::system_error(ENOTDIR, std::generic_category(), "unlink");
    }
    std::lock_guard directory_guard(
        directory.dir_children().mutation_mutex);
    refresh_directory(state, parent);
    InodePin item = pin_cached_child(
        state, directory, name);
    if (!item) {
      throw std::system_error(ENOENT, std::generic_category(), "unlink");
    }
    if (item->directory()) {
      throw std::system_error(EISDIR, std::generic_category(), "unlink");
    }
    const std::string key = item_key(state, *item);
    const std::string path = object_request_path(state, key);
    PathMutationGuard mutation(
        state, path, false, {}, false, "unlink",
        true, true);
    const bool hidden = state.local_cache &&
        hide_open_readers(state, key, path);
    bool preserve_readers = false;
    OpenHandle* writer = nullptr;
    std::unique_ptr<OpenRequestGuard> writer_pin;
    if (!hidden) {
      std::lock_guard open_guard(state.open_files_mutex);
      const auto opened = state.open_files.find(path);
      if (opened != state.open_files.end()) {
        if (state.local_cache && opened->second.readers != 0) {
          preserve_readers = std::all_of(
              opened->second.handles.begin(), opened->second.handles.end(),
              [](const OpenHandle* handle) {
                return handle != nullptr && !handle->writable &&
                    handle->cache_entry && handle->cache_entry->fully_clean();
              });
        }
        if (opened->second.writer) {
          const auto found = std::find_if(
              opened->second.handles.begin(), opened->second.handles.end(),
              [](const OpenHandle* handle) {
                return handle != nullptr && handle->writable;
              });
          if (found == opened->second.handles.end()) {
            throw std::logic_error("writer registry has no handle");
          }
          writer = *found;
          writer_pin = std::make_unique<OpenRequestGuard>(*writer);
        }
      }
    }
    std::shared_ptr<CacheEntry> discarded_write;
    bool writer_marker = false;
    if (writer != nullptr && state.local_cache && writer->cache_entry &&
        writer->cache_entry->dirty()) {
      state.local_cache->create_pending_delete(key);
      try {
        std::lock_guard open_guard(state.open_files_mutex);
        const auto inserted = state.pending_deletes.try_emplace(
            path, State::PendingDelete{
                .key = key,
                .restore_key = {},
                .replacement_etag = {},
                .rollback = false,
                .deleting = true,
            }).second;
        if (!inserted) {
          throw std::logic_error("pending writer unlink collision");
        }
        writer_marker = true;
      } catch (...) {
        state.local_cache->finish_pending_delete(key);
        throw;
      }
    }
    try {
      if (writer != nullptr) {
        std::unique_lock writer_guard(writer->mutex);
        while (writer->write_in_progress ||
               writer->write_state == WRITE_SEALING) {
          wait_handle(*writer, writer_guard);
        }
        delete_object(state, key, {});
        writer->unlinked.store(true, std::memory_order_release);
        writer->stale.store(true, std::memory_order_release);
        if (writer->write_state != WRITE_SEALED) {
          fail_write(*writer, ESTALE);
        }
        discarded_write = writer->cache_entry;
      } else if (!hidden) {
        delete_object(state, key, {});
      }
    } catch (...) {
      if (writer_marker) {
        state.local_cache->finish_pending_delete(key);
        std::lock_guard open_guard(state.open_files_mutex);
        state.pending_deletes.erase(path);
      }
      throw;
    }
    if (discarded_write) {
      discarded_write->discard_write();
    }
    if (writer_marker) {
      std::lock_guard open_guard(state.open_files_mutex);
      const auto pending = state.pending_deletes.find(path);
      if (pending != state.pending_deletes.end()) {
        pending->second.deleting = false;
      }
    }
    const bool preserved = hidden || (state.local_cache &&
        state.local_cache->remove(key, preserve_readers));
    if (!preserved) {
      std::lock_guard open_guard(state.open_files_mutex);
      const auto opened = state.open_files.find(path);
      if (opened != state.open_files.end()) {
        for (OpenHandle* handle : opened->second.handles) {
          if (handle != nullptr) {
            if (handle->writable) {
              if (handle != writer) {
                throw std::logic_error(
                    "multiple writers registered for one object");
              }
            } else {
              handle->stale.store(true, std::memory_order_release);
            }
          }
        }
      }
    }
    if (!item->detached()) {
      remove_item(state, *item);
    }
    fuse_reply_err(request, 0);
  } catch (...) {
    invalidate_directory(state, parent);
    reply_callback_error(request);
  }
}

void put_empty_object(State& state, std::string_view path,
                      bool no_replace) {
  std::vector<Header> headers;
  if (no_replace) {
    headers.push_back(Header{"if-none-match", "*"});
  }
  HeaderList authorization = authorization_headers(
      state, "PUT", path, headers, kEmptyPayloadSha256);
  headers.insert(headers.end(),
                 std::make_move_iterator(authorization.begin()),
                 std::make_move_iterator(authorization.end()));
  HttpPool::Lease client = state.http->acquire();
  client->begin_upload("PUT", path, headers, 0);
  const Response response = client->finish_upload();
  if (response.status == 412) {
    throw std::system_error(EEXIST, std::generic_category(),
                            "directory marker already exists");
  }
  require_s3_success(response, "PutObject directory marker");
}

struct AsyncDirectoryAction {
  enum Kind { CREATE, MKDIR, RMDIR };
  State& state;
  FuseReactor& reactor;
  fuse_req_t request;
  fuse_ino_t parent;
  std::string name;
  fuse_file_info file{};
  Kind kind;
  unsigned phase = 0;
  InodePin directory;
  InodePin item;
  DirectoryGuards locks;
  size_t lock_index = 0;
  IoMutex* waiting = nullptr;
  uint64_t notification = 0;
  AsyncIoRequest wait;
  DirectoryContinuation listing;
  AsyncCacheRetirement retirement;
  std::unique_ptr<AsyncSignedS3Request> remote;
  Response response;

  AsyncDirectoryAction(State& s, FuseReactor& r, fuse_req_t req,
                          fuse_ino_t p, const char* n, fuse_file_info* info, Kind k)
      : state(s), reactor(r), request(req), parent(p), name(n), kind(k) {
    InodeBase& base = inode_item(state, parent);
    if (!base.directory()) {
      throw std::system_error(ENOTDIR, std::generic_category(), "directory operation");
    }
    retain_inode_count(base.open_count, "directory mutation pin");
    directory = InodePin(state, base);
    if (info != nullptr) file = *info;
    listing.complete = listed;
    listing.context = this;
    retirement.complete = listed;
    retirement.context = this;
  }

  void fail(std::exception_ptr error) noexcept {
    std::unique_ptr<AsyncDirectoryAction> owner(this);
    invalidate_directory(state, parent);
    try {
      std::rethrow_exception(error);
    } catch (...) {
      reply_callback_error(request);
    }
  }

  static void listed(void* context, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncDirectoryAction*>(context);
    if (error) task->fail(std::move(error));
    else task->advance();
  }

  static void unlocked(void* context, ssize_t result) noexcept {
    auto* task = static_cast<AsyncDirectoryAction*>(context);
    task->waiting->end_async_wait();
    task->waiting = nullptr;
    try {
      if (result < 0) {
        throw std::system_error(-int(result), std::generic_category(), "wait for directory mutation");
      }
      task->advance();
    } catch (...) {
      task->fail(std::current_exception());
    }
  }

  static void received(void* context, Response&& response, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncDirectoryAction*>(context);
    task->response = std::move(response);
    task->remote.reset();
    if (error) task->fail(std::move(error));
    else task->advance();
  }

  void prepare_locks() {
    locks.directories.push_back(&directory->dir_children());
    if (kind == RMDIR) {
      item = pin_cached_child(state, *directory, name);
      if (!item) throw std::system_error(ENOENT, std::generic_category(), "rmdir");
      if (!item->directory()) throw std::system_error(ENOTDIR, std::generic_category(), "rmdir");
      locks.directories.push_back(&item->dir_children());
    }
    std::sort(locks.directories.begin(), locks.directories.end(), std::less<Directory*>());
    locks.guards.reserve(locks.directories.size());
    for (Directory* children : locks.directories) {
      locks.guards.emplace_back(children->mutation_mutex, std::defer_lock);
    }
  }

  bool acquire_locks() {
    for (; lock_index < locks.guards.size(); ++lock_index) {
      auto& lock = locks.guards[lock_index];
      if (lock.try_lock()) continue;
      IoMutex& mutex = *lock.mutex();
      const int fd = mutex.begin_async_wait();
      if (lock.try_lock()) {
        mutex.end_async_wait();
        continue;
      }
      waiting = &mutex;
      wait = {};
      wait.kind = AsyncIoRequest::READ;
      wait.fd = fd;
      wait.data = &notification;
      wait.length = sizeof(notification);
      wait.complete = unlocked;
      wait.context = this;
      wait.timeout_ms = 0;
      if (!reactor.submit(wait)) {
        const int error = errno;
        mutex.end_async_wait();
        waiting = nullptr;
        throw std::system_error(error, std::generic_category(), "submit directory lock wait");
      }
      return false;
    }
    return true;
  }

  void advance() noexcept {
    std::unique_ptr<AsyncDirectoryAction> owner(this);
    try {
      for (;;) {
        switch (phase) {
          case 0:
            phase = 1;
            if (await_directory_refresh(state, reactor, parent, listing)) {
              owner.release();
              return;
            }
            break;
          case 1:
            prepare_locks();
            phase = 2;
            break;
          case 2:
            if (!acquire_locks()) {
              owner.release();
              return;
            }
            phase = 3;
            if (await_directory_refresh(state, reactor, parent, listing)) {
              owner.release();
              return;
            }
            break;
          case 3:
            if (directory->detached()) {
              throw std::system_error(ESTALE, std::generic_category(), "directory mutation");
            }
            if (kind == CREATE) {
              if (pin_cached_child(state, *directory, name)) {
                throw std::system_error(EEXIST, std::generic_category(), "create");
              }
              bool retry = false;
              create_cached_file(request, parent, name.c_str(), &file, &locks.guards.front(), &retry);
              if (retry) { phase = 2; lock_index = 0; break; }
              return;
            }
            phase = 4;
            if (kind == RMDIR) {
              InodePin current = pin_cached_child(state, *directory, name);
              if (current.get() != item.get()) {
                throw std::system_error(ENOENT, std::generic_category(), "rmdir changed entry");
              }
              if (await_directory_refresh(state, reactor, item_inode(item.get()), listing, true)) {
                owner.release();
                return;
              }
            }
            break;
          case 4: {
            AsyncHttpRequest args;
            if (kind == RMDIR) {
              std::shared_lock guard(item->dir_children().mutex);
              if (!item->dir_children().empty()) {
                throw std::system_error(ENOTEMPTY, std::generic_category(), "rmdir");
              }
              args.method = "DELETE";
              args.path = object_request_path(state, item_key(state, *item));
            } else {
              args.method = "PUT";
              args.path = object_request_path(state, item_key(state, *directory) + name + '/');
              args.headers.push_back(Header{"if-none-match", "*"});
            }
            remote = std::make_unique<AsyncSignedS3Request>(
                state, reactor, std::move(args), received, this, kind == MKDIR ? 1 : 4);
            phase = 5;
            if (!remote->start()) {
              throw std::system_error(errno, std::generic_category(), "submit directory mutation");
            }
            owner.release();
            return;
          }
          case 5:
            if (kind == RMDIR) {
              if (response.status != 200 && response.status != 204 && response.status != 404) {
                throw_s3_response(response, "DeleteObject");
              }
              if (!item->detached()) remove_item(state, *item);
              fuse_reply_err(request, 0);
            } else {
              if (response.status == 412) {
                throw std::system_error(EEXIST, std::generic_category(), "directory marker already exists");
              }
              require_s3_success(response, "PutObject directory marker");
              ListedChild child;
              child.name = name;
              child.directory = true;
              // The successful conditional PUT established exclusivity at S3.
              // An overlapping LIST may already have cached this same marker.
              const fuse_ino_t inode = install_item(state, parent, std::move(child), 0, false, false, true);
              fuse_entry_param entry{};
              entry.ino = inode;
              entry.generation = 1;
              entry.attr_timeout = entry.entry_timeout = remaining_directory_timeout(*directory);
              fill_inode_stat(state, inode, inode_item(state, inode), entry.attr);
              if (fuse_reply_entry(request, &entry) != 0) {
                forget_inode(inode, 1);
                sweep_retired_items(state);
              }
            }
            return;
          default: abort();
        }
      }
    } catch (...) {
      invalidate_directory(state, parent);
      reply_callback_error(request);
    }
  }
};

void start_directory_action(fuse_req_t request, fuse_ino_t parent,
                              const char* name, fuse_file_info* file, int kind) {
  try {
    (new AsyncDirectoryAction(state_from(request), *current_fuse_reactor(),
        request, parent, name, file, AsyncDirectoryAction::Kind(kind)))->advance();
  } catch (...) {
    reply_callback_error(request);
  }
}

void ngs3fs_mkdir(fuse_req_t request, fuse_ino_t parent, const char* name,
                  mode_t) {
  if (name == nullptr || !valid_fuse_component(name)) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  if (current_fuse_reactor() != nullptr) {
    start_directory_action(request, parent, name, nullptr, AsyncDirectoryAction::MKDIR);
    return;
  }
  State& state = state_from(request);
  try {
    InodeBase& directory = inode_item(state, parent);
    if (!directory.directory()) {
      throw std::system_error(ENOTDIR, std::generic_category(), "mkdir");
    }
    fuse_ino_t inode;
    double timeout;
    {
      std::lock_guard directory_guard(
          directory.dir_children().mutation_mutex);
      refresh_directory(state, parent);
      const std::string directory_key = item_key(state, directory);
      const std::string key = directory_key + name + '/';
      ListedChild child;
      child.name      = name;
      child.directory = true;
      put_empty_object(state, object_request_path(state, key), true);
      // A concurrent LIST can already contain our successful conditional PUT.
      inode = install_item(
          state, parent, std::move(child), 0, false, false, true);
      timeout = remaining_directory_timeout(directory);
    }
    fuse_entry_param entry{};
    entry.ino           = inode;
    entry.generation    = 1;
    entry.attr_timeout  = timeout;
    entry.entry_timeout = timeout;
    fill_inode_stat(state, inode, inode_item(state, inode), entry.attr);
    if (fuse_reply_entry(request, &entry) != 0) {
      forget_inode(inode, 1);
      sweep_retired_items(state);
    }
  } catch (...) {
    invalidate_directory(state, parent);
    reply_callback_error(request);
  }
}

void ngs3fs_rmdir(fuse_req_t request, fuse_ino_t parent,
                  const char* name) {
  if (name == nullptr || !valid_fuse_component(name)) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  if (current_fuse_reactor() != nullptr) {
    start_directory_action(request, parent, name, nullptr, AsyncDirectoryAction::RMDIR);
    return;
  }
  State& state = state_from(request);
  try {
    refresh_directory(state, parent);
    InodePin item = pin_cached_child(
        state, inode_item(state, parent), name);
    if (!item) {
      throw std::system_error(ENOENT, std::generic_category(), "rmdir");
    }
    const fuse_ino_t inode = item_inode(item.get());
    DirectoryGuards locks = lock_directories(state, {parent, inode});
    refresh_directory(state, parent);
    {
      InodePin current = pin_cached_child(
          state, inode_item(state, parent), name);
      if (!current || current.get() != item.get()) {
        throw std::system_error(ENOENT, std::generic_category(), "rmdir");
      }
    }
    if (!item->directory()) {
      throw std::system_error(ENOTDIR, std::generic_category(), "rmdir");
    }
    refresh_directory(state, inode, true);
    {
      std::shared_lock guard(item->dir_children().mutex);
      if (!item->dir_children().empty()) {
        throw std::system_error(ENOTEMPTY, std::generic_category(), "rmdir");
      }
    }
    const std::string key = item_key(state, *item);
    delete_object(state, key, {});
    if (!item->detached()) {
      remove_item(state, *item);
    }
    fuse_reply_err(request, 0);
  } catch (...) {
    invalidate_directory(state, parent);
    reply_callback_error(request);
  }
}

struct AsyncRename {
  State& state;
  FuseReactor& reactor;
  fuse_req_t request;
  fuse_ino_t parent, new_parent;
  std::string name, new_name, source_key, destination_key, source_path, destination_path;
  std::string hidden_key, hidden_path;
  unsigned flags;
  unsigned phase = 0;
  InodePin source_parent, destination_parent, source, destination;
  DirectoryGuards locks;
  DirectoryPublicationGuard publication;
  size_t lock_index = 0;
  DirectoryContinuation listing;
  AsyncCacheRetirement retirement;
  AsyncLocalCacheWork local;
  AsyncIoRequest wait;
  uint64_t notification = 0;
  std::unique_ptr<PathMutationGuard> mutation;
  AsyncReaderLocks readers;
  bool readers_initialized = false;
  bool destination_hidden = false;
  bool destination_preserved = false;
  bool cache_destination_processed = false;
  bool cache_renamed = false;
  bool cache_retry = false;
  bool destination_snapshot = false;
  bool destination_qualified = false;
  bool preserve_destination = false;
  std::vector<std::shared_ptr<CacheEntry>> destination_entries;
  ObjectMetadata metadata;
  std::unique_ptr<AsyncSignedS3Request> head;
  std::unique_ptr<AsyncHideReaders> hide;
  std::unique_ptr<AsyncRemoteRename> remote;
  std::unique_ptr<AsyncPendingDelete> cleanup;
  std::exception_ptr failure;

  AsyncRename(State& s, FuseReactor& r, fuse_req_t req, fuse_ino_t p, const char* n,
                 fuse_ino_t np, const char* nn, unsigned f)
      : state(s), reactor(r), request(req), parent(p), new_parent(np), name(n), new_name(nn), flags(f) {
    InodeBase& first = inode_item(state, parent);
    InodeBase& second = inode_item(state, new_parent);
    if (!first.directory() || !second.directory()) throw std::system_error(ENOTDIR, std::generic_category(), "rename");
    retain_inode_count(first.open_count, "rename parent pin");
    source_parent = InodePin(state, first);
    retain_inode_count(second.open_count, "rename destination pin");
    destination_parent = InodePin(state, second);
    listing.complete = resumed;
    listing.context = this;
    readers.complete = resumed;
    readers.context = this;
    retirement.complete = resumed;
    retirement.context = this;
  }

  void fail(std::exception_ptr error) noexcept {
    if (destination_hidden && !failure && phase <= 11) {
      failure = error;
      try { cleanup_hidden(); return; }
      catch (...) {}
    }
    std::unique_ptr<AsyncRename> owner(this);
    invalidate_directory(state, parent);
    if (new_parent != parent) invalidate_directory(state, new_parent);
    try { std::rethrow_exception(error); }
    catch (...) { reply_callback_error(request); }
  }

  static void resumed(void* context, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncRename*>(context);
    if (error) task->fail(std::move(error));
    else task->advance();
  }

  static void unlocked(void* context, ssize_t result) noexcept {
    auto* task = static_cast<AsyncRename*>(context);
    task->locks.guards[task->lock_index].mutex()->end_async_wait();
    if (result < 0) task->fail(std::make_exception_ptr(
        std::system_error(-int(result), std::generic_category(), "rename directory wait")));
    else task->advance();
  }

  void prepare_locks() {
    source = pin_cached_child(state, *source_parent, name);
    if (!source) throw std::system_error(ENOENT, std::generic_category(), "rename");
    destination = pin_cached_child(state, *destination_parent, new_name);
    locks.directories = {&source_parent->dir_children(), &destination_parent->dir_children()};
    if (source->directory()) locks.directories.push_back(&source->dir_children());
    if (destination && destination->directory()) locks.directories.push_back(&destination->dir_children());
    std::sort(locks.directories.begin(), locks.directories.end(), std::less<Directory*>());
    locks.directories.erase(std::unique(locks.directories.begin(), locks.directories.end()), locks.directories.end());
    locks.guards.reserve(locks.directories.size());
    for (Directory* directory : locks.directories) {
      locks.guards.emplace_back(directory->mutation_mutex, std::defer_lock);
    }
  }

  bool acquire_locks() {
    for (; lock_index < locks.guards.size(); ++lock_index) {
      auto& lock = locks.guards[lock_index];
      if (lock.try_lock()) continue;
      const int fd = lock.mutex()->begin_async_wait();
      if (lock.try_lock()) { lock.mutex()->end_async_wait(); continue; }
      wait = {};
      wait.kind = AsyncIoRequest::READ;
      wait.fd = fd;
      wait.data = &notification;
      wait.length = sizeof(notification);
      wait.timeout_ms = 0;
      wait.complete = unlocked;
      wait.context = this;
      if (reactor.submit(wait)) return false;
      const int error = errno;
      lock.mutex()->end_async_wait();
      throw std::system_error(error, std::generic_category(), "submit rename directory wait");
    }
    return true;
  }

  void validate() {
    InodePin first = pin_cached_child(state, *source_parent, name);
    InodePin second = pin_cached_child(state, *destination_parent, new_name);
    if (first.get() != source.get() || second.get() != destination.get()) {
      throw std::system_error(EAGAIN, std::generic_category(), "rename entry changed while locking");
    }
    if (source_parent->detached() || destination_parent->detached()) {
      throw std::system_error(ESTALE, std::generic_category(), "rename parent detached");
    }
    if (destination && (flags & RENAME_NOREPLACE)) throw std::system_error(EEXIST, std::generic_category(), "rename");
    source_key = item_key(state, *source);
    destination_key = item_key(state, *destination_parent) + new_name;
    if (source->directory()) destination_key += '/';
    source_path = object_request_path(state, source_key);
    destination_path = object_request_path(state, destination_key);
    mutation = std::make_unique<PathMutationGuard>(state, source_path, source->directory(),
        destination_path, source->directory(), "rename", state.local_cache && !source->directory());
    if (source->directory()) {
      if (destination && !destination->directory()) throw std::system_error(ENOTDIR, std::generic_category(), "rename");
      if (destination_key.starts_with(source_key)) throw std::system_error(EINVAL, std::generic_category(), "move directory into itself");
    } else if (destination && destination->directory()) {
      throw std::system_error(EISDIR, std::generic_category(), "rename");
    }
  }

  static void headed(void* context, Response&& response, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncRename*>(context);
    task->head.reset();
    try {
      if (error) std::rethrow_exception(error);
      task->metadata = decode_head_response(response);
    } catch (...) { task->fail(std::current_exception()); return; }
    task->advance();
  }

  static void hidden_readers(void* context, bool hidden, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncRename*>(context);
    try {
      if (hidden) {
        task->hidden_key = task->hide->take_hidden_key();
        task->hidden_path = task->hide->take_hidden_path();
      }
      task->destination_hidden = hidden;
      task->hide.reset();
      if (error) std::rethrow_exception(error);
    } catch (...) { task->fail(std::current_exception()); return; }
    task->advance();
  }

  void cleanup_hidden() {
    {
      std::lock_guard guard(state.open_files_mutex);
      const auto pending = state.pending_deletes.find(hidden_path);
      if (pending != state.pending_deletes.end()) {
        if (!failure) pending->second.rollback = false;
        pending->second.deleting = false;
      }
    }
    cleanup = std::make_unique<AsyncPendingDelete>(state, reactor, hidden_path, cleaned, this);
    if (!cleanup->start()) {
      if (failure) std::rethrow_exception(failure);
      throw std::system_error(errno, std::generic_category(), "submit rename cleanup");
    }
  }

  static void renamed(void* context, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncRename*>(context);
    task->remote.reset();
    if (error) {
      task->failure = error;
      if (!task->destination_hidden) { task->fail(error); return; }
      try { task->cleanup_hidden(); }
      catch (...) { task->fail(error); }
    } else task->advance();
  }

  static void cleaned(void* context, std::exception_ptr) noexcept {
    auto* task = static_cast<AsyncRename*>(context);
    task->cleanup.reset();
    if (task->failure) task->fail(task->failure);
    else task->advance();
  }

  static void remove_destination(void* value) {
    auto* task = static_cast<AsyncRename*>(value);
    if (!task->destination_qualified) {
      task->preserve_destination = cached_readers_fully_clean(std::move(task->destination_entries));
      task->destination_qualified = true;
    }
    task->destination_preserved = task->state.local_cache->remove(
        task->destination_key, task->preserve_destination, &task->cache_retry, false);
  }

  static void destination_removed(void* value, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncRename*>(value);
    if (error) { task->fail(std::move(error)); return; }
    task->cache_destination_processed = !task->cache_retry;
    task->advance();
  }

  static void rename_cache(void* value) {
    auto* task = static_cast<AsyncRename*>(value);
    task->state.local_cache->rename(
        task->source_key, task->destination_key, &task->cache_retry, false);
  }

  static void cache_rename_done(void* value, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncRename*>(value);
    if (error) { task->fail(std::move(error)); return; }
    task->cache_renamed = !task->cache_retry;
    task->advance();
  }

  static void commit_marker(void* value) {
    auto* task = static_cast<AsyncRename*>(value);
    task->state.local_cache->commit_pending_delete(task->hidden_key);
  }

  static void marker_committed(void* value, std::exception_ptr error) noexcept {
    auto* task = static_cast<AsyncRename*>(value);
    if (error) {
      try { std::rethrow_exception(error); }
      catch (const std::exception& failure) {
        fprintf(stderr, "warning: remote rename committed before pending-delete metadata: path=%s: %s\n",
                task->hidden_path.c_str(), failure.what());
      } catch (...) {
        fprintf(stderr, "warning: remote rename committed before pending-delete metadata: path=%s\n",
                task->hidden_path.c_str());
      }
    }
    task->advance();
  }

  bool publish() {
    if (!source->directory()) {
      if (!cache_destination_processed) {
        destination_preserved = destination_hidden;
        if (!destination_hidden && destination && state.local_cache) {
          if (!destination_snapshot) {
            std::lock_guard guard(state.open_files_mutex);
            const auto opened = state.open_files.find(destination_path);
            if (opened != state.open_files.end() && opened->second.readers != 0) {
              destination_entries = cached_reader_entries(opened->second.handles);
            }
            destination_snapshot = true;
          }
          if (cache_retry && !retirement.ready(state, reactor, destination_key,
                                                 nullptr, preserve_destination)) return false;
          cache_retry = false;
          local.start(state, reactor, remove_destination, destination_removed, this);
          return false;
        }
        cache_destination_processed = true;
      }
      if (!cache_renamed && state.local_cache) {
        if (cache_retry && !retirement.ready(state, reactor, destination_key)) return false;
        cache_retry = false;
        local.start(state, reactor, rename_cache, cache_rename_done, this);
        return false;
      }
      finish_open_file_rename(state, source_path, destination_path, destination_key,
                               destination_preserved, readers.locks.handles);
      readers.locks.identities.clear();
      readers.locks.pins.clear();
    }
    if (destination && !destination->detached()) remove_item(state, *destination);
    if (!source->detached() && !destination_parent->detached() &&
        move_cached_item(*source, *destination_parent, new_name)) {
      if (!source->directory()) source->set_fsize(metadata.size);
    } else {
      invalidate_directory(state, parent);
      invalidate_directory(state, new_parent);
    }
    fuse_reply_err(request, 0);
    return true;
  }

  void advance() noexcept {
    std::unique_ptr<AsyncRename> owner(this);
    try {
      for (;;) switch (phase) {
        case 0:
          phase = 1;
          if (await_directory_refresh(state, reactor, parent, listing)) { owner.release(); return; }
          break;
        case 1:
          phase = 2;
          if (new_parent != parent && await_directory_refresh(state, reactor, new_parent, listing)) { owner.release(); return; }
          break;
        case 2:
          prepare_locks();
          if (parent == new_parent && name == new_name) { fuse_reply_err(request, 0); return; }
          phase = 3;
          break;
        case 3:
          if (!acquire_locks()) { owner.release(); return; }
          phase = 4;
          if (await_directory_refresh(state, reactor, parent, listing)) { owner.release(); return; }
          break;
        case 4:
          phase = 5;
          if (new_parent != parent && await_directory_refresh(state, reactor, new_parent, listing)) { owner.release(); return; }
          break;
        case 5:
          validate();
          phase = 6;
          if (source->directory() && await_directory_refresh(state, reactor, item_inode(source.get()), listing, true)) { owner.release(); return; }
          break;
        case 6:
          if (source->directory()) {
            std::shared_lock guard(source->dir_children().mutex);
            if (!source->dir_children().empty()) throw std::system_error(EXDEV, std::generic_category(), "non-empty S3 directory rename is not atomic");
          }
          phase = 7;
          if (source->directory() && destination && await_directory_refresh(state, reactor, item_inode(destination.get()), listing, true)) { owner.release(); return; }
          break;
        case 7:
          if (source->directory() && destination) {
            std::shared_lock guard(destination->dir_children().mutex);
            if (!destination->dir_children().empty()) throw std::system_error(ENOTEMPTY, std::generic_category(), "rename destination directory");
          }
          phase = 8;
          break;
        case 8:
          publication.start(*source_parent, *destination_parent, source.get(), destination.get());
          if (state.local_cache && !source->directory()) {
            if (!readers_initialized) {
              readers.snapshot(state, reactor, source_path);
              readers_initialized = true;
            }
            if (!readers.acquire()) { owner.release(); return; }
            for (OpenHandle* handle : readers.locks.handles) {
              handle->object_path.reserve(destination_path.size());
              handle->key.reserve(destination_key.size());
            }
          }
          {
            AsyncHttpRequest args;
            args.method = "HEAD";
            args.path = source_path;
            head = std::make_unique<AsyncSignedS3Request>(state, reactor, std::move(args), headed, this);
            phase = 9;
            if (!head->start()) throw std::system_error(errno, std::generic_category(), "submit rename HeadObject");
            owner.release(); return;
          }
        case 9:
          phase = 10;
          if (!source->directory() && destination && state.local_cache) {
            hide = std::make_unique<AsyncHideReaders>(state, reactor, destination_key, destination_path,
                destination_key, metadata.etag, hidden_readers, this);
            if (!hide->start()) throw std::system_error(errno, std::generic_category(), "submit rename destination preservation");
            owner.release(); return;
          }
          break;
        case 10:
          remote = std::make_unique<AsyncRemoteRename>(state, reactor, source_key, metadata.size,
              metadata.etag, metadata.version_id, destination_path,
              !source->directory() && (flags & RENAME_NOREPLACE), renamed, this);
          phase = 11;
          if (!remote->start()) {
            const auto error = std::make_exception_ptr(std::system_error(errno, std::generic_category(), "submit remote rename"));
            owner.release();
            renamed(this, error);
            return;
          }
          owner.release(); return;
        case 11:
          if (destination_hidden) {
            phase = 111;
            local.start(state, reactor, commit_marker, marker_committed, this);
            owner.release(); return;
          }
          phase = 12;
          break;
        case 111:
          phase = 12;
          cleanup_hidden();
          owner.release(); return;
        case 12:
          if (!publish()) owner.release();
          return;
        default: abort();
      }
    } catch (...) { owner.release(); fail(std::current_exception()); }
  }
};

void ngs3fs_rename(fuse_req_t request, fuse_ino_t parent, const char* name,
                   fuse_ino_t new_parent, const char* new_name,
                   unsigned int flags) {
  if (name == nullptr || new_name == nullptr) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  if ((flags & ~unsigned(RENAME_NOREPLACE)) != 0) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  if (!valid_fuse_component(name) || !valid_fuse_component(new_name)) {
    fuse_reply_err(request, EINVAL);
    return;
  }

  State& state = state_from(request);
  try {
    if (FuseReactor* reactor = current_fuse_reactor()) {
      auto task = std::make_unique<AsyncRename>(state, *reactor, request, parent, name, new_parent, new_name, flags);
      task.release()->advance();
      return;
    }
    InodePin source;
    InodePin destination;
    {
      DirectoryGuards parent_locks =
          lock_directories(state, {parent, new_parent});
      refresh_directory(state, parent);
      if (new_parent != parent) {
        refresh_directory(state, new_parent);
      }
      source = pin_cached_child(
          state, inode_item(state, parent), name);
      if (!source) {
        throw std::system_error(ENOENT, std::generic_category(), "rename");
      }
      destination = pin_cached_child(
          state, inode_item(state, new_parent), new_name);
    }
    if (parent == new_parent && name == std::string_view(new_name)) {
      fuse_reply_err(request, 0);
      return;
    }
    const fuse_ino_t source_inode      = item_inode(source.get());
    const fuse_ino_t destination_inode = destination
                                             ? item_inode(destination.get())
                                             : 0;
    DirectoryGuards locks = lock_directories(
        state, {parent, new_parent, source_inode, destination_inode});
    refresh_directory(state, parent);
    if (new_parent != parent) {
      refresh_directory(state, new_parent);
    }

    std::string source_key;
    std::string destination_directory_key;
    bool source_directory;
    bool destination_directory = false;
    {
      InodeBase& source_parent = inode_item(state, parent);
      InodeBase& destination_parent = inode_item(state, new_parent);
      if (!source_parent.directory() || !destination_parent.directory()) {
        throw std::system_error(ENOTDIR, std::generic_category(), "rename");
      }
      InodePin current_source = pin_cached_child(
          state, source_parent, name);
      if (!current_source || current_source.get() != source.get()) {
        throw std::system_error(EAGAIN, std::generic_category(),
                                "rename source changed while locking");
      }
      InodePin current_destination = pin_cached_child(
          state, destination_parent, new_name);
      if (current_destination.get() != destination.get()) {
        throw std::system_error(EAGAIN, std::generic_category(),
                                "rename destination changed while locking");
      }
      if (destination && (flags & RENAME_NOREPLACE) != 0) {
        throw std::system_error(EEXIST, std::generic_category(), "rename");
      }
      source_key = item_key(state, *source);
      source_directory = source->directory();
      destination_directory_key =
          item_key(state, destination_parent);
      if (destination) {
        destination_directory = destination->directory();
      }
    }
    std::string destination_key = destination_directory_key + new_name;
    if (source_directory) {
      destination_key.push_back('/');
    }
    const std::string source_path = object_request_path(state, source_key);
    const std::string destination_path =
        object_request_path(state, destination_key);
    PathMutationGuard mutation(
        state, source_path, source_directory,
        destination_path, source_directory, "rename",
        state.local_cache != nullptr && !source_directory);
    if (source_directory) {
      if (destination_inode != 0 && !destination_directory) {
        throw std::system_error(ENOTDIR, std::generic_category(), "rename");
      }
      if (destination_key.starts_with(source_key)) {
        throw std::system_error(EINVAL, std::generic_category(),
                                "move directory into itself");
      }
      refresh_directory(state, source_inode, true);
      {
        std::shared_lock guard(source->dir_children().mutex);
        if (!source->dir_children().empty()) {
          throw std::system_error(
              EXDEV, std::generic_category(),
              "non-empty S3 directory rename is not atomic");
        }
      }
      if (destination_inode != 0) {
        refresh_directory(state, destination_inode, true);
        {
          std::shared_lock guard(destination->dir_children().mutex);
          if (!destination->dir_children().empty()) {
            throw std::system_error(ENOTEMPTY, std::generic_category(),
                                    "rename destination directory");
          }
        }
      }
      DirectoryPublicationGuard publication;
      publication.start(inode_item(state, parent), inode_item(state, new_parent),
                        source.get(), destination.get());
      const ObjectMetadata metadata = head_object(state, source_path);
      rename_remote_object(
          state, source_key, metadata.size, metadata.etag,
          metadata.version_id, destination_path, false);
      if (destination && !destination->detached()) {
        remove_item(state, *destination);
      }
      InodeBase* target = new_parent == FUSE_ROOT_ID
                              ? state.root_item.get()
                              : reinterpret_cast<InodeBase*>(
                                    uintptr_t(new_parent));
      if (!source->detached() && target != nullptr &&
          !target->detached()) {
        if (!move_cached_item(*source, *target, new_name)) {
          invalidate_directory(state, parent);
          invalidate_directory(state, new_parent);
        }
      } else {
        invalidate_directory(state, parent);
        invalidate_directory(state, new_parent);
      }
      fuse_reply_err(request, 0);
      return;
    }
    if (destination_inode != 0 && destination_directory) {
      throw std::system_error(EISDIR, std::generic_category(), "rename");
    }
    OpenHandleIdentityLocks source_locks;
    if (state.local_cache) {
      source_locks = lock_open_handle_identities(
          state, source_path, "rename source");
      for (OpenHandle* handle : source_locks.handles) {
        handle->object_path.reserve(destination_path.size());
        handle->key.reserve(destination_key.size());
      }
    }
    DirectoryPublicationGuard publication;
    publication.start(inode_item(state, parent), inode_item(state, new_parent));
    const ObjectMetadata metadata = head_object(state, source_path);
    std::string hidden_key;
    std::string hidden_path;
    const bool destination_hidden = destination && state.local_cache &&
        hide_open_readers(
            state, destination_key, destination_path,
            destination_key, metadata.etag, &hidden_key, &hidden_path);
    try {
      rename_remote_object(
          state, source_key, metadata.size, metadata.etag,
          metadata.version_id, destination_path,
          (flags & RENAME_NOREPLACE) != 0);
    } catch (...) {
      if (destination_hidden) {
        {
          std::lock_guard guard(state.open_files_mutex);
          const auto pending = state.pending_deletes.find(hidden_path);
          if (pending != state.pending_deletes.end()) {
            pending->second.deleting = false;
          }
        }
        finish_pending_delete(state, hidden_path);
      }
      throw;
    }
    if (destination_hidden) {
      try {
        state.local_cache->commit_pending_delete(hidden_key);
      } catch (const std::exception& error) {
        fprintf(stderr,
                "warning: remote rename committed before pending-delete "
                "metadata: path=%s: %s\n",
                hidden_path.c_str(), error.what());
      }
      {
        std::lock_guard guard(state.open_files_mutex);
        const auto pending = state.pending_deletes.find(hidden_path);
        if (pending != state.pending_deletes.end()) {
          pending->second.rollback = false;
          pending->second.deleting = false;
        }
      }
      finish_pending_delete(state, hidden_path);
    }
    const bool destination_preserved = destination_hidden ||
        (destination && preserve_renamed_destination(
            state, destination_path, destination_key));
    if (state.local_cache) {
      state.local_cache->rename(source_key, destination_key);
    }
    finish_open_file_rename(
        state, source_path, destination_path, destination_key,
        destination_preserved, source_locks.handles);
    source_locks.identities.clear();
    source_locks.pins.clear();
    if (destination && !destination->detached()) {
      remove_item(state, *destination);
    }
    InodeBase* target = new_parent == FUSE_ROOT_ID
                            ? state.root_item.get()
                            : reinterpret_cast<InodeBase*>(
                                  uintptr_t(new_parent));
    if (!source->detached() && target != nullptr && !target->detached()) {
      if (move_cached_item(*source, *target, new_name)) {
        source->set_fsize(metadata.size);
      } else {
        invalidate_directory(state, parent);
        invalidate_directory(state, new_parent);
      }
    } else {
      invalidate_directory(state, parent);
      invalidate_directory(state, new_parent);
    }
    fuse_reply_err(request, 0);
  } catch (...) {
    invalidate_directory(state, parent);
    if (new_parent != parent) {
      invalidate_directory(state, new_parent);
    }
    reply_callback_error(request);
  }
}

void ngs3fs_link(fuse_req_t request, fuse_ino_t, fuse_ino_t, const char*) {
  fuse_reply_err(request, ENOTSUP);
}

void ngs3fs_statfs(fuse_req_t request, fuse_ino_t) {
  struct statvfs status{};
  constexpr uint64_t frsize = 4096;
  status.f_bsize   = kPreferredIoSize;
  status.f_frsize  = frsize;
  status.f_blocks  = UINT64_MAX / frsize;
  status.f_bfree   = status.f_blocks;
  status.f_bavail  = status.f_blocks;
  status.f_files   = UINT64_MAX;
  status.f_ffree   = UINT64_MAX;
  status.f_favail  = UINT64_MAX;
  status.f_namemax = NAME_MAX;
  fuse_reply_statfs(request, &status);
}

void ngs3fs_fsyncdir(fuse_req_t request, fuse_ino_t, int,
                     fuse_file_info*) {
  fuse_reply_err(request, 0);
}

void print_help() {
  fputs(
      "ngs3fs options:\n"
      "  -e, --endpoint-host HOST  S3 endpoint (required)\n"
      "  -p, --endpoint-port PORT  endpoint port (default 80)\n"
      "  -a, --authority VALUE     HTTP/2 :authority (default HOST:PORT)\n"
      "  -b, --bucket NAME         S3 bucket (required)\n"
      "  -k, --prefix PREFIX       optional raw object-key prefix\n"
      "  -r, --region REGION       SigV4 region (default us-east-1)\n"
      "      --expected-bucket-owner ACCOUNT\n"
      "                             reject requests to a bucket owned by a "
      "different account\n"
      "      --requester-pays       acknowledge requester-pays billing\n"
      "      --io-engine MODE       FUSE loop: auto, legacy, or uring "
      "(default legacy)\n"
      "      --reactors N           io_uring reactor count (default 1)\n"
      "  -K, --checksum ALGORITHM  upload checksum: auto, default, none, "
      "crc32, crc32c, crc64nvme, sha1, sha256, md5, xxhash64, "
      "xxhash3, xxhash128, sha512, crc64xz (default auto)\n"
      "      --verify-read-checksum\n"
      "                             background best-effort verification; "
      "the first read may finish first (default off)\n"
      "  -L, --cache-dir PATH      persistent sparse local cache (default off)\n"
      "      --cache-size BYTES    maximum physical cache allocation; "
      "0 is unlimited (default 0)\n"
      "      --cache-reserve SIZE|PERCENT\n"
      "                             preserve cache-filesystem free space "
      "(default 5%)\n"
      "      --connect-timeout MS  TCP connect timeout (default 5000)\n"
      "      --request-timeout MS  no-I/O-progress timeout "
      "(default 30000)\n"
      "      --protocol-probe-timeout MS\n"
      "                             cleartext HTTP/2 probe timeout "
      "(default 1000)\n"
      "      --socket-buffer-size BYTES\n"
      "                             TCP receive buffer per connection; "
      "0 keeps kernel autotuning (default max(2 MiB, max_read))\n"
      "      --metadata-timeout MS credential metadata timeout "
      "(default 1000)\n"
      "      --stats-interval SEC emit aggregate JSON stats to stderr; "
      "0 disables (default 0)\n"
      "      --max-prefetch-memory SIZE\n"
      "                             process-wide speculative prefetch budget; "
      "0 means automatic 10% of physical RAM (page rounded); explicit "
      "values are page-aligned and at least 256 KiB\n"
      "      --max-file-prefetch-memory SIZE\n"
      "                             per-file speculative prefetch budget; "
      "0 means automatic min(file size, twice the maximum prefetch window); "
      "explicit values are page-aligned and at least 256 KiB\n"
      "  -T, --dir-cache-timeout MS\n"
      "                             directory cache TTL (default 1000)\n"
      "  -I, --max-cached-inodes N soft inode-cache limit "
      "(default 1000000)\n"
      "  -P, --part-size BYTES     multipart part size (default 8 MiB)\n"
      "  -c, --max-uploads N       concurrent uploads (default 4)\n"
      "  -C, --max-connections N   connection pool size (default 8)\n"
      "  -B, --max-pinned-memory BYTES\n"
      "                             retained write budget (default 256 MiB)\n"
      "  environment:\n"
      "    UNSTABLE_NGS3FS_MAX_PREFETCH_WINDOW_SIZE\n"
      "                             maximum adaptive read prefetch "
      "window in bytes (default 134217728)\n"
      "  credentials            AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, "
      "AWS_SESSION_TOKEN, shared profiles, credential_process, web "
      "identity, ECS, or IMDSv2\n"
      "  -u, --uid UID             getattr owner (default mount user)\n"
      "  -g, --gid GID             getattr group (default mount group)\n"
      "  -m, --file-mode OCTAL     object file mode (default 0644)\n"
      "  -D, --dir-mode OCTAL      directory mode (default 0755)\n"
      "  -M, --metrics             emit per-read latency JSONL\n"
      "  -S, --tls                 require TLS; port 443 enables it "
      "automatically\n"
      "usage: ngs3fs [options] [FUSE options] <mountpoint>\n",
      stdout);
}

int run(int argc, char** argv) {
  fuse_args arguments = FUSE_ARGS_INIT(0, nullptr);
  MountConfig config;
  load_environment(config);
  try {
    if (!parse_arguments(argc, argv, config, arguments)) {
      throw std::runtime_error("unable to allocate FUSE arguments");
    }
    Pipe pipe_probe = Pipe::create(kPreferredIoSize);
    config.maximum_read_size = pipe_probe.capacity();
    if (!config.socket_receive_buffer_explicit) {
      config.socket_receive_buffer_size = std::max(
          kDefaultSocketReceiveBufferSize, config.maximum_read_size);
    }
    Pipe write_pipe_probe = Pipe::create(kPreferredIoSize * 2);
    config.maximum_write_size = std::min(
        kPreferredIoSize, write_pipe_probe.capacity() / 2);
    if (config.maximum_write_size == 0) {
      throw std::runtime_error("write splice pipe has no payload capacity");
    }
    if (config.maximum_write_size < kPreferredIoSize) {
      fprintf(stderr,
              "warning: reducing FUSE max_write to %zu bytes so libfuse "
              "can retain requests in its splice receive pipe\n",
              config.maximum_write_size);
    }
    prepare_file_descriptor_budget(config, pipe_probe.capacity());
    const std::string maximum_read_option =
        "-omax_read=" + std::to_string(config.maximum_read_size);
    if (!add_fuse_argument(arguments, maximum_read_option.c_str())) {
      throw std::runtime_error("unable to set FUSE maximum read size");
    }
    if (!add_fuse_argument(arguments, "-odefault_permissions")) {
      throw std::runtime_error("unable to enable kernel permission checks");
    }
    if (!add_fuse_argument(arguments, "-oauto_unmount")) {
      throw std::runtime_error("unable to enable automatic unmount");
    }
  } catch (const std::exception& error) {
    fprintf(stderr, "%s\n", error.what());
    fuse_opt_free_args(&arguments);
    return 2;
  }

  fuse_cmdline_opts options{};
  if (fuse_parse_cmdline(&arguments, &options) != 0) {
    fuse_opt_free_args(&arguments);
    return 2;
  }
  if (options.show_help) {
    print_help();
    fuse_cmdline_help();
    fuse_opt_free_args(&arguments);
    free(options.mountpoint);
    return 0;
  }
  if (options.mountpoint == nullptr) {
    fprintf(stderr, "missing mountpoint\n");
    fuse_opt_free_args(&arguments);
    return 2;
  }
  char* canonical_mountpoint = ::realpath(options.mountpoint, nullptr);
  if (canonical_mountpoint == nullptr) {
    fprintf(stderr, "invalid mountpoint %s: %s\n",
            options.mountpoint, strerror(errno));
    fuse_opt_free_args(&arguments);
    free(options.mountpoint);
    return 2;
  }
  free(options.mountpoint);
  options.mountpoint = canonical_mountpoint;
  if (config.endpoint_host.empty() || config.bucket.empty()) {
    fprintf(stderr, "--endpoint-host and --bucket are required\n");
    fuse_opt_free_args(&arguments);
    free(options.mountpoint);
    return 2;
  }
  if (config.authority.find_first_of("\r\n") != std::string::npos ||
      config.region.find_first_of("\r\n") != std::string::npos ||
      config.expected_bucket_owner.find_first_of("\r\n") !=
          std::string::npos) {
    fprintf(stderr,
            "authority, region, and expected bucket owner must not contain "
            "CR or LF\n");
    fuse_opt_free_args(&arguments);
    free(options.mountpoint);
    return 2;
  }

  std::string splice_error;
  const bool splice_available = splice_preflight(splice_error);
  if (!splice_available) {
    fprintf(stderr,
            "warning: splice(2) preflight failed: %s; FD-backed FUSE "
            "writes will use the copied fallback\n",
            splice_error.c_str());
  }
  int result = 1;
  try {
    State state(std::move(config));
    state.splice_available = splice_available;
    configure_checksum(state);
    initialize_local_cache(state);
    if (state.local_cache) {
      fprintf(stderr,
              "warning: fsync persists only the local cache; the first flush "
              "publishes and completes the S3 write\n");
    } else {
      fprintf(stderr,
              "warning: fsync is intentionally non-durable; only the first "
              "flush publishes and completes an S3 write\n");
    }
    fuse_lowlevel_ops ops{};
    ops.init         = ngs3fs_init;
    ops.forget       = ngs3fs_forget;
    ops.forget_multi = ngs3fs_forget_multi;
    ops.lookup       = ngs3fs_lookup;
    ops.getattr      = ngs3fs_getattr;
    ops.setattr      = ngs3fs_setattr;
    ops.mkdir        = ngs3fs_mkdir;
    ops.unlink       = ngs3fs_unlink;
    ops.rmdir        = ngs3fs_rmdir;
    ops.rename       = ngs3fs_rename;
    ops.link         = ngs3fs_link;
    ops.open         = ngs3fs_open;
    ops.create       = ngs3fs_create;
    ops.read         = ngs3fs_read;
    ops.flush        = ngs3fs_flush;
    ops.release      = ngs3fs_release;
    ops.fsync        = ngs3fs_fsync;
    ops.statfs       = ngs3fs_statfs;
    ops.opendir      = ngs3fs_opendir;
    ops.readdir      = ngs3fs_readdir;
    ops.releasedir   = ngs3fs_releasedir;
    ops.fsyncdir     = ngs3fs_fsyncdir;
    ops.write_buf    = ngs3fs_write_buf;
    fuse_session* session = fuse_session_new(
        &arguments, &ops, sizeof(ops), &state);
    if (session == nullptr) {
      throw std::runtime_error("failed to create FUSE session");
    }
    std::unique_ptr<FuseReactorGroup> reactors;

    bool signal_handlers_installed =
        fuse_set_signal_handlers(session) == 0;
    if (!signal_handlers_installed) {
      fprintf(stderr, "warning: failed to install FUSE signal handlers\n");
    }
    if (fuse_session_mount(session, options.mountpoint) == 0) {
      const uint32_t requested_read_ahead = kKernelReadAheadSize;
      uint32_t effective_read_ahead = requested_read_ahead;
      std::string read_ahead_error;
      if (!set_kernel_read_ahead(options.mountpoint,
                                 effective_read_ahead,
                                 read_ahead_error)) {
        fprintf(stderr,
                "warning: unable to set kernel read-ahead to %u bytes: %s; "
                "falling back to kernel value %u bytes\n",
                requested_read_ahead, read_ahead_error.c_str(),
                effective_read_ahead);
      }
      fuse_loop_config* loop_config = fuse_loop_cfg_create();
      if (loop_config != nullptr) {
        fuse_loop_cfg_set_clone_fd(loop_config, 1);
        fuse_loop_cfg_set_idle_threads(loop_config, 10);
      }
      {
        std::lock_guard guard(state.session_mutex);
        state.session = session;
      }
      bool run_legacy = state.config.io_engine == IO_ENGINE_LEGACY || state.config.tls;
      if (state.config.tls && state.config.io_engine != IO_ENGINE_LEGACY) {
        fprintf(stderr, "warning: TLS uses the legacy engine; io_uring is "
                        "enabled only for plaintext connections\n");
      }
      if (!run_legacy) {
        reactors = std::make_unique<FuseReactorGroup>();
        std::string reactor_error;
        if (reactors->initialize(session, state.config.reactor_count,
                                 kFuseReactorQueueDepth,
                                 state.config.request_timeout_ms,
                                 reactor_error)) {
          result = reactors->run();
          reactors->report_stats();
          if (result != 0) {
            fprintf(stderr, "io_uring engine failed: %s\n",
                    strerror(-result));
          }
        } else if (state.config.io_engine == IO_ENGINE_URING) {
          fprintf(stderr, "unable to start io_uring engine: %s\n",
                  reactor_error.c_str());
          result = -EIO;
        } else {
          fprintf(stderr,
                  "warning: unable to start io_uring engine: %s; using "
                  "the legacy engine\n",
                  reactor_error.c_str());
          run_legacy = true;
        }
      }
      if (run_legacy) {
        if (loop_config == nullptr) {
          fprintf(stderr, "unable to allocate legacy FUSE loop config\n");
          result = -ENOMEM;
        } else {
          result = fuse_session_loop_mt(session, loop_config);
        }
      }
      if (loop_config != nullptr) {
        fuse_loop_cfg_destroy(loop_config);
      }
      {
        std::lock_guard guard(state.session_mutex);
        state.session = nullptr;
      }
      if (signal_handlers_installed) {
        fuse_remove_signal_handlers(session);
        signal_handlers_installed = false;
      }
      state.stop_background_tasks();
      if (reactors) {
        reactors->shutdown();
      }
      fuse_session_unmount(session);
    } else {
      fprintf(stderr, "failed to mount %s: %s\n",
              options.mountpoint, strerror(errno));
    }
    if (signal_handlers_installed) {
      fuse_remove_signal_handlers(session);
    }
    fuse_session_destroy(session);
    reactors.reset();
  } catch (const std::exception& error) {
    fprintf(stderr, "ngs3fs startup failed: %s\n", error.what());
  }

  fuse_opt_free_args(&arguments);
  free(options.mountpoint);
  return result;
}
