#include "cache.hpp"
#include "credentials.hpp"
#include "http.hpp"
#include "io.hpp"
#include "s3.hpp"

#include <fuse_lowlevel.h>
#include <xxhash.h>

#include <fcntl.h>
#include <getopt.h>
#include <linux/fs.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
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

constexpr size_t kMaximumListResponseSize = 8U * 1024U * 1024U;
constexpr unsigned kDirectoryListLimit    = 1000;

enum ChecksumService {
  CHECKSUM_SERVICE_UNKNOWN,
  CHECKSUM_SERVICE_AWS,
  CHECKSUM_SERVICE_OSS,
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
  uint32_t io_size                  = kPreferredIoSize;
  uint32_t read_ahead_size          = kPreferredIoSize;
  size_t socket_receive_buffer_size = kDefaultSocketReceiveBufferSize;
  uint64_t part_size                = 8ULL * 1024ULL * 1024ULL;
  uint64_t cache_size               = 0;
  uint64_t cache_reserve            = 5;
  uint64_t max_pinned_memory        = 256ULL * 1024ULL * 1024ULL;
  uint64_t directory_cache_ns       = 1000ULL * 1000ULL * 1000ULL;
  size_t max_cached_inodes          = 1'000'000;
  uint32_t stats_interval_seconds   = 0;
  size_t maximum_cache_fetch_size   = 8U * 1024U * 1024U;
  unsigned max_uploads              = 4;
  unsigned max_connections          = 8;
  int connect_timeout_ms            = kConnectTimeoutMs;
  int request_timeout_ms            = kRequestIoTimeoutMs;
  int protocol_probe_timeout_ms     = kProtocolProbeTimeoutMs;
  int metadata_timeout_ms           = 1000;
  ChecksumAlgorithm checksum        = CHECKSUM_AUTO;
  ChecksumService checksum_service  = CHECKSUM_SERVICE_UNKNOWN;
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
  std::vector<PipeSegment> segments;
  ChecksumValue checksum;
  uint64_t bytes  = 0;
  unsigned number = 0;
};

struct State;
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
  virtual void flush(State& state, OpenHandle& handle) = 0;
  virtual void fsync(State& state, OpenHandle& handle, bool data_only) = 0;
  virtual void release(State& state, OpenHandle& handle) noexcept = 0;
};

std::unique_ptr<FileReader> make_file_reader(State& state,
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

  explicit HttpPool(const MountConfig& config) {
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
    const size_t usable = slots_.size() - 1;
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

 private:
  Lease acquire_slots(size_t usable) {
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
  }

  std::vector<std::unique_ptr<Slot>> slots_;
  std::atomic<size_t> next_{0};
  std::atomic<uint64_t> released_{0};
};

class UploadScheduler {
 private:
  struct Job {
    const void* owner;
    std::function<void()> run;
  };

 public:
  explicit UploadScheduler(unsigned concurrency) {
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
      jobs_.push_back(Job{owner, std::move(run)});
    }
    condition_.notify_one();
  }

 private:
  void worker() noexcept {
    for (;;) {
      Job job;
      {
        std::unique_lock guard(mutex_);
        condition_.wait(guard, [&] { return stopping_ || !jobs_.empty(); });
        if (jobs_.empty()) {
          return;
        }
        auto selected = jobs_.begin();
        if (last_owner_ != nullptr) {
          const auto different = std::find_if(
              jobs_.begin(), jobs_.end(),
              [&](const Job& candidate) {
                return candidate.owner != last_owner_;
              });
          if (different != jobs_.end()) {
            selected = different;
          }
        }
        job = std::move(*selected);
        jobs_.erase(selected);
        last_owner_ = job.owner;
      }
      try {
        job.run();
      } catch (const std::exception& error) {
        fprintf(stderr, "unhandled upload job error: %s\n", error.what());
      } catch (...) {
        fprintf(stderr, "unhandled upload job error\n");
      }
    }
  }

  std::vector<std::thread> threads_;
  std::deque<Job> jobs_;
  std::mutex mutex_;
  std::condition_variable condition_;
  const void* last_owner_ = nullptr;
  bool stopping_ = false;
};

struct OpenHandle {
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
  std::unique_ptr<FileReader> reader;
  std::unique_ptr<FileWriter> writer;
  std::vector<std::string> part_etags;
  std::vector<ssostr<96>> part_checksums;
  std::vector<uint64_t> part_checksum_values;
  std::vector<uint64_t> part_sizes;
  std::vector<std::pair<uint64_t, uint64_t>> partial_write_pages;
  std::unique_ptr<RetainedPart> current_part;
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
  std::atomic<uint64_t> request_state{0};
  std::atomic<bool> stale{false};
  std::atomic<bool> unlinked{false};
  mutable std::shared_mutex identity_mutex;
  std::mutex mutex;
  std::condition_variable condition;
};

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
    const uint64_t previous =
        handle_.request_state.fetch_sub(1, std::memory_order_release);
    if ((previous & kActiveMask) == 1) {
      handle_.request_state.notify_all();
    }
  }

  OpenRequestGuard(const OpenRequestGuard&) = delete;
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
        root_item(std::make_unique<InodeDir>()),
        http(std::make_unique<HttpPool>(config)),
        uploads(std::make_unique<UploadScheduler>(config.max_uploads)),
        cache_reclaimer([this](std::stop_token stop) {
          cache_reclaim_loop(stop, this);
        }) {
    root_item->set_parent(root_item.get());
    if (config.stats_interval_seconds != 0) {
      stats_reporter = std::jthread([this](std::stop_token stop) {
        stats_report_loop(stop, this);
      });
    }
  }

  ~State() {
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
    uploads.reset();
    for (const auto& [inode, retired] : retired_items) {
      (void)inode;
      detach_parent_slot_if_owned(*retired.item, retired.parent);
      delete_inode(retired.item);
    }
    retired_items.clear();
  }

  struct OpenFileState {
    std::vector<OpenHandle*> handles;
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
  };

  MountConfig config;
  CredentialProvider credentials;
  time_t directory_mtime;
  std::unique_ptr<InodeDir> root_item;
  std::unique_ptr<LocalCache> local_cache;
  AmzDateTimeCache date_time;
  std::mutex retired_mutex;
  std::mutex open_files_mutex;
  std::condition_variable open_files_condition;
  std::mutex cache_mutex;
  std::mutex session_mutex;
  std::mutex metrics_mutex;
  std::mutex credentials_mutex;
  std::mutex rename_probe_mutex;
  std::condition_variable credentials_condition;
  std::mutex budget_mutex;
  std::condition_variable budget_condition;
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
  fuse_session* session = nullptr;
  Credentials express_credentials;
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
          ",\"credential_source\":\"%s\"}\n",
          event,
          state.remote_reads.load(std::memory_order_relaxed),
          state.remote_read_bytes.load(std::memory_order_relaxed),
          state.fuse_writes.load(std::memory_order_relaxed),
          state.fuse_write_bytes.load(std::memory_order_relaxed),
          state.request_errors.load(std::memory_order_relaxed),
          state.cached_inodes.load(std::memory_order_relaxed), open_handles,
          pinned_bytes, fallback_bytes, state.credentials.source_name());
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

void invalidate_page_cache(State& state, fuse_ino_t inode,
                           off_t offset = 0, off_t length = 0) noexcept {
  int error = 0;
  {
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

void queue_page_invalidation(State& state, fuse_ino_t inode,
                             off_t offset, off_t length) noexcept {
  try {
    std::lock_guard guard(state.cache_mutex);
    const auto duplicate = std::find_if(
        state.pending_invalidations.begin(),
        state.pending_invalidations.end(),
        [&](const State::PendingInvalidation& pending) {
          return pending.inode == inode && pending.offset == offset &&
              pending.length == length;
        });
    if (duplicate == state.pending_invalidations.end()) {
      state.pending_invalidations.push_back(
          State::PendingInvalidation{inode, offset, length});
    }
    state.cache_condition.notify_all();
  } catch (...) {
    if (!state.page_cache_invalidate_warned.exchange(
            true, std::memory_order_relaxed)) {
      fprintf(stderr,
              "warning: unable to queue stale page-cache invalidation: "
              "inode=%" PRIu64 "\n",
              uint64_t(inode));
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
      size_t(state.config.read_ahead_size));
  const size_t threshold = read_ahead / 4;
  if (file_size < threshold || offset == 0 || size > threshold) {
    return;
  }

  const uint64_t now = fuse_monotonic_ns_noexcept();
  if (now == 0) {
    return;
  }
  constexpr uint64_t interval = 60ULL * 1000ULL * 1000ULL * 1000ULL;
  uint64_t previous =
      state.random_read_warning_ns.load(std::memory_order_relaxed);
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
}

bool parse_arguments(int argc, char** argv, MountConfig& config,
                     fuse_args& fuse_arguments) {
  if (!add_fuse_argument(fuse_arguments, argv[0])) {
    return false;
  }

  constexpr int io_size_option                    = 256;
  constexpr int verify_read_checksum_option       = 257;
  constexpr int connect_timeout_option            = 258;
  constexpr int request_timeout_option            = 259;
  constexpr int protocol_probe_timeout_option     = 260;
  constexpr int metadata_timeout_option           = 261;
  constexpr int stats_interval_option             = 262;
  constexpr int socket_buffer_size_option          = 263;
  constexpr int cache_size_option                  = 264;
  constexpr int cache_reserve_option               = 265;
  constexpr int maximum_cache_fetch_size_option    = 266;
  constexpr int expected_bucket_owner_option       = 267;
  constexpr int requester_pays_option              = 268;
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
      {"io-size", required_argument, nullptr, io_size_option},
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
      {"max-cache-fetch-size", required_argument, nullptr,
       maximum_cache_fetch_size_option},
      {"expected-bucket-owner", required_argument, nullptr,
       expected_bucket_owner_option},
      {"requester-pays", no_argument, nullptr, requester_pays_option},
      {"read-ahead", required_argument, nullptr, 'R'},
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
      "-e:p:a:b:k:r:K:L:R:T:I:P:c:C:B:u:g:m:D:MShVdfso:";

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
      case io_size_option: {
        const uint64_t value = parse_required_size("--io-size");
        if (value == 0 || value > std::numeric_limits<uint32_t>::max()) {
          throw std::invalid_argument(
              "--io-size must be nonzero and at most UINT32_MAX");
        }
        config.io_size = static_cast<uint32_t>(value);
        break;
      }
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
      case maximum_cache_fetch_size_option: {
        const uint64_t value =
            parse_required_size("--max-cache-fetch-size");
        if (value > std::numeric_limits<size_t>::max()) {
          throw std::invalid_argument("--max-cache-fetch-size is too large");
        }
        config.maximum_cache_fetch_size = size_t(value);
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
      case 'R': {
        const uint64_t value = parse_required_size("--read-ahead");
        const long page_size = ::sysconf(_SC_PAGESIZE);
        if (page_size <= 0 ||
            value > std::numeric_limits<uint32_t>::max() ||
            value % uint64_t(page_size) != 0) {
          throw std::invalid_argument(
              "--read-ahead must be page-aligned and at most UINT32_MAX");
        }
        config.read_ahead_size = static_cast<uint32_t>(value);
        break;
      }
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
    config.authority = format_authority(
        config.endpoint_host, config.endpoint_port);
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
  constexpr size_t minimum_cache_fetch_size = 1024U * 1024U;
  if (config.maximum_cache_fetch_size < minimum_cache_fetch_size ||
      config.maximum_cache_fetch_size % size_t(page_size) != 0) {
    throw std::invalid_argument(
        "--max-cache-fetch-size must be page-aligned and at least 1 MiB");
  }
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
  std::vector<std::pair<std::string, bool>> path;
  std::vector<std::shared_lock<std::shared_mutex>> guards;
  const InodeBase* current = &item;
  while (current != state.root_item.get()) {
    InodeBase* parent = current->parent();
    if (parent == nullptr || parent == current) {
      throw std::logic_error("inode parent chain does not reach root");
    }
    const Directory& children = parent->dir_children();
    guards.emplace_back(children.mutex);
    if (current->parent() != parent || current->detached() ||
        current->dentry_slot == UINT32_MAX ||
        current->dentry_slot >= children.end_i() ||
        children.is_deleted(current->dentry_slot) ||
        children.val(current->dentry_slot) != current) {
      throw std::system_error(
          EAGAIN, std::generic_category(), "inode path changed");
    }
    const terark::fstring name = children.key(current->dentry_slot);
    path.emplace_back(
        std::string(name.data(), name.size()), current->directory());
    current = parent;
  }
  std::string key = state.config.prefix;
  for (auto i = path.rbegin(); i != path.rend(); ++i) {
    key += i->first;
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
    headers.push_back(Header{"x-amz-request-payer", "requester"});
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
    std::lock_guard guard(state.credentials_mutex);
    state.express_refreshing = false;
    state.credentials_condition.notify_all();
    throw;
  }

  {
    std::lock_guard guard(state.credentials_mutex);
    state.express_credentials = std::move(credentials);
    state.express_expiration_ns = expiration;
    state.express_refreshing = false;
  }
  state.credentials_condition.notify_all();
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

  ensure_express_session(state);
  Credentials credentials;
  {
    std::lock_guard guard(state.credentials_mutex);
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
      .maximum_fetch_size = state.config.maximum_cache_fetch_size,
      .page_size = size_t(::sysconf(_SC_PAGESIZE)),
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
  const auto request_id = response.headers.find("x-amz-request-id");
  if (request_id != response.headers.end()) {
    append_s3_diagnostic(message, "request_id",
                         sso_view(request_id->second));
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
        found = true;
      }
    }
  }
  return found;
}

ObjectMetadata head_object(State& state, std::string_view path) {
  const Response response = request_with_retries([&] {
    const auto authorization = authorization_headers(
        state, "HEAD", path, {}, kEmptyPayloadSha256);
    HttpPool::Lease client = state.http->acquire();
    return client->request_no_body("HEAD", path, authorization);
  }, "HeadObject");
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
  const auto version = response.headers.find("x-amz-version-id");
  if (version != response.headers.end() && version->second != "null") {
    assign_string(metadata.version_id, version->second);
  }
  const auto write_id = response.headers.find("x-amz-meta-ngs3fs-write-id");
  if (write_id != response.headers.end()) {
    assign_string(metadata.write_id, write_id->second);
  }
  return metadata;
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

bool refresh_open_metadata(State& state, OpenHandle& handle) {
  InodeFile& item = *handle.item;
  InodeMetadataGuard metadata_guard(item);
  ObjectMetadata metadata = head_object(state, handle.object_path);
  const ObjectGeneration generation = object_generation(
      metadata.etag, metadata.version_id, metadata.size, metadata.mtime);
  const uint64_t previous_epoch =
      item.generation_epoch.load(std::memory_order_acquire) &
      ((1ULL << 63) - 1);
  const bool changed = previous_epoch != 0 &&
      item.generation_hash.load(std::memory_order_relaxed) != generation;
  const bool keep_cache = item.page_cache_valid() && !changed &&
      item.fsize.load(std::memory_order_relaxed) == metadata.size;
  if (changed) {
    item.set_page_cache_valid(false);
  }
  item.fsize.store(metadata.size, std::memory_order_relaxed);
  item.mtime.store(metadata.mtime, std::memory_order_relaxed);
  handle.generation_epoch = publish_inode_generation(item, generation);
  handle.stale.store(false, std::memory_order_release);
  item.set_page_cache_valid(keep_cache);
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
    invalidate_page_cache(state, handle.inode);
  }
  if (old_readers) {
    throw std::system_error(
        EBUSY, std::generic_category(),
        "old object generation still has open readers");
  }
  return keep_cache;
}

void register_open_handle(State& state, fuse_ino_t inode,
                          OpenHandle& handle) {
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
  std::vector<std::unique_lock<std::shared_mutex>> identities;
};

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
      result.handles.push_back(handle);
      result.pins.push_back(std::make_unique<OpenRequestGuard>(*handle));
    }
  }
  result.identities.reserve(result.handles.size());
  for (OpenHandle* handle : result.handles) {
    result.identities.emplace_back(handle->identity_mutex);
  }
  return result;
}

bool preserve_renamed_destination(State& state, std::string_view path,
                                  std::string_view key) {
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
  return can_preserve && state.local_cache->remove(key, true);
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
    if (!source_handles.empty()) {
      throw std::logic_error("rename source reader registry disappeared");
    }
    return;
  }
  if (source->second.handles != source_handles) {
    throw std::logic_error("rename source reader registry changed");
  }
  for (OpenHandle* handle : source->second.handles) {
    if (handle == nullptr) {
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
                                           size_t length) {
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
      state.config.checksum_service != CHECKSUM_SERVICE_OSS;
  request.verify_checksum = verify_checksum;
  const int signing_mode =
      state.range_signing_mode.load(std::memory_order_relaxed);
  const bool sign_range = state.config.directory_bucket || signing_mode == 2;
  request.range_signed = sign_range;
  const std::array<uint64_t, 2> date = state.date_time.now();
  const bool cacheable = !state.config.directory_bucket && !sign_range &&
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
    authorization_headers(
        headers, state, "GET", request.path(),
        std::span(canonical_headers.data(), canonical_count),
        kEmptyPayloadSha256, true,
        std::string_view(date_text.data(), date_text.size()));
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

ListedPage list_directory_page(State& state, std::string_view prefix,
                               std::string_view token) {
  std::string query = "delimiter=%2F&encoding-type=url&list-type=2&prefix=";
  query += uri_encode(prefix, false);
  if (!token.empty()) {
    query += "&continuation-token=" + uri_encode(token, false);
  }
  query += "&max-keys=" + std::to_string(kDirectoryListLimit);
  const std::string path = query_path(
      state.config.bucket_path.empty() ? "/" : state.config.bucket_path,
      query);
  const Response response = request_with_retries([&] {
    const HeaderList headers = authorization_headers(
        state, "GET", path, {}, kEmptyPayloadSha256);
    HttpPool::Lease client = state.http->acquire();
    return client->request_no_body(
        "GET", path, headers, kMaximumListResponseSize);
  }, "ListObjectsV2");
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
      value, value + 1, std::memory_order_relaxed,
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
  }
  item.set_pending(false);
  item.set_truncate_pending(false);
  retire_item(state, item);
  return true;
}

fuse_ino_t install_item(State& state, fuse_ino_t parent,
                        ListedChild child,
                        uint32_t listing_generation = 0,
                        bool exclusive = false,
                        bool pending = false,
                        bool lookup = false,
                        bool defer_file_over_directory = false) {
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
  std::vector<std::unique_lock<std::mutex>> guards;
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
                                uint32_t generation) {
  std::vector<InodeBase*> stale;
  {
    Directory& children = directory.dir_children();
    std::unique_lock guard(children.mutex);
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

void refresh_directory_locked(State& state, fuse_ino_t inode,
                              bool force = false) {
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
  uint32_t generation = ++children.listing_generation;
  if (generation == 0) {
    generation = ++children.listing_generation;
    std::unique_lock guard(children.mutex);
    for (auto [name, child] : children) {
      (void)name;
      child->listing_generation.store(0, std::memory_order_relaxed);
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
                false, false, false, true);
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
        install_item(state, inode, std::move(child), generation);
      }
    }
    prune_directory_generation(state, inode_item(state, inode), generation);
    const uint64_t refreshed = fuse_monotonic_ns();
    const uint64_t ttl       = state.config.directory_cache_ns;
    const uint64_t expire    = ttl > UINT64_MAX - refreshed
                                   ? UINT64_MAX
                                   : refreshed + ttl;
    inode_item(state, inode).expire.store(expire, std::memory_order_relaxed);
  } catch (...) {
    invalidate_directory(state, inode);
    throw;
  }
  sweep_retired_items(state);
}

void refresh_directory(State& state, fuse_ino_t inode, bool force = false) {
  InodeBase& item = inode_item(state, inode);
  if (!item.directory()) {
    throw std::system_error(ENOTDIR, std::generic_category(), "directory");
  }
  cache_touch(static_cast<InodeDir&>(item));
  std::lock_guard directory_guard(item.dir_children().mutation_mutex);
  refresh_directory_locked(state, inode, force);
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
      invalidate_page_cache(*state, pending.inode,
                            pending.offset, pending.length);
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
    throw std::runtime_error("GetObject returned a mismatched S3 checksum");
  }
}

void update_written_metadata(State& state, OpenHandle& handle,
                             const Response& response,
                             std::string_view body_etag = {}) {
  ObjectMetadata metadata;
  bool used_head = false;
  time_t mtime;
  const auto modified = response.headers.find("last-modified");
  if (modified != response.headers.end()) {
    mtime = parse_http_mtime(sso_view(modified->second));
  } else {
    metadata  = head_object(state, handle.object_path);
    used_head = true;
    mtime     = metadata.mtime;
  }
  const auto etag = response.headers.find("etag");
  if (etag != response.headers.end()) {
    assign_string(handle.etag, etag->second);
  } else if (!body_etag.empty()) {
    handle.etag.assign(body_etag);
  } else if (used_head) {
    handle.etag = std::move(metadata.etag);
  } else {
    handle.etag.clear();
  }
  const auto version = response.headers.find("x-amz-version-id");
  if (version != response.headers.end() && version->second != "null") {
    assign_string(handle.version_id, version->second);
  } else if (used_head) {
    handle.version_id = std::move(metadata.version_id);
  } else {
    handle.version_id.clear();
  }
  InodeFile& item = *handle.item;
  InodeMetadataGuard metadata_guard(item);
  item.fsize.store(handle.stream_offset, std::memory_order_relaxed);
  item.mtime.store(mtime, std::memory_order_relaxed);
  item.set_page_cache_valid(!handle.page_cache_store_failed);
  item.set_pending(false);
  handle.generation_epoch = publish_inode_generation(
      item, object_generation(handle.etag, handle.version_id,
                              handle.stream_offset, mtime));
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
  handle.condition.notify_all();
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
            state.config.part_size, state.waiting_writers);
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
                .size  = length,
                .flags = fuse_buf_flags(0),
                .mem   = nullptr,
                .fd    = -1,
                .pos   = 0,
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
      handle.condition.wait(guard);
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
      handle.condition.notify_all();
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
  handle.condition.notify_all();
}

void submit_part(State& state, OpenHandle& handle,
                 std::shared_ptr<RetainedPart> part) {
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
    state.uploads->submit(
        &handle, [&state, &handle, part = std::move(part)] {
          upload_part_job(state, handle, part);
        });
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

void submit_current_part(State& state, OpenHandle& handle) {
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
    submit_part(state, handle, std::move(part));
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
    handle.condition.wait(guard);
  }
  while (handle.write_state == WRITE_SEALING) {
    handle.condition.wait(guard);
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
        handle.condition.wait(guard);
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
    handle.condition.notify_all();
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

void ngs3fs_init(void* userdata, fuse_conn_info* connection) {
  auto& state = *static_cast<State*>(userdata);
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
  connection->max_readahead = state.config.read_ahead_size;
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

void ngs3fs_lookup(fuse_req_t request, fuse_ino_t parent, const char* name) {
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
    cache_touch(static_cast<InodeDir&>(directory));
    refresh_directory(state, parent);
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
          inode_item(state, inode).set_truncate_pending(true);
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
  bool registered      = false;
  bool budget_reserved = false;
  bool keep_cache      = false;
  std::unique_ptr<OpenHandle> handle;
  std::string registered_path;
  try {
    handle = std::make_unique<OpenHandle>();
    handle->writable = writable;
    InodeBase& item = inode_item(state, inode);
    if (item.directory()) {
      throw std::system_error(EISDIR, std::generic_category(), "open");
    }
    const bool truncate_pending = item.truncate_pending();
    register_open_handle(state, inode, *handle);
    if (writable) {
      inode_item(state, inode).set_truncate_pending(false);
    }
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
    InodeBase& directory = inode_item(state, parent);
    std::lock_guard guard(directory.dir_children().mutation_mutex);
    InodeBase& item = inode_reference(state, inode);
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

void ngs3fs_create(fuse_req_t request, fuse_ino_t parent, const char* name,
                   mode_t, fuse_file_info* file) {
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
      std::lock_guard directory_guard(
          directory.dir_children().mutation_mutex);
      refresh_directory_locked(state, parent);
      ListedChild child;
      child.name = name;
      inode = install_item(
          state, parent, std::move(child), 0, true, true, true);
      inode_item(state, inode).set_fsize(0);
      register_open_handle(state, inode, *handle);
      path       = handle->object_path;
      registered = true;
      timeout    = remaining_directory_timeout(directory);
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
    if (budget_reserved) {
      release_part_budget(state);
    }
    if (inode != 0) {
      abandon_created_inode(state, parent, inode);
    }
    if (registered) {
      unregister_open_handle(state, path, *handle);
    }
    reply_callback_error(request);
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

std::vector<CacheChecksumPart> get_checksum_manifest(
    State& state, const OpenHandle& handle) {
  std::vector<CacheChecksumPart> result;
  unsigned expected_number = 1;
  unsigned marker          = 0;
  uint64_t offset          = 0;
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
    if (marker != 0) {
      marker_text = std::to_string(marker);
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
      uint64_t size   = 0;
      const std::string number_text =
          xml.required_text(*part, "PartNumber");
      const std::string size_text = xml.required_text(*part, "Size");
      if (!parse_unsigned(trim_xml_space(number_text), number) ||
          number != expected_number || number > kMaximumMultipartParts ||
          !parse_unsigned(trim_xml_space(size_text), size) || size == 0 ||
          offset > handle.size || size > handle.size - offset) {
        throw std::runtime_error(
            "GetObjectAttributes returned an invalid part sequence");
      }
      ChecksumAlgorithm algorithm = CHECKSUM_NONE;
      std::string value;
      if (!part_checksum(xml, *part, state.config.checksum,
                         algorithm, value)) {
        throw std::runtime_error(
            "GetObjectAttributes omitted a usable part checksum");
      }
      result.push_back(CacheChecksumPart{
          .offset    = offset,
          .size      = size,
          .algorithm = uint32_t(algorithm),
          .value     = std::move(value),
      });
      offset += size;
      ++expected_number;
    }
    const bool truncated = xml.required_bool(parts, "IsTruncated");
    if (!truncated) {
      if (offset != handle.size) {
        throw std::runtime_error(
            "GetObjectAttributes part sizes do not match the object");
      }
      return result;
    }
    uint64_t next = 0;
    const std::string next_text =
        xml.required_text(parts, "NextPartNumberMarker");
    if (!parse_unsigned(trim_xml_space(next_text), next) ||
        next <= marker || next != expected_number - 1 ||
        next > kMaximumMultipartParts) {
      throw std::runtime_error(
          "GetObjectAttributes returned invalid part pagination");
    }
    marker = unsigned(next);
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
    handle.cache_read_window = std::min(
        state.config.maximum_cache_fetch_size,
        std::max(initial, handle.cache_read_window * 2));
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
  const size_t expansion = cache_fetch_expansion(
      state, handle, offset, wanted);
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
    size_t selected_expansion = wanted;
    if (expansion > wanted) {
      lease = state.http->try_acquire_bulk();
      if (lease) {
        selected_expansion = expansion;
      }
    }
    const CacheFetchClaim claim = entry.claim_fetch(
        offset, wanted, selected_expansion);
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
                state, handle.inode, off_t(claim.offset),
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
              state, handle.inode, off_t(claim.offset),
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
              state, handle.inode, off_t(claim.offset),
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
        break;
      } catch (...) {
        if (attempt == 3) {
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
    if (state.config.verify_read_checksum && unsigned_offset == 0 &&
        uint64_t(wanted) == object_size) {
      verify_read_checksum(response, pipe, wanted, state.config.checksum);
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
            state, handle.inode, offset, off_t(wanted));
      }
    }
  }
}

class UncachedFileReader final : public FileReader {
 public:
  void read(State& state, OpenHandle& handle, fuse_req_t request,
            fuse_ino_t inode, size_t size, off_t offset) override {
    read_open_file(state, handle, false, request, inode, size, offset);
  }
};

class CachedFileReader final : public FileReader {
 public:
  void read(State& state, OpenHandle& handle, fuse_req_t request,
            fuse_ino_t inode, size_t size, off_t offset) override {
    read_open_file(state, handle, true, request, inode, size, offset);
  }
};

class UncachedFileWriter final : public FileWriter {
 public:
  void write(State& state, OpenHandle& handle, fuse_req_t request,
             fuse_ino_t inode, fuse_bufvec* input, off_t offset) override;
  void flush(State& state, OpenHandle& handle) override;
  void fsync(State& state, OpenHandle& handle, bool data_only) override;
  void release(State& state, OpenHandle& handle) noexcept override;
};

class CachedFileWriter final : public FileWriter {
 public:
  void write(State& state, OpenHandle& handle, fuse_req_t request,
             fuse_ino_t inode, fuse_bufvec* input, off_t offset) override;
  void flush(State& state, OpenHandle& handle) override;
  void fsync(State& state, OpenHandle& handle, bool data_only) override;
  void release(State& state, OpenHandle& handle) noexcept override;
};

std::unique_ptr<FileReader> make_file_reader(State& state,
                                              OpenHandle&) {
  if (state.local_cache) {
    return std::make_unique<CachedFileReader>();
  }
  return std::make_unique<UncachedFileReader>();
}

std::unique_ptr<FileWriter> make_file_writer(State& state,
                                              OpenHandle&) {
  if (state.local_cache) {
    return std::make_unique<CachedFileWriter>();
  }
  return std::make_unique<UncachedFileWriter>();
}

void ngs3fs_read(fuse_req_t request, fuse_ino_t inode, size_t size,
                 off_t offset, fuse_file_info* file) {
  try {
    State& state = state_from(request);
    OpenHandle& handle = handle_required(file);
    OpenRequestGuard active(handle);
    if (!handle.reader) {
      throw std::system_error(EBADF, std::generic_category(),
                              "missing file reader");
    }
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
      handle.condition.wait(handle_guard);
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
    handle.write_in_progress = false;
    handle.condition.notify_all();
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
    const ssize_t result = ::pwrite(
        fd, bytes.data() + written, bytes.size() - written,
        off_t(offset + written));
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
        ? ::pread(source_fd, bytes.data(), wanted, off_t(source_offset))
        : ::read(source_fd, bytes.data(), wanted);
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
  handle.condition.notify_all();
}

void submit_cached_part(State& state, OpenHandle& handle,
                        uint64_t offset, size_t length) {
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
    state.uploads->submit(
        &handle, [&state, &handle, number, offset, length] {
          upload_cached_part_job(state, handle, number, offset, length);
        });
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
                               bool include_final) {
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
    submit_cached_part(state, handle, offset, length);
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
      handle.condition.wait(guard);
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
    handle.condition.notify_all();
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
    handle.condition.wait(guard);
  }
  while (handle.write_state == WRITE_SEALING) {
    handle.condition.wait(guard);
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
        handle.condition.wait(guard);
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
    handle.condition.notify_all();
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
    handle.condition.wait(guard);
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
    handle.condition.wait(handle_guard);
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
    handle.writer->write(state, handle, request, inode, input, offset);
  } catch (...) {
    reply_callback_error(request);
  }
}

void ngs3fs_flush(fuse_req_t request, fuse_ino_t,
                  fuse_file_info* file) {
  try {
    OpenHandle& handle = handle_required(file);
    if (handle.writer) {
      handle.writer->flush(state_from(request), handle);
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
      handle.writer->fsync(state_from(request), handle, data_only != 0);
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

void UncachedFileWriter::release(State& state,
                                 OpenHandle& handle) noexcept {
  bool release_budget = false;
  bool discard_pending = false;
  bool registered      = false;
  try {
    std::unique_lock handle_guard(handle.mutex);
    while (handle.write_in_progress ||
           handle.write_state == WRITE_SEALING) {
      handle.condition.wait(handle_guard);
    }
    if (handle.write_state == WRITE_OPEN) {
      fail_write(handle, EIO);
    }
    while (handle.pending_parts != 0) {
      handle.condition.wait(handle_guard);
    }
    release_budget = handle.current_reservation;
    handle.current_reservation = false;
    handle.current_part.reset();
    abort_multipart(state, handle);
    discard_pending = handle.write_state != WRITE_SEALED;
  } catch (...) {
  }
  {
    std::lock_guard guard(handle.mutex);
    registered        = handle.registered;
    handle.registered = false;
  }
  if (discard_pending) {
    discard_pending_inode(state, handle);
  }
  if (registered) {
    unregister_open_handle(state, handle.object_path, handle);
  } else {
    release_open_inode(state, handle);
  }
  if (release_budget) {
    release_part_budget(state);
  }
}

void CachedFileWriter::release(State& state,
                               OpenHandle& handle) noexcept {
  try {
    std::unique_lock guard(handle.mutex);
    while (handle.write_in_progress ||
           handle.write_state == WRITE_SEALING) {
      handle.condition.wait(guard);
    }
    while (handle.pending_parts != 0) {
      handle.condition.wait(guard);
    }
    if (handle.unlinked.load(std::memory_order_acquire)) {
      const bool registered = handle.registered;
      handle.registered = false;
      guard.unlock();
      abort_multipart(state, handle);
      if (handle.cache_entry) {
        handle.cache_entry->discard_write();
      }
      if (registered) {
        unregister_open_handle(state, handle.object_path, handle);
      } else {
        release_open_inode(state, handle);
      }
      return;
    }
    const bool dirty = handle.cache_entry && handle.cache_entry->dirty();
    if (dirty && handle.write_state != WRITE_SEALED) {
      fprintf(stderr,
              "warning: cached write preserved for recovery: path=%s "
              "bytes=%" PRIu64 "\n",
              handle.object_path.c_str(), handle.stream_offset);
      const bool registered = handle.registered;
      handle.registered = false;
      guard.unlock();
      {
        std::lock_guard open_guard(state.open_files_mutex);
        state.recovery_paths.insert(handle.object_path);
      }
      if (registered) {
        unregister_open_handle(state, handle.object_path, handle);
      } else {
        release_open_inode(state, handle);
      }
      return;
    }
    const bool registered = handle.registered;
    handle.registered = false;
    guard.unlock();
    if (registered) {
      unregister_open_handle(state, handle.object_path, handle);
    } else {
      release_open_inode(state, handle);
    }
    if (handle.write_state != WRITE_SEALED) {
      discard_pending_inode(state, handle);
    }
  } catch (...) {
    bool registered = false;
    {
      std::lock_guard guard(handle.mutex);
      registered        = handle.registered;
      handle.registered = false;
    }
    if (registered) {
      unregister_open_handle(state, handle.object_path, handle);
    } else {
      release_open_inode(state, handle);
    }
  }
}

void ngs3fs_release(fuse_req_t request, fuse_ino_t,
                    fuse_file_info* file) {
  State& state = state_from(request);
  std::unique_ptr<OpenHandle> handle;
  try {
    handle.reset(&handle_required(file));
  } catch (...) {
    reply_callback_error(request);
    return;
  }
  file->fh = 0;
  constexpr uint64_t closing    = 1ULL << 63;
  constexpr uint64_t active_mask = closing - 1;
  uint64_t requests =
      handle->request_state.fetch_or(closing, std::memory_order_acq_rel);
  while ((requests & active_mask) != 0) {
    handle->request_state.wait(requests, std::memory_order_acquire);
    requests = handle->request_state.load(std::memory_order_acquire);
  }
  if (handle->writer) {
    const std::string path = handle->object_path;
    handle->writer->release(state, *handle);
    finish_pending_delete(state, path);
  } else if (handle->registered) {
    std::string path;
    {
      std::shared_lock identity_guard(handle->identity_mutex);
      path = handle->object_path;
    }
    handle->registered = false;
    unregister_open_handle(state, path, *handle);
    finish_pending_delete(state, path);
  } else {
    release_open_inode(state, *handle);
  }
  fuse_reply_err(request, 0);
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
    {
      std::lock_guard guard(item.dir_children().mutation_mutex);
      if (item.detached()) {
        throw std::system_error(ESTALE, std::generic_category(), "opendir");
      }
      retain_inode_count(item.open_count, "directory open count");
    }
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
    DirHandle* handle = reinterpret_cast<DirHandle*>(uintptr_t(file->fh));
    InodeDir* directory = handle->item;
    InodeBase* item = inode == FUSE_ROOT_ID
                          ? static_cast<InodeBase*>(state.root_item.get())
                          : reinterpret_cast<InodeBase*>(uintptr_t(inode));
    if (item == nullptr || !item->directory() || directory != item) {
      throw std::system_error(ENOTDIR, std::generic_category(), "readdir");
    }
    uint8_t initialized = handle->initialized.load(std::memory_order_acquire);
    while (initialized != 2) {
      if (initialized == 0 && handle->initialized.compare_exchange_weak(
              initialized, 1, std::memory_order_acquire,
              std::memory_order_acquire)) {
        try {
          if (!directory->detached() &&
              directory->expire.load(std::memory_order_relaxed) <=
                  fuse_monotonic_ns()) {
            refresh_directory(state, inode);
          }
          handle->initialized.store(2, std::memory_order_release);
          handle->initialized.notify_all();
          initialized = 2;
          break;
        } catch (...) {
          handle->initialized.store(0, std::memory_order_release);
          handle->initialized.notify_all();
          throw;
        }
      }
      if (initialized == 1) {
        handle->initialized.wait(1, std::memory_order_acquire);
      }
      initialized = handle->initialized.load(std::memory_order_acquire);
    }
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
      if (opened == state.open_files.end() ||
          opened->second.handles != reader_locks.handles) {
        throw std::logic_error(
            "reader registry changed during native unlink");
      }
      const bool inserted = state.pending_deletes.try_emplace(
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
    if (opened == state.open_files.end() ||
        opened->second.handles != reader_locks.handles) {
      throw std::logic_error("reader registry changed during native rename");
    }
    for (OpenHandle* handle : opened->second.handles) {
      handle->object_path.assign(hidden_path);
      handle->key.assign(hidden_key);
    }
    auto node = state.open_files.extract(opened);
    node.key().assign(hidden_path);
    state.open_files.insert(std::move(node));
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
  return true;
}

void remove_item(State& state, InodeBase& item) {
  if (detach_cached_item(state, item)) {
    sweep_retired_items(state);
  }
}

void ngs3fs_unlink(fuse_req_t request, fuse_ino_t parent,
                   const char* name) {
  if (name == nullptr || !valid_fuse_component(name)) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  State& state = state_from(request);
  try {
    InodeBase& directory = inode_item(state, parent);
    if (!directory.directory()) {
      throw std::system_error(ENOTDIR, std::generic_category(), "unlink");
    }
    std::lock_guard directory_guard(
        directory.dir_children().mutation_mutex);
    refresh_directory_locked(state, parent);
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
          writer->condition.wait(writer_guard);
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

void ngs3fs_mkdir(fuse_req_t request, fuse_ino_t parent, const char* name,
                  mode_t) {
  if (name == nullptr || !valid_fuse_component(name)) {
    fuse_reply_err(request, EINVAL);
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
      refresh_directory_locked(state, parent);
      const std::string directory_key = item_key(state, directory);
      const std::string key = directory_key + name + '/';
      ListedChild child;
      child.name      = name;
      child.directory = true;
      put_empty_object(state, object_request_path(state, key), true);
      inode = install_item(
          state, parent, std::move(child), 0, true, false, true);
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
    refresh_directory_locked(state, parent);
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
    refresh_directory_locked(state, inode, true);
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
    InodePin source;
    InodePin destination;
    {
      DirectoryGuards parent_locks =
          lock_directories(state, {parent, new_parent});
      refresh_directory_locked(state, parent);
      if (new_parent != parent) {
        refresh_directory_locked(state, new_parent);
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
    refresh_directory_locked(state, parent);
    if (new_parent != parent) {
      refresh_directory_locked(state, new_parent);
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
      refresh_directory_locked(state, source_inode, true);
      {
        std::shared_lock guard(source->dir_children().mutex);
        if (!source->dir_children().empty()) {
          throw std::system_error(
              EXDEV, std::generic_category(),
              "non-empty S3 directory rename is not atomic");
        }
      }
      if (destination_inode != 0) {
        refresh_directory_locked(state, destination_inode, true);
        {
          std::shared_lock guard(destination->dir_children().mutex);
          if (!destination->dir_children().empty()) {
            throw std::system_error(ENOTEMPTY, std::generic_category(),
                                    "rename destination directory");
          }
        }
      }
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
  auto& state = *static_cast<State*>(fuse_req_userdata(request));
  struct statvfs status{};
  constexpr uint64_t frsize = 4096;
  status.f_bsize   = state.config.io_size;
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
      "      --max-cache-fetch-size BYTES\n"
      "                             maximum adaptive cache fill "
      "(default 8 MiB, minimum 1 MiB)\n"
      "      --io-size BYTES       statfs optimal I/O size; accepts "
      "KiB/MiB (default 256 KiB)\n"
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
      "  -R, --read-ahead BYTES    kernel read-ahead; accepts "
      "KiB/MiB (default 256 KiB)\n"
      "  -T, --dir-cache-timeout MS\n"
      "                             directory cache TTL (default 1000)\n"
      "  -I, --max-cached-inodes N soft inode-cache limit "
      "(default 1000000)\n"
      "  -P, --part-size BYTES     multipart part size (default 8 MiB)\n"
      "  -c, --max-uploads N       concurrent uploads (default 4)\n"
      "  -C, --max-connections N   connection pool size (default 8)\n"
      "  -B, --max-pinned-memory BYTES\n"
      "                             retained write budget (default 256 MiB)\n"
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

    if (fuse_set_signal_handlers(session) != 0) {
      fprintf(stderr, "warning: failed to install FUSE signal handlers\n");
    }
    if (fuse_session_mount(session, options.mountpoint) == 0) {
      const uint32_t requested_read_ahead = state.config.read_ahead_size;
      std::string read_ahead_error;
      if (!set_kernel_read_ahead(options.mountpoint,
                                 state.config.read_ahead_size,
                                 read_ahead_error)) {
        fprintf(stderr,
                "warning: unable to set kernel read-ahead to %u bytes: %s; "
                "falling back to kernel value %u bytes\n",
                requested_read_ahead, read_ahead_error.c_str(),
                state.config.read_ahead_size);
      }
      fuse_loop_config loop_config{};
      loop_config.clone_fd         = 1;
      loop_config.max_idle_threads = 10;
      {
        std::lock_guard guard(state.session_mutex);
        state.session = session;
      }
      result = fuse_session_loop_mt(session, &loop_config);
      {
        std::lock_guard guard(state.session_mutex);
        state.session = nullptr;
      }
      fuse_session_unmount(session);
    } else {
      fprintf(stderr, "failed to mount %s: %s\n",
              options.mountpoint, strerror(errno));
    }
    fuse_remove_signal_handlers(session);
    fuse_session_destroy(session);
  } catch (const std::exception& error) {
    fprintf(stderr, "ngs3fs startup failed: %s\n", error.what());
  }

  fuse_opt_free_args(&arguments);
  free(options.mountpoint);
  return result;
}
