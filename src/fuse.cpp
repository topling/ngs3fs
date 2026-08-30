#include "http.hpp"
#include "io.hpp"
#include "s3.hpp"

#include <fuse_lowlevel.h>

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
#include <future>
#include <fstream>
#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <exception>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

constexpr size_t kMaximumListResponseSize = 8U * 1024U * 1024U;
constexpr unsigned kDirectoryListLimit    = 1000;

struct MountConfig {
  std::string endpoint_host;
  uint16_t endpoint_port          = 80;
  std::string authority;
  std::string bucket;
  std::string bucket_path;
  std::string prefix;
  std::string region              = "us-east-1";
  Credentials credentials;
  size_t maximum_read_size        = kPreferredIoSize;
  size_t maximum_write_size       = kPreferredIoSize;
  uint32_t read_ahead_size        = kPreferredIoSize;
  uint64_t part_size              = 8ULL * 1024ULL * 1024ULL;
  uint64_t max_pinned_memory      = 256ULL * 1024ULL * 1024ULL;
  uint64_t directory_cache_ns     = 1000ULL * 1000ULL * 1000ULL;
  size_t max_cached_inodes        = 1'000'000;
  unsigned max_uploads            = 4;
  unsigned max_connections        = 8;
  uid_t uid                       = ::getuid();
  gid_t gid                       = ::getgid();
  mode_t file_mode                = 0644;
  mode_t directory_mode           = 0755;
  bool report_metrics             = false;
  bool directory_bucket           = false;
  bool tls                        = false;
};

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
  uint64_t bytes  = 0;
  unsigned number = 0;
};

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
              config.authority, config.tls);
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
        std::cerr << "unhandled upload job error: " << error.what() << '\n';
      } catch (...) {
        std::cerr << "unhandled upload job error\n";
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
  std::string etag;
  std::string version_id;
  std::string upload_id;
  std::vector<std::string> part_etags;
  std::unique_ptr<RetainedPart> current_part;
  uint64_t size             = 0;
  uint64_t stream_offset    = 0;
  size_t pending_parts      = 0;
  unsigned next_part_number = 1;
  WriteState write_state    = WRITE_OPEN;
  int write_error           = 0;
  bool current_reservation  = false;
  bool multipart_starting   = false;
  bool multipart_required   = false;
  bool part_limit_warned    = false;
  bool write_in_progress    = false;
  bool writable             = false;
  bool create_exclusive     = false;
  bool registered           = false;
  std::mutex mutex;
  std::condition_variable condition;
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

struct State {
  explicit State(MountConfig value)
      : config(std::move(value)),
        directory_mtime(wall_time_seconds()),
        root_item(std::make_unique<InodeDir>()),
        http(std::make_unique<HttpPool>(config)),
        uploads(std::make_unique<UploadScheduler>(config.max_uploads)),
        cache_reclaimer([this](std::stop_token stop) {
          cache_reclaim_loop(stop, this);
        }) {
    root_item->set_parent(root_item.get());
  }

  ~State() {
    cache_reclaimer.request_stop();
    cache_condition.notify_all();
    if (cache_reclaimer.joinable()) {
      cache_reclaimer.join();
    }
    uploads.reset();
    for (const auto& [inode, item] : retired_items) {
      (void)inode;
      delete_inode(item);
    }
    retired_items.clear();
  }

  struct OpenFileState {
    size_t readers = 0;
    bool writer     = false;
  };

  struct BlockedPath {
    const void* owner;
    std::string path;
    bool prefix;
  };

  MountConfig config;
  time_t directory_mtime;
  std::unique_ptr<InodeDir> root_item;
  AmzDateTimeCache date_time;
  std::mutex retired_mutex;
  std::mutex open_files_mutex;
  std::mutex cache_mutex;
  std::mutex metrics_mutex;
  std::mutex credentials_mutex;
  std::mutex rename_probe_mutex;
  std::condition_variable credentials_condition;
  std::mutex budget_mutex;
  std::condition_variable budget_condition;
  std::condition_variable cache_condition;
  std::map<fuse_ino_t, InodeBase*> retired_items;
  std::atomic<size_t> retired_count{0};
  std::map<std::string, OpenFileState, std::less<>> open_files;
  std::vector<BlockedPath> blocked_paths;
  std::unique_ptr<HttpPool> http;
  std::unique_ptr<UploadScheduler> uploads;
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
  bool budget_exhausted            = false;
  bool cache_budget_warned         = false;
  uint64_t cache_warning_ns        = 0;
  bool splice_available            = true;
  bool atomic_o_trunc              = false;
  std::atomic<int> rename_object_support{0};
  std::atomic<int> range_signing_mode{0};
  std::jthread cache_reclaimer;
};

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
                    const char* operation)
      : state_(&state), count_(second.empty() ? 1 : 2) {
    pending_[0] = State::BlockedPath{this, std::string(first), first_prefix};
    if (count_ == 2) {
      pending_[1] = State::BlockedPath{
          this, std::string(second), second_prefix};
    }

    std::lock_guard guard(state.open_files_mutex);
    for (const auto& [path, opened] : state.open_files) {
      (void)opened;
      for (size_t i = 0; i < count_; ++i) {
        if (blocked_path_contains(pending_[i], path)) {
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

bool retry_after_delay(const Response& response) noexcept {
  const auto header = response.headers.find("retry-after");
  if (header == response.headers.end()) {
    return false;
  }
  uint64_t seconds = 0;
  if (!parse_unsigned(sso_view(header->second), seconds)) {
    return false;
  }
  seconds = std::min<uint64_t>(seconds, 30);
  timespec delay{.tv_sec = time_t(seconds), .tv_nsec = 0};
  while (::nanosleep(&delay, &delay) != 0 && errno == EINTR) {
  }
  return true;
}

template<class Request>
Response request_with_retries(Request&& request, const char* operation,
                              bool* ambiguous = nullptr) {
  std::exception_ptr last_error;
  for (unsigned attempt = 0; attempt != 4; ++attempt) {
    bool delayed = false;
    try {
      Response response = request();
      if (!retryable_status(response.status) || attempt == 3) {
        return response;
      }
      if (ambiguous != nullptr) {
        *ambiguous = true;
      }
      std::cerr << "warning: retrying " << operation
                << " after HTTP status " << response.status
                << " attempt=" << attempt + 1 << '\n';
      delayed = retry_after_delay(response);
    } catch (...) {
      last_error = std::current_exception();
      if (ambiguous != nullptr) {
        *ambiguous = true;
      }
      if (attempt == 3) {
        std::rethrow_exception(last_error);
      }
      std::cerr << "warning: retrying " << operation
                << " after transport failure attempt="
                << attempt + 1 << '\n';
    }
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
  if (const char* value = getenv("AWS_ACCESS_KEY_ID")) {
    config.credentials.access_key_id = value;
  }
  if (const char* value = getenv("AWS_SECRET_ACCESS_KEY")) {
    config.credentials.secret_access_key = value;
  }
  if (const char* value = getenv("AWS_SESSION_TOKEN")) {
    config.credentials.session_token = value;
  }
  if (const char* value = getenv("AWS_REGION")) {
    config.region = value;
  } else if (const char* value = getenv("AWS_DEFAULT_REGION")) {
    config.region = value;
  }
}

std::string_view trim_ascii(std::string_view value) noexcept {
  while (!value.empty() && isspace(u_char(value.front())) != 0) {
    value.remove_prefix(1);
  }
  while (!value.empty() && isspace(u_char(value.back())) != 0) {
    value.remove_suffix(1);
  }
  return value;
}

void load_shared_credentials(MountConfig& config) {
  if (!config.credentials.access_key_id.empty()) {
    return;
  }
  std::string path;
  if (const char* value = getenv("AWS_SHARED_CREDENTIALS_FILE")) {
    path = value;
  } else if (const char* value = getenv("HOME")) {
    path = std::string(value) + "/.aws/credentials";
  } else {
    return;
  }
  const char* profile_value = getenv("AWS_PROFILE");
  const std::string profile =
      profile_value == nullptr ? "default" : profile_value;
  std::ifstream input(path);
  if (!input) {
    return;
  }
  bool selected = false;
  std::string line;
  while (std::getline(input, line)) {
    std::string_view text = trim_ascii(line);
    if (text.empty() || text.front() == '#' || text.front() == ';') {
      continue;
    }
    if (text.front() == '[' && text.back() == ']') {
      std::string_view section = trim_ascii(
          text.substr(1, text.size() - 2));
      if (section.starts_with("profile ")) {
        section.remove_prefix(8);
        section = trim_ascii(section);
      }
      selected = section == profile;
      continue;
    }
    if (!selected) {
      continue;
    }
    const size_t equal = text.find('=');
    if (equal == std::string_view::npos) {
      continue;
    }
    const std::string_view name = trim_ascii(text.substr(0, equal));
    const std::string_view value = trim_ascii(text.substr(equal + 1));
    if (name == "aws_access_key_id") {
      config.credentials.access_key_id.assign(value);
    } else if (name == "aws_secret_access_key") {
      config.credentials.secret_access_key.assign(value);
    } else if (name == "aws_session_token") {
      config.credentials.session_token.assign(value);
    }
  }
}

bool parse_arguments(int argc, char** argv, MountConfig& config,
                     fuse_args& fuse_arguments) {
  if (!add_fuse_argument(fuse_arguments, argv[0])) {
    return false;
  }

  constexpr option long_options[] = {
      {"endpoint-host", required_argument, nullptr, 'e'},
      {"endpoint-port", required_argument, nullptr, 'p'},
      {"authority", required_argument, nullptr, 'a'},
      {"bucket", required_argument, nullptr, 'b'},
      {"prefix", required_argument, nullptr, 'k'},
      {"region", required_argument, nullptr, 'r'},
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
      "-e:p:a:b:k:r:R:T:I:P:c:C:B:u:g:m:D:MShVdfso:";

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
  std::cerr << "warning: RLIMIT_NOFILE=" << limit.rlim_cur
            << " is below the estimated " << needed
            << " descriptors needed to use --max-pinned-memory="
            << config.max_pinned_memory
            << "; writes may fail before reaching the configured memory "
               "budget\n";
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
  try {
    throw;
  } catch (const std::system_error& error) {
    const int value = error.code().value();
    std::cerr << "ngs3fs request failed: " << error.what() << '\n';
    fuse_reply_err(request, value > 0 ? value : EIO);
  } catch (const std::exception& error) {
    std::cerr << "ngs3fs request failed: " << error.what() << '\n';
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

std::string session_xml_text(std::string_view xml,
                             std::string_view name) {
  const std::string open = '<' + std::string(name) + '>';
  const std::string close = "</" + std::string(name) + '>';
  const size_t begin = xml.find(open);
  if (begin == std::string_view::npos) {
    return {};
  }
  const size_t value = begin + open.size();
  const size_t end = xml.find(close, value);
  return end == std::string_view::npos
             ? std::string{}
             : std::string(xml.substr(value, end - value));
}

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
    std::vector<Header> headers{
        Header{"x-amz-create-session-mode", "ReadWrite"},
    };
    HeaderList authorization;
    authorization_headers_for_credentials(
        authorization, state, state.config.credentials, "GET", "/?session",
        headers, kEmptyPayloadSha256);
    headers.insert(headers.end(),
                   std::make_move_iterator(authorization.begin()),
                   std::make_move_iterator(authorization.end()));
    std::unique_ptr<HttpClient> client = HttpClient::connect(
        state.config.endpoint_host, state.config.endpoint_port,
        state.config.authority, state.config.tls);
    const Response response = client->request_no_body(
        "GET", "/?session", headers, kMaximumListResponseSize);
    if (response.status != 200) {
      throw std::runtime_error("CreateSession failed with status " +
                               std::to_string(response.status));
    }
    const std::string body(
        reinterpret_cast<const char*>(response.body.data()),
        response.body.size());
    credentials.access_key_id = session_xml_text(body, "AccessKeyId");
    credentials.secret_access_key =
        session_xml_text(body, "SecretAccessKey");
    credentials.session_token = session_xml_text(body, "SessionToken");
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
    authorization_headers_for_credentials(
        output, state, state.config.credentials, method, request_path,
        signed_headers, payload_hash, fast_get, datetime);
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
  augmented.push_back(Header{"x-amz-s3session-token", token});
  authorization_headers_for_credentials(
      output, state, credentials, method, request_path, augmented,
      payload_hash);
  output.push_back(Header{"x-amz-s3session-token", token});
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
  authorization_headers_for_credentials(
      output, state, state.config.credentials, method, request_path,
      signed_headers, payload_hash);
  return output;
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

[[noreturn]] void throw_s3_status(int status, const char* operation) {
  throw std::system_error(
      errno_for_s3_status(status), std::generic_category(),
      std::string(operation) + " failed with HTTP status " +
          std::to_string(status));
}

struct ObjectMetadata {
  uint64_t size = 0;
  std::string etag;
  std::string version_id;
};

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
    throw_s3_status(response.status, "S3 HEAD");
  }
  const auto content_length = response.headers.find("content-length");
  if (content_length == response.headers.end()) {
    throw std::runtime_error("S3 HEAD omitted content-length");
  }
  ObjectMetadata metadata;
  if (!parse_unsigned(sso_view(content_length->second), metadata.size)) {
    throw std::runtime_error("S3 HEAD returned invalid content-length");
  }
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
  return metadata;
}

bool recover_write_commit(State& state, const OpenHandle& handle,
                          Response& response) noexcept {
  try {
    const ObjectMetadata metadata = head_object(state, handle.object_path);
    if (metadata.size != handle.stream_offset ||
        (!handle.etag.empty() && metadata.etag == handle.etag)) {
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
    std::cerr << "warning: recovered an ambiguous S3 write commit with "
                 "HeadObject: "
              << handle.object_path << '\n';
    return true;
  } catch (...) {
    return false;
  }
}

void refresh_open_metadata(State& state, OpenHandle& handle) {
  ObjectMetadata metadata = head_object(state, handle.object_path);
  handle.item->fsize.store(metadata.size, std::memory_order_relaxed);
  handle.size        = metadata.size;
  handle.etag        = std::move(metadata.etag);
  handle.version_id  = std::move(metadata.version_id);
}

void register_open_handle(State& state, fuse_ino_t inode,
                          OpenHandle& handle) {
  std::lock_guard open_files_guard(state.open_files_mutex);
  InodeBase& base = inode_item(state, inode);
  if (!base.regular()) {
    throw std::system_error(EISDIR, std::generic_category(), "open");
  }
  InodeFile& item = static_cast<InodeFile&>(base);
  const std::string key = item_key(state, item);
  handle.inode       = inode;
  handle.item        = &item;
  handle.object_path = object_request_path(state, key);
  handle.size        = item.fsize.load(std::memory_order_relaxed);
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
  while (!item.open_count.compare_exchange_weak(
      count, count + 1, std::memory_order_relaxed,
      std::memory_order_relaxed)) {
    if (count == UINT32_MAX) {
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
  handle.id         = ++state.next_handle_id;
  handle.registered = true;
}

void unregister_open_handle(State& state, std::string_view path,
                            bool writable, InodeFile* item) noexcept {
  {
    std::lock_guard open_files_guard(state.open_files_mutex);
    const auto position = state.open_files.find(path);
    if (position != state.open_files.end()) {
      State::OpenFileState& opened = position->second;
      if (writable) {
        opened.writer = false;
      } else if (opened.readers != 0) {
        --opened.readers;
      }
      if (!opened.writer && opened.readers == 0) {
        state.open_files.erase(position);
      }
    }
  }
  if (item != nullptr) {
    if (release_inode_count(item->open_count)) {
      sweep_retired_items(state);
    }
  }
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
        std::cerr << "failed to discard transport receive credit: "
                  << error.what() << '\n';
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
  const int signing_mode =
      state.range_signing_mode.load(std::memory_order_relaxed);
  const bool sign_range = state.config.directory_bucket || signing_mode == 2;
  request.range_signed = sign_range;
  const std::array<uint64_t, 2> date = state.date_time.now();
  const bool cacheable = !state.config.directory_bucket && !sign_range;
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
    std::array<Header, 2> canonical_headers;
    size_t canonical_count = 0;
    if (use_if_match) {
      canonical_headers[canonical_count++] = if_match;
    }
    if (sign_range) {
      canonical_headers[canonical_count++] =
          Header{"range", ssostr<32>(range.data(), range_length)};
    }
    authorization_headers(
        headers, state, "GET", request.path(),
        std::span(canonical_headers.data(), canonical_count),
        kEmptyPayloadSha256, true,
        std::string_view(date_text.data(), date_text.size()));
    worker.authorization_count = headers.size();
    headers.push_back(
        Header{"range", ssostr<32>(range.data(), range_length)});
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

std::string query_path(std::string_view path, std::string_view query) {
  std::string result(path);
  result.push_back(path.find('?') == std::string_view::npos ? '?' : '&');
  result.append(query);
  return result;
}

std::string response_text(const Response& response) {
  return std::string(
      reinterpret_cast<const char*>(response.body.data()),
      response.body.size());
}

std::string xml_text(std::string_view xml, std::string_view name) {
  const std::string open = '<' + std::string(name) + '>';
  const std::string close = "</" + std::string(name) + '>';
  const size_t begin = xml.find(open);
  if (begin == std::string_view::npos) {
    return {};
  }
  const size_t value = begin + open.size();
  const size_t end = xml.find(close, value);
  if (end == std::string_view::npos) {
    return {};
  }
  return std::string(xml.substr(value, end - value));
}

std::string xml_decode(std::string_view text) {
  const auto append_code_point = [](std::string& output, uint32_t value) {
    if (value >= 0xd800 && value <= 0xdfff) {
      throw std::runtime_error("invalid XML surrogate in ListObjectsV2");
    }
    if (value <= 0x7f) {
      output.push_back(char(value));
    } else if (value <= 0x7ff) {
      output.push_back(char(0xc0 | (value >> 6)));
      output.push_back(char(0x80 | (value & 0x3f)));
    } else if (value <= 0xffff) {
      output.push_back(char(0xe0 | (value >> 12)));
      output.push_back(char(0x80 | ((value >> 6) & 0x3f)));
      output.push_back(char(0x80 | (value & 0x3f)));
    } else if (value <= 0x10ffff) {
      output.push_back(char(0xf0 | (value >> 18)));
      output.push_back(char(0x80 | ((value >> 12) & 0x3f)));
      output.push_back(char(0x80 | ((value >> 6) & 0x3f)));
      output.push_back(char(0x80 | (value & 0x3f)));
    } else {
      throw std::runtime_error("invalid XML code point in ListObjectsV2");
    }
  };
  std::string result;
  result.reserve(text.size());
  size_t p = 0;
  while (p < text.size()) {
    if (text[p] != '&') {
      result.push_back(text[p++]);
      continue;
    }
    const size_t end = text.find(';', p + 1);
    if (end == std::string_view::npos) {
      throw std::runtime_error("invalid XML entity in ListObjectsV2");
    }
    const std::string_view entity = text.substr(p, end - p + 1);
    if (entity == "&amp;") {
      result.push_back('&');
    } else if (entity == "&lt;") {
      result.push_back('<');
    } else if (entity == "&gt;") {
      result.push_back('>');
    } else if (entity == "&quot;") {
      result.push_back('"');
    } else if (entity == "&apos;") {
      result.push_back('\'');
    } else if (entity.starts_with("&#") && entity.size() > 3) {
      std::string_view digits = entity.substr(2, entity.size() - 3);
      int base = 10;
      if (!digits.empty() && (digits.front() == 'x' ||
                              digits.front() == 'X')) {
        digits.remove_prefix(1);
        base = 16;
      }
      uint32_t value = 0;
      const auto parsed = std::from_chars(
          digits.data(), digits.data() + digits.size(), value, base);
      if (digits.empty() || parsed.ec != std::errc{} ||
          parsed.ptr != digits.data() + digits.size()) {
        throw std::runtime_error(
            "invalid numeric XML entity in ListObjectsV2");
      }
      append_code_point(result, value);
    } else {
      throw std::runtime_error("unsupported XML entity in ListObjectsV2");
    }
    p = end + 1;
  }
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

template<class Function>
void for_each_xml_block(std::string_view xml, std::string_view name,
                        Function function) {
  const std::string open  = '<' + std::string(name) + '>';
  const std::string close = "</" + std::string(name) + '>';
  size_t p = 0;
  for (;;) {
    const size_t begin = xml.find(open, p);
    if (begin == std::string_view::npos) {
      return;
    }
    const size_t value = begin + open.size();
    const size_t end   = xml.find(close, value);
    if (end == std::string_view::npos) {
      throw std::runtime_error("truncated ListObjectsV2 XML");
    }
    function(xml.substr(value, end - value));
    p = end + close.size();
  }
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
    throw_s3_status(response.status, "ListObjectsV2");
  }
  const std::string xml = response_text(response);
  ListedPage page;
  page.children.reserve(kDirectoryListLimit);
  for_each_xml_block(xml, "Contents", [&](std::string_view block) {
    const std::string key = percent_decode(xml_decode(xml_text(block, "Key")));
    if (!key.starts_with(prefix) || key.size() == prefix.size()) {
      return;
    }
    std::string_view relative(key.data() + prefix.size(),
                              key.size() - prefix.size());
    const bool marker = relative.ends_with('/');
    if (marker) {
      relative.remove_suffix(1);
    }
    if (!valid_fuse_component(relative)) {
      return;
    }
    uint64_t size = 0;
    if (!parse_unsigned(xml_decode(xml_text(block, "Size")), size)) {
      throw std::runtime_error("ListObjectsV2 returned invalid Size");
    }
    ListedChild child;
    child.name.assign(relative);
    child.size      = size;
    child.directory = marker;
    const std::string modified =
        xml_decode(xml_text(block, "LastModified"));
    if (!modified.empty()) {
      child.mtime = parse_s3_mtime(modified);
    }
    page.children.push_back(std::move(child));
  });
  for_each_xml_block(xml, "CommonPrefixes", [&](std::string_view block) {
    std::string key = percent_decode(xml_decode(xml_text(block, "Prefix")));
    if (!key.starts_with(prefix) ||
        key.size() <= prefix.size() || key.back() != '/') {
      return;
    }
    key.pop_back();
    const std::string_view relative(
        key.data() + prefix.size(), key.size() - prefix.size());
    if (!valid_fuse_component(relative)) {
      return;
    }
    ListedChild child;
    child.name.assign(relative);
    child.directory = true;
    page.children.push_back(std::move(child));
  });
  page.truncated = xml_text(xml, "IsTruncated") == "true";
  if (page.truncated) {
    page.token = xml_decode(xml_text(xml, "NextContinuationToken"));
    if (page.token.empty()) {
      throw std::runtime_error("ListObjectsV2 omitted NextContinuationToken");
    }
  }
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

std::vector<ListedObject> list_prefix_objects(State& state,
                                              std::string_view prefix) {
  std::vector<ListedObject> objects;
  std::string token;
  for (;;) {
    std::string query = "encoding-type=url&list-type=2&prefix=";
    query += uri_encode(prefix, false);
    if (!token.empty()) {
      query += "&continuation-token=" + uri_encode(token, false);
    }
    const std::string path = query_path(
        state.config.bucket_path.empty() ? "/" : state.config.bucket_path,
        query);
    const Response response = request_with_retries([&] {
      const HeaderList headers = authorization_headers(
          state, "GET", path, {}, kEmptyPayloadSha256);
      HttpPool::Lease client = state.http->acquire();
      return client->request_no_body(
          "GET", path, headers, kMaximumListResponseSize);
    }, "ListObjectsV2 prefix scan");
    if (response.status != 200) {
      throw_s3_status(response.status, "ListObjectsV2 prefix scan");
    }
    const std::string xml = response_text(response);
    for_each_xml_block(xml, "Contents", [&](std::string_view block) {
      ListedObject object;
      object.key = percent_decode(xml_decode(xml_text(block, "Key")));
      if (!object.key.starts_with(prefix)) {
        return;
      }
      if (!parse_unsigned(xml_decode(xml_text(block, "Size")), object.size)) {
        throw std::runtime_error(
            "ListObjectsV2 prefix scan returned invalid Size");
      }
      object.etag = xml_decode(xml_text(block, "ETag"));
      const std::string modified =
          xml_decode(xml_text(block, "LastModified"));
      if (!modified.empty()) {
        object.mtime = parse_s3_mtime(modified);
      }
      objects.push_back(std::move(object));
    });
    if (xml_text(xml, "IsTruncated") != "true") {
      break;
    }
    token = xml_decode(xml_text(xml, "NextContinuationToken"));
    if (token.empty()) {
      throw std::runtime_error(
          "ListObjectsV2 prefix scan omitted NextContinuationToken");
    }
  }
  return objects;
}

template<class Function>
void for_each_prefix_page(State& state, std::string_view prefix,
                          Function function) {
  std::string token;
  for (;;) {
    std::string query = "encoding-type=url&list-type=2&prefix=";
    query += uri_encode(prefix, false);
    if (!token.empty()) {
      query += "&continuation-token=" + uri_encode(token, false);
    }
    const std::string path = query_path(
        state.config.bucket_path.empty() ? "/" : state.config.bucket_path,
        query);
    const Response response = request_with_retries([&] {
      const HeaderList headers = authorization_headers(
          state, "GET", path, {}, kEmptyPayloadSha256);
      HttpPool::Lease client = state.http->acquire();
      return client->request_no_body(
          "GET", path, headers, kMaximumListResponseSize);
    }, "ListObjectsV2 streaming prefix scan");
    if (response.status != 200) {
      throw_s3_status(response.status,
                      "ListObjectsV2 streaming prefix scan");
    }
    const std::string xml = response_text(response);
    std::vector<ListedObject> page;
    for_each_xml_block(xml, "Contents", [&](std::string_view block) {
      ListedObject object;
      object.key = percent_decode(xml_decode(xml_text(block, "Key")));
      if (!object.key.starts_with(prefix)) {
        return;
      }
      if (!parse_unsigned(xml_decode(xml_text(block, "Size")), object.size)) {
        throw std::runtime_error(
            "ListObjectsV2 streaming scan returned invalid Size");
      }
      object.etag = xml_decode(xml_text(block, "ETag"));
      const std::string modified =
          xml_decode(xml_text(block, "LastModified"));
      if (!modified.empty()) {
        object.mtime = parse_s3_mtime(modified);
      }
      page.push_back(std::move(object));
    });
    function(std::move(page));
    if (xml_text(xml, "IsTruncated") != "true") {
      return;
    }
    token = xml_decode(xml_text(xml, "NextContinuationToken"));
    if (token.empty()) {
      throw std::runtime_error(
          "ListObjectsV2 streaming scan omitted NextContinuationToken");
    }
  }
}

bool prefix_has_objects(State& state, std::string_view prefix) {
  std::string query = "encoding-type=url&list-type=2&max-keys=1&prefix=";
  query += uri_encode(prefix, false);
  const std::string path = query_path(
      state.config.bucket_path.empty() ? "/" : state.config.bucket_path,
      query);
  const Response response = request_with_retries([&] {
    const HeaderList headers = authorization_headers(
        state, "GET", path, {}, kEmptyPayloadSha256);
    HttpPool::Lease client = state.http->acquire();
    return client->request_no_body(
        "GET", path, headers, kMaximumListResponseSize);
  }, "ListObjectsV2 rename verification");
  if (response.status != 200) {
    throw_s3_status(response.status, "ListObjectsV2 rename verification");
  }
  const std::string xml = response_text(response);
  bool found = false;
  for_each_xml_block(xml, "Contents", [&](std::string_view block) {
    const std::string key =
        percent_decode(xml_decode(xml_text(block, "Key")));
    found = found || key.starts_with(prefix);
  });
  return found;
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
      terark::fstring(name.data(), name.size()));
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
    for (auto [name, child] : item.dir_children()) {
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
    for (auto [name, child] : item.dir_children()) {
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
    try {
      std::cerr << "error: inode cache accounting underflow: used=" << old
                << " deleting=" << count << '\n';
    } catch (...) {
    }
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
  for (auto i = state.retired_items.begin();
       i != state.retired_items.end();) {
    InodeBase* item = i->second;
    if (!item_tree_reclaimable(*item)) {
      ++i;
      continue;
    }
    i = state.retired_items.erase(i);
    state.retired_count.fetch_sub(1, std::memory_order_relaxed);
    cache_inode_tree_deleted(state, *item);
    delete_inode(item);
  }
}

void retire_item(State& state, InodeBase& item) {
  mark_item_tree_detached(state, item);
  std::lock_guard guard(state.retired_mutex);
  const auto retired = state.retired_items.emplace(item_inode(&item), &item);
  if (!retired.second) {
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
    item.dentry_slot = UINT32_MAX;
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
  {
    std::unique_lock guard(children.mutex);
    if (parent_item.detached()) {
      throw std::system_error(ESTALE, std::generic_category(), "directory");
    }
    const terark::fstring key(child.name.data(), child.name.size());
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
      stale->dentry_slot     = UINT32_MAX;
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
      file.mtime.store(child.mtime, std::memory_order_relaxed);
      file.fsize.store(child.size, std::memory_order_relaxed);
    }
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
      child->dentry_slot = UINT32_MAX;
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
            terark::fstring(child.name.data(), child.name.size()));
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
  try {
    std::cerr << "warning: inode cache remains above soft limit: used="
              << state.cached_inodes.load(std::memory_order_relaxed)
              << " limit=" << state.config.max_cached_inodes
              << "; referenced, open, pending, or fresh entries are retained\n";
  } catch (...) {
  }
}

void cache_reclaim_loop(std::stop_token stop, State* state) noexcept {
  while (!stop.stop_requested()) {
    size_t scan_limit = 0;
    {
      std::unique_lock guard(state->cache_mutex);
      state->cache_condition.wait_for(
          guard, std::chrono::milliseconds(250), [&] {
            return stop.stop_requested() ||
                state->cached_inodes.load(std::memory_order_acquire) >
                    state->config.max_cached_inodes;
          });
      if (stop.stop_requested()) {
        return;
      }
      if (state->cached_inodes.load(std::memory_order_acquire) <=
          state->config.max_cached_inodes) {
        state->cache_budget_warned = false;
        continue;
      }
      scan_limit = state->cache_clock_size * 2 + 1;
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
        try {
          std::cerr << "warning: inode cache reclamation failed: "
                    << error.what() << '\n';
        } catch (...) {
        }
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

void update_written_metadata(OpenHandle& handle,
                             const Response& response,
                             std::string_view body_etag = {}) {
  InodeFile& item = *handle.item;
  item.fsize.store(handle.stream_offset, std::memory_order_relaxed);
  item.mtime.store(wall_time_seconds(), std::memory_order_relaxed);
  item.set_pending(false);
  const auto etag = response.headers.find("etag");
  if (etag != response.headers.end()) {
    assign_string(handle.etag, etag->second);
  } else if (!body_etag.empty()) {
    handle.etag.assign(body_etag);
  } else {
    handle.etag.clear();
  }
  const auto version = response.headers.find("x-amz-version-id");
  if (version != response.headers.end() && version->second != "null") {
    assign_string(handle.version_id, version->second);
  } else {
    handle.version_id.clear();
  }
}

void require_s3_success(const Response& response, const char* operation) {
  if (response.status < 200 || response.status >= 300) {
    throw_s3_status(response.status, operation);
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
    std::cerr << "warning: pinned write budget exhausted: used="
              << state.pinned_bytes
              << " limit=" << state.config.max_pinned_memory
              << " part_size=" << state.config.part_size
              << " waiting_handles=" << state.waiting_writers << '\n';
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
      std::cerr << "warning: pinned write budget accounting underflow\n";
      state.pinned_bytes = 0;
    } else {
      state.pinned_bytes -= state.config.part_size;
    }
    if (state.budget_exhausted &&
        state.pinned_bytes <=
            state.config.max_pinned_memory - state.config.part_size) {
      std::cerr << "pinned write budget recovered: used="
                << state.pinned_bytes
                << " limit=" << state.config.max_pinned_memory << '\n';
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
      const HeaderList headers = authorization_headers(
          state, "POST", path, {}, kEmptyPayloadSha256);
      HttpPool::Lease client = state.http->acquire();
      response = client->request_no_body("POST", path, headers);
    } catch (...) {
      std::cerr << "error: CreateMultipartUpload outcome unknown; an orphan "
                   "upload may require lifecycle cleanup: "
                << handle.object_path << '\n';
      throw;
    }
    if (retryable_status(response.status)) {
      std::cerr << "error: not retrying ambiguous CreateMultipartUpload: "
                << "status=" << response.status
                << " path=" << handle.object_path << '\n';
    }
    require_s3_success(response, "CreateMultipartUpload");
    std::string upload_id = xml_text(response_text(response), "UploadId");
    if (upload_id.empty()) {
      throw std::runtime_error(
          "CreateMultipartUpload response omitted UploadId");
    }
    {
      std::lock_guard guard(handle.mutex);
      handle.upload_id = std::move(upload_id);
      handle.part_etags.reserve(kMaximumMultipartParts);
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
                        const RetainedPart& part) {
  ensure_multipart(state, handle);
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
    append_authorization(headers, state, "PUT", path, kUnsignedPayload);
    HttpPool::Lease client = state.http->acquire_bulk();
    send_retained_body(client.client(), "PUT", path, headers, part);
    return client->finish_upload();
  }, "UploadPart");
  require_s3_success(response, "UploadPart");
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
  int error_code                 = 0;
  try {
    {
      std::lock_guard guard(handle.mutex);
      if (handle.write_state == WRITE_FAILED) {
        throw std::system_error(handle.write_error,
                                std::generic_category(),
                                "write handle failed");
      }
    }
    etag = upload_part(state, handle, *part);
  } catch (const std::system_error& error) {
    error_code = error.code().value();
  } catch (...) {
    error_code = EIO;
  }
  part.reset();
  release_part_budget(state);
  std::lock_guard guard(handle.mutex);
  if (error_code == 0) {
    handle.part_etags[part_number - 1] = std::move(etag);
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
    std::cerr << "warning: copying memory-backed FUSE_WRITE data: bytes="
              << bytes << " cumulative_bytes="
              << state.fallback_write_bytes << '\n';
    state.fallback_warning_ns = now;
  }
}

std::string complete_body(const OpenHandle& handle) {
  std::string body;
  body.reserve(64 + handle.part_etags.size() * 96);
  body += "<CompleteMultipartUpload>";
  for (size_t i = 0; i < handle.part_etags.size(); ++i) {
    body += "<Part><PartNumber>";
    body += std::to_string(i + 1);
    body += "</PartNumber><ETag>";
    body += handle.part_etags[i];
    body += "</ETag></Part>";
  }
  body += "</CompleteMultipartUpload>";
  return body;
}

Response put_object(State& state, OpenHandle& handle,
                    const RetainedPart* part) {
  bool ambiguous = false;
  try {
    Response response = request_with_retries([&] {
      std::vector<Header> headers{
          Header{"content-type", "application/octet-stream"},
      };
      if (!handle.etag.empty()) {
        headers.push_back(Header{"if-match", handle.etag});
      } else if (handle.create_exclusive) {
        headers.push_back(Header{"if-none-match", "*"});
      }
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
    return response;
  } catch (...) {
    if (ambiguous) {
      Response recovered;
      if (recover_write_commit(state, handle, recovered)) {
        return recovered;
      }
      std::cerr << "error: PutObject commit outcome unknown: "
                << handle.object_path << '\n';
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
  const std::string body = complete_body(handle);
  std::vector<Header> headers{
      Header{"content-type", "application/xml"},
  };
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
        update_written_metadata(handle, recovered);
        handle.upload_id.clear();
        handle.part_etags.clear();
        return;
      }
      std::cerr << "error: CompleteMultipartUpload commit outcome unknown: "
                << handle.object_path << '\n';
      throw std::system_error(EIO, std::generic_category(),
                              "multipart commit outcome unknown");
    }
    throw;
  }
  if (ambiguous && response.status == 404) {
    Response recovered;
    if (recover_write_commit(state, handle, recovered)) {
      update_written_metadata(handle, recovered);
      handle.upload_id.clear();
      handle.part_etags.clear();
      return;
    }
    std::cerr << "error: CompleteMultipartUpload commit outcome unknown: "
              << handle.object_path << '\n';
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
  const std::string xml = response_text(response);
  if (xml.find("<Error>") != std::string::npos) {
    throw std::runtime_error("CompleteMultipartUpload returned an S3 error");
  }
  update_written_metadata(handle, response, xml_text(xml, "ETag"));
  handle.upload_id.clear();
  handle.part_etags.clear();
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
      part.reset();
      if (reserved) {
        release_part_budget(state);
      }
      update_written_metadata(handle, response);
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
      for (const std::string& etag : handle.part_etags) {
        if (etag.empty()) {
          throw std::system_error(EIO, std::generic_category(),
                                  "multipart upload omitted a part ETag");
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
    std::cerr << "failed to abort S3 multipart upload: "
              << error.what() << '\n';
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
  const char* data = response.body.empty()
                         ? ""
                         : reinterpret_cast<const char*>(response.body.data());
  const std::string body(data, response.body.size());
  return body.find("NotImplemented") != std::string::npos ||
         body.find("InvalidRequest") != std::string::npos || body.empty();
}

bool rename_object_source_missing(const Response& response) {
  if (response.status != 400 && response.status != 404) {
    return false;
  }
  const char* data = response.body.empty()
                         ? ""
                         : reinterpret_cast<const char*>(response.body.data());
  const std::string body(data, response.body.size());
  return body.find("NoSuchKey") != std::string::npos ||
         body.find("NoSuchObject") != std::string::npos;
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
    throw std::runtime_error(
        "failed to remove RenameObject capability probe with status " +
        std::to_string(response.status));
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
  throw std::runtime_error("RenameObject capability probe failed with status " +
                           std::to_string(response.status));
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
    std::cerr << "warning: failed to abort multipart copy: "
              << error.what() << '\n';
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
    std::cerr << "error: multipart-copy CreateMultipartUpload outcome "
                 "unknown; lifecycle cleanup may be required: "
              << destination << '\n';
    throw;
  }
  require_s3_success(created, "multipart-copy CreateMultipartUpload");
  const std::string upload_id =
      session_xml_text(std::string_view(
          reinterpret_cast<const char*>(created.body.data()),
          created.body.size()), "UploadId");
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
      const std::string body(
          reinterpret_cast<const char*>(copied.body.data()),
          copied.body.size());
      std::string etag = session_xml_text(body, "ETag");
      if (etag.empty() || body.find("<Error>") != std::string::npos) {
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
    append_authorization(headers, state, "POST", complete_path,
                         kUnsignedPayload);
    HttpPool::Lease client = state.http->acquire();
    client->begin_upload("POST", complete_path, headers, body.size());
    client->upload_bytes(std::span(
        reinterpret_cast<const std::byte*>(body.data()), body.size()));
    Response completed;
    try {
      completed = client->finish_upload();
    } catch (...) {
      complete_outcome_unknown = true;
      throw;
    }
    if (completed.status == 412 && no_replace) {
      throw std::system_error(EEXIST, std::generic_category(),
                              "multipart-copy destination exists");
    }
    require_s3_success(completed, "multipart-copy completion");
    const std::string completed_body(
        reinterpret_cast<const char*>(completed.body.data()),
        completed.body.size());
    if (completed_body.find("<Error>") != std::string::npos) {
      throw std::runtime_error(
          "multipart-copy completion returned an S3 error");
    }
  } catch (...) {
    if (complete_outcome_unknown) {
      std::cerr << "error: multipart-copy completion outcome unknown: "
                << destination << '\n';
    } else {
      abort_copy_upload(state, destination, upload_id);
    }
    throw;
  }
}

void rename_remote_object(State& state, std::string_view key,
                          uint64_t size, std::string_view etag,
                          std::string_view version_id,
                          std::string_view destination,
                          bool no_replace) {
  const std::string source = object_request_path(state, key);
  const std::string rename_source = request_path_without_query(source);
  const std::string_view source_etag = etag;

  if (state.rename_object_support.load(std::memory_order_acquire) == 0) {
    std::lock_guard probe_guard(state.rename_probe_mutex);
    if (state.rename_object_support.load(std::memory_order_relaxed) == 0) {
      state.rename_object_support.store(
          probe_rename_object(state, source) ? 1 : -1,
          std::memory_order_release);
    }
  }

  if (state.rename_object_support.load(std::memory_order_acquire) > 0) {
    const std::string rename_path = std::string(destination) + "?renameObject";
    const std::string client_token = rename_client_token();
    std::vector headers{
        Header{"x-amz-rename-source", rename_source},
        Header{"x-amz-client-token", client_token},
    };
    if (!source_etag.empty()) {
      headers.push_back(
          Header{"x-amz-rename-source-if-match", source_etag});
    }
    if (no_replace) {
      headers.push_back(Header{"if-none-match", "*"});
    }
    const Response response = request_with_retries([&] {
      std::vector<Header> request_headers = headers;
      auto authorization = authorization_headers(
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
      return;
    }
    if (response.status == 412) {
      throw std::system_error(
          no_replace ? EEXIST : ESTALE, std::generic_category(),
          no_replace ? "RenameObject destination exists"
                     : "RenameObject source changed");
    }
    if (!rename_object_unsupported(response)) {
      throw std::runtime_error("RenameObject failed with status " +
                               std::to_string(response.status));
    }
    state.rename_object_support.store(-1, std::memory_order_release);
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
    const char* copied_data =
        copied.body.empty()
            ? ""
            : reinterpret_cast<const char*>(copied.body.data());
    const std::string copied_body(copied_data, copied.body.size());
    if (copied.status == 412) {
      throw std::system_error(
          no_replace ? EEXIST : ESTALE, std::generic_category(),
          no_replace ? "CopyObject destination exists"
                     : "CopyObject source changed");
    }
    if (copied.status != 200 ||
        copied_body.find("<Error>") != std::string::npos) {
      const bool marker_copy_unsupported =
          size == 0 && key.ends_with('/') &&
          (copied.status == 400 || copied.status == 405 ||
           copied.status == 501);
      if (marker_copy_unsupported) {
        put_empty_object(state, destination);
        delete_object(state, key, etag);
        return;
      }
      throw std::runtime_error(
          "CopyObject rename fallback failed with status " +
          std::to_string(copied.status));
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
    throw std::runtime_error(
        "CopyObject succeeded but DeleteObject failed with status " +
        std::to_string(deleted.status));
  }
}

void rename_remote_directory(State& state, std::string_view source_prefix,
                             std::string_view destination_prefix) {
  std::cerr << "warning: starting non-atomic directory rename: source="
            << source_prefix << " destination=" << destination_prefix
            << '\n';
  const size_t concurrency = std::max<size_t>(
      1, std::min<size_t>(state.config.max_uploads,
                          state.config.max_connections - 1));
  std::deque<std::future<void>> active;
  std::atomic<size_t> completed{0};
  size_t discovered = 0;
  std::exception_ptr failure;
  const auto wait_one = [&] {
    if (active.empty()) {
      return;
    }
    try {
      active.front().get();
    } catch (...) {
      if (!failure) {
        failure = std::current_exception();
      }
    }
    active.pop_front();
  };
  try {
    for_each_prefix_page(state, source_prefix,
                         [&](std::vector<ListedObject> page) {
      for (ListedObject& object : page) {
        while (active.size() >= concurrency) {
          wait_one();
          if (failure) {
            std::rethrow_exception(failure);
          }
        }
        ++discovered;
        active.push_back(std::async(
            std::launch::async,
            [&state, object = std::move(object),
             source_prefix = std::string(source_prefix),
             destination_prefix = std::string(destination_prefix),
             &completed] () mutable {
          const std::string_view suffix(
              object.key.data() + source_prefix.size(),
              object.key.size() - source_prefix.size());
          const std::string destination_key =
              destination_prefix + std::string(suffix);
          rename_remote_object(
              state, object.key, object.size, object.etag, {},
              object_request_path(state, destination_key), false);
          completed.fetch_add(1, std::memory_order_relaxed);
        }));
      }
    });
  } catch (const std::exception& error) {
    while (!active.empty()) {
      wait_one();
    }
    std::cerr << "error: non-atomic directory rename partially completed: "
              << "source=" << source_prefix
              << " destination=" << destination_prefix
              << " completed=" << completed.load(std::memory_order_relaxed)
              << " discovered=" << discovered
              << " error=" << error.what() << '\n';
    throw std::system_error(EIO, std::generic_category(),
                            "directory rename partially completed");
  }
  while (!active.empty()) {
    wait_one();
  }
  if (failure) {
    try {
      std::rethrow_exception(failure);
    } catch (const std::exception& error) {
      std::cerr << "error: non-atomic directory rename partially completed: "
                << "source=" << source_prefix
                << " destination=" << destination_prefix
                << " completed="
                << completed.load(std::memory_order_relaxed)
                << " discovered=" << discovered
                << " error=" << error.what() << '\n';
    }
    throw std::system_error(EIO, std::generic_category(),
                            "directory rename partially completed");
  }
  if (discovered == 0) {
    throw std::system_error(ENOENT, std::generic_category(),
                            "directory prefix has no objects");
  }
  if (prefix_has_objects(state, source_prefix)) {
    std::cerr << "error: objects appeared or remained under the source "
                 "prefix during directory rename: "
              << source_prefix << '\n';
    throw std::system_error(EAGAIN, std::generic_category(),
                            "directory changed during rename");
  }
}

void ngs3fs_init(void* userdata, fuse_conn_info* connection) {
  auto& state = *static_cast<State*>(userdata);
  unsigned int desired = FUSE_CAP_ASYNC_READ | FUSE_CAP_ATOMIC_O_TRUNC;
  if (state.splice_available) {
    desired |= FUSE_CAP_SPLICE_READ | FUSE_CAP_SPLICE_WRITE |
               FUSE_CAP_SPLICE_MOVE;
  }
  connection->want &= ~FUSE_CAP_WRITEBACK_CACHE;
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
    refresh_directory(state, parent);
    cache_touch(static_cast<InodeDir&>(directory));
    const double timeout = remaining_directory_timeout(directory);
    fuse_ino_t inode = 0;
    fuse_entry_param entry{};
    {
      Directory& children = directory.dir_children();
      std::shared_lock guard(children.mutex);
      const auto i = children.find(
          terark::fstring(name, strlen(name)));
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
        update_written_metadata(truncated, response);
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
  bool registered = false;
  bool budget_reserved = false;
  InodeFile* registered_item = nullptr;
  std::string registered_path;
  try {
    auto handle = std::make_unique<OpenHandle>();
    handle->writable = writable;
    InodeBase& item = inode_item(state, inode);
    if (item.directory()) {
      throw std::system_error(EISDIR, std::generic_category(), "open");
    }
    if (writable && (file->flags & O_TRUNC) == 0 &&
        !item.truncate_pending()) {
      throw std::system_error(EOPNOTSUPP, std::generic_category(),
                              "write open requires truncation");
    }
    register_open_handle(state, inode, *handle);
    if (writable) {
      inode_item(state, inode).set_truncate_pending(false);
    }
    registered_path = handle->object_path;
    registered_item = handle->item;
    registered      = true;
    if (!writable || !handle->create_exclusive) {
      refresh_open_metadata(state, *handle);
    }

    if (writable) {
      if (!reserve_part_budget(state, false)) {
        throw std::system_error(EAGAIN, std::generic_category(),
                                "pinned write budget exhausted");
      }
      budget_reserved = true;
      handle->current_reservation = true;
      handle->size = 0;
      handle->stream_offset = 0;
    }

    file->fh = reinterpret_cast<uint64_t>(handle.release());
    budget_reserved = false;
    // All handles stay buffered. O_RDWR is rejected above, so Linux rejects
    // MAP_SHARED writable mappings without requiring direct I/O.
    file->direct_io   = 0;
    file->keep_cache  = 0;
    file->nonseekable = writable ? 1 : 0;
    file->noflush     = writable ? 0 : 1;
    if (fuse_reply_open(request, file) != 0) {
      std::unique_ptr<OpenHandle> failed_handle(handle_optional(file));
      file->fh = 0;
      if (failed_handle->current_reservation) {
        release_part_budget(state);
        failed_handle->current_reservation = false;
      }
      unregister_open_handle(state, failed_handle->object_path, writable,
                             failed_handle->item);
      registered = false;
    }
  } catch (...) {
    if (budget_reserved) {
      release_part_budget(state);
    }
    if (registered) {
      unregister_open_handle(state, registered_path, writable,
                             registered_item);
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
  InodeFile* item      = nullptr;
  std::string path;
  try {
    InodeBase& directory = inode_item(state, parent);
    if (!directory.directory()) {
      throw std::system_error(ENOTDIR, std::generic_category(), "create");
    }
    auto handle = std::make_unique<OpenHandle>();
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
      item       = handle->item;
      registered = true;
      timeout    = remaining_directory_timeout(directory);
    }
    if (!reserve_part_budget(state, false)) {
      throw std::system_error(EAGAIN, std::generic_category(),
                              "pinned write budget exhausted");
    }
    budget_reserved             = true;
    handle->current_reservation = true;

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
      unregister_open_handle(state, failed->object_path, true,
                             failed->item);
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
      unregister_open_handle(state, path, true, item);
    }
    reply_callback_error(request);
  }
}

void ngs3fs_read(fuse_req_t request, fuse_ino_t inode, size_t size,
                 off_t offset, fuse_file_info* file) {
  if (offset < 0) {
    fuse_reply_err(request, EINVAL);
    return;
  }

  try {
    State& state = state_from(request);
    OpenHandle& handle = handle_required(file);
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
    const bool report_metrics = state.config.report_metrics;
    const uint64_t fuse_start_ns =
        report_metrics ? fuse_monotonic_ns() : 0;
    const uint64_t cpu_start_ns =
        report_metrics ? fuse_thread_cpu_ns() : 0;

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
            range_request.headers, false, report_metrics);
        if (response.status == 403 && !state.config.directory_bucket &&
            !range_request.range_signed && attempt != 3) {
          int expected = 0;
          const bool selected_signed =
              state.range_signing_mode.compare_exchange_strong(
                  expected, 2, std::memory_order_relaxed);
          const bool retry_signed = selected_signed || expected == 2;
          if (retry_signed) {
            if (selected_signed) {
              std::cerr << "warning: S3 endpoint rejected an unsigned Range "
                           "header; signing Range for this mount\n";
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
        if (retryable_status(response.status) && attempt != 3) {
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
    const bool full_object_response =
        response.status == 200 && unsigned_offset == 0 &&
        wanted == object_size;
    if ((response.status != 206 && !full_object_response) ||
        response.body_bytes != wanted) {
      if (response.status != 200 && response.status != 206) {
        throw_s3_status(response.status, "GetObject range");
      }
      throw std::runtime_error(
          "unexpected S3 range response bytes=" +
          std::to_string(response.body_bytes));
    }

    fuse_bufvec buffers = FUSE_BUFVEC_INIT(wanted);
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
      std::cerr << "{\"event\":\"read\",\"offset\":"
                << unsigned_offset << ",\"bytes\":" << wanted
                << ",\"fetched_bytes\":" << wanted
                << ",\"cache_hit\":false"
                << ",\"total_ns\":" << total_ns
                << ",\"transport_span_ns\":" << transport_span_ns
                << ",\"residual_ns\":" << residual_ns
                << ",\"cpu_ns\":" << cpu_complete_ns - cpu_start_ns
                << ",\"external_bytes\":"
                << response.externally_spliced_bytes
                << ",\"fallback_bytes\":"
                << response.fallback_copied_bytes
                << ",\"transport_splice_calls\":"
                << response.transport_splice_calls << "}\n";
    }

    // Return transport flow-control credit only after libfuse has drained the
    // pipe into /dev/fuse. HTTP/1.1 has no application-managed credit, so its
    // consume operation is intentionally a no-op.
    if (reply_result != 0) {
      std::cerr << "fuse_reply_data failed: "
                << strerror(-reply_result) << '\n';
    }
    try {
      credit.consume();
    } catch (const std::exception& error) {
      // The FUSE request was already answered. Do not attempt a second reply.
      std::cerr << "failed to return transport receive credit: " << error.what()
                << '\n';
    }
    if (reply_result == 0) {
      worker.transport_pipe = std::move(pipe);
    }
  } catch (...) {
    reply_callback_error(request);
  }
}

void ngs3fs_write_buf(fuse_req_t request, fuse_ino_t inode,
                      fuse_bufvec* input, off_t offset,
                      fuse_file_info* file) {
  if (offset < 0) {
    fuse_reply_err(request, EINVAL);
    return;
  }
  if (input == nullptr) {
    fuse_reply_err(request, EINVAL);
    return;
  }

  try {
    State& state = state_from(request);
    OpenHandle& handle = handle_required(file);
    if (handle.inode != inode) {
      throw std::system_error(EBADF, std::generic_category(), "write inode");
    }
    if (!handle.writable) {
      throw std::system_error(EBADF, std::generic_category(), "write_buf");
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
      std::cerr << "warning: sequential write is approaching the configured "
                   "10,000-part limit: bytes="
                << end << " maximum=" << maximum_size << '\n';
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
    fuse_reply_write(request, length);
  } catch (...) {
    reply_callback_error(request);
  }
}

void ngs3fs_flush(fuse_req_t request, fuse_ino_t,
                  fuse_file_info* file) {
  try {
    OpenHandle& handle = handle_required(file);
    if (!handle.writable) {
      fuse_reply_err(request, 0);
      return;
    }
    State& state = state_from(request);
    seal_write(state, handle);
    bool unregister = false;
    {
      std::lock_guard handle_guard(handle.mutex);
      unregister = handle.registered;
      handle.registered = false;
    }
    if (unregister) {
      unregister_open_handle(state, handle.object_path, true, handle.item);
    }
    fuse_reply_err(request, 0);
  } catch (...) {
    reply_callback_error(request);
  }
}

void ngs3fs_fsync(fuse_req_t request, fuse_ino_t, int,
                  fuse_file_info* file) {
  try {
    OpenHandle& handle = handle_required(file);
    if (!handle.writable) {
      fuse_reply_err(request, 0);
      return;
    }
    std::unique_lock handle_guard(handle.mutex);
    while (handle.write_in_progress) {
      handle.condition.wait(handle_guard);
    }
    if (handle.write_state == WRITE_FAILED) {
      throw std::system_error(handle.write_error,
                              std::generic_category(),
                              "write handle failed");
    }
    fuse_reply_err(request, 0);
  } catch (...) {
    reply_callback_error(request);
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

  bool registered;
  bool release_budget = false;
  bool discard_pending = false;
  if (handle->writable) {
    std::unique_lock handle_guard(handle->mutex);
    while (handle->write_in_progress ||
           handle->write_state == WRITE_SEALING) {
      handle->condition.wait(handle_guard);
    }
    if (handle->write_state == WRITE_OPEN) {
      fail_write(*handle, EIO);
    }
    while (handle->pending_parts != 0) {
      handle->condition.wait(handle_guard);
    }
    release_budget = handle->current_reservation;
    handle->current_reservation = false;
    handle->current_part.reset();
    abort_multipart(state, *handle);
    discard_pending = handle->write_state != WRITE_SEALED;
    registered = handle->registered;
    handle->registered = false;
  } else {
    registered = handle->registered;
    handle->registered = false;
  }
  if (discard_pending) {
    InodeFile* item = handle->item;
    if (item != nullptr && !item->detached() && item->pending()) {
      InodeBase* parent = item->parent();
      if (parent != nullptr && parent->directory() && !parent->detached()) {
        std::lock_guard guard(parent->dir_children().mutation_mutex);
        if (!item->detached() && item->pending()) {
          remove_item(state, *item);
        }
      }
    }
  }
  if (registered) {
    unregister_open_handle(state, handle->object_path, handle->writable,
                           handle->item);
  }
  if (release_budget) {
    release_part_budget(state);
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
    release_inode_count(item->open_count);
    state.cache_condition.notify_all();
    sweep_retired_items(state);
    InodeBase* expected = inode == FUSE_ROOT_ID
                              ? static_cast<InodeBase*>(state.root_item.get())
                              : reinterpret_cast<InodeBase*>(uintptr_t(inode));
    if (item != expected) {
      throw std::system_error(
          EBADF, std::generic_category(), "releasedir inode");
    }
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
    throw_s3_status(response.status, "DeleteObject");
  }
}

void remove_item(State& state, InodeBase& item) {
  if (detach_cached_item(state, item)) {
    sweep_retired_items(state);
  }
}

bool move_cached_item(InodeBase& item, InodeBase& new_parent,
                      std::string_view new_name) {
  InodeBase& old_parent     = *item.parent();
  Directory& old_directory = old_parent.dir_children();
  Directory& new_directory = new_parent.dir_children();
  size_t destination_slot;
  if (&old_directory == &new_directory) {
    std::unique_lock guard(old_directory.mutex);
    if (item.detached() || new_parent.detached()) {
      return false;
    }
    const auto inserted = new_directory.insert_i(
        terark::fstring(new_name.data(), new_name.size()), &item);
    if (!inserted.second) {
      throw std::logic_error(
          "rename destination cache entry already exists");
    }
    destination_slot = inserted.first;
    old_directory.erase_i(item.dentry_slot);
    item.dentry_slot = uint32_t(destination_slot);
    item.set_parent(&new_parent);
    item.set_detached(false);
  } else {
    std::scoped_lock guard(old_directory.mutex, new_directory.mutex);
    if (item.detached() || new_parent.detached()) {
      return false;
    }
    const auto inserted = new_directory.insert_i(
        terark::fstring(new_name.data(), new_name.size()), &item);
    if (!inserted.second) {
      throw std::logic_error(
          "rename destination cache entry already exists");
    }
    destination_slot = inserted.first;
    old_directory.erase_i(item.dentry_slot);
    item.dentry_slot = uint32_t(destination_slot);
    item.set_parent(&new_parent);
    item.set_detached(false);
  }
  return true;
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
        state, path, false, {}, false, "unlink");
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
  if ((flags & ~RENAME_NOREPLACE) != 0) {
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
        destination_path, source_directory, "rename");
    if (source_directory) {
      if (destination_inode != 0 && !destination_directory) {
        throw std::system_error(ENOTDIR, std::generic_category(), "rename");
      }
      if (destination_key.starts_with(source_key)) {
        throw std::system_error(EINVAL, std::generic_category(),
                                "move directory into itself");
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
        const std::string destination_key_snapshot =
            item_key(state, *destination);
        delete_object(state, destination_key_snapshot, {});
      }
      rename_remote_directory(state, source_key, destination_key);
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
    const ObjectMetadata metadata = head_object(state, source_path);
    rename_remote_object(
        state, source_key, metadata.size, metadata.etag,
        metadata.version_id, destination_path,
        (flags & RENAME_NOREPLACE) != 0);
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
  constexpr uint64_t block_size = 4096;
  status.f_bsize   = block_size;
  status.f_frsize  = block_size;
  status.f_blocks  = UINT64_MAX / block_size;
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
  std::cout
      << "ngs3fs options:\n"
      << "  -e, --endpoint-host HOST  S3 endpoint (required)\n"
      << "  -p, --endpoint-port PORT  endpoint port (default 80)\n"
      << "  -a, --authority VALUE     HTTP/2 :authority (default HOST:PORT)\n"
      << "  -b, --bucket NAME         S3 bucket (required)\n"
      << "  -k, --prefix PREFIX       optional raw object-key prefix\n"
      << "  -r, --region REGION       SigV4 region (default us-east-1)\n"
      << "  -R, --read-ahead BYTES    kernel read-ahead; accepts "
         "KiB/MiB (default 256 KiB)\n"
      << "  -T, --dir-cache-timeout MS\n"
         "                             directory cache TTL "
         "(default 1000)\n"
      << "  -I, --max-cached-inodes N soft inode-cache limit "
         "(default 1000000)\n"
      << "  -P, --part-size BYTES     multipart part size (default 8 MiB)\n"
      << "  -c, --max-uploads N       concurrent uploads (default 4)\n"
      << "  -C, --max-connections N   connection pool size (default 8)\n"
      << "  -B, --max-pinned-memory BYTES\n"
         "                             retained write budget (default 256 MiB)\n"
      << "  credentials            AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, "
         "AWS_SESSION_TOKEN\n"
      << "  -u, --uid UID             getattr owner (default mount user)\n"
      << "  -g, --gid GID             getattr group (default mount group)\n"
      << "  -m, --file-mode OCTAL     object file mode (default 0644)\n"
      << "  -D, --dir-mode OCTAL      directory mode (default 0755)\n"
      << "  -M, --metrics             emit per-read latency JSONL\n"
      << "  -S, --tls                 require TLS; port 443 enables it "
         "automatically\n"
      << "usage: ngs3fs [options] [FUSE options] <mountpoint>\n";
}

int run(int argc, char** argv) {
  fuse_args arguments = FUSE_ARGS_INIT(0, nullptr);
  MountConfig config;
  load_environment(config);
  load_shared_credentials(config);
  try {
    if (!parse_arguments(argc, argv, config, arguments)) {
      throw std::runtime_error("unable to allocate FUSE arguments");
    }
    Pipe pipe_probe = Pipe::create(kPreferredIoSize);
    config.maximum_read_size = pipe_probe.capacity();
    Pipe write_pipe_probe = Pipe::create(kPreferredIoSize * 2);
    config.maximum_write_size = std::min(
        kPreferredIoSize, write_pipe_probe.capacity() / 2);
    if (config.maximum_write_size == 0) {
      throw std::runtime_error("write splice pipe has no payload capacity");
    }
    if (config.maximum_write_size < kPreferredIoSize) {
      std::cerr << "warning: reducing FUSE max_write to "
                << config.maximum_write_size
                << " bytes so libfuse can retain requests in its splice "
                   "receive pipe\n";
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
    std::cerr << error.what() << '\n';
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
    std::cerr << "missing mountpoint\n";
    fuse_opt_free_args(&arguments);
    return 2;
  }
  char* canonical_mountpoint = ::realpath(options.mountpoint, nullptr);
  if (canonical_mountpoint == nullptr) {
    std::cerr << "invalid mountpoint " << options.mountpoint << ": "
              << strerror(errno) << '\n';
    fuse_opt_free_args(&arguments);
    free(options.mountpoint);
    return 2;
  }
  free(options.mountpoint);
  options.mountpoint = canonical_mountpoint;
  if (config.endpoint_host.empty() || config.bucket.empty()) {
    std::cerr << "--endpoint-host and --bucket are required\n";
    fuse_opt_free_args(&arguments);
    free(options.mountpoint);
    return 2;
  }
  if (config.credentials.access_key_id.empty() !=
      config.credentials.secret_access_key.empty()) {
    std::cerr << "both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are "
                 "required for signed requests\n";
    fuse_opt_free_args(&arguments);
    free(options.mountpoint);
    return 2;
  }
  if (config.authority.find_first_of("\r\n") != std::string::npos ||
      config.region.find_first_of("\r\n") != std::string::npos ||
      config.credentials.access_key_id.find_first_of("\r\n") !=
          std::string::npos ||
      config.credentials.session_token.find_first_of("\r\n") !=
          std::string::npos) {
    std::cerr << "authority, region, and credential header values must not "
                 "contain CR or LF\n";
    fuse_opt_free_args(&arguments);
    free(options.mountpoint);
    return 2;
  }

  std::string splice_error;
  const bool splice_available = splice_preflight(splice_error);
  if (!splice_available) {
    std::cerr
        << "warning: splice(2) preflight failed: " << splice_error
        << "; FD-backed FUSE writes will use the copied fallback\n";
  }
  std::cerr
      << "warning: fsync is intentionally non-durable; only the first flush "
         "publishes and completes an S3 write\n";

  int result = 1;
  try {
    State state(std::move(config));
    state.splice_available = splice_available;
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
      std::cerr << "warning: failed to install FUSE signal handlers\n";
    }
    if (fuse_session_mount(session, options.mountpoint) == 0) {
      const uint32_t requested_read_ahead = state.config.read_ahead_size;
      std::string read_ahead_error;
      if (!set_kernel_read_ahead(options.mountpoint,
                                 state.config.read_ahead_size,
                                 read_ahead_error)) {
        std::cerr << "warning: unable to set kernel read-ahead to "
                  << requested_read_ahead << " bytes: " << read_ahead_error
                  << "; falling back to kernel value "
                  << state.config.read_ahead_size << " bytes\n";
      }
      fuse_loop_config loop_config{};
      loop_config.clone_fd         = 1;
      loop_config.max_idle_threads = 10;
      result = fuse_session_loop_mt(session, &loop_config);
      fuse_session_unmount(session);
    } else {
      std::cerr << "failed to mount " << options.mountpoint << ": "
                << strerror(errno) << '\n';
    }
    fuse_remove_signal_handlers(session);
    fuse_session_destroy(session);
  } catch (const std::exception& error) {
    std::cerr << "ngs3fs startup failed: " << error.what() << '\n';
  }

  fuse_opt_free_args(&arguments);
  free(options.mountpoint);
  return result;
}
