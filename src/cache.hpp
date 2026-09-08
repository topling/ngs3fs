#pragma once

#include "http.hpp"

#include <sys/types.h>

#include <atomic>
#include <array>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <vector>

class CacheRetirementPending : public std::system_error {
 public:
  CacheRetirementPending();
};

enum CachePageState : uint8_t {
  CACHE_PAGE_MISSING      = 0b00,
  CACHE_PAGE_READ_PENDING = 0b01,
  CACHE_PAGE_CLEAN        = 0b10,
  CACHE_PAGE_BAD          = 0b11,
};

constexpr size_t kDefaultMaxPrefetchWindowSize = 128U * 1024U * 1024U;
constexpr size_t kCacheBitmapUnit              = 32U * 1024U;
constexpr size_t kDefaultCacheBlockSize        = 2U * 1024U * 1024U;

struct CacheConfig {
  std::string root;
  std::string namespace_id;
  uint64_t maximum_bytes          = 0;
  uint64_t reserve_bytes          = 0;
  unsigned reserve_percent        = 5;
  size_t max_prefetch_window_size = kDefaultMaxPrefetchWindowSize;
  size_t page_size                = 4096;
  size_t block_size               = kDefaultCacheBlockSize;
  uint64_t upload_part_size       = 8ULL * 1024ULL * 1024ULL;
  uint32_t checksum_algorithm     = 0;
  bool reserve_is_percent         = true;
  bool unlimited                  = false;
};

struct CacheIdentity {
  std::string_view key;
  std::string_view etag;
  std::string_view version_id;
  uint64_t size = 0;
  time_t mtime  = 0;
};

struct CacheIdentitySnapshot {
  std::string key;
  std::string etag;
  std::string version_id;
  uint64_t size  = 0;
  uint64_t epoch = 0;
};

struct CacheFetchClaim {
  uint64_t offset = 0;
  uint64_t epoch  = 0;
  uint64_t id     = 0;
  size_t length   = 0;

  explicit operator bool() const noexcept { return length != 0; }
};

struct CacheChecksumPart {
  uint64_t offset    = 0;
  uint64_t size      = 0;
  uint32_t algorithm = 0;
  std::string value;
};

enum CacheChecksumAction : uint8_t {
  CACHE_CHECKSUM_NONE,
  CACHE_CHECKSUM_WAIT,
  CACHE_CHECKSUM_VERIFY,
  CACHE_CHECKSUM_BAD,
};

struct CacheChecksumClaim {
  CacheChecksumAction action = CACHE_CHECKSUM_NONE;
  uint64_t offset            = 0;
  uint64_t size              = 0;
  uint64_t epoch             = 0;
  size_t part                = 0;
  uint32_t algorithm         = 0;
  std::string value;
};

struct CachePendingDelete {
  std::string key;
  std::string restore_key;
  std::string replacement_etag;
  bool rollback = false;
};

class IoExecutor;
class LocalCache;
class CacheEntry;
class CacheAsyncOperation;
class CacheCapacityOperation;
class CacheKeyOperation;

struct CacheAsyncResult {
  std::shared_ptr<CacheEntry> entry;
  std::exception_ptr error;
  bool value = false;
};

// Caller-owned operation storage. The request, callback context, CacheEntry
// (for entry operations), and LocalCache must outlive completion. Accepted
// operations complete exactly once and never inline on the submitting stack.
// The callback may destroy or immediately reuse the request.
class CacheAsyncRequest {
 public:
  using Complete = void (*)(void*, CacheAsyncResult) noexcept;

  CacheAsyncRequest() = default;
  ~CacheAsyncRequest();
  CacheAsyncRequest(const CacheAsyncRequest&) = delete;
  CacheAsyncRequest& operator=(const CacheAsyncRequest&) = delete;

  [[nodiscard]] bool pending() const noexcept {
    return implementation_ != nullptr;
  }

  Complete complete = nullptr;
  void* context      = nullptr;

 private:
  friend class CacheAsyncOperation;
  friend class CacheOpenOperation;
  friend class CacheMarkerOperation;
  friend class CacheFileOperation;
  friend class CacheNamespaceOperation;
  friend class CacheEntry;
  friend class LocalCache;
  void* implementation_ = nullptr;
};

class CacheEntry {
 public:
  ~CacheEntry();

  CacheEntry(const CacheEntry&) = delete;
  CacheEntry& operator=(const CacheEntry&) = delete;

  [[nodiscard]] int data_fd() const noexcept;
  [[nodiscard]] std::string_view key() const noexcept { return key_; }
  [[nodiscard]] uint64_t size() const noexcept;
  [[nodiscard]] uint64_t epoch() const noexcept;
  [[nodiscard]] uint64_t written_end() const noexcept;
  [[nodiscard]] std::string etag() const;
  [[nodiscard]] std::string version_id() const;
  [[nodiscard]] CacheIdentitySnapshot identity_snapshot() const;
  [[nodiscard]] std::string write_id() const;
  [[nodiscard]] size_t page_size() const noexcept;
  [[nodiscard]] size_t block_size() const noexcept;
  [[nodiscard]] bool stale() const noexcept;
  [[nodiscard]] bool dirty() const noexcept;
  [[nodiscard]] bool exported() const noexcept;
  [[nodiscard]] bool try_export(bool verify);

  [[nodiscard]] bool range_clean(uint64_t offset, size_t length) const;
  [[nodiscard]] bool range_available_or_pending(
      uint64_t offset, size_t length) const;
  [[nodiscard]] bool range_bad(uint64_t offset, size_t length) const;
  [[nodiscard]] bool fully_clean() const;
  bool prepare_read(uint64_t offset, size_t length);
  bool prepare_read_async(IoExecutor& executor, CacheAsyncRequest& request,
                          uint64_t offset, size_t length) noexcept;
  void begin_write();
  bool begin_write_async(IoExecutor& executor,
                         CacheAsyncRequest& request) noexcept;
  void prepare_write(uint64_t offset, size_t length);
  bool prepare_write_async(IoExecutor& executor, CacheAsyncRequest& request,
                           uint64_t offset, size_t length) noexcept;
  void publish_dirty(uint64_t offset, size_t length, uint64_t written_end);
  void set_upload_id(std::string_view upload_id);
  bool set_upload_id_async(IoExecutor& executor, CacheAsyncRequest& request,
                           std::string_view upload_id) noexcept;
  [[nodiscard]] std::string upload_id() const;
  void isolate_write() noexcept;
  bool mark_commit_pending_async(IoExecutor& executor,
                                 CacheAsyncRequest& request) noexcept;
  void discard_write() noexcept;
  bool discard_write_async(IoExecutor& executor,
                           CacheAsyncRequest& request) noexcept;
  void sync_write();
  bool sync_write_async(IoExecutor& executor,
                        CacheAsyncRequest& request) noexcept;
  void commit_write(const CacheIdentity& identity);
  bool commit_write_async(IoExecutor& executor, CacheAsyncRequest& request,
                          const CacheIdentity& identity) noexcept;
  [[nodiscard]] bool pin_clean(uint64_t offset, size_t length);
  void pin(uint64_t offset, size_t length);
  void unpin(uint64_t offset, size_t length) noexcept;
  void touch(uint64_t offset, size_t length) noexcept;
  void disable_eviction() noexcept;
  CacheFetchClaim claim_fetch(uint64_t wanted_offset, size_t wanted_length,
                              size_t expansion);
  void wait_for_range(uint64_t offset, size_t length);
  // Returns -1 when a recheck is already useful; otherwise registers a waiter
  // before exposing the notification fd. Pair with end_async_wait after CQE.
  int begin_async_wait(uint64_t offset, size_t length);
  int begin_checksum_wait(uint64_t offset, size_t length);
  int begin_manifest_wait();
  // Used after LocalCache::retiring_entry marks this generation stale. Returns
  // -1 when quiescent; otherwise registers on the existing cache eventfd.
  int begin_retire_wait();
  bool checksum_failed(uint64_t offset, size_t length) const;
  void end_async_wait() noexcept;
  void publish_clean(const CacheFetchClaim& claim, size_t published,
                     size_t length, bool final);
  void finish_fetch(const CacheFetchClaim& claim) noexcept;
  void rollback_fetch(const CacheFetchClaim& claim) noexcept;
  void fail_fetch(const CacheFetchClaim& claim) noexcept;
  void mark_bad(const CacheFetchClaim& claim) noexcept;
  // Full-object checks are best effort if any published block was evicted or
  // acquired by another fetch. A successful pin lasts until claim retirement.
  [[nodiscard]] bool pin_fetch_verification(const CacheFetchClaim& claim);
  void begin_retry(const CacheFetchClaim& claim);
  // Quarantine the pinned scope, then wait for old reply pins to drain.
  // As with begin_async_wait, a returned fd is paired with end_async_wait.
  int begin_retry_wait(const CacheFetchClaim& claim);
  void finish_retry(const CacheFetchClaim& claim, bool valid) noexcept;
  bool begin_checksum_manifest(bool wait = true);
  void finish_checksum_manifest(
      std::vector<CacheChecksumPart> parts);
  void checksum_manifest_unavailable() noexcept;
  [[nodiscard]] bool checksum_manifest_available() const noexcept;
  CacheChecksumClaim claim_checksum(uint64_t offset, size_t length,
                                    bool skip_in_progress = false);
  void wait_for_checksum(uint64_t offset, size_t length);
  void checksum_mismatch(const CacheChecksumClaim& claim) noexcept;
  void wait_for_checksum_retry(const CacheChecksumClaim& claim);
  int begin_checksum_retry_wait(const CacheChecksumClaim& claim);
  void finish_checksum(const CacheChecksumClaim& claim,
                       bool valid) noexcept;
  // A retry signed for an identity that was renamed while its handle closed
  // proves nothing about the current key. Drop the affected clean pages so a
  // later opener refetches them instead of poisoning or trusting this part.
  void abandon_checksum(const CacheChecksumClaim& claim) noexcept;

 private:
  friend class CacheAsyncOperation;
  friend class CacheCapacityOperation;
  friend class CacheKeyOperation;
  friend class CacheOpenOperation;
  friend class CacheMarkerOperation;
  friend class CacheFileOperation;
  friend class CacheNamespaceOperation;
  friend class LocalCache;

  CacheEntry(LocalCache& owner, std::string key, int data_fd, int meta_fd,
             int dirty_fd, void* mapping, size_t mapping_size,
             uint64_t size, bool punch_missing = true);

  [[nodiscard]] CachePageState page_state(size_t page) const noexcept;
  void set_page_state(size_t page, CachePageState state) noexcept;
  [[nodiscard]] bool range_has_state(uint64_t offset, size_t length,
                                     CachePageState state) const noexcept;
  [[nodiscard]] bool range_all_state(uint64_t offset, size_t length,
                                     CachePageState state) const noexcept;
  [[nodiscard]] bool range_ready_locked(uint64_t offset,
                                        size_t length) const noexcept;
  [[nodiscard]] size_t block_first_page(uint64_t offset) const noexcept;
  [[nodiscard]] size_t block_last_page(uint64_t offset) const noexcept;
  bool end_fetch_locked(const CacheFetchClaim& claim) noexcept;
  bool begin_retry_locked(const CacheFetchClaim& claim);
  bool begin_checksum_retry_locked(const CacheChecksumClaim& claim);
  bool retry_pins_ready_locked(uint64_t offset, size_t length) const noexcept;
  uint8_t checksum_blocked_locked(uint64_t offset, size_t length) const noexcept;
  void wait_locked(std::unique_lock<std::mutex>& guard);
  void notify_waiters_locked() noexcept;
  void ensure_write_capacity(uint64_t end);
  void retire_generation(bool wait = true);
  uint64_t evict_one_region() noexcept;

  LocalCache* owner_ = nullptr;
  std::string key_;
  int data_fd_       = -1;
  int meta_fd_       = -1;
  int dirty_fd_      = -1;
  void* mapping_     = nullptr;
  size_t mapping_size_ = 0;
  uint64_t size_       = 0;
  uint64_t epoch_      = 0;
  size_t page_size_    = 0;
  size_t block_size_   = 0;
  size_t page_count_   = 0;
  size_t bitmap_offset_ = 0;
  mutable std::mutex mutex_;
  int wait_fd_ = -1;
  unsigned waiters_ = 0;
  std::vector<uint8_t> referenced_;
  std::vector<uint32_t> region_pins_;
  struct ActiveFetch {
    uint64_t id     = 0;
    uint64_t epoch  = 0;
    uint64_t offset = 0;
    size_t length   = 0;
    // Complete blocks already handed back to the cache may be evicted and
    // acquired by another fetch while this GET still owns its pending tail.
    size_t published = 0;
    bool verifying  = false;
    bool retrying   = false;
    std::vector<CachePageState> prior_states;
  };
  std::vector<ActiveFetch> active_claims_;
  std::vector<uint8_t> reserved_units_;
  std::vector<CacheChecksumPart> checksum_parts_;
  std::vector<uint8_t> checksum_states_;
  uint64_t next_claim_id_ = 1;
  size_t clock_hand_       = 0;
  size_t checksum_ops_     = 0;
  size_t pinned_regions_   = 0;
  uint8_t checksum_manifest_ = 0;
  bool eviction_disabled_  = false;
  bool unlinked_data_      = false;
  bool unlinked_meta_      = false;
  bool detached_           = false;
  bool stale_              = false;
  bool async_metadata_inflight_ = false;
  std::deque<CacheAsyncOperation*> async_metadata_queue_;
};

class LocalCache {
 public:
  explicit LocalCache(CacheConfig config);
  ~LocalCache();

  LocalCache(const LocalCache&) = delete;
  LocalCache& operator=(const LocalCache&) = delete;

  // Bounded, non-blocking in-memory lookup for reactor callers. A miss or any
  // contended cache lock returns nullptr so a worker can use open().
  std::shared_ptr<CacheEntry> try_open(
      const CacheIdentity& identity) noexcept;
  std::shared_ptr<CacheEntry> open(const CacheIdentity& identity, bool wait = true);
  bool open_async(IoExecutor& executor, CacheAsyncRequest& request,
                  const CacheIdentity& identity) noexcept;
  std::shared_ptr<CacheEntry> create_writer(const CacheIdentity& base,
                                            uint64_t maximum_size, bool wait = true);
  bool create_writer_async(IoExecutor& executor, CacheAsyncRequest& request,
                           const CacheIdentity& base,
                           uint64_t maximum_size) noexcept;
  std::shared_ptr<CacheEntry> retiring_entry(
      std::string_view key, const CacheIdentity* reuse = nullptr,
      bool preserve_generation = false);
  std::vector<std::shared_ptr<CacheEntry>> recover_dirty(
      std::vector<std::string>* isolated_keys = nullptr);
  std::vector<CachePendingDelete> recover_pending_deletes();
  void create_pending_delete(
      std::string_view key, std::string_view restore_key = {},
      std::string_view replacement_etag = {});
  bool create_pending_delete_async(
      IoExecutor& executor, CacheAsyncRequest& request,
      std::string_view key, std::string_view restore_key = {},
      std::string_view replacement_etag = {}) noexcept;
  void commit_pending_delete(std::string_view key);
  bool commit_pending_delete_async(
      IoExecutor& executor, CacheAsyncRequest& request,
      std::string_view key) noexcept;
  void finish_pending_delete(std::string_view key) noexcept;
  bool finish_pending_delete_async(
      IoExecutor& executor, CacheAsyncRequest& request,
      std::string_view key) noexcept;
  bool remove(std::string_view key, bool preserve_generation,
              bool* retry = nullptr, bool wait = true) noexcept;
  bool remove_async(IoExecutor& executor, CacheAsyncRequest& request,
                    std::string_view key,
                    bool preserve_generation) noexcept;
  bool rename(std::string_view old_key, std::string_view new_key,
              bool* retry = nullptr, bool wait = true) noexcept;
  bool rename_async(IoExecutor& executor, CacheAsyncRequest& request,
                    std::string_view old_key,
                    std::string_view new_key) noexcept;
  [[nodiscard]] const CacheConfig& config() const noexcept { return config_; }

 private:
  friend class CacheAsyncOperation;
  friend class CacheCapacityOperation;
  friend class CacheKeyOperation;
  friend class CacheOpenOperation;
  friend class CacheMarkerOperation;
  friend class CacheFileOperation;
  friend class CacheNamespaceOperation;
  friend class CacheEntry;
  friend struct CacheTestAccess;

  void probe_filesystem();
  bool prepare_range(CacheEntry& entry, uint64_t offset, size_t length,
                     bool write);
  bool try_reserve_capacity(uint64_t bytes);
  bool reserve_capacity(uint64_t bytes);
  bool reclaim_closed_async(
      IoExecutor& executor, CacheAsyncRequest& request,
      const std::shared_ptr<CacheEntry>& expected) noexcept;
  bool open_async_if_idle(
      IoExecutor& executor, CacheAsyncRequest& request,
      const CacheIdentity& identity) noexcept;
  void cancel_reservation(uint64_t bytes) noexcept;
  void finish_reservation(uint64_t reserved, uint64_t allocated) noexcept;
  bool evict_one();
  bool evict_cold();
  bool reclaim_closed_clean(
      const std::shared_ptr<CacheEntry>& entry) noexcept;
  std::recursive_mutex& key_mutex(std::string_view key) noexcept;
  void punch_range(int fd, uint64_t offset, uint64_t length) noexcept;
  int create_dirty_marker(std::string_view key, uint64_t epoch);
  void remove_dirty_marker(std::string_view key) noexcept;
  void add_allocated(int64_t delta) noexcept;
  [[nodiscard]] uint64_t allocated_bytes() const noexcept;
  [[nodiscard]] uint64_t reserve_floor_bytes() const;
  struct KeepaliveSlot {
    std::shared_ptr<CacheEntry> entry;
    size_t base_metadata_bytes = 0;
  };
  static constexpr size_t kKeepaliveEntryLimit = 64;
  // Admission budget for the fixed mapping and per-block arrays. Variable
  // fetch/checksum state is outside this fixed-base budget.
  static constexpr size_t kKeepaliveBaseMetadataLimit =
      8U * 1024U * 1024U;
  void retain_entry_locked(
      const std::shared_ptr<CacheEntry>& entry,
      std::vector<std::shared_ptr<CacheEntry>>& released);
  void release_keepalive_locked(
      CacheEntry* entry,
      std::vector<std::shared_ptr<CacheEntry>>& released);
  void release_key_keepalive_locked(
      std::string_view key,
      std::vector<std::shared_ptr<CacheEntry>>& released);

  CacheConfig config_;
  int root_fd_         = -1;
  int superblock_fd_   = -1;
  int data_root_fd_    = -1;
  int objects_root_fd_ = -1;
  int dirty_root_fd_   = -1;
  int pending_root_fd_ = -1;
  int lock_fd_ = -1;
  size_t name_max_ = 255;
  void* superblock_mapping_ = nullptr;
  mutable std::mutex mutex_;
  mutable std::mutex capacity_mutex_;
  std::array<std::recursive_mutex, 127> key_mutexes_;
  std::vector<std::weak_ptr<CacheEntry>> entries_;
  std::vector<KeepaliveSlot> keepalive_;
  std::unordered_set<std::string> async_keys_;
  std::deque<CacheKeyOperation*> async_key_waiters_;
  size_t keepalive_base_metadata_bytes_ = 0;
  uint64_t pending_reservations_ = 0;
  std::atomic<size_t> clock_entry_{0};
  std::atomic<bool> cold_scan_warned_{false};
};

std::string cache_encode_component(std::string_view component,
                                   size_t name_max);
