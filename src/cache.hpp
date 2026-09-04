#pragma once

#include "http.hpp"

#include <sys/types.h>

#include <atomic>
#include <array>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <vector>

enum CachePageState : uint8_t {
  CACHE_PAGE_MISSING      = 0b00,
  CACHE_PAGE_READ_PENDING = 0b01,
  CACHE_PAGE_CLEAN        = 0b10,
  CACHE_PAGE_BAD          = 0b11,
};

struct CacheConfig {
  std::string root;
  std::string namespace_id;
  uint64_t maximum_bytes       = 0;
  uint64_t reserve_bytes       = 0;
  unsigned reserve_percent     = 5;
  size_t maximum_fetch_size    = 8U * 1024U * 1024U;
  size_t page_size             = 4096;
  uint64_t upload_part_size    = 8ULL * 1024ULL * 1024ULL;
  uint32_t checksum_algorithm  = 0;
  bool reserve_is_percent      = true;
};

struct CacheIdentity {
  std::string_view key;
  std::string_view etag;
  std::string_view version_id;
  uint64_t size = 0;
  time_t mtime  = 0;
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

class LocalCache;

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
  [[nodiscard]] std::string write_id() const;
  [[nodiscard]] size_t page_size() const noexcept;
  [[nodiscard]] bool stale() const noexcept;
  [[nodiscard]] bool dirty() const noexcept;

  [[nodiscard]] bool range_clean(uint64_t offset, size_t length) const;
  [[nodiscard]] bool range_bad(uint64_t offset, size_t length) const;
  [[nodiscard]] bool fully_clean() const;
  bool prepare_read(uint64_t offset, size_t length);
  void begin_write();
  void prepare_write(uint64_t offset, size_t length);
  void publish_dirty(uint64_t offset, size_t length, uint64_t written_end);
  void set_upload_id(std::string_view upload_id);
  [[nodiscard]] std::string upload_id() const;
  void isolate_write() noexcept;
  void discard_write() noexcept;
  void sync_write();
  void commit_write(const CacheIdentity& identity);
  [[nodiscard]] bool pin_clean(uint64_t offset, size_t length);
  void pin(uint64_t offset, size_t length);
  void unpin(uint64_t offset, size_t length) noexcept;
  void touch(uint64_t offset, size_t length) noexcept;
  void disable_eviction() noexcept;
  CacheFetchClaim claim_fetch(uint64_t wanted_offset, size_t wanted_length,
                              size_t expansion);
  void wait_for_range(uint64_t offset, size_t length);
  void publish_clean(const CacheFetchClaim& claim, size_t published,
                     size_t length, bool final);
  void finish_fetch(const CacheFetchClaim& claim) noexcept;
  void fail_fetch(const CacheFetchClaim& claim) noexcept;
  void mark_bad(const CacheFetchClaim& claim) noexcept;
  void begin_retry(const CacheFetchClaim& claim);
  void finish_retry(const CacheFetchClaim& claim, bool valid) noexcept;
  bool begin_checksum_manifest();
  void finish_checksum_manifest(
      std::vector<CacheChecksumPart> parts);
  void checksum_manifest_unavailable() noexcept;
  [[nodiscard]] bool checksum_manifest_available() const noexcept;
  CacheChecksumClaim claim_checksum(uint64_t offset, size_t length);
  void wait_for_checksum(uint64_t offset, size_t length);
  void checksum_mismatch(const CacheChecksumClaim& claim) noexcept;
  void finish_checksum(const CacheChecksumClaim& claim,
                       bool valid) noexcept;

 private:
  friend class LocalCache;

  CacheEntry(LocalCache& owner, std::string key, int data_fd, int meta_fd,
             int dirty_fd, void* mapping, size_t mapping_size,
             uint64_t size);

  [[nodiscard]] CachePageState page_state(size_t page) const noexcept;
  void set_page_state(size_t page, CachePageState state) noexcept;
  [[nodiscard]] bool range_has_state(uint64_t offset, size_t length,
                                     CachePageState state) const noexcept;
  [[nodiscard]] bool range_all_state(uint64_t offset, size_t length,
                                     CachePageState state) const noexcept;
  [[nodiscard]] bool range_ready_locked(uint64_t offset,
                                        size_t length) const noexcept;
  bool end_fetch_locked(const CacheFetchClaim& claim) noexcept;
  void ensure_write_capacity(uint64_t end);
  void retire_generation();
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
  size_t page_count_   = 0;
  size_t bitmap_offset_ = 0;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::vector<uint8_t> referenced_;
  std::vector<uint32_t> region_pins_;
  std::vector<uint64_t> active_claims_;
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
};

class LocalCache {
 public:
  explicit LocalCache(CacheConfig config);
  ~LocalCache();

  LocalCache(const LocalCache&) = delete;
  LocalCache& operator=(const LocalCache&) = delete;

  std::shared_ptr<CacheEntry> open(const CacheIdentity& identity);
  std::shared_ptr<CacheEntry> create_writer(const CacheIdentity& base,
                                            uint64_t maximum_size);
  std::vector<std::shared_ptr<CacheEntry>> recover_dirty(
      std::vector<std::string>* isolated_keys = nullptr);
  std::vector<CachePendingDelete> recover_pending_deletes();
  void create_pending_delete(
      std::string_view key, std::string_view restore_key = {},
      std::string_view replacement_etag = {});
  void commit_pending_delete(std::string_view key);
  void finish_pending_delete(std::string_view key) noexcept;
  bool remove(std::string_view key, bool preserve_generation) noexcept;
  bool rename(std::string_view old_key, std::string_view new_key) noexcept;
  [[nodiscard]] const CacheConfig& config() const noexcept { return config_; }

 private:
  friend class CacheEntry;

  void probe_filesystem();
  bool prepare_range(CacheEntry& entry, uint64_t offset, size_t length,
                     bool write);
  bool reserve_capacity(uint64_t bytes);
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

  CacheConfig config_;
  int root_fd_         = -1;
  int superblock_fd_   = -1;
  int data_root_fd_    = -1;
  int objects_root_fd_ = -1;
  int dirty_root_fd_   = -1;
  int pending_root_fd_ = -1;
  int lock_fd_ = -1;
  void* superblock_mapping_ = nullptr;
  mutable std::mutex mutex_;
  mutable std::mutex capacity_mutex_;
  std::array<std::recursive_mutex, 127> key_mutexes_;
  std::vector<std::weak_ptr<CacheEntry>> entries_;
  uint64_t pending_reservations_ = 0;
  std::atomic<size_t> clock_entry_{0};
  std::atomic<bool> cold_scan_warned_{false};
};

std::string cache_encode_component(std::string_view component,
                                   size_t name_max);
