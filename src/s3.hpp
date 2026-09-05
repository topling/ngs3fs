#pragma once

#include "http.hpp"

#include <terark/hash_strmap.hpp>
#include <tinyxml2.h>

#include <atomic>
#include <array>
#include <stddef.h>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <time.h>
#include <utility>

class S3Xml {
 public:
  explicit S3Xml(std::string_view xml, std::string_view operation);

  const tinyxml2::XMLElement& root(
      std::string_view expected = {}) const;
  const tinyxml2::XMLElement& result_root(
      std::string_view expected) const;
  bool root_is(std::string_view name) const noexcept;

  static bool named(const tinyxml2::XMLElement& element,
                    std::string_view name) noexcept;
  const tinyxml2::XMLElement* first_child(
      const tinyxml2::XMLElement& parent,
      std::string_view name) const noexcept;
  const tinyxml2::XMLElement* next_sibling(
      const tinyxml2::XMLElement& element,
      std::string_view name) const noexcept;
  const tinyxml2::XMLElement& required_child(
      const tinyxml2::XMLElement& parent,
      std::string_view name) const;

  std::string required_text(const tinyxml2::XMLElement& parent,
                            std::string_view name) const;
  std::string optional_text(const tinyxml2::XMLElement& parent,
                            std::string_view name) const;
  bool required_bool(const tinyxml2::XMLElement& parent,
                     std::string_view name) const;

 private:
  const tinyxml2::XMLElement* unique_child(
      const tinyxml2::XMLElement& parent, std::string_view name,
      bool required) const;
  std::string element_text(const tinyxml2::XMLElement& element) const;
  [[noreturn]] void fail(std::string_view message) const;

  tinyxml2::XMLDocument document_;
  ssostr<64> operation_;
};

struct S3ErrorInfo {
  std::string code;
  std::string message;
  std::string request_id;
  std::string host_id;
};

bool parse_s3_error(std::string_view xml, S3ErrorInfo& error) noexcept;
bool s3_content_range_matches(std::string_view value,
                              uint64_t offset, size_t length,
                              uint64_t object_size) noexcept;

struct InodeBase;
struct InodeFile;
struct InodeDir;

struct DirectoryContinuation;

struct Directory : terark::hash_strmap<
    InodeBase*,
    terark::fstring_func::hash_unalign,
    terark::fstring_func::equal_unalign,
    terark::ValueInline,
    terark::FastCopy,
    unsigned int,
    terark::HSM_HashTp,
    true> {
  Directory();
  Directory(const Directory&) = delete;
  Directory& operator=(const Directory&) = delete;
  ~Directory();

  IoMutex mutation_mutex;
  mutable std::shared_mutex mutex;
  InodeDir* clock_prev = nullptr;
  InodeDir* clock_next = nullptr;
  std::atomic<bool> clock_referenced{false};
  std::atomic<bool> refreshing{false};
  DirectoryContinuation* refresh_waiters = nullptr;
  uint32_t listing_generation = 0;
  bool clock_linked           = false;
};

struct alignas(16) InodeBase {
  static constexpr uintptr_t kRegularFlag         = 0b0001;
  static constexpr uintptr_t kDetachedFlag        = 0b0010;
  static constexpr uintptr_t kPendingFlag         = 0b0100;
  static constexpr uintptr_t kTruncatePendingFlag = 0b1000;
  static constexpr uintptr_t kFlagMask            = 0b1111;

  explicit InodeBase(InodeBase* parent, bool regular) noexcept
      : parent_flags(uintptr_t(parent) |
                     (regular ? kRegularFlag : 0)) {}
  InodeBase(const InodeBase&) = delete;
  InodeBase& operator=(const InodeBase&) = delete;
  InodeBase(InodeBase&&) = delete;
  InodeBase& operator=(InodeBase&&) = delete;

  [[nodiscard]] InodeBase* parent() const noexcept {
    return reinterpret_cast<InodeBase*>(
        parent_flags.load(std::memory_order_relaxed) & ~kFlagMask);
  }

  void set_parent(InodeBase* value) noexcept {
    uintptr_t current = parent_flags.load(std::memory_order_relaxed);
    do {
    } while (!parent_flags.compare_exchange_weak(
        current, uintptr_t(value) | (current & kFlagMask),
        std::memory_order_relaxed, std::memory_order_relaxed));
  }

  [[nodiscard]] bool regular() const noexcept {
    return parent_flags.load(std::memory_order_relaxed) & kRegularFlag;
  }

  [[nodiscard]] bool directory() const noexcept {
    return !regular();
  }

  [[nodiscard]] bool detached() const noexcept {
    return parent_flags.load(std::memory_order_relaxed) & kDetachedFlag;
  }

  void set_detached(bool value) noexcept {
    set_flag(kDetachedFlag, value);
  }

  [[nodiscard]] bool pending() const noexcept {
    return parent_flags.load(std::memory_order_relaxed) & kPendingFlag;
  }

  void set_pending(bool value) noexcept {
    set_flag(kPendingFlag, value);
  }

  [[nodiscard]] bool truncate_pending() const noexcept {
    return parent_flags.load(std::memory_order_relaxed) &
        kTruncatePendingFlag;
  }

  void set_truncate_pending(bool value) noexcept {
    set_flag(kTruncatePendingFlag, value);
  }

  Directory& dir_children() noexcept;
  const Directory& dir_children() const noexcept;
  time_t get_mtime() const noexcept;
  uint64_t get_fsize() const noexcept;
  void set_fsize(uint64_t value) noexcept;

  std::atomic<uintptr_t> parent_flags;
  std::atomic<uint64_t> expire{0};
  uint32_t dentry_slot = UINT32_MAX;
  std::atomic<uint32_t> listing_generation{0};
  std::atomic<uint32_t> nlookup{0};
  std::atomic<uint32_t> open_count{0};

 protected:
  ~InodeBase() = default;

 private:
  void set_flag(uintptr_t flag, bool value) noexcept {
    if (value) {
      parent_flags.fetch_or(flag, std::memory_order_relaxed);
    } else {
      parent_flags.fetch_and(~flag, std::memory_order_relaxed);
    }
  }
};

struct InodeFile : InodeBase {
  explicit InodeFile(InodeBase* parent = nullptr) noexcept
      : InodeBase(parent, true) {}

  // expire is a directory-only timestamp. Regular files reuse the same
  // storage without increasing InodeFile's size.
  [[nodiscard]] bool page_cache_valid() const noexcept {
    return expire.load(std::memory_order_acquire) != 0;
  }

  void set_page_cache_valid(bool value) noexcept {
    expire.store(value, std::memory_order_release);
  }

  std::atomic<time_t> mtime{0};
  std::atomic<uint64_t> fsize{0};
  std::atomic<uint64_t> generation_hash{0};
  std::atomic<uint64_t> generation_epoch{0};
};

struct InodeDir : InodeBase {
  explicit InodeDir(InodeBase* parent = nullptr) noexcept
      : InodeBase(parent, false) {}

  Directory children;
};

inline Directory& InodeBase::dir_children() noexcept {
  return static_cast<InodeDir*>(this)->children;
}

inline const Directory& InodeBase::dir_children() const noexcept {
  return static_cast<const InodeDir*>(this)->children;
}

inline time_t InodeBase::get_mtime() const noexcept {
  return regular()
             ? static_cast<const InodeFile*>(this)->mtime.load(
                   std::memory_order_relaxed)
             : 0;
}

inline uint64_t InodeBase::get_fsize() const noexcept {
  return regular()
             ? static_cast<const InodeFile*>(this)->fsize.load(
                   std::memory_order_relaxed)
             : 0;
}

inline void InodeBase::set_fsize(uint64_t value) noexcept {
  static_cast<InodeFile*>(this)->fsize.store(
      value, std::memory_order_relaxed);
}

void delete_inode(InodeBase* item) noexcept;

// Update an inode's directory entry while retaining the old inode's stale
// slot safely. These operations only touch the inode tree and its directory
// locks; they do not depend on FUSE or mount state.
void detach_parent_slot_if_owned(InodeBase& item,
                                 InodeBase* parent) noexcept;
bool move_cached_item(InodeBase& item, InodeBase& new_parent,
                      std::string_view new_name);

static_assert(alignof(InodeBase) == 16);
static_assert(sizeof(InodeBase) == 32);
static_assert(sizeof(InodeFile) == 64);

// S3 request-path helpers and SigV4 signing.
// Request paths are stored in their on-wire, percent-encoded form. Keeping
// that representation prevents control headers from encoding '%' a second
// time.
bool is_encoded_request_path(std::string_view p);
bool request_path_is_directory_marker(std::string_view p);
std::string request_path_without_query(std::string_view p);

std::string make_copy_source(std::string_view bucket,
                             std::string_view source_path,
                             std::string_view version_id = {},
                             bool virtual_hosted = false);
bool authority_uses_virtual_bucket(std::string_view authority,
                                   std::string_view bucket);

inline constexpr std::string_view kEmptyPayloadSha256 =
    "e3b0c44298fc1c149afbf4c8996fb924"
    "27ae41e4649b934ca495991b7852b855";
inline constexpr std::string_view kUnsignedPayload = "UNSIGNED-PAYLOAD";

enum ChecksumAlgorithm {
  CHECKSUM_AUTO,
  CHECKSUM_PROTOCOL_DEFAULT,
  CHECKSUM_NONE,
  CHECKSUM_CRC32,
  CHECKSUM_CRC32C,
  CHECKSUM_CRC64NVME,
  CHECKSUM_SHA1,
  CHECKSUM_SHA256,
  CHECKSUM_MD5,
  CHECKSUM_XXHASH64,
  CHECKSUM_XXHASH3,
  CHECKSUM_XXHASH128,
  CHECKSUM_SHA512,
  CHECKSUM_CRC64XZ,
};

bool ascii_equal_ignore_case(std::string_view first,
                             std::string_view second) noexcept;
bool parse_checksum_algorithm(std::string_view text,
                              ChecksumAlgorithm& algorithm) noexcept;
std::string_view checksum_option_name(ChecksumAlgorithm algorithm) noexcept;
std::string_view checksum_s3_name(ChecksumAlgorithm algorithm) noexcept;
std::string_view checksum_header_name(ChecksumAlgorithm algorithm) noexcept;
std::string_view checksum_xml_name(ChecksumAlgorithm algorithm) noexcept;
bool gcs_checksum_from_header(std::string_view value,
                              ChecksumAlgorithm preferred,
                              ChecksumAlgorithm& algorithm,
                              std::string_view& expected) noexcept;
bool checksum_is_s3(ChecksumAlgorithm algorithm) noexcept;
std::string_view checksum_multipart_type(
    ChecksumAlgorithm algorithm) noexcept;

struct ChecksumValue {
  ssostr<96> base64;
  uint64_t integer = 0;
};

ChecksumValue crc64_checksum_value(uint64_t value);
uint64_t combine_crc64(ChecksumAlgorithm algorithm, uint64_t first,
                       uint64_t second, uint64_t second_length);

class DataChecksum {
 public:
  explicit DataChecksum(ChecksumAlgorithm algorithm);

  DataChecksum(const DataChecksum&) = delete;
  DataChecksum& operator=(const DataChecksum&) = delete;

  void update(std::span<const std::byte> bytes);
  ChecksumValue finish();

 private:
  static constexpr size_t kContextBytes = 1024;

  alignas(64) std::array<std::byte, kContextBytes> context_{};
  ChecksumAlgorithm algorithm_;
  bool finished_ = false;
};

struct Credentials {
  std::string access_key_id;
  std::string secret_access_key;
  std::string session_token;
};

struct SignRequest {
  std::string_view method;
  std::string_view canonical_uri;
  std::string_view canonical_query;
  std::string_view authority;
  std::span<const Header> headers;
  std::string_view payload_hash;
  std::string_view amz_datetime;
  std::string_view region;
  std::string_view service = "s3";
  bool fast_get = false;
};

struct HeaderList {
  static constexpr size_t capacity = 8;

  void reserve(size_t wanted) const {
    if (wanted > capacity) {
      throw std::length_error("too many signed headers");
    }
  }

  void clear() noexcept {
    for (size_t i = 0; i < count; ++i) {
      values[i].name.clear();
      values[i].value.clear();
    }
    count = 0;
  }

  Header& emplace_back() {
    reserve(count + 1);
    return values[count++];
  }

  void push_back(Header value) {
    emplace_back() = std::move(value);
  }

  [[nodiscard]] bool empty() const noexcept { return count == 0; }
  [[nodiscard]] size_t size() const noexcept { return count; }
  [[nodiscard]] Header* data() noexcept { return values.data(); }
  [[nodiscard]] const Header* data() const noexcept { return values.data(); }
  Header* begin() noexcept { return data(); }
  Header* end() noexcept { return data() + count; }
  const Header* begin() const noexcept { return data(); }
  const Header* end() const noexcept { return data() + count; }
  Header& back() noexcept { return values[count - 1]; }
  const Header& back() const noexcept { return values[count - 1]; }

  operator std::span<const Header>() const noexcept {
    return std::span(data(), count);
  }

  std::array<Header, capacity> values;
  size_t count = 0;
};

struct Signature {
  HeaderList headers;
  std::string canonical_request_hash;
  std::string signed_header_names;
};

Signature sign_v4(const Credentials& credentials, const SignRequest& request);
HeaderList sign_v4_headers(const Credentials& credentials,
                           const SignRequest& request);
void sign_v4_headers(HeaderList& headers, const Credentials& credentials,
                     const SignRequest& request);
ssostr<32> amz_datetime(int64_t epoch_seconds);
ssostr<32> amz_datetime_now();
time_t parse_s3_mtime(std::string_view value);
time_t parse_http_mtime(std::string_view value);
std::string uri_encode(std::string_view value, bool preserve_slashes);
