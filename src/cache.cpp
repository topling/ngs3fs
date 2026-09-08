#include "cache.hpp"
#include "io.hpp"

#include <openssl/sha.h>
#include <openssl/rand.h>

#include <fcntl.h>
#include <dirent.h>
#include <sys/file.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <linux/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <errno.h>
#include <filesystem>
#include <functional>
#include <limits.h>
#include <optional>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <stdexcept>
#include <system_error>

constexpr char kCacheEscapePrefix[] = ".~ngs3fs~.";
constexpr char kCacheValueName[]    = ".~ngs3fs~.value";
constexpr uint32_t kCacheRootVersion = 2;
constexpr uint32_t kCacheMetaVersion = 5;
constexpr uint32_t kCacheByteOrder    = 0x01020304;
constexpr uint32_t kCachePathVersion  = 1;
constexpr uint32_t kCacheMetaDirty    = 1;
constexpr uint32_t kCacheMetaExported = 2;
constexpr uint8_t kChecksumUnknown     = 0;
constexpr uint8_t kChecksumLoading     = 1;
constexpr uint8_t kChecksumAvailable   = 2;
constexpr uint8_t kChecksumUnavailable = 3;
constexpr uint8_t kPartUnverified      = 0;
constexpr uint8_t kPartVerifying       = 1;
constexpr uint8_t kPartRetrying        = 2;
constexpr uint8_t kPartVerified        = 3;
constexpr uint8_t kPartBad             = 4;
constexpr size_t kCacheRootHeaderSize = 4096;
constexpr size_t kCacheMetaHeaderSize = 4096;
constexpr size_t kCacheKeyCapacity     = 1024;
constexpr size_t kCacheEtagCapacity    = 512;
constexpr size_t kCacheVersionCapacity = 1024;
constexpr size_t kCacheUploadIdCapacity = 1024;
constexpr size_t kCacheWriteIdCapacity = 32;

struct CacheRootHeader {
  std::array<char, 8> magic;
  uint32_t version;
  uint32_t header_size;
  uint32_t byte_order;
  uint32_t bitmap_unit;
  uint32_t path_version;
  uint32_t namespace_length;
  uint64_t next_epoch;
  uint64_t allocated_bytes;
  std::array<char, 3968> namespace_id;
};

static_assert(sizeof(CacheRootHeader) <= kCacheRootHeaderSize);

struct CacheMetaHeader {
  std::array<char, 8> magic;
  uint32_t version;
  uint32_t header_size;
  uint32_t bitmap_unit;
  uint32_t flags;
  uint64_t object_size;
  int64_t mtime;
  uint64_t generation_epoch;
  uint64_t written_end;
  uint32_t key_length;
  uint32_t etag_length;
  uint32_t version_length;
  uint32_t upload_id_length;
  uint32_t write_id_length;
  uint32_t reserved;
  uint64_t upload_part_size;
  uint32_t checksum_algorithm;
  uint32_t write_phase;
  std::array<char, kCacheKeyCapacity> key;
  std::array<char, kCacheEtagCapacity> etag;
  std::array<char, kCacheVersionCapacity> version_id;
  std::array<char, kCacheUploadIdCapacity> upload_id;
  std::array<char, kCacheWriteIdCapacity> write_id;
};

static_assert(sizeof(CacheMetaHeader) < kCacheMetaHeaderSize);

struct CacheDirtyMarker {
  std::array<char, 8> magic;
  uint32_t version;
  uint32_t header_size;
  uint64_t generation_epoch;
  uint32_t key_length;
  uint32_t reserved;
  std::array<char, kCacheKeyCapacity> key;
};

static_assert(sizeof(CacheDirtyMarker) < kCacheMetaHeaderSize);

struct CachePendingDeleteMarker {
  std::array<char, 8> magic;
  uint32_t version;
  uint32_t header_size;
  uint32_t phase;
  uint32_t key_length;
  uint32_t restore_key_length;
  uint32_t replacement_etag_length;
  std::array<char, kCacheKeyCapacity> key;
  std::array<char, kCacheKeyCapacity> restore_key;
  std::array<char, kCacheEtagCapacity> replacement_etag;
};

static_assert(sizeof(CachePendingDeleteMarker) < kCacheMetaHeaderSize);

bool cache_root_matches(const CacheRootHeader& header,
                        const CacheConfig& config) noexcept {
  constexpr std::array<char, 8> magic{'N', 'G', 'S', '3', 'R', 'O', 'O', 'T'};
  return header.magic == magic &&
      header.version == kCacheRootVersion &&
      header.header_size == kCacheRootHeaderSize &&
      header.byte_order == kCacheByteOrder &&
      header.bitmap_unit == kCacheBitmapUnit &&
      header.path_version == kCachePathVersion &&
      header.namespace_length == config.namespace_id.size() &&
      header.namespace_length <= header.namespace_id.size() &&
      memcmp(header.namespace_id.data(), config.namespace_id.data(),
             config.namespace_id.size()) == 0;
}

void cache_initialize_root(CacheRootHeader& header,
                           const CacheConfig& config) {
  if (config.namespace_id.size() > header.namespace_id.size()) {
    throw std::system_error(EOVERFLOW, std::generic_category(),
                            "cache namespace identity is too large");
  }
  memset(&header, 0, sizeof(header));
  header.magic            = {'N', 'G', 'S', '3', 'R', 'O', 'O', 'T'};
  header.version          = kCacheRootVersion;
  header.header_size      = kCacheRootHeaderSize;
  header.byte_order       = kCacheByteOrder;
  header.bitmap_unit      = uint32_t(kCacheBitmapUnit);
  header.path_version     = kCachePathVersion;
  header.namespace_length = uint32_t(config.namespace_id.size());
  header.next_epoch       = 1;
  memcpy(header.namespace_id.data(), config.namespace_id.data(),
         config.namespace_id.size());
}

[[noreturn]] void cache_throw_errno(const char* operation) {
  throw std::system_error(errno, std::generic_category(), operation);
}

size_t cache_round_up(size_t value, size_t alignment) {
  if (alignment == 0 || value > SIZE_MAX - (alignment - 1)) {
    throw std::overflow_error("cache size overflow");
  }
  return (value + alignment - 1) / alignment * alignment;
}

std::string cache_hex(std::span<const unsigned char> bytes) {
  constexpr char digits[] = "0123456789abcdef";
  std::string result(bytes.size() * 2, '\0');
  for (size_t i = 0; i < bytes.size(); ++i) {
    result[i * 2]     = digits[bytes[i] >> 4];
    result[i * 2 + 1] = digits[bytes[i] & 15];
  }
  return result;
}

std::string cache_base64url(std::string_view value) {
  constexpr char alphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  std::string result;
  result.reserve((value.size() * 4 + 2) / 3);
  size_t i = 0;
  while (i + 3 <= value.size()) {
    const unsigned a = u_char(value[i++]);
    const unsigned b = u_char(value[i++]);
    const unsigned c = u_char(value[i++]);
    result.push_back(alphabet[a >> 2]);
    result.push_back(alphabet[((a & 3) << 4) | (b >> 4)]);
    result.push_back(alphabet[((b & 15) << 2) | (c >> 6)]);
    result.push_back(alphabet[c & 63]);
  }
  if (i != value.size()) {
    const unsigned a = u_char(value[i++]);
    result.push_back(alphabet[a >> 2]);
    if (i == value.size()) {
      result.push_back(alphabet[(a & 3) << 4]);
    } else {
      const unsigned b = u_char(value[i]);
      result.push_back(alphabet[((a & 3) << 4) | (b >> 4)]);
      result.push_back(alphabet[(b & 15) << 2]);
    }
  }
  return result;
}

bool cache_component_needs_escape(std::string_view value) noexcept {
  if (value.empty() || value == "." || value == ".." ||
      value.starts_with(kCacheEscapePrefix)) {
    return true;
  }
  for (char ch : value) {
    const u_char byte = u_char(ch);
    if (byte < 0x20 || byte == 0x7f) {
      return true;
    }
  }
  return false;
}

std::string cache_encode_component(std::string_view component,
                                   size_t name_max) {
  if (!cache_component_needs_escape(component) &&
      component.size() <= name_max) {
    return std::string(component);
  }
  std::string encoded = std::string(kCacheEscapePrefix) + "b" +
      cache_base64url(component);
  if (encoded.size() <= name_max) {
    return encoded;
  }
  std::array<unsigned char, SHA256_DIGEST_LENGTH> digest;
  SHA256(reinterpret_cast<const unsigned char*>(component.data()),
         component.size(), digest.data());
  encoded = std::string(kCacheEscapePrefix) + "h" + cache_hex(digest);
  if (encoded.size() > name_max) {
    throw std::system_error(ENAMETOOLONG, std::generic_category(),
                            "cache filesystem NAME_MAX is too small");
  }
  return encoded;
}

void cache_mkdir_if_missing(int parent, const char* name) {
  if (::mkdirat(parent, name, 0700) != 0 && errno != EEXIST) {
    cache_throw_errno("mkdirat(cache)");
  }
}

void cache_secure_directory(int fd, const char* operation) {
  struct stat status{};
  if (::fstat(fd, &status) != 0) {
    cache_throw_errno(operation);
  }
  if (!S_ISDIR(status.st_mode) || status.st_uid != ::geteuid()) {
    throw std::runtime_error(
        "cache directory is not owned by the mounting user");
  }
  if ((status.st_mode & 0777) != 0700 && ::fchmod(fd, 0700) != 0) {
    cache_throw_errno("fchmod(cache directory)");
  }
}

UniqueFd cache_open_directory(int parent, const char* name) {
  const int fd = ::openat(parent, name,
                          O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (fd < 0) {
    cache_throw_errno("openat(cache directory)");
  }
  try {
    cache_secure_directory(fd, "fstat(cache directory)");
  } catch (...) {
    ::close(fd);
    throw;
  }
  return UniqueFd(fd);
}

UniqueFd cache_make_directory(int parent, const char* name) {
  cache_mkdir_if_missing(parent, name);
  return cache_open_directory(parent, name);
}

bool cache_is_directory(int parent, const char* name) {
  struct stat status{};
  if (::fstatat(parent, name, &status, AT_SYMLINK_NOFOLLOW) != 0) {
    if (errno == ENOENT) {
      return false;
    }
    cache_throw_errno("fstatat(cache path)");
  }
  if (S_ISLNK(status.st_mode)) {
    throw std::system_error(ELOOP, std::generic_category(),
                            "symlink inside cache tree");
  }
  return S_ISDIR(status.st_mode);
}

void cache_promote_file(int parent, const std::string& name) {
  static std::atomic<uint64_t> sequence{0};
  std::string temporary = std::string(kCacheEscapePrefix) + "promote-" +
      std::to_string(::getpid()) + "-" + std::to_string(
          sequence.fetch_add(1, std::memory_order_relaxed));
  if (::renameat(parent, name.c_str(), parent, temporary.c_str()) != 0) {
    cache_throw_errno("renameat(cache promotion source)");
  }
  try {
    if (::mkdirat(parent, name.c_str(), 0700) != 0) {
      cache_throw_errno("mkdirat(cache promotion)");
    }
    UniqueFd directory = cache_open_directory(parent, name.c_str());
    if (::renameat(parent, temporary.c_str(), directory.get(),
                   kCacheValueName) != 0) {
      cache_throw_errno("renameat(cache promotion value)");
    }
  } catch (...) {
    ::unlinkat(parent, name.c_str(), AT_REMOVEDIR);
    ::renameat(parent, temporary.c_str(), parent, name.c_str());
    throw;
  }
}

struct CacheLeaf {
  UniqueFd parent;
  std::string name;
};

CacheLeaf cache_leaf(int tree_root, std::string_view key, size_t name_max) {
  UniqueFd current(::fcntl(tree_root, F_DUPFD_CLOEXEC, 0));
  if (!current) {
    cache_throw_errno("fcntl(cache tree root)");
  }
  size_t begin = 0;
  for (;;) {
    const size_t slash = key.find('/', begin);
    const bool leaf = slash == std::string_view::npos;
    const std::string_view component = leaf
        ? key.substr(begin) : key.substr(begin, slash - begin);
    std::string encoded = cache_encode_component(component, name_max);
    if (leaf) {
      if (cache_is_directory(current.get(), encoded.c_str())) {
        current = cache_open_directory(current.get(), encoded.c_str());
        encoded = kCacheValueName;
      }
      return CacheLeaf{std::move(current), std::move(encoded)};
    }

    struct stat status{};
    if (::fstatat(current.get(), encoded.c_str(), &status,
                  AT_SYMLINK_NOFOLLOW) != 0) {
      if (errno != ENOENT) cache_throw_errno("fstatat(cache prefix)");
      cache_mkdir_if_missing(current.get(), encoded.c_str());
    } else if (S_ISREG(status.st_mode)) {
      cache_promote_file(current.get(), encoded);
    } else if (!S_ISDIR(status.st_mode)) {
      throw std::system_error(ELOOP, std::generic_category(),
                              "non-directory inside cache tree");
    }
    current = cache_open_directory(current.get(), encoded.c_str());
    begin = slash + 1;
  }
}

std::optional<CacheLeaf> cache_find_leaf(
    int tree_root, std::string_view key, size_t name_max) {
  UniqueFd current(::fcntl(tree_root, F_DUPFD_CLOEXEC, 0));
  if (!current) {
    cache_throw_errno("fcntl(cache tree root)");
  }
  size_t begin = 0;
  for (;;) {
    const size_t slash = key.find('/', begin);
    const bool leaf = slash == std::string_view::npos;
    const std::string_view component = leaf
        ? key.substr(begin) : key.substr(begin, slash - begin);
    std::string encoded = cache_encode_component(component, name_max);
    struct stat status{};
    if (::fstatat(current.get(), encoded.c_str(), &status,
                  AT_SYMLINK_NOFOLLOW) != 0) {
      if (errno == ENOENT) return std::nullopt;
      cache_throw_errno("fstatat(cache lookup)");
    }
    if (leaf) {
      if (S_ISDIR(status.st_mode)) {
        current = cache_open_directory(current.get(), encoded.c_str());
        encoded = kCacheValueName;
        if (::fstatat(current.get(), encoded.c_str(), &status,
                      AT_SYMLINK_NOFOLLOW) != 0) {
          if (errno == ENOENT) return std::nullopt;
          cache_throw_errno("fstatat(cache value lookup)");
        }
      }
      if (!S_ISREG(status.st_mode)) {
        throw std::runtime_error("non-regular cache object path");
      }
      return CacheLeaf{std::move(current), std::move(encoded)};
    }
    if (!S_ISDIR(status.st_mode)) return std::nullopt;
    current = cache_open_directory(current.get(), encoded.c_str());
    begin = slash + 1;
  }
}

size_t cache_bitmap_words(uint64_t size, size_t page_size) {
  const uint64_t pages = size == 0 ? 0 : (size - 1) / page_size + 1;
  if (pages > SIZE_MAX) {
    throw std::overflow_error("cache object has too many pages");
  }
  return (size_t(pages) + 31) / 32;
}

size_t cache_mapping_size(uint64_t size, size_t page_size) {
  const size_t words = cache_bitmap_words(size, page_size);
  if (words > (SIZE_MAX - kCacheMetaHeaderSize) / sizeof(uint64_t)) {
    throw std::overflow_error("cache metadata is too large");
  }
  return kCacheMetaHeaderSize + words * sizeof(uint64_t);
}

bool cache_identity_matches(const CacheMetaHeader& header,
                            const CacheIdentity& identity,
                            size_t bitmap_unit) noexcept {
  constexpr std::array<char, 8> magic{'N', 'G', 'S', '3', 'C', 'A', 'C', 'H'};
  if (header.magic != magic || header.version != kCacheMetaVersion ||
      header.header_size != kCacheMetaHeaderSize ||
      (header.flags & kCacheMetaDirty) != 0 ||
      header.bitmap_unit != bitmap_unit ||
      header.object_size != identity.size ||
      header.generation_epoch == 0 ||
      header.key_length != identity.key.size() ||
      header.etag_length != identity.etag.size() ||
      header.version_length != identity.version_id.size() ||
      header.key_length > header.key.size() ||
      header.etag_length > header.etag.size() ||
      header.version_length > header.version_id.size() ||
      header.upload_id_length > header.upload_id.size() ||
      header.write_id_length > header.write_id.size()) {
    return false;
  }
  if ((identity.key.size() != 0 &&
       memcmp(header.key.data(), identity.key.data(), identity.key.size()) != 0) ||
      (identity.etag.size() != 0 &&
       memcmp(header.etag.data(), identity.etag.data(), identity.etag.size()) != 0) ||
      (identity.version_id.size() != 0 &&
       memcmp(header.version_id.data(), identity.version_id.data(),
              identity.version_id.size()) != 0)) {
    return false;
  }
  if (!identity.version_id.empty() || !identity.etag.empty()) {
    return true;
  }
  return header.mtime == identity.mtime;
}

bool cache_recovery_header_matches(const CacheMetaHeader& header,
                                   std::string_view key,
                                   uint64_t epoch,
                                   const CacheConfig& config,
                                   size_t mapping_size) noexcept {
  constexpr std::array<char, 8> magic{'N', 'G', 'S', '3', 'C', 'A', 'C', 'H'};
  return header.magic == magic &&
      header.version == kCacheMetaVersion &&
      header.header_size == kCacheMetaHeaderSize &&
      header.bitmap_unit == kCacheBitmapUnit &&
      (header.flags & kCacheMetaDirty) != 0 &&
      header.generation_epoch == epoch &&
      header.key_length == key.size() &&
      header.key_length <= header.key.size() &&
      header.etag_length <= header.etag.size() &&
      header.version_length <= header.version_id.size() &&
      header.upload_id_length <= header.upload_id.size() &&
      header.write_id_length == kCacheWriteIdCapacity &&
      header.upload_part_size == config.upload_part_size &&
      header.checksum_algorithm == config.checksum_algorithm &&
      (header.write_phase == 1 || header.write_phase == 2) &&
      ((header.write_phase == 1 && header.upload_id_length == 0) ||
       (header.write_phase == 2 && header.upload_id_length != 0)) &&
      header.written_end == header.object_size &&
      mapping_size >= cache_mapping_size(header.written_end,
                                         kCacheBitmapUnit) &&
      memcmp(header.key.data(), key.data(), key.size()) == 0;
}

void cache_initialize_header(CacheMetaHeader& header,
                             const CacheIdentity& identity,
                             size_t bitmap_unit,
                             uint64_t epoch) {
  if (identity.key.size() > header.key.size() ||
      identity.etag.size() > header.etag.size() ||
      identity.version_id.size() > header.version_id.size()) {
    throw std::system_error(EOVERFLOW, std::generic_category(),
                            "cache identity exceeds metadata capacity");
  }
  memset(&header, 0, sizeof(header));
  header.magic       = {'N', 'G', 'S', '3', 'C', 'A', 'C', 'H'};
  header.version     = kCacheMetaVersion;
  header.header_size = kCacheMetaHeaderSize;
  header.bitmap_unit = uint32_t(bitmap_unit);
  header.object_size = identity.size;
  header.mtime       = identity.mtime;
  header.generation_epoch = epoch;
  header.key_length     = uint32_t(identity.key.size());
  header.etag_length    = uint32_t(identity.etag.size());
  header.version_length = uint32_t(identity.version_id.size());
  if (!identity.key.empty()) {
    memcpy(header.key.data(), identity.key.data(), identity.key.size());
  }
  if (!identity.etag.empty()) {
    memcpy(header.etag.data(), identity.etag.data(), identity.etag.size());
  }
  if (!identity.version_id.empty()) {
    memcpy(header.version_id.data(), identity.version_id.data(),
           identity.version_id.size());
  }
}

void cache_initialize_write_id(CacheMetaHeader& header) {
  std::array<unsigned char, kCacheWriteIdCapacity / 2> random;
  if (RAND_bytes(random.data(), int(random.size())) != 1) {
    throw std::runtime_error("unable to create cache write ID");
  }
  const std::string id = cache_hex(random);
  static_assert(kCacheWriteIdCapacity == 32);
  memcpy(header.write_id.data(), id.data(), id.size());
  header.write_id_length = uint32_t(id.size());
}

void cache_close_fd(int& fd) noexcept {
  if (fd >= 0) {
    ::close(fd);
    fd = -1;
  }
}

void advise_cache_data_file(int fd) noexcept {
  (void)::posix_fadvise(fd, 0, 0, POSIX_FADV_NOREUSE);
}

uint64_t cache_tree_allocation(int directory) {
  const int duplicate = ::openat(
      directory, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (duplicate < 0) {
    cache_throw_errno("open(cache allocation scan)");
  }
  DIR* stream = ::fdopendir(duplicate);
  if (stream == nullptr) {
    ::close(duplicate);
    cache_throw_errno("fdopendir(cache allocation scan)");
  }
  uint64_t bytes = 0;
  try {
    for (;;) {
      errno = 0;
      dirent* item = ::readdir(stream);
      if (item == nullptr) {
        if (errno != 0) {
          cache_throw_errno("readdir(cache allocation scan)");
        }
        break;
      }
      if (strcmp(item->d_name, ".") == 0 ||
          strcmp(item->d_name, "..") == 0) {
        continue;
      }
      struct stat status{};
      if (::fstatat(directory, item->d_name, &status,
                    AT_SYMLINK_NOFOLLOW) != 0) {
        cache_throw_errno("fstatat(cache allocation scan)");
      }
      uint64_t item_bytes = 0;
      if (S_ISDIR(status.st_mode)) {
        UniqueFd child = cache_open_directory(directory, item->d_name);
        item_bytes = cache_tree_allocation(child.get());
      } else if (S_ISREG(status.st_mode)) {
        item_bytes = uint64_t(status.st_blocks) * 512;
      } else {
        throw std::runtime_error("non-regular object inside cache tree");
      }
      bytes = bytes > UINT64_MAX - item_bytes
          ? UINT64_MAX : bytes + item_bytes;
    }
  } catch (...) {
    ::closedir(stream);
    throw;
  }
  ::closedir(stream);
  return bytes;
}

struct CacheCleanRecord {
  std::string key;
  std::string etag;
  std::string version_id;
  uint64_t size = 0;
  time_t mtime  = 0;
};

bool cache_visit_clean_directory(
    int directory, int data_root, const CacheConfig& config, size_t name_max,
    const std::function<bool(CacheCleanRecord&&)>& visit) {
  const int duplicate = ::openat(
      directory, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (duplicate < 0) {
    cache_throw_errno("open(cache clean directory scan)");
  }
  DIR* stream = ::fdopendir(duplicate);
  if (stream == nullptr) {
    ::close(duplicate);
    cache_throw_errno("fdopendir(cache clean directory)");
  }
  bool found = false;
  try {
    for (;;) {
      errno = 0;
      dirent* item = ::readdir(stream);
      if (item == nullptr) {
        if (errno != 0) {
          cache_throw_errno("readdir(cache clean directory)");
        }
        break;
      }
      if (strcmp(item->d_name, ".") == 0 ||
          strcmp(item->d_name, "..") == 0) {
        continue;
      }
      struct stat meta_status{};
      if (::fstatat(directory, item->d_name, &meta_status,
                    AT_SYMLINK_NOFOLLOW) != 0) {
        cache_throw_errno("fstatat(cache clean entry)");
      }
      if (S_ISDIR(meta_status.st_mode)) {
        UniqueFd child = cache_open_directory(directory, item->d_name);
        if (cache_visit_clean_directory(child.get(), data_root, config,
                                        name_max, visit)) {
          found = true;
          break;
        }
        continue;
      }
      if (!S_ISREG(meta_status.st_mode) ||
          meta_status.st_size < off_t(sizeof(CacheMetaHeader))) {
        continue;
      }
      UniqueFd fd(::openat(directory, item->d_name,
                           O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
      if (!fd) {
        cache_throw_errno("open(cache clean metadata scan)");
      }
      CacheMetaHeader header{};
      ssize_t read_bytes;
      do {
        read_bytes = ::pread(fd.get(), &header, sizeof(header), 0);
      } while (read_bytes < 0 && errno == EINTR);
      constexpr std::array<char, 8> magic{
          'N', 'G', 'S', '3', 'C', 'A', 'C', 'H'};
      if (read_bytes != ssize_t(sizeof(header)) || header.magic != magic ||
          header.version != kCacheMetaVersion ||
          header.header_size != kCacheMetaHeaderSize ||
          header.bitmap_unit != kCacheBitmapUnit ||
          (header.flags & kCacheMetaDirty) != 0 ||
          header.generation_epoch == 0 ||
          header.key_length == 0 || header.key_length > header.key.size() ||
          header.etag_length > header.etag.size() ||
          header.version_length > header.version_id.size()) {
        continue;
      }
      size_t mapping_size;
      try {
        mapping_size = cache_mapping_size(header.object_size,
                                          kCacheBitmapUnit);
      } catch (...) {
        continue;
      }
      if (uint64_t(meta_status.st_size) < mapping_size) {
        continue;
      }
      const std::string key(header.key.data(), header.key_length);
      std::optional<CacheLeaf> data = cache_find_leaf(
          data_root, key, name_max);
      if (!data) {
        continue;
      }
      struct stat data_status{};
      if (::fstatat(data->parent.get(), data->name.c_str(), &data_status,
                    AT_SYMLINK_NOFOLLOW) != 0 ||
          !S_ISREG(data_status.st_mode) ||
          uint64_t(std::max<off_t>(data_status.st_size, 0)) !=
              header.object_size) {
        continue;
      }
      if (visit(CacheCleanRecord{
          .key = key,
          .etag = std::string(header.etag.data(), header.etag_length),
          .version_id = std::string(
              header.version_id.data(), header.version_length),
          .size = header.object_size,
          .mtime = time_t(header.mtime),
      })) {
        found = true;
        break;
      }
    }
  } catch (...) {
    ::closedir(stream);
    throw;
  }
  ::closedir(stream);
  return found;
}

class CachePathWalker {
 public:
  // On success, an owned parent descriptor is transferred to the callback
  // whenever parent_owned is true.  Keeping the opened parent (rather than a
  // reconstructed relative path) preserves O_NOFOLLOW for every component.
  using Complete = void (*)(void*, bool, int, bool, std::string,
                            std::exception_ptr) noexcept;

  bool start(IoExecutor& executor, int root, std::string_view key,
             bool create, Complete complete, void* context,
             size_t name_max) {
    if (complete_ != nullptr) throw std::logic_error("cache path walker busy");
    executor_ = &executor;
    root_ = root;
    current_fd_ = root;
    next_fd_ = -1;
    create_ = create;
    complete_ = complete;
    context_ = context;
    components_.clear();
    temporary_.clear();
    error_ = {};
    index_ = 0;
    current_owned_ = false;
    leaf_directory_ = false;
    value_after_close_ = false;
    size_t begin = 0;
    for (;;) {
      const size_t slash = key.find('/', begin);
      const std::string_view component = slash == std::string_view::npos
          ? key.substr(begin) : key.substr(begin, slash - begin);
      components_.push_back(cache_encode_component(component, name_max));
      if (slash == std::string_view::npos) break;
      begin = slash + 1;
    }
    if (components_.empty()) throw std::invalid_argument("empty cache key");
    phase_ = Phase::STAT_COMPONENT;
    return submit_stat();
  }

 private:
  enum class Phase {
    STAT_COMPONENT, MKDIR_COMPONENT, OPEN_COMPONENT,
    PROMOTE_RENAME_OUT, PROMOTE_MKDIR, PROMOTE_OPEN,
    PROMOTE_RENAME_IN, CLOSE_PARENT, STAT_VALUE,
    ERROR_CLOSE_NEXT, ERROR_CLOSE_CURRENT,
  };

  void reset_io(AsyncIoRequest::Kind kind, int fd) noexcept {
    io_ = AsyncIoRequest{};
    io_.kind = kind;
    io_.fd = fd;
    io_.complete = completed;
    io_.context = this;
  }

  bool submit_stat() noexcept {
    memset(&status_, 0, sizeof(status_));
    reset_io(AsyncIoRequest::STATX, current_fd_);
    io_.path = components_[index_].c_str();
    io_.data = &status_;
    io_.flags = AT_SYMLINK_NOFOLLOW;
    io_.mask = STATX_TYPE;
    return submit();
  }

  bool submit_mkdir() noexcept {
    reset_io(AsyncIoRequest::MKDIRAT, current_fd_);
    io_.path = components_[index_].c_str();
    io_.mode = 0700;
    return submit();
  }

  bool submit_open_component() noexcept {
    reset_io(AsyncIoRequest::OPENAT, current_fd_);
    io_.path = components_[index_].c_str();
    io_.flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW;
    return submit();
  }

  bool submit_rename(int source, const char* source_name,
                     int destination, const char* destination_name) noexcept {
    reset_io(AsyncIoRequest::RENAMEAT, source);
    io_.output_fd = destination;
    io_.path = source_name;
    io_.path2 = destination_name;
    return submit();
  }

  bool submit_close(int fd) noexcept {
    reset_io(AsyncIoRequest::CLOSE, fd);
    return submit();
  }

  bool submit() noexcept {
    if (executor_->submit(io_)) return true;
    begin_error(make_error(errno == 0 ? EIO : errno,
                           "submit asynchronous cache path operation"));
    return false;
  }

  static std::exception_ptr make_error(int error, const char* what) noexcept {
    try {
      return std::make_exception_ptr(std::system_error(
          error, std::generic_category(), what));
    } catch (...) {
      return std::current_exception();
    }
  }

  static void completed(void* context, ssize_t result) noexcept {
    static_cast<CachePathWalker*>(context)->advance(result);
  }

  void advance(ssize_t result) noexcept {
    try {
      const int error = result < 0 ? int(-result) : 0;
      switch (phase_) {
        case Phase::STAT_COMPONENT: {
          const bool leaf = index_ + 1 == components_.size();
          if (error == ENOENT) {
            if (leaf) { finish(false, components_[index_]); return; }
            if (!create_) { finish_missing(); return; }
            phase_ = Phase::MKDIR_COMPONENT;
            submit_mkdir();
            return;
          }
          if (error != 0) throw std::system_error(
              error, std::generic_category(), "statx(cache path component)");
          const mode_t type = mode_t(status_.stx_mode & S_IFMT);
          if (leaf) {
            if (type == S_IFREG) {
              finish(true, components_[index_]);
              return;
            }
            if (type != S_IFDIR) {
              throw std::system_error(ELOOP, std::generic_category(),
                                      "non-regular cache object path");
            }
            leaf_directory_ = true;
            phase_ = Phase::OPEN_COMPONENT;
            submit_open_component();
            return;
          }
          if (type == S_IFDIR) {
            phase_ = Phase::OPEN_COMPONENT;
            submit_open_component();
            return;
          }
          if (type != S_IFREG) {
            if (create_) {
              throw std::system_error(ELOOP, std::generic_category(),
                                      "non-directory inside cache tree");
            }
            finish_missing();
            return;
          }
          if (!create_) { finish_missing(); return; }
          temporary_ = std::string(kCacheEscapePrefix) + "promote-" +
              std::to_string(::getpid()) + "-" +
              std::to_string(sequence_.fetch_add(1, std::memory_order_relaxed));
          phase_ = Phase::PROMOTE_RENAME_OUT;
          submit_rename(current_fd_, components_[index_].c_str(),
                        current_fd_, temporary_.c_str());
          return;
        }

        case Phase::MKDIR_COMPONENT:
          if (error == EEXIST) {
            phase_ = Phase::STAT_COMPONENT;
            submit_stat();
            return;
          }
          if (error != 0) throw std::system_error(
              error, std::generic_category(), "mkdirat(cache path component)");
          phase_ = Phase::OPEN_COMPONENT;
          submit_open_component();
          return;

        case Phase::OPEN_COMPONENT:
          if (error != 0) throw std::system_error(
              error, std::generic_category(), "openat(cache path component)");
          next_fd_ = int(result);
          if (leaf_directory_) {
            leaf_directory_ = false;
            if (current_owned_) {
              phase_ = Phase::CLOSE_PARENT;
              value_after_close_ = true;
              submit_close(current_fd_);
              return;
            }
            current_fd_ = next_fd_;
            next_fd_ = -1;
            current_owned_ = true;
            phase_ = Phase::STAT_VALUE;
            submit_value_stat();
            return;
          }
          transition_to_child();
          return;

        case Phase::PROMOTE_RENAME_OUT:
          if (error == ENOENT) {
            phase_ = Phase::STAT_COMPONENT;
            submit_stat();
            return;
          }
          if (error != 0) throw std::system_error(
              error, std::generic_category(), "renameat(cache promotion source)");
          phase_ = Phase::PROMOTE_MKDIR;
          submit_mkdir();
          return;

        case Phase::PROMOTE_MKDIR:
          if (error != 0) throw std::system_error(
              error, std::generic_category(), "mkdirat(cache promotion)");
          phase_ = Phase::PROMOTE_OPEN;
          submit_open_component();
          return;

        case Phase::PROMOTE_OPEN:
          if (error != 0) throw std::system_error(
              error, std::generic_category(), "openat(cache promotion)");
          next_fd_ = int(result);
          phase_ = Phase::PROMOTE_RENAME_IN;
          submit_rename(current_fd_, temporary_.c_str(),
                        next_fd_, kCacheValueName);
          return;

        case Phase::PROMOTE_RENAME_IN:
          if (error != 0) throw std::system_error(
              error, std::generic_category(), "renameat(cache promotion value)");
          transition_to_child();
          return;

        case Phase::CLOSE_PARENT:
          if (error != 0) throw std::system_error(
              error, std::generic_category(), "close(cache path parent)");
          current_fd_ = next_fd_;
          next_fd_ = -1;
          current_owned_ = true;
          if (value_after_close_) {
            value_after_close_ = false;
            phase_ = Phase::STAT_VALUE;
            submit_value_stat();
            return;
          }
          ++index_;
          phase_ = Phase::STAT_COMPONENT;
          submit_stat();
          return;

        case Phase::STAT_VALUE:
          if (error == ENOENT) { finish(false, kCacheValueName); return; }
          if (error != 0) throw std::system_error(
              error, std::generic_category(), "statx(cache value)");
          if ((status_.stx_mode & S_IFMT) != S_IFREG) {
            throw std::system_error(ELOOP, std::generic_category(),
                                    "non-regular cache value path");
          }
          finish(true, kCacheValueName);
          return;

        case Phase::ERROR_CLOSE_NEXT:
          if (error != 0 && next_fd_ >= 0) ::close(next_fd_);
          next_fd_ = -1;
          close_current_or_finish_error();
          return;

        case Phase::ERROR_CLOSE_CURRENT:
          if (error != 0 && current_owned_ && current_fd_ >= 0) {
            ::close(current_fd_);
          }
          current_fd_ = root_;
          current_owned_ = false;
          finish_error_now();
          return;
      }
    } catch (...) {
      begin_error(std::current_exception());
    }
  }

  void transition_to_child() {
    if (current_owned_) {
      phase_ = Phase::CLOSE_PARENT;
      submit_close(current_fd_);
      return;
    }
    current_fd_ = next_fd_;
    next_fd_ = -1;
    current_owned_ = true;
    ++index_;
    phase_ = Phase::STAT_COMPONENT;
    submit_stat();
  }

  bool submit_value_stat() noexcept {
    memset(&status_, 0, sizeof(status_));
    reset_io(AsyncIoRequest::STATX, current_fd_);
    io_.path = kCacheValueName;
    io_.data = &status_;
    io_.flags = AT_SYMLINK_NOFOLLOW;
    io_.mask = STATX_TYPE;
    return submit();
  }

  void finish(bool exists, std::string leaf) noexcept {
    Complete complete = complete_;
    void* context = context_;
    complete_ = nullptr;
    const int parent = current_fd_;
    const bool owned = current_owned_;
    current_fd_ = root_;
    current_owned_ = false;
    complete(context, exists, parent, owned, std::move(leaf), {});
  }

  void finish_missing() noexcept {
    error_ = {};
    begin_cleanup();
  }

  void begin_error(std::exception_ptr error) noexcept {
    if (!error_) error_ = std::move(error);
    begin_cleanup();
  }

  void begin_cleanup() noexcept {
    if (next_fd_ >= 0) {
      const int fd = next_fd_;
      phase_ = Phase::ERROR_CLOSE_NEXT;
      reset_io(AsyncIoRequest::CLOSE, fd);
      if (executor_->submit(io_)) return;
      ::close(fd);
      next_fd_ = -1;
    }
    close_current_or_finish_error();
  }

  void close_current_or_finish_error() noexcept {
    if (current_owned_ && current_fd_ >= 0) {
      const int fd = current_fd_;
      phase_ = Phase::ERROR_CLOSE_CURRENT;
      reset_io(AsyncIoRequest::CLOSE, fd);
      if (executor_->submit(io_)) return;
      ::close(fd);
      current_fd_ = root_;
      current_owned_ = false;
    }
    finish_error_now();
  }

  void finish_error_now() noexcept {
    Complete complete = complete_;
    void* context = context_;
    std::exception_ptr error = std::move(error_);
    complete_ = nullptr;
    complete(context, false, -1, false, {}, std::move(error));
  }

  inline static std::atomic<uint64_t> sequence_{0};
  IoExecutor* executor_ = nullptr;
  AsyncIoRequest io_;
  struct statx status_{};
  std::vector<std::string> components_;
  std::string temporary_;
  std::exception_ptr error_;
  Complete complete_ = nullptr;
  void* context_ = nullptr;
  int root_ = -1;
  int current_fd_ = -1;
  int next_fd_ = -1;
  size_t index_ = 0;
  Phase phase_ = Phase::STAT_COMPONENT;
  bool create_ = false;
  bool current_owned_ = false;
  bool leaf_directory_ = false;
  bool value_after_close_ = false;
};

class CacheCapacityOperation {
 public:
  enum class StartResult { RESERVED, PENDING, FAILED };
  using Complete = void (*)(void*, std::exception_ptr) noexcept;

  StartResult start(IoExecutor& executor, LocalCache& cache, uint64_t bytes,
                    Complete complete, void* context) {
    if (pending_ || complete == nullptr || bytes == 0) {
      errno = EINVAL;
      return StartResult::FAILED;
    }
    executor_ = &executor;
    cache_ = &cache;
    bytes_ = bytes;
    complete_ = complete;
    context_ = context;
    if (cache_->try_reserve_capacity(bytes_)) {
      reset();
      return StartResult::RESERVED;
    }
    snapshot_live_entries();
    if (!claim_live_region()) {
      if (!start_reclaim_or_cold()) {
        reset();
        errno = ENOSPC;
        return StartResult::FAILED;
      }
      pending_ = true;
      return StartResult::PENDING;
    }
    phase_ = Phase::STAT_BEFORE;
    if (!submit_statx()) {
      restore_claim();
      reset();
      return StartResult::FAILED;
    }
    pending_ = true;
    return StartResult::PENDING;
  }

 private:
  enum class Phase {
    STAT_BEFORE, PUNCH, STAT_AFTER,
    COLD_YIELD, COLD_PATH_STAT, COLD_META_OPEN,
    COLD_META_STAT, COLD_META_READ,
  };

  void snapshot_live_entries() {
    std::lock_guard guard(cache_->mutex_);
    entries_.reserve(cache_->entries_.size());
    for (auto i = cache_->entries_.begin(); i != cache_->entries_.end();) {
      std::shared_ptr<CacheEntry> entry = i->lock();
      if (!entry) {
        i = cache_->entries_.erase(i);
        continue;
      }
      entries_.push_back(std::move(entry));
      ++i;
    }
    if (!entries_.empty()) {
      entry_index_ = cache_->clock_entry_.fetch_add(
          1, std::memory_order_relaxed) % entries_.size();
    }
  }

  bool claim_live_region() {
    while (entry_attempts_ < entries_.size() * 2) {
      std::shared_ptr<CacheEntry> entry =
          entries_[entry_index_++ % entries_.size()];
      ++entry_attempts_;
      std::unique_lock guard(entry->mutex_);
      const auto& header =
          *static_cast<const CacheMetaHeader*>(entry->mapping_);
      if (cache_->config_.unlimited || entry->stale_ ||
          entry->eviction_disabled_ ||
          (header.flags & (kCacheMetaDirty | kCacheMetaExported)) != 0 ||
          entry->referenced_.empty()) {
        continue;
      }
      const size_t region_size = entry->block_size_;
      for (size_t scan = 0; scan < entry->referenced_.size(); ++scan) {
        const size_t region =
            entry->clock_hand_++ % entry->referenced_.size();
        if (entry->referenced_[region] != 0) {
          entry->referenced_[region] = 0;
          continue;
        }
        const uint64_t offset = uint64_t(region) * region_size;
        const uint64_t end =
            std::min<uint64_t>(entry->size_, offset + region_size);
        const size_t first = size_t(offset / entry->page_size_);
        const size_t last = size_t((end - 1) / entry->page_size_) + 1;
        clean_pages_.clear();
        bool evictable = entry->region_pins_[region] == 0;
        for (size_t page = first; page < last && evictable; ++page) {
          const CachePageState state = entry->page_state(page);
          if (state == CACHE_PAGE_CLEAN) clean_pages_.push_back(page);
          if (state != CACHE_PAGE_CLEAN && state != CACHE_PAGE_MISSING) {
            evictable = false;
          }
        }
        if (!evictable || clean_pages_.empty()) continue;
        for (size_t page : clean_pages_) {
          // READ_PENDING blocks a new fetch while the hole punch is in flight.
          entry->set_page_state(page, CACHE_PAGE_READ_PENDING);
        }
        victim_ = std::move(entry);
        offset_ = offset;
        length_ = region_size;
        return true;
      }
    }
    return false;
  }

  void finish_claim(bool punched) noexcept {
    if (!victim_) return;
    std::lock_guard guard(victim_->mutex_);
    for (size_t page : clean_pages_) {
      if (victim_->page_state(page) != CACHE_PAGE_READ_PENDING) abort();
      victim_->set_page_state(
          page, punched ? CACHE_PAGE_MISSING : CACHE_PAGE_CLEAN);
    }
    victim_->notify_waiters_locked();
    victim_.reset();
    clean_pages_.clear();
  }

  void restore_claim() noexcept { finish_claim(false); }

  void reset_io(AsyncIoRequest::Kind kind, int fd) noexcept {
    io_ = AsyncIoRequest{};
    io_.kind = kind;
    io_.fd = fd;
    io_.complete = completed;
    io_.context = this;
  }

  bool submit_statx() noexcept {
    status_ = {};
    reset_io(AsyncIoRequest::STATX, victim_->data_fd_);
    io_.path = "";
    io_.data = &status_;
    io_.flags = AT_EMPTY_PATH;
    io_.mask = STATX_BLOCKS | STATX_TYPE;
    return submit();
  }

  bool submit_punch() noexcept {
    reset_io(AsyncIoRequest::FALLOCATE, victim_->data_fd_);
    io_.flags = FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE;
    io_.input_offset = off_t(offset_);
    io_.length = size_t(length_);
    return submit();
  }

  bool start_cold_scan() {
    if (cold_exhausted_) return false;
    if (cold_stack_.empty()) {
      const int duplicate = ::openat(
          cache_->objects_root_fd_, ".",
          O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
      if (duplicate < 0) return false;
      DIR* stream = ::fdopendir(duplicate);
      if (stream == nullptr) {
        ::close(duplicate);
        return false;
      }
      cold_stack_.push_back(stream);
    }
    return scan_cold_batch();
  }

  bool start_reclaim_or_cold() {
    while (reclaim_index_ < entries_.size()) {
      std::shared_ptr<CacheEntry> entry =
          std::move(entries_[reclaim_index_++]);
      bool kept = false;
      {
        std::lock_guard cache_guard(cache_->mutex_);
        kept = std::any_of(
            cache_->keepalive_.begin(), cache_->keepalive_.end(),
            [&](const auto& slot) { return slot.entry.get() == entry.get(); });
      }
      if (entry.use_count() != (kept ? 2 : 1)) continue;
      {
        std::lock_guard entry_guard(entry->mutex_);
        const auto& header =
            *static_cast<const CacheMetaHeader*>(entry->mapping_);
        if (entry->stale_ || entry->detached_ ||
            entry->eviction_disabled_ ||
            (header.flags & (kCacheMetaDirty | kCacheMetaExported)) != 0 ||
            !entry->active_claims_.empty() || entry->checksum_ops_ != 0 ||
            entry->pinned_regions_ != 0) {
          continue;
        }
        bool all_missing = true;
        for (size_t page = 0; page < entry->page_count_; ++page) {
          if (entry->page_state(page) != CACHE_PAGE_MISSING) {
            all_missing = false;
            break;
          }
        }
        if (!all_missing) continue;
      }
      reclaim_entry_ = std::move(entry);
      reclaim_request_.complete = reclaim_completed;
      reclaim_request_.context = this;
      if (cache_->reclaim_closed_async(
              *executor_, reclaim_request_, reclaim_entry_)) {
        return true;
      }
      reclaim_entry_.reset();
    }
    return start_cold_scan();
  }

  static void reclaim_completed(void* context,
                                CacheAsyncResult) noexcept {
    auto* operation = static_cast<CacheCapacityOperation*>(context);
    operation->reclaim_entry_.reset();
    try {
      if (operation->cache_->try_reserve_capacity(operation->bytes_)) {
        operation->finish({});
        return;
      }
      if (!operation->start_reclaim_or_cold()) {
        operation->finish(make_error(
            ENOSPC, "reserve asynchronous cache capacity"));
      }
    } catch (...) {
      operation->finish(std::current_exception());
    }
  }

  bool scan_cold_batch() {
    size_t examined = 0;
    while (!cold_stack_.empty()) {
      if (examined >= 32) {
        phase_ = Phase::COLD_YIELD;
        return submit_cold_yield();
      }
      errno = 0;
      dirent* item = ::readdir(cold_stack_.back());
      ++examined;
      if (item == nullptr) {
        const int error = errno;
        ::closedir(cold_stack_.back());
        cold_stack_.pop_back();
        if (error != 0) throw std::system_error(
            error, std::generic_category(), "enumerate cold cache metadata");
        continue;
      }
      if (strcmp(item->d_name, ".") == 0 ||
          strcmp(item->d_name, "..") == 0) {
        continue;
      }
      cold_name_ = item->d_name;
      phase_ = Phase::COLD_PATH_STAT;
      if (!submit_cold_path_stat()) return false;
      return true;
    }
    cold_exhausted_ = true;
    return false;
  }

  bool continue_cold_scan() {
    if (scan_cold_batch()) return true;
    finish(make_error(ENOSPC, "reserve asynchronous cache capacity"));
    return false;
  }

  static void cold_entry_opened(void* context,
                                CacheAsyncResult result) noexcept {
    auto* operation = static_cast<CacheCapacityOperation*>(context);
    operation->cold_meta_fd_.reset();
    if (!result.error && result.entry) {
      operation->entries_.clear();
      operation->entries_.push_back(std::move(result.entry));
      operation->entry_index_ = 0;
      operation->entry_attempts_ = 0;
      operation->reclaim_index_ = 0;
      if (operation->claim_live_region()) {
        operation->phase_ = Phase::STAT_BEFORE;
        operation->submit_statx();
        return;
      }
    }
    try {
      if (!operation->start_reclaim_or_cold()) {
        operation->finish(make_error(
            ENOSPC, "reserve asynchronous cache capacity"));
      }
    } catch (...) {
      operation->finish(std::current_exception());
    }
  }

  bool open_cold_entry() {
    constexpr std::array<char, 8> magic{
        'N', 'G', 'S', '3', 'C', 'A', 'C', 'H'};
    if (cold_header_.magic != magic ||
        cold_header_.version != kCacheMetaVersion ||
        cold_header_.header_size != kCacheMetaHeaderSize ||
        cold_header_.bitmap_unit != kCacheBitmapUnit ||
        (cold_header_.flags & kCacheMetaDirty) != 0 ||
        cold_header_.generation_epoch == 0 ||
        cold_header_.key_length == 0 ||
        cold_header_.key_length > cold_header_.key.size() ||
        cold_header_.etag_length > cold_header_.etag.size() ||
        cold_header_.version_length > cold_header_.version_id.size()) {
      return continue_cold_scan();
    }
    size_t mapping_size;
    try {
      mapping_size = cache_mapping_size(
          cold_header_.object_size, kCacheBitmapUnit);
    } catch (...) {
      return continue_cold_scan();
    }
    if (uint64_t(cold_status_.stx_size) < mapping_size) {
      return continue_cold_scan();
    }
    cold_key_.assign(cold_header_.key.data(), cold_header_.key_length);
    cold_etag_.assign(cold_header_.etag.data(), cold_header_.etag_length);
    cold_version_.assign(
        cold_header_.version_id.data(), cold_header_.version_length);
    CacheIdentity identity{
        .key = cold_key_,
        .etag = cold_etag_,
        .version_id = cold_version_,
        .size = cold_header_.object_size,
        .mtime = time_t(cold_header_.mtime),
    };
    cold_request_.complete = cold_entry_opened;
    cold_request_.context = this;
    if (cache_->open_async_if_idle(
            *executor_, cold_request_, identity)) return true;
    return continue_cold_scan();
  }

  bool submit_cold_path_stat() noexcept {
    cold_status_ = {};
    reset_io(AsyncIoRequest::STATX, ::dirfd(cold_stack_.back()));
    io_.path = cold_name_.c_str();
    io_.data = &cold_status_;
    io_.flags = AT_SYMLINK_NOFOLLOW;
    io_.mask = STATX_SIZE | STATX_TYPE;
    return submit();
  }

  bool submit_cold_yield() noexcept {
    cold_status_ = {};
    reset_io(AsyncIoRequest::STATX, ::dirfd(cold_stack_.back()));
    io_.path = "";
    io_.data = &cold_status_;
    io_.flags = AT_EMPTY_PATH;
    io_.mask = STATX_TYPE;
    return submit();
  }

  bool submit_cold_meta_open() noexcept {
    reset_io(AsyncIoRequest::OPENAT, ::dirfd(cold_stack_.back()));
    io_.path = cold_name_.c_str();
    io_.flags = O_RDONLY | O_CLOEXEC | O_NOFOLLOW;
    return submit();
  }

  bool submit_cold_meta_stat() noexcept {
    cold_status_ = {};
    reset_io(AsyncIoRequest::STATX, cold_meta_fd_.get());
    io_.path = "";
    io_.data = &cold_status_;
    io_.flags = AT_EMPTY_PATH;
    io_.mask = STATX_SIZE | STATX_TYPE;
    return submit();
  }

  bool submit_cold_meta_read() noexcept {
    cold_header_ = {};
    reset_io(AsyncIoRequest::PREAD, cold_meta_fd_.get());
    io_.data = &cold_header_;
    io_.length = sizeof(cold_header_);
    io_.input_offset = 0;
    io_.exact = true;
    return submit();
  }

  bool submit() noexcept {
    if (executor_->submit(io_)) return true;
    const int error = errno == 0 ? EIO : errno;
    if (!pending_) {
      errno = error;
      return false;
    }
    restore_claim();
    finish(make_error(error, "submit asynchronous cache eviction"));
    return true;
  }

  static std::exception_ptr make_error(int error, const char* what) noexcept {
    try {
      return std::make_exception_ptr(std::system_error(
          error, std::generic_category(), what));
    } catch (...) {
      return std::current_exception();
    }
  }

  static void completed(void* context, ssize_t result) noexcept {
    static_cast<CacheCapacityOperation*>(context)->advance(result);
  }

  void advance(ssize_t result) noexcept {
    try {
      if (result < 0) {
        if (result == -ECANCELED || result == -ENOTCONN) {
          restore_claim();
          finish(make_error(
              int(-result), "cancel asynchronous cache eviction"));
          return;
        }
        if (phase_ >= Phase::COLD_YIELD) {
          cold_meta_fd_.reset();
          continue_cold_scan();
          return;
        }
        restore_claim();
        retry_or_finish();
        return;
      }
      switch (phase_) {
        case Phase::STAT_BEFORE:
          before_blocks_ = status_.stx_blocks;
          phase_ = Phase::PUNCH;
          submit_punch();
          return;
        case Phase::PUNCH:
          {
            std::shared_ptr<CacheEntry> punched = victim_;
            finish_claim(true);
            victim_ = std::move(punched);
          }
          phase_ = Phase::STAT_AFTER;
          submit_statx();
          return;
        case Phase::STAT_AFTER: {
          const uint64_t after_blocks = status_.stx_blocks;
          if (before_blocks_ > after_blocks) {
            cache_->add_allocated(-int64_t(std::min<uint64_t>(
                (before_blocks_ - after_blocks) * 512, INT64_MAX)));
          }
          victim_.reset();
          retry_or_finish();
          return;
        }
        case Phase::COLD_YIELD:
          continue_cold_scan();
          return;
        case Phase::COLD_PATH_STAT:
          if ((cold_status_.stx_mode & S_IFMT) == S_IFDIR) {
            const int child = ::openat(
                ::dirfd(cold_stack_.back()), cold_name_.c_str(),
                O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
            if (child >= 0) {
              DIR* stream = ::fdopendir(child);
              if (stream != nullptr) cold_stack_.push_back(stream);
              else ::close(child);
            }
            continue_cold_scan();
            return;
          }
          if ((cold_status_.stx_mode & S_IFMT) != S_IFREG ||
              cold_status_.stx_size < sizeof(CacheMetaHeader)) {
            continue_cold_scan();
            return;
          }
          phase_ = Phase::COLD_META_OPEN;
          submit_cold_meta_open();
          return;
        case Phase::COLD_META_OPEN:
          cold_meta_fd_.reset(int(result));
          phase_ = Phase::COLD_META_STAT;
          submit_cold_meta_stat();
          return;
        case Phase::COLD_META_STAT:
          if ((cold_status_.stx_mode & S_IFMT) != S_IFREG ||
              cold_status_.stx_size < sizeof(CacheMetaHeader)) {
            cold_meta_fd_.reset();
            continue_cold_scan();
            return;
          }
          phase_ = Phase::COLD_META_READ;
          submit_cold_meta_read();
          return;
        case Phase::COLD_META_READ:
          if (result != ssize_t(sizeof(CacheMetaHeader))) {
            cold_meta_fd_.reset();
            continue_cold_scan();
            return;
          }
          open_cold_entry();
          return;
      }
    } catch (...) {
      restore_claim();
      finish(std::current_exception());
    }
  }

  void retry_or_finish() {
    if (cache_->try_reserve_capacity(bytes_)) {
      finish({});
      return;
    }
    if (!claim_live_region()) {
      if (start_reclaim_or_cold()) return;
      finish(make_error(ENOSPC, "reserve asynchronous cache capacity"));
      return;
    }
    phase_ = Phase::STAT_BEFORE;
    submit_statx();
  }

  void finish(std::exception_ptr error) noexcept {
    Complete complete = complete_;
    void* context = context_;
    reset();
    complete(context, std::move(error));
  }

  void reset() noexcept {
    for (DIR* stream : cold_stack_) ::closedir(stream);
    cold_stack_.clear();
    cold_meta_fd_.reset();
    reclaim_entry_.reset();
    cold_name_.clear();
    cold_key_.clear();
    cold_etag_.clear();
    cold_version_.clear();
    executor_ = nullptr;
    cache_ = nullptr;
    entries_.clear();
    victim_.reset();
    clean_pages_.clear();
    complete_ = nullptr;
    context_ = nullptr;
    bytes_ = 0;
    entry_index_ = 0;
    entry_attempts_ = 0;
    reclaim_index_ = 0;
    offset_ = 0;
    length_ = 0;
    before_blocks_ = 0;
    pending_ = false;
    cold_exhausted_ = false;
  }

  IoExecutor* executor_ = nullptr;
  LocalCache* cache_ = nullptr;
  AsyncIoRequest io_;
  struct statx status_{};
  struct statx cold_status_{};
  CacheMetaHeader cold_header_{};
  CacheAsyncRequest cold_request_;
  CacheAsyncRequest reclaim_request_;
  UniqueFd cold_meta_fd_;
  std::vector<DIR*> cold_stack_;
  std::vector<std::shared_ptr<CacheEntry>> entries_;
  std::shared_ptr<CacheEntry> victim_;
  std::shared_ptr<CacheEntry> reclaim_entry_;
  std::vector<size_t> clean_pages_;
  std::string cold_name_;
  std::string cold_key_;
  std::string cold_etag_;
  std::string cold_version_;
  Complete complete_ = nullptr;
  void* context_ = nullptr;
  uint64_t bytes_ = 0;
  uint64_t offset_ = 0;
  uint64_t length_ = 0;
  uint64_t before_blocks_ = 0;
  size_t entry_index_ = 0;
  size_t entry_attempts_ = 0;
  size_t reclaim_index_ = 0;
  Phase phase_ = Phase::STAT_BEFORE;
  bool pending_ = false;
  bool cold_exhausted_ = false;
};

class CacheKeyOperation {
 protected:
  enum class GateResult { ACTIVE, QUEUED, FAILED };

  CacheKeyOperation(LocalCache& cache, IoExecutor& executor) noexcept
      : gate_cache_(&cache), gate_executor_(&executor) {}

  GateResult acquire_key_gate(std::string_view first,
                              std::string_view second = {},
                              bool wait = true) noexcept {
    try {
      gate_keys_[0].assign(first);
      gate_count_ = 1;
      if (!second.empty() && second != first) {
        gate_keys_[1].assign(second);
        gate_count_ = 2;
      }
      std::lock_guard guard(gate_cache_->mutex_);
      if (gate_available_locked()) {
        claim_gate_locked();
        return GateResult::ACTIVE;
      }
      if (!wait) {
        errno = EBUSY;
        return GateResult::FAILED;
      }
      wait_fd_.reset(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK));
      if (!wait_fd_) return GateResult::FAILED;
      gate_cache_->async_key_waiters_.push_back(this);
      gate_waiting_ = true;
      wait_io_ = AsyncIoRequest{};
      wait_io_.kind = AsyncIoRequest::READ;
      wait_io_.fd = wait_fd_.get();
      wait_io_.data = &wait_value_;
      wait_io_.length = sizeof(wait_value_);
      wait_io_.exact = true;
      wait_io_.complete = queued_completed;
      wait_io_.context = this;
      if (!gate_executor_->submit(wait_io_)) {
        gate_cache_->async_key_waiters_.pop_back();
        gate_waiting_ = false;
        wait_fd_.reset();
        return GateResult::FAILED;
      }
      return GateResult::QUEUED;
    } catch (...) {
      errno = ENOMEM;
      return GateResult::FAILED;
    }
  }

  void abandon_key_gate() noexcept {
    {
      std::lock_guard guard(gate_cache_->mutex_);
      if (gate_waiting_) {
        auto& waiters = gate_cache_->async_key_waiters_;
        const auto found = std::find(waiters.begin(), waiters.end(), this);
        if (found != waiters.end()) waiters.erase(found);
        gate_waiting_ = false;
      }
      if (gate_held_) {
        release_gate_locked();
        wake_ready_locked();
      }
    }
  }

  void release_key_gate() noexcept { abandon_key_gate(); }
  [[nodiscard]] bool key_gate_held() const noexcept { return gate_held_; }

 private:
  virtual void activate_key_gate() noexcept = 0;
  virtual void key_gate_failed(std::exception_ptr) noexcept = 0;

  static void queued_completed(void* context, ssize_t result) noexcept {
    static_cast<CacheKeyOperation*>(context)->queued_ready(result);
  }

  void queued_ready(ssize_t result) noexcept {
    wait_fd_.reset();
    if (result == ssize_t(sizeof(wait_value_))) {
      activate_key_gate();
      return;
    }
    {
      std::lock_guard guard(gate_cache_->mutex_);
      if (gate_waiting_) {
        auto& waiters = gate_cache_->async_key_waiters_;
        const auto found = std::find(waiters.begin(), waiters.end(), this);
        if (found != waiters.end()) waiters.erase(found);
        gate_waiting_ = false;
      }
      if (gate_held_) release_gate_locked();
      wake_ready_locked();
    }
    key_gate_failed(gate_error(result < 0 ? int(-result) : EIO));
  }

  static std::exception_ptr gate_error(int error) noexcept {
    try {
      return std::make_exception_ptr(std::system_error(
          error, std::generic_category(), "wait for cache key operation"));
    } catch (...) {
      return std::current_exception();
    }
  }

  void wake_queued() noexcept {
    const uint64_t wake = 1;
    ssize_t result;
    do {
      result = ::write(wait_fd_.get(), &wake, sizeof(wake));
    } while (result < 0 && errno == EINTR);
    if (result != ssize_t(sizeof(wake))) abort();
  }

  bool gate_available_locked() const noexcept {
    for (size_t i = 0; i < gate_count_; ++i) {
      for (const std::string& active : gate_cache_->async_keys_) {
        if (keys_conflict(active, gate_keys_[i])) return false;
      }
    }
    return true;
  }

  static bool keys_conflict(std::string_view left,
                            std::string_view right) noexcept {
    if (left == right) return true;
    if (left.size() < right.size()) {
      return right.starts_with(left) && right[left.size()] == '/';
    }
    return left.starts_with(right) && left[right.size()] == '/';
  }

  void claim_gate_locked() {
    for (size_t i = 0; i < gate_count_; ++i) {
      gate_cache_->async_keys_.insert(gate_keys_[i]);
    }
    gate_waiting_ = false;
    gate_held_ = true;
  }

  void release_gate_locked() noexcept {
    for (size_t i = 0; i < gate_count_; ++i) {
      gate_cache_->async_keys_.erase(gate_keys_[i]);
    }
    gate_held_ = false;
  }

  void wake_ready_locked() {
    auto& waiters = gate_cache_->async_key_waiters_;
    for (auto i = waiters.begin(); i != waiters.end();) {
      CacheKeyOperation* candidate = *i;
      if (!candidate->gate_available_locked()) {
        ++i;
        continue;
      }
      i = waiters.erase(i);
      candidate->claim_gate_locked();
      candidate->wake_queued();
    }
  }

  LocalCache* gate_cache_ = nullptr;
  IoExecutor* gate_executor_ = nullptr;
  AsyncIoRequest wait_io_;
  UniqueFd wait_fd_;
  uint64_t wait_value_ = 0;
  std::array<std::string, 2> gate_keys_;
  size_t gate_count_ = 0;
  bool gate_waiting_ = false;
  bool gate_held_ = false;
};

class CacheAsyncOperation {
 public:
  enum class Kind {
    PREPARE_READ,
    PREPARE_WRITE,
    BEGIN_WRITE,
    SET_UPLOAD_ID,
    MARK_COMMIT_PENDING,
    DISCARD_WRITE,
    SYNC_WRITE,
    COMMIT_WRITE,
  };

  CacheAsyncOperation(IoExecutor& executor, CacheAsyncRequest& request,
                      CacheEntry& entry, Kind kind) noexcept
      : executor_(&executor), request_(&request), entry_(&entry), kind_(kind) {}

  bool start_prepare_read(uint64_t offset, size_t length) {
    if (length != 0 && offset > UINT64_MAX - length) {
      errno = EOVERFLOW;
      return false;
    }
    offset_ = offset;
    length_ = length;
    return begin_or_queue();
  }

  bool activate_prepare_read() {
    start_ = offset_ / entry_->owner_->config_.block_size *
        entry_->owner_->config_.block_size;
    reserve_ = length_ == 0 ? 0 :
        cache_round_up(length_, entry_->owner_->config_.block_size);
    phase_ = Phase::RANGE_STAT_BEFORE;
    return submit_statx(entry_->data_fd_);
  }

  bool start_prepare_write(uint64_t offset, size_t length) {
    if (length != 0 && offset > UINT64_MAX - length) {
      errno = EOVERFLOW;
      return false;
    }
    offset_ = offset;
    length_ = length;
    end_ = offset + length;
    return begin_or_queue();
  }

  bool activate_prepare_write() {
    {
      std::unique_lock guard(entry_->mutex_);
      const auto& header = *static_cast<const CacheMetaHeader*>(entry_->mapping_);
      if (entry_->stale_ || (header.flags & kCacheMetaExported) != 0) {
        return reject_locked(guard, ESTALE);
      }
    }
    if (length_ == 0) return start_dirty_marker();
    const uint64_t unit = entry_->owner_->config_.upload_part_size;
    if (unit == 0 || end_ > UINT64_MAX - (unit - 1)) return reject(EOVERFLOW);
    const uint64_t capacity = (end_ + unit - 1) / unit * unit;
    wanted_mapping_size_ = cache_mapping_size(capacity, entry_->page_size_);
    {
      std::lock_guard guard(entry_->mutex_);
      old_mapping_size_ = entry_->mapping_size_;
    }
    first_part_ = size_t(offset_ / unit);
    last_part_  = size_t((end_ - 1) / unit);
    current_part_ = first_part_;
    if (wanted_mapping_size_ > old_mapping_size_) {
      phase_ = Phase::META_STAT_BEFORE;
      return submit_statx(entry_->meta_fd_);
    }
    return start_next_write_range();
  }

  bool start_begin_write() {
    return begin_or_queue();
  }

  bool start_set_upload_id(std::string_view upload_id) {
    if (upload_id.size() > kCacheUploadIdCapacity) {
      errno = EOVERFLOW;
      return false;
    }
    upload_id_.assign(upload_id);
    return begin_or_queue();
  }

  bool activate_set_upload_id() {
    {
      std::unique_lock guard(entry_->mutex_);
      if (entry_->stale_) return reject_locked(guard, ESTALE);
      auto& header = *static_cast<CacheMetaHeader*>(entry_->mapping_);
      memset(header.upload_id.data(), 0, header.upload_id.size());
      memcpy(header.upload_id.data(), upload_id_.data(), upload_id_.size());
      header.upload_id_length = uint32_t(upload_id_.size());
      header.upload_part_size = entry_->owner_->config_.upload_part_size;
      header.write_phase      = upload_id_.empty() ? 1 : 2;
    }
    phase_ = Phase::UPDATE_META;
    return submit_fsync(entry_->meta_fd_, false);
  }

  bool start_mark_commit_pending() {
    return begin_or_queue();
  }

  bool activate_mark_commit_pending() {
    {
      std::unique_lock guard(entry_->mutex_);
      if (entry_->stale_) return reject_locked(guard, ESTALE);
      auto& header = *static_cast<CacheMetaHeader*>(entry_->mapping_);
      if ((header.flags & kCacheMetaDirty) == 0) {
        return reject_locked(guard, EINVAL);
      }
      header.write_phase = 3;
    }
    phase_ = Phase::UPDATE_META;
    return submit_fsync(entry_->meta_fd_, false);
  }

  bool start_discard_write() {
    return begin_or_queue();
  }

  bool activate_discard_write() {
    {
      std::unique_lock guard(entry_->mutex_);
      auto& header = *static_cast<CacheMetaHeader*>(entry_->mapping_);
      header.flags       &= ~kCacheMetaDirty;
      header.write_phase  = 0;
      entry_->stale_      = true;
      entry_->detached_   = true;
      marker_fd_          = entry_->dirty_fd_;
      if (marker_fd_ >= 0) entry_->dirty_fd_ = -1;
    }
    if (marker_fd_ < 0) {
      phase_ = Phase::DISCARD_NO_MARKER;
      return submit_statx(entry_->data_fd_);
    }
    marker_path_action_ = MarkerPathAction::REMOVE;
    return path_.start(*executor_, entry_->owner_->dirty_root_fd_,
                       entry_->key_, false, marker_path_resolved, this,
                       entry_->owner_->name_max_);
  }

  bool start_sync_write() {
    return begin_or_queue();
  }

  bool activate_sync_write() {
    {
      std::unique_lock guard(entry_->mutex_);
      if (entry_->stale_) return reject_locked(guard, ESTALE);
    }
    phase_ = Phase::SYNC_DATA;
    return submit_fsync(entry_->data_fd_, true);
  }

  bool start_commit_write(const CacheIdentity& identity) {
    if (identity.etag.size() > kCacheEtagCapacity ||
        identity.version_id.size() > kCacheVersionCapacity) {
      errno = EOVERFLOW;
      return false;
    }
    identity_key_.assign(identity.key);
    etag_.assign(identity.etag);
    version_id_.assign(identity.version_id);
    identity_size_ = identity.size;
    identity_mtime_ = identity.mtime;
    return begin_or_queue();
  }

  bool activate_commit_write() {
    {
      std::unique_lock guard(entry_->mutex_);
      if (identity_key_ != entry_->key_ || identity_size_ != entry_->size_) {
        return reject_locked(guard, EINVAL);
      }
      auto& header = *static_cast<CacheMetaHeader*>(entry_->mapping_);
      memset(header.etag.data(), 0, header.etag.size());
      memset(header.version_id.data(), 0, header.version_id.size());
      memcpy(header.etag.data(), etag_.data(), etag_.size());
      memcpy(header.version_id.data(), version_id_.data(), version_id_.size());
      header.etag_length      = uint32_t(etag_.size());
      header.version_length   = uint32_t(version_id_.size());
      header.mtime            = identity_mtime_;
      header.flags           &= ~kCacheMetaDirty;
      header.write_phase      = 0;
      header.upload_id_length = 0;
      entry_->eviction_disabled_ = false;
    }
    phase_ = Phase::COMMIT_META;
    return submit_fsync(entry_->meta_fd_, false);
  }

  void abandon_start() noexcept {
    if (reservation_ != 0) {
      entry_->owner_->cancel_reservation(reservation_);
      reservation_ = 0;
    }
    if (request_->implementation_ == this) {
      {
        std::lock_guard guard(entry_->mutex_);
        (void)release_entry_operation_locked();
      }
      request_->implementation_ = nullptr;
    }
  }

 private:
  enum class Phase {
    RANGE_STAT_BEFORE, RANGE_FALLOCATE, RANGE_STAT_AFTER,
    META_STAT_BEFORE, META_FALLOCATE, META_STAT_AFTER,
    MARKER_ALREADY, MARKER_OPEN, MARKER_FALLOCATE, MARKER_WRITE,
    MARKER_STAT_AFTER,
    UPDATE_META, DISCARD_NO_MARKER, SYNC_DATA, SYNC_META, SYNC_MARKER,
    COMMIT_META, COMMIT_MARKER_STAT, COMMIT_MARKER_CLOSE,
    COMMIT_MARKER_UNLINK, FINAL_MARKER_CLOSE, FINAL_PARENT_CLOSE,
  };

  enum class MarkerPathAction { NONE, CREATE, REMOVE };
  enum class CapacityAction { NONE, RANGE, META, MARKER };

  static void capacity_completed(void* context,
                                 std::exception_ptr error) noexcept {
    auto* operation = static_cast<CacheAsyncOperation*>(context);
    if (error) {
      if (operation->capacity_action_ == CapacityAction::RANGE &&
          operation->kind_ == Kind::PREPARE_READ) {
        operation->finish(false);
      } else {
        operation->finish(std::move(error));
      }
      return;
    }
    operation->reservation_ = operation->capacity_bytes_;
    try {
      operation->capacity_ready();
    } catch (...) {
      operation->finish(std::current_exception());
    }
  }

  bool request_capacity(uint64_t bytes, CapacityAction action) {
    capacity_bytes_ = bytes;
    capacity_action_ = action;
    const CacheCapacityOperation::StartResult result = capacity_.start(
        *executor_, *entry_->owner_, bytes, capacity_completed, this);
    if (result == CacheCapacityOperation::StartResult::RESERVED) {
      reservation_ = bytes;
      return capacity_ready();
    }
    if (result == CacheCapacityOperation::StartResult::PENDING) {
      accepted_ = true;
      return true;
    }
    const int error = errno == 0 ? ENOSPC : errno;
    if (action == CapacityAction::RANGE && kind_ == Kind::PREPARE_READ) {
      finish(false);
      return false;
    }
    throw std::system_error(error, std::generic_category(),
                            "reserve asynchronous cache capacity");
  }

  bool capacity_ready() {
    const CapacityAction action = std::exchange(
        capacity_action_, CapacityAction::NONE);
    switch (action) {
      case CapacityAction::RANGE:
        phase_ = Phase::RANGE_FALLOCATE;
        return submit_fallocate(entry_->data_fd_, FALLOC_FL_KEEP_SIZE,
                                start_, reserve_);
      case CapacityAction::META:
        phase_ = Phase::META_FALLOCATE;
        return submit_fallocate(entry_->meta_fd_, 0, old_mapping_size_,
                                wanted_mapping_size_ - old_mapping_size_);
      case CapacityAction::MARKER:
        marker_path_action_ = MarkerPathAction::CREATE;
        return path_.start(*executor_, entry_->owner_->dirty_root_fd_,
                           entry_->key_, true, marker_path_resolved, this,
                           entry_->owner_->name_max_);
      case CapacityAction::NONE:
        abort();
    }
    abort();
  }

  static void marker_path_resolved(void* context, bool exists, int parent_fd,
                                   bool parent_owned, std::string leaf,
                                   std::exception_ptr error) noexcept {
    auto* operation = static_cast<CacheAsyncOperation*>(context);
    if (error) { operation->finish(std::move(error)); return; }
    operation->marker_parent_fd_ = parent_fd;
    operation->marker_parent_owned_ = parent_owned;
    operation->marker_name_ = std::move(leaf);
    operation->marker_named_ = exists ||
        operation->marker_path_action_ == MarkerPathAction::CREATE;
    if (operation->marker_path_action_ == MarkerPathAction::CREATE) {
      operation->phase_ = Phase::MARKER_OPEN;
      operation->submit_open_marker();
      return;
    }
    operation->phase_ = Phase::COMMIT_MARKER_STAT;
    operation->submit_statx(operation->marker_fd_);
  }

  enum class BeginResult { REJECTED, ACTIVE, QUEUED };

  BeginResult begin_entry_operation() noexcept {
    if (request_->complete == nullptr || request_->pending()) {
      errno = EINVAL;
      return BeginResult::REJECTED;
    }
    std::lock_guard guard(entry_->mutex_);
    request_->implementation_ = this;
    if (entry_->async_metadata_inflight_) {
      wait_fd_.reset(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK));
      if (!wait_fd_) {
        request_->implementation_ = nullptr;
        return BeginResult::REJECTED;
      }
      try {
        entry_->async_metadata_queue_.push_back(this);
      } catch (...) {
        request_->implementation_ = nullptr;
        errno = ENOMEM;
        return BeginResult::REJECTED;
      }
      queued_linked_ = true;
      wait_io_ = AsyncIoRequest{};
      wait_io_.kind     = AsyncIoRequest::READ;
      wait_io_.fd       = wait_fd_.get();
      wait_io_.data     = &wait_value_;
      wait_io_.length   = sizeof(wait_value_);
      wait_io_.exact    = true;
      wait_io_.complete = queued_completed;
      wait_io_.context  = this;
      if (!executor_->submit(wait_io_)) {
        entry_->async_metadata_queue_.pop_back();
        queued_linked_ = false;
        request_->implementation_ = nullptr;
        wait_fd_.reset();
        return BeginResult::REJECTED;
      }
      accepted_ = true;
      return BeginResult::QUEUED;
    }
    entry_->async_metadata_inflight_ = true;
    return BeginResult::ACTIVE;
  }

  bool begin_or_queue() {
    const BeginResult result = begin_entry_operation();
    if (result == BeginResult::REJECTED) return false;
    if (result == BeginResult::QUEUED) return true;
    return activate();
  }

  bool activate() {
    switch (kind_) {
      case Kind::PREPARE_READ: return activate_prepare_read();
      case Kind::PREPARE_WRITE: return activate_prepare_write();
      case Kind::BEGIN_WRITE: return start_dirty_marker();
      case Kind::SET_UPLOAD_ID: return activate_set_upload_id();
      case Kind::MARK_COMMIT_PENDING: return activate_mark_commit_pending();
      case Kind::DISCARD_WRITE: return activate_discard_write();
      case Kind::SYNC_WRITE: return activate_sync_write();
      case Kind::COMMIT_WRITE: return activate_commit_write();
    }
    abort();
  }

  static void queued_completed(void* context, ssize_t result) noexcept {
    static_cast<CacheAsyncOperation*>(context)->queued_ready(result);
  }

  void queued_ready(ssize_t result) noexcept {
    if (result == ssize_t(sizeof(wait_value_))) {
      wait_fd_.reset();
      try {
        (void)activate();
      } catch (...) {
        finish(std::current_exception());
      }
      return;
    }
    {
      std::lock_guard guard(entry_->mutex_);
      if (queued_linked_) {
        const auto found = std::find(
            entry_->async_metadata_queue_.begin(),
            entry_->async_metadata_queue_.end(), this);
        if (found == entry_->async_metadata_queue_.end()) abort();
        entry_->async_metadata_queue_.erase(found);
        queued_linked_ = false;
      } else {
        (void)release_entry_operation_locked();
      }
      wait_fd_.reset();
    }
    const int error = result < 0 ? int(-result) : EIO;
    deliver_result(CacheAsyncResult{
        .entry = {},
        .error = make_error(error, "wait for cache entry operation"),
        .value = false});
  }

  static std::exception_ptr make_error(int error,
                                       const char* operation) noexcept {
    try {
      return std::make_exception_ptr(std::system_error(
          error, std::generic_category(), operation));
    } catch (...) {
      return std::current_exception();
    }
  }

  void wake_queued() noexcept {
    const uint64_t wake = 1;
    ssize_t result;
    do {
      result = ::write(wait_fd_.get(), &wake, sizeof(wake));
    } while (result < 0 && errno == EINTR);
    if (result != ssize_t(sizeof(wake))) abort();
  }

  CacheAsyncOperation* release_entry_operation_locked() noexcept {
    if (entry_->async_metadata_queue_.empty()) {
      entry_->async_metadata_inflight_ = false;
      return nullptr;
    }
    CacheAsyncOperation* next = entry_->async_metadata_queue_.front();
    entry_->async_metadata_queue_.pop_front();
    if (!next->queued_linked_) abort();
    next->queued_linked_ = false;
    next->wake_queued();
    return next;
  }

  bool reject(int error) noexcept {
    if (accepted_) {
      finish_error(error, "activate queued cache operation");
      return false;
    }
    if (reservation_ != 0) {
      entry_->owner_->cancel_reservation(reservation_);
      reservation_ = 0;
    }
    {
      std::unique_lock guard(entry_->mutex_);
      (void)release_entry_operation_locked();
    }
    request_->implementation_ = nullptr;
    errno = error;
    return false;
  }

  template<class Guard>
  bool reject_locked(Guard& guard, int error) noexcept {
    if (accepted_) {
      guard.unlock();
      finish_error(error, "activate queued cache operation");
      return false;
    }
    (void)release_entry_operation_locked();
    guard.unlock();
    request_->implementation_ = nullptr;
    errno = error;
    return false;
  }

  void reset_io(AsyncIoRequest::Kind kind, int fd) noexcept {
    io_ = AsyncIoRequest{};
    io_.kind     = kind;
    io_.fd       = fd;
    io_.complete = completed;
    io_.context  = this;
  }

  bool submit_statx(int fd) noexcept {
    memset(&status_, 0, sizeof(status_));
    reset_io(AsyncIoRequest::STATX, fd);
    io_.path  = "";
    io_.data  = &status_;
    io_.flags = AT_EMPTY_PATH;
    io_.mask  = STATX_BLOCKS | STATX_SIZE | STATX_TYPE;
    return submit_or_finish();
  }

  bool submit_fallocate(int fd, int mode, uint64_t offset,
                        uint64_t length) noexcept {
    reset_io(AsyncIoRequest::FALLOCATE, fd);
    io_.flags        = unsigned(mode);
    io_.input_offset = off_t(offset);
    io_.length       = size_t(length);
    return submit_or_finish();
  }

  bool submit_fsync(int fd, bool data_only) noexcept {
    reset_io(AsyncIoRequest::FSYNC, fd);
    io_.flags = data_only ? 1U : 0U;
    return submit_or_finish();
  }

  bool submit_open_marker() noexcept {
    reset_io(AsyncIoRequest::OPENAT, marker_parent_fd_);
    io_.path  = marker_name_.c_str();
    io_.flags = O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW;
    io_.mode  = 0600;
    return submit_or_finish();
  }

  bool submit_marker_write() noexcept {
    reset_io(AsyncIoRequest::PWRITE, marker_fd_);
    io_.data          = &marker_;
    io_.length        = sizeof(marker_);
    io_.output_offset = 0;
    io_.exact         = true;
    return submit_or_finish();
  }

  bool submit_marker_close() noexcept {
    reset_io(AsyncIoRequest::CLOSE, marker_fd_);
    return submit_or_finish();
  }

  bool submit_marker_unlink() noexcept {
    reset_io(AsyncIoRequest::UNLINKAT, marker_parent_fd_);
    io_.path = marker_name_.c_str();
    return submit_or_finish();
  }

  bool submit_or_finish() noexcept {
    if (executor_->submit(io_)) {
      accepted_ = true;
      return true;
    }
    const int error = errno == 0 ? EIO : errno;
    if (!accepted_) return reject(error);
    finish_error(error, "submit cache io_uring operation");
    return false;
  }

  static void completed(void* context, ssize_t result) noexcept {
    static_cast<CacheAsyncOperation*>(context)->advance(result);
  }

  void advance(ssize_t result) noexcept {
    try {
      if (phase_ == Phase::FINAL_PARENT_CLOSE) {
        if (result < 0 && marker_parent_fd_ >= 0) {
          ::close(marker_parent_fd_);
        }
        marker_parent_fd_ = -1;
        complete_result(std::move(final_result_));
        return;
      }
      if (phase_ == Phase::FINAL_MARKER_CLOSE) {
        if (result < 0 && marker_fd_ >= 0) ::close(marker_fd_);
        marker_fd_ = -1;
        marker_fd_owned_ = false;
        finish_result(std::move(final_result_));
        return;
      }
      if (result < 0) {
        const int error = int(-result);
        if (reservation_ != 0) {
          entry_->owner_->cancel_reservation(reservation_);
          reservation_ = 0;
        }
        if (kind_ == Kind::PREPARE_READ &&
            phase_ == Phase::RANGE_FALLOCATE &&
            (error == ENOSPC || error == EDQUOT)) {
          finish(false);
          return;
        }
        if (phase_ == Phase::COMMIT_MARKER_UNLINK && error == ENOENT) {
          finish(true);
          return;
        }
        throw std::system_error(error, std::generic_category(),
                                "asynchronous cache filesystem operation");
      }
      switch (phase_) {
        case Phase::FINAL_PARENT_CLOSE:
          return;

        case Phase::FINAL_MARKER_CLOSE:
          return;

        case Phase::DISCARD_NO_MARKER:
          finish(true);
          return;

        case Phase::UPDATE_META:
          finish(true);
          return;

        case Phase::MARKER_ALREADY:
          finish(true);
          return;

        case Phase::RANGE_STAT_BEFORE:
          before_blocks_ = status_.stx_blocks;
          if (reserve_ == 0) { finish(true); return; }
          request_capacity(reserve_, CapacityAction::RANGE);
          return;

        case Phase::RANGE_FALLOCATE:
          phase_ = Phase::RANGE_STAT_AFTER;
          submit_statx(entry_->data_fd_);
          return;

        case Phase::RANGE_STAT_AFTER: {
          const uint64_t after_blocks = status_.stx_blocks;
          entry_->owner_->finish_reservation(
              reservation_, after_blocks > before_blocks_
                  ? (after_blocks - before_blocks_) * 512 : 0);
          reservation_ = 0;
          if (kind_ == Kind::PREPARE_READ) { finish(true); return; }
          {
            std::lock_guard guard(entry_->mutex_);
            if (entry_->reserved_units_.size() <= current_part_) {
              entry_->reserved_units_.resize(current_part_ + 1, 0);
            }
            entry_->reserved_units_[current_part_] = 1;
          }
          ++current_part_;
          start_next_write_range();
          return;
        }

        case Phase::META_STAT_BEFORE: {
          before_blocks_ = status_.stx_blocks;
          const uint64_t bytes = cache_round_up(
              wanted_mapping_size_ - old_mapping_size_,
              entry_->owner_->config_.page_size);
          request_capacity(bytes, CapacityAction::META);
          return;
        }

        case Phase::META_FALLOCATE:
          phase_ = Phase::META_STAT_AFTER;
          submit_statx(entry_->meta_fd_);
          return;

        case Phase::META_STAT_AFTER: {
          const uint64_t after_blocks = status_.stx_blocks;
          entry_->owner_->finish_reservation(
              reservation_, after_blocks > before_blocks_
                  ? (after_blocks - before_blocks_) * 512 : 0);
          reservation_ = 0;
          {
            std::lock_guard guard(entry_->mutex_);
            if (entry_->mapping_size_ != old_mapping_size_) {
              throw std::logic_error("cache metadata mapping changed in flight");
            }
            void* expanded = ::mremap(entry_->mapping_, old_mapping_size_,
                                      wanted_mapping_size_, MREMAP_MAYMOVE);
            if (expanded == MAP_FAILED) cache_throw_errno("mremap(cache metadata)");
            entry_->mapping_      = expanded;
            entry_->mapping_size_ = wanted_mapping_size_;
          }
          start_next_write_range();
          return;
        }

        case Phase::MARKER_OPEN:
          marker_fd_ = int(result);
          marker_fd_owned_ = true;
          phase_ = Phase::MARKER_FALLOCATE;
          submit_fallocate(marker_fd_, 0, 0, kCacheMetaHeaderSize);
          return;

        case Phase::MARKER_FALLOCATE:
          phase_ = Phase::MARKER_WRITE;
          submit_marker_write();
          return;

        case Phase::MARKER_WRITE:
          phase_ = Phase::MARKER_STAT_AFTER;
          submit_statx(marker_fd_);
          return;

        case Phase::MARKER_STAT_AFTER: {
          entry_->owner_->finish_reservation(
              reservation_, status_.stx_blocks * 512);
          reservation_ = 0;
          {
            std::lock_guard guard(entry_->mutex_);
            if (entry_->stale_ || entry_->epoch_ != marker_.generation_epoch) {
              throw std::system_error(ESTALE, std::generic_category(),
                                      "cache generation changed during write");
            }
            entry_->dirty_fd_ = marker_fd_;
            marker_fd_ = -1;
            marker_fd_owned_ = false;
            auto& header = *static_cast<CacheMetaHeader*>(entry_->mapping_);
            header.flags |= kCacheMetaDirty;
            header.write_phase = 1;
          }
          finish(true);
          return;
        }

        case Phase::SYNC_DATA:
          phase_ = Phase::SYNC_META;
          submit_fsync(entry_->meta_fd_, false);
          return;

        case Phase::SYNC_META: {
          int dirty_fd;
          {
            std::lock_guard guard(entry_->mutex_);
            dirty_fd = entry_->dirty_fd_;
          }
          if (dirty_fd < 0) { finish(true); return; }
          phase_ = Phase::SYNC_MARKER;
          submit_fsync(dirty_fd, true);
          return;
        }

        case Phase::SYNC_MARKER:
          finish(true);
          return;

        case Phase::COMMIT_META: {
          std::unique_lock guard(entry_->mutex_);
          marker_fd_ = entry_->dirty_fd_;
          if (marker_fd_ < 0) { finish_locked(guard, true); return; }
          guard.unlock();
          marker_path_action_ = MarkerPathAction::REMOVE;
          path_.start(*executor_, entry_->owner_->dirty_root_fd_,
                      entry_->key_, false, marker_path_resolved, this,
                      entry_->owner_->name_max_);
          return;
        }

        case Phase::COMMIT_MARKER_STAT:
          marker_blocks_ = status_.stx_blocks;
          {
            std::lock_guard guard(entry_->mutex_);
            if (entry_->dirty_fd_ == marker_fd_) entry_->dirty_fd_ = -1;
          }
          marker_fd_owned_ = true;
          phase_ = Phase::COMMIT_MARKER_CLOSE;
          submit_marker_close();
          return;

        case Phase::COMMIT_MARKER_CLOSE:
          marker_fd_ = -1;
          if (!marker_named_) {
            if (marker_blocks_ != 0) {
              entry_->owner_->add_allocated(-int64_t(std::min<uint64_t>(
                  marker_blocks_ * 512, INT64_MAX)));
            }
            finish(true);
            return;
          }
          phase_ = Phase::COMMIT_MARKER_UNLINK;
          submit_marker_unlink();
          return;

        case Phase::COMMIT_MARKER_UNLINK:
          if (marker_blocks_ != 0) {
            entry_->owner_->add_allocated(-int64_t(std::min<uint64_t>(
                marker_blocks_ * 512, INT64_MAX)));
          }
          finish(true);
          return;
      }
    } catch (...) {
      finish(std::current_exception());
    }
  }

  bool start_next_write_range() {
    const uint64_t unit = entry_->owner_->config_.upload_part_size;
    while (current_part_ <= last_part_) {
      bool reserved;
      {
        std::lock_guard guard(entry_->mutex_);
        reserved = current_part_ < entry_->reserved_units_.size() &&
            entry_->reserved_units_[current_part_] != 0;
      }
      if (!reserved) {
        start_   = uint64_t(current_part_) * unit;
        reserve_ = unit;
        phase_   = Phase::RANGE_STAT_BEFORE;
        return submit_statx(entry_->data_fd_);
      }
      ++current_part_;
    }
    return start_dirty_marker();
  }

  bool start_dirty_marker() {
    uint64_t epoch;
    {
      std::unique_lock guard(entry_->mutex_);
      if (entry_->stale_) return reject_locked(guard, ESTALE);
      if (entry_->dirty_fd_ >= 0) {
        const int dirty_fd = entry_->dirty_fd_;
        guard.unlock();
        phase_ = Phase::MARKER_ALREADY;
        return submit_statx(dirty_fd);
      }
      epoch = entry_->epoch_;
    }
    marker_ = CacheDirtyMarker{};
    marker_.magic            = {'N', 'G', 'S', '3', 'D', 'I', 'R', 'T'};
    marker_.version          = kCacheMetaVersion;
    marker_.header_size      = kCacheMetaHeaderSize;
    marker_.generation_epoch = epoch;
    marker_.key_length       = uint32_t(entry_->key_.size());
    memcpy(marker_.key.data(), entry_->key_.data(), entry_->key_.size());
    return request_capacity(
        kCacheMetaHeaderSize, CapacityAction::MARKER);
  }

  void finish_error(int error, const char* operation) noexcept {
    try {
      finish(std::make_exception_ptr(std::system_error(
          error, std::generic_category(), operation)));
    } catch (...) {
      finish(std::current_exception());
    }
  }

  void finish(std::exception_ptr error) noexcept {
    if (reservation_ != 0) {
      entry_->owner_->cancel_reservation(reservation_);
      reservation_ = 0;
    }
    finish_result(CacheAsyncResult{
        .entry = {}, .error = std::move(error), .value = false});
  }

  void finish(bool value) noexcept {
    finish_result(CacheAsyncResult{.entry = {}, .error = {}, .value = value});
  }

  template<class Guard>
  void finish_locked(Guard& guard, bool value) noexcept {
    guard.unlock();
    finish_result(CacheAsyncResult{.entry = {}, .error = {}, .value = value});
  }

  void finish_result(CacheAsyncResult result) noexcept {
    if (marker_fd_owned_ && marker_fd_ >= 0) {
      final_result_ = std::move(result);
      phase_ = Phase::FINAL_MARKER_CLOSE;
      reset_io(AsyncIoRequest::CLOSE, marker_fd_);
      if (executor_->submit(io_)) return;
      ::close(marker_fd_);
      marker_fd_ = -1;
      marker_fd_owned_ = false;
      result = std::move(final_result_);
    }
    if (marker_parent_owned_ && marker_parent_fd_ >= 0) {
      final_result_ = std::move(result);
      const int fd = marker_parent_fd_;
      marker_parent_owned_ = false;
      phase_ = Phase::FINAL_PARENT_CLOSE;
      reset_io(AsyncIoRequest::CLOSE, fd);
      if (executor_->submit(io_)) return;
      ::close(fd);
      marker_parent_fd_ = -1;
      complete_result(std::move(final_result_));
      return;
    }
    complete_result(std::move(result));
  }

  void complete_result(CacheAsyncResult result) noexcept {
    {
      std::lock_guard guard(entry_->mutex_);
      (void)release_entry_operation_locked();
    }
    deliver_result(std::move(result));
  }

  void deliver_result(CacheAsyncResult result) noexcept {
    CacheAsyncRequest* request = request_;
    CacheAsyncRequest::Complete callback = request->complete;
    void* context = request->context;
    request->implementation_ = nullptr;
    callback(context, std::move(result));
    delete this;
  }

  IoExecutor* executor_ = nullptr;
  CacheAsyncRequest* request_ = nullptr;
  CacheEntry* entry_ = nullptr;
  AsyncIoRequest io_;
  AsyncIoRequest wait_io_;
  UniqueFd wait_fd_;
  CacheCapacityOperation capacity_;
  CachePathWalker path_;
  Kind kind_ = Kind::PREPARE_READ;
  Phase phase_ = Phase::RANGE_STAT_BEFORE;
  struct statx status_{};
  CacheDirtyMarker marker_{};
  std::string etag_;
  std::string identity_key_;
  std::string version_id_;
  std::string upload_id_;
  std::string marker_name_;
  uint64_t offset_ = 0;
  uint64_t identity_size_ = 0;
  time_t identity_mtime_ = 0;
  size_t length_ = 0;
  uint64_t end_ = 0;
  uint64_t start_ = 0;
  uint64_t reserve_ = 0;
  uint64_t reservation_ = 0;
  uint64_t before_blocks_ = 0;
  uint64_t marker_blocks_ = 0;
  uint64_t wait_value_ = 0;
  uint64_t capacity_bytes_ = 0;
  size_t wanted_mapping_size_ = 0;
  size_t old_mapping_size_ = 0;
  size_t first_part_ = 0;
  size_t last_part_ = 0;
  size_t current_part_ = 0;
  int marker_fd_ = -1;
  int marker_parent_fd_ = -1;
  bool accepted_ = false;
  bool queued_linked_ = false;
  CapacityAction capacity_action_ = CapacityAction::NONE;
  bool marker_named_ = false;
  bool marker_fd_owned_ = false;
  bool marker_parent_owned_ = false;
  CacheAsyncResult final_result_;
  MarkerPathAction marker_path_action_ = MarkerPathAction::NONE;
};

class CacheOpenOperation {
 public:
  CacheOpenOperation(IoExecutor& executor, CacheAsyncRequest& request,
                     std::shared_ptr<CacheEntry> entry) noexcept
      : executor_(&executor), request_(&request), entry_(std::move(entry)) {}

  bool start() noexcept {
    if (request_->complete == nullptr || request_->pending()) {
      errno = EINVAL;
      return false;
    }
    request_->implementation_ = this;
    io_.kind     = AsyncIoRequest::STATX;
    io_.fd       = entry_->data_fd();
    io_.path     = "";
    io_.data     = &status_;
    io_.flags    = AT_EMPTY_PATH;
    io_.mask     = STATX_TYPE;
    io_.complete = completed;
    io_.context  = this;
    if (executor_->submit(io_)) return true;
    request_->implementation_ = nullptr;
    return false;
  }

 private:
  static void completed(void* context, ssize_t result) noexcept {
    auto* operation = static_cast<CacheOpenOperation*>(context);
    CacheAsyncRequest* request = operation->request_;
    CacheAsyncRequest::Complete callback = request->complete;
    void* callback_context = request->context;
    CacheAsyncResult cache_result;
    if (result < 0) {
      try {
        cache_result.error = std::make_exception_ptr(std::system_error(
            int(-result), std::generic_category(),
            "statx(asynchronous cache hit)"));
      } catch (...) {
        cache_result.error = std::current_exception();
      }
    } else {
      cache_result.entry = std::move(operation->entry_);
      cache_result.value = true;
    }
    request->implementation_ = nullptr;
    callback(callback_context, std::move(cache_result));
    delete operation;
  }

  IoExecutor* executor_ = nullptr;
  CacheAsyncRequest* request_ = nullptr;
  std::shared_ptr<CacheEntry> entry_;
  AsyncIoRequest io_;
  struct statx status_{};
};

class CacheMarkerOperation final : private CacheKeyOperation {
 public:
  enum class Kind { CREATE_PENDING, COMMIT_PENDING, FINISH_PENDING };

  CacheMarkerOperation(IoExecutor& executor, CacheAsyncRequest& request,
                       LocalCache& cache, Kind kind)
      : CacheKeyOperation(cache, executor), executor_(&executor), request_(&request),
        cache_(&cache), kind_(kind) {}

  bool start(std::string_view key, std::string_view restore_key,
             std::string_view replacement_etag) {
    if (request_->complete == nullptr || request_->pending()) {
      errno = EINVAL;
      return false;
    }
    if (key.empty() || key.size() > kCacheKeyCapacity ||
        restore_key.size() > kCacheKeyCapacity ||
        replacement_etag.size() > kCacheEtagCapacity) {
      errno = EOVERFLOW;
      return false;
    }
    key_.assign(key);
    if (kind_ == Kind::CREATE_PENDING) {
      marker_.magic        = {'N', 'G', 'S', '3', 'P', 'E', 'N', 'D'};
      marker_.version      = kCacheMetaVersion;
      marker_.header_size = kCacheMetaHeaderSize;
      marker_.phase        = restore_key.empty() ? 2U : 1U;
      marker_.key_length   = uint32_t(key.size());
      marker_.restore_key_length = uint32_t(restore_key.size());
      marker_.replacement_etag_length = uint32_t(replacement_etag.size());
      memcpy(marker_.key.data(), key.data(), key.size());
      memcpy(marker_.restore_key.data(), restore_key.data(), restore_key.size());
      memcpy(marker_.replacement_etag.data(), replacement_etag.data(),
             replacement_etag.size());
    }
    request_->implementation_ = this;
    const GateResult gate = acquire_key_gate(key_);
    if (gate == GateResult::FAILED) {
      request_->implementation_ = nullptr;
      return false;
    }
    if (gate == GateResult::QUEUED) {
      accepted_ = true;
      return true;
    }
    return activate_start();
  }

  bool activate_start() {
    if (kind_ == Kind::CREATE_PENDING) {
      const CacheCapacityOperation::StartResult capacity = capacity_.start(
          *executor_, *cache_, kCacheMetaHeaderSize,
          capacity_completed, this);
      if (capacity == CacheCapacityOperation::StartResult::FAILED) {
        if (accepted_) {
          finish(marker_error(errno == 0 ? ENOSPC : errno,
                              "reserve queued cache marker"));
          return true;
        }
        return reject(errno == 0 ? ENOSPC : errno);
      }
      if (capacity == CacheCapacityOperation::StartResult::PENDING) {
        accepted_ = true;
        return true;
      }
      reservation_ = kCacheMetaHeaderSize;
      return start_create_path();
    }
    if (kind_ == Kind::COMMIT_PENDING) {
      return path_.start(*executor_, cache_->pending_root_fd_, key_, false,
                         path_resolved, this, cache_->name_max_);
    }
    return path_.start(*executor_, cache_->pending_root_fd_, key_, false,
                       path_resolved, this, cache_->name_max_);
  }

  void abandon_start() noexcept {
    if (reservation_ != 0) {
      cache_->cancel_reservation(reservation_);
      reservation_ = 0;
    }
    abandon_key_gate();
    if (request_->implementation_ == this) request_->implementation_ = nullptr;
  }

 private:
  void activate_key_gate() noexcept override {
    try {
      (void)activate_start();
    } catch (...) {
      finish(std::current_exception());
    }
  }

  void key_gate_failed(std::exception_ptr error) noexcept override {
    finish(std::move(error));
  }
  enum class Phase {
    CREATE_OPEN, CREATE_FALLOCATE, CREATE_WRITE, CREATE_STAT, CREATE_FSYNC,
    CREATE_CLOSE, COMMIT_OPEN, COMMIT_WRITE, COMMIT_FSYNC, COMMIT_CLOSE,
    FINISH_STAT, FINISH_UNLINK, FAIL_CLOSE, FINAL_PARENT_CLOSE,
  };

  static void capacity_completed(void* context,
                                 std::exception_ptr error) noexcept {
    auto* operation = static_cast<CacheMarkerOperation*>(context);
    if (error) {
      operation->finish(std::move(error));
      return;
    }
    operation->reservation_ = kCacheMetaHeaderSize;
    if (!operation->start_create_path()) {
      operation->finish(marker_error(
          errno == 0 ? EIO : errno, "resolve cache marker path"));
    }
  }

  bool start_create_path() noexcept {
    return path_.start(*executor_, cache_->pending_root_fd_, key_, true,
                       path_resolved, this, cache_->name_max_);
  }

  static void path_resolved(void* context, bool exists, int parent_fd,
                            bool parent_owned, std::string leaf,
                            std::exception_ptr error) noexcept {
    auto* operation = static_cast<CacheMarkerOperation*>(context);
    if (error) { operation->finish(std::move(error)); return; }
    operation->parent_fd_ = parent_fd;
    operation->parent_owned_ = parent_owned;
    operation->name_ = std::move(leaf);
    if (operation->kind_ == Kind::CREATE_PENDING) {
      operation->phase_ = Phase::CREATE_OPEN;
      operation->submit_open(
          O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW);
      return;
    }
    if (!exists) {
      if (operation->kind_ == Kind::FINISH_PENDING) operation->finish(true);
      else operation->finish(marker_error(
          ENOENT, "cache pending-delete marker"));
      return;
    }
    if (operation->kind_ == Kind::COMMIT_PENDING) {
      operation->phase_ = Phase::COMMIT_OPEN;
      operation->submit_open(O_RDWR | O_CLOEXEC | O_NOFOLLOW);
      return;
    }
    operation->phase_ = Phase::FINISH_STAT;
    operation->submit_stat_path();
  }

  static std::exception_ptr marker_error(int error, const char* what) noexcept {
    try {
      return std::make_exception_ptr(std::system_error(
          error, std::generic_category(), what));
    } catch (...) {
      return std::current_exception();
    }
  }

  void reset_io(AsyncIoRequest::Kind kind, int fd) noexcept {
    io_ = AsyncIoRequest{};
    io_.kind = kind;
    io_.fd = fd;
    io_.complete = completed;
    io_.context = this;
  }

  bool submit_open(unsigned flags) noexcept {
    reset_io(AsyncIoRequest::OPENAT, parent_fd_);
    io_.path = name_.c_str();
    io_.flags = flags;
    io_.mode = 0600;
    return submit();
  }

  bool submit_fallocate() noexcept {
    reset_io(AsyncIoRequest::FALLOCATE, fd_);
    io_.input_offset = 0;
    io_.length = kCacheMetaHeaderSize;
    return submit();
  }

  bool submit_write(const void* data, size_t length, off_t offset) noexcept {
    reset_io(AsyncIoRequest::PWRITE, fd_);
    io_.data = const_cast<void*>(data);
    io_.length = length;
    io_.output_offset = offset;
    io_.exact = true;
    return submit();
  }

  bool submit_stat_fd() noexcept {
    memset(&status_, 0, sizeof(status_));
    reset_io(AsyncIoRequest::STATX, fd_);
    io_.path = "";
    io_.data = &status_;
    io_.flags = AT_EMPTY_PATH;
    io_.mask = STATX_BLOCKS | STATX_TYPE;
    return submit();
  }

  bool submit_stat_path() noexcept {
    memset(&status_, 0, sizeof(status_));
    reset_io(AsyncIoRequest::STATX, parent_fd_);
    io_.path = name_.c_str();
    io_.data = &status_;
    io_.flags = AT_SYMLINK_NOFOLLOW;
    io_.mask = STATX_BLOCKS | STATX_TYPE;
    return submit();
  }

  bool submit_fsync() noexcept {
    reset_io(AsyncIoRequest::FSYNC, fd_);
    return submit();
  }

  bool submit_close() noexcept {
    reset_io(AsyncIoRequest::CLOSE, fd_);
    return submit();
  }

  bool submit_unlink() noexcept {
    reset_io(AsyncIoRequest::UNLINKAT, parent_fd_);
    io_.path = name_.c_str();
    return submit();
  }

  bool submit() noexcept {
    if (executor_->submit(io_)) {
      accepted_ = true;
      return true;
    }
    const int error = errno == 0 ? EIO : errno;
    if (!accepted_) return reject(error);
    fail(error, "submit pending-delete cache operation");
    return false;
  }

  bool reject(int error) noexcept {
    abandon_start();
    errno = error;
    return false;
  }

  static void completed(void* context, ssize_t result) noexcept {
    static_cast<CacheMarkerOperation*>(context)->advance(result);
  }

  void advance(ssize_t result) noexcept {
    try {
      if (phase_ == Phase::FINAL_PARENT_CLOSE) {
        if (result < 0 && parent_fd_ >= 0) ::close(parent_fd_);
        parent_fd_ = -1;
        complete_now(std::move(final_result_));
        return;
      }
      if (phase_ == Phase::FAIL_CLOSE) {
        if (result < 0 && fd_ >= 0) ::close(fd_);
        fd_ = -1;
        finish(failure_);
        return;
      }
      if (result < 0) {
        const int error = int(-result);
        if (phase_ == Phase::FINISH_STAT && error == ENOENT) {
          finish(true);
          return;
        }
        fail(error, "asynchronous pending-delete cache operation");
        return;
      }
      switch (phase_) {
        case Phase::FINAL_PARENT_CLOSE:
          return;
        case Phase::CREATE_OPEN:
          fd_ = int(result);
          phase_ = Phase::CREATE_FALLOCATE;
          submit_fallocate();
          return;
        case Phase::CREATE_FALLOCATE:
          phase_ = Phase::CREATE_WRITE;
          submit_write(&marker_, sizeof(marker_), 0);
          return;
        case Phase::CREATE_WRITE:
          phase_ = Phase::CREATE_STAT;
          submit_stat_fd();
          return;
        case Phase::CREATE_STAT:
          cache_->finish_reservation(
              reservation_, status_.stx_blocks * 512);
          reservation_ = 0;
          phase_ = Phase::CREATE_FSYNC;
          submit_fsync();
          return;
        case Phase::CREATE_FSYNC:
          phase_ = Phase::CREATE_CLOSE;
          submit_close();
          return;
        case Phase::CREATE_CLOSE:
          fd_ = -1;
          finish(true);
          return;
        case Phase::COMMIT_OPEN:
          fd_ = int(result);
          phase_ = Phase::COMMIT_WRITE;
          submit_write(&committed_phase_, sizeof(committed_phase_),
                       off_t(offsetof(CachePendingDeleteMarker, phase)));
          return;
        case Phase::COMMIT_WRITE:
          phase_ = Phase::COMMIT_FSYNC;
          submit_fsync();
          return;
        case Phase::COMMIT_FSYNC:
          phase_ = Phase::COMMIT_CLOSE;
          submit_close();
          return;
        case Phase::COMMIT_CLOSE:
          fd_ = -1;
          finish(true);
          return;
        case Phase::FINISH_STAT:
          blocks_ = status_.stx_blocks;
          phase_ = Phase::FINISH_UNLINK;
          submit_unlink();
          return;
        case Phase::FINISH_UNLINK:
          if (blocks_ != 0) {
            cache_->add_allocated(-int64_t(std::min<uint64_t>(
                blocks_ * 512, INT64_MAX)));
          }
          finish(true);
          return;
        case Phase::FAIL_CLOSE:
          return;
      }
    } catch (...) {
      finish(std::current_exception());
    }
  }

  void fail(int error, const char* what) noexcept {
    try {
      failure_ = std::make_exception_ptr(std::system_error(
          error, std::generic_category(), what));
    } catch (...) {
      failure_ = std::current_exception();
    }
    if (reservation_ != 0) {
      cache_->cancel_reservation(reservation_);
      reservation_ = 0;
    }
    if (fd_ >= 0 && phase_ != Phase::FAIL_CLOSE) {
      phase_ = Phase::FAIL_CLOSE;
      if (executor_->submit((reset_close(), io_))) return;
      ::close(fd_);
      fd_ = -1;
    }
    finish(failure_);
  }

  AsyncIoRequest& reset_close() noexcept {
    reset_io(AsyncIoRequest::CLOSE, fd_);
    return io_;
  }

  void finish(bool value) noexcept {
    finish(CacheAsyncResult{.entry = {}, .error = {}, .value = value});
  }

  void finish(std::exception_ptr error) noexcept {
    finish(CacheAsyncResult{
        .entry = {}, .error = std::move(error), .value = false});
  }

  void finish(CacheAsyncResult result) noexcept {
    if (reservation_ != 0) {
      cache_->cancel_reservation(reservation_);
      reservation_ = 0;
    }
    if (parent_owned_ && parent_fd_ >= 0) {
      final_result_ = std::move(result);
      const int fd = parent_fd_;
      parent_owned_ = false;
      phase_ = Phase::FINAL_PARENT_CLOSE;
      reset_io(AsyncIoRequest::CLOSE, fd);
      if (executor_->submit(io_)) return;
      ::close(fd);
      parent_fd_ = -1;
      complete_now(std::move(final_result_));
      return;
    }
    complete_now(std::move(result));
  }

  void complete_now(CacheAsyncResult result) noexcept {
    release_key_gate();
    CacheAsyncRequest* request = request_;
    CacheAsyncRequest::Complete callback = request->complete;
    void* context = request->context;
    request->implementation_ = nullptr;
    callback(context, std::move(result));
    delete this;
  }

  IoExecutor* executor_ = nullptr;
  CacheAsyncRequest* request_ = nullptr;
  LocalCache* cache_ = nullptr;
  Kind kind_ = Kind::CREATE_PENDING;
  Phase phase_ = Phase::CREATE_OPEN;
  AsyncIoRequest io_;
  CacheCapacityOperation capacity_;
  CachePathWalker path_;
  CachePendingDeleteMarker marker_{};
  struct statx status_{};
  std::exception_ptr failure_;
  std::string key_;
  std::string name_;
  uint64_t reservation_ = 0;
  uint64_t blocks_ = 0;
  int fd_ = -1;
  int parent_fd_ = -1;
  uint32_t committed_phase_ = 2;
  bool accepted_ = false;
  bool parent_owned_ = false;
  CacheAsyncResult final_result_;
};

class CacheFileOperation final : private CacheKeyOperation {
 public:
  enum class Kind { OPEN, CREATE_WRITER };

  CacheFileOperation(IoExecutor& executor, CacheAsyncRequest& request,
                     LocalCache& cache, Kind kind)
      : CacheKeyOperation(cache, executor), executor_(&executor), request_(&request),
        cache_(&cache), kind_(kind) {}

  bool start(const CacheIdentity& identity, uint64_t maximum_size,
             bool wait_for_key = true, bool require_absent = false) {
    if (request_->complete == nullptr || request_->pending()) {
      errno = EINVAL;
      return false;
    }
    if (identity.key.size() > kCacheKeyCapacity ||
        identity.etag.size() > kCacheEtagCapacity ||
        identity.version_id.size() > kCacheVersionCapacity ||
        (kind_ == Kind::CREATE_WRITER && maximum_size == 0)) {
      errno = EOVERFLOW;
      return false;
    }
    key_.assign(identity.key);
    etag_.assign(identity.etag);
    version_id_.assign(identity.version_id);
    object_size_ = kind_ == Kind::CREATE_WRITER ? 0 : identity.size;
    mtime_ = kind_ == Kind::CREATE_WRITER ? 0 : identity.mtime;
    maximum_size_ = maximum_size;
    mapping_size_ = kind_ == Kind::CREATE_WRITER
        ? kCacheMetaHeaderSize
        : cache_mapping_size(object_size_, kCacheBitmapUnit);
    request_->implementation_ = this;
    const GateResult gate = acquire_key_gate(key_, {}, wait_for_key);
    if (gate == GateResult::FAILED) {
      request_->implementation_ = nullptr;
      return false;
    }
    if (gate == GateResult::QUEUED) {
      accepted_ = true;
      return true;
    }
    if (require_absent) {
      bool present = false;
      {
        std::lock_guard guard(cache_->mutex_);
        for (const auto& weak : cache_->entries_) {
          std::shared_ptr<CacheEntry> entry = weak.lock();
          if (entry && entry->key_ == key_) {
            present = true;
            break;
          }
        }
      }
      if (present) {
        abandon_key_gate();
        request_->implementation_ = nullptr;
        errno = EBUSY;
        return false;
      }
    }
    return inspect_registry();
  }

  void abandon_start() noexcept {
    if (reservation_ != 0) {
      cache_->cancel_reservation(reservation_);
      reservation_ = 0;
    }
    abandon_key_gate();
    if (request_->implementation_ == this) request_->implementation_ = nullptr;
  }

 private:
  void activate_key_gate() noexcept override {
    try {
      (void)inspect_registry();
    } catch (...) {
      fail(std::current_exception());
    }
  }


  void key_gate_failed(std::exception_ptr error) noexcept override {
    fail(std::move(error));
  }
  enum class Phase {
    HIT_STAT, RETIRE_WAIT,
    OPEN_DATA, OPEN_META, META_STAT, EXISTING_HEADER_READ,
    EXISTING_POPULATE, DATA_STAT, RECOVERY_STAT_BEFORE,
    RECOVERY_PUNCH, RECOVERY_STAT_AFTER,
    RECREATE_DATA_UNLINK, RECREATE_DATA_CLOSE, RECREATE_DATA_OPEN,
    TRUNCATE_DATA, TRUNCATE_META, TRUNCATED_META_STAT,
    TRUNCATED_DATA_STAT, ALLOCATE_META, ALLOCATED_META_STAT,
    ALLOCATED_DATA_STAT, NEW_POPULATE,
    FAIL_CLOSE_META, FAIL_CLOSE_DATA, FAIL_CLOSE_META_PARENT,
    FAIL_CLOSE_DATA_PARENT, FINAL_CLOSE_META_PARENT,
    FINAL_CLOSE_DATA_PARENT,
  };

  CacheIdentity identity() const noexcept {
    return CacheIdentity{
        .key = key_, .etag = etag_, .version_id = version_id_,
        .size = object_size_, .mtime = mtime_};
  }

  static void capacity_completed(void* context,
                                 std::exception_ptr error) noexcept {
    auto* operation = static_cast<CacheFileOperation*>(context);
    if (error) {
      operation->finish(std::move(error));
      return;
    }
    operation->reservation_ = operation->capacity_bytes_;
    operation->start_metadata_allocation();
  }

  void start_metadata_allocation() noexcept {
    phase_ = Phase::ALLOCATE_META;
    submit_fallocate(meta_fd_, mapping_size_);
  }

  bool inspect_registry() {
    std::shared_ptr<CacheEntry> pending;
    std::vector<std::shared_ptr<CacheEntry>> released;
    {
      std::unique_lock guard(cache_->mutex_);
      for (auto i = cache_->entries_.begin(); i != cache_->entries_.end();) {
        std::shared_ptr<CacheEntry> entry = i->lock();
        if (!entry) {
          i = cache_->entries_.erase(i);
          continue;
        }
        if (entry->key_ != key_) {
          ++i;
          continue;
        }
        bool matches = false;
        bool busy = false;
        {
          std::lock_guard entry_guard(entry->mutex_);
          const auto& header =
              *static_cast<const CacheMetaHeader*>(entry->mapping_);
          matches = kind_ == Kind::OPEN && !entry->detached_ &&
              !entry->stale_ &&
              cache_identity_matches(header, identity(), kCacheBitmapUnit);
          if (matches) {
            cache_->retain_entry_locked(entry, released);
          } else {
            cache_->release_keepalive_locked(entry.get(), released);
            entry->stale_ = true;
            entry->notify_waiters_locked();
            busy = !entry->active_claims_.empty() ||
                entry->checksum_ops_ != 0 || entry->pinned_regions_ != 0 ||
                entry->async_metadata_inflight_;
          }
        }
        if (matches) {
          hit_ = std::move(entry);
          guard.unlock();
          phase_ = Phase::HIT_STAT;
          return submit_statx_fd(hit_->data_fd());
        }
        if (busy) {
          pending = std::move(entry);
          break;
        }
        i = cache_->entries_.erase(i);
      }
    }
    if (pending) {
      retiring_ = std::move(pending);
      const int fd = retiring_->begin_retire_wait();
      if (fd >= 0) {
        phase_ = Phase::RETIRE_WAIT;
        notification_ = 0;
        reset_io(AsyncIoRequest::READ, fd);
        io_.data = &notification_;
        io_.length = sizeof(notification_);
        return submit();
      }
      retiring_.reset();
      return inspect_registry();
    }
    resolving_data_ = true;
    return path_.start(*executor_, cache_->data_root_fd_, key_, true,
                       path_resolved, this, cache_->name_max_);
  }

  static void path_resolved(void* context, bool, int parent_fd,
                            bool parent_owned, std::string leaf,
                            std::exception_ptr error) noexcept {
    auto* operation = static_cast<CacheFileOperation*>(context);
    if (error) { operation->fail(std::move(error)); return; }
    if (operation->resolving_data_) {
      operation->resolving_data_ = false;
      operation->data_parent_fd_ = parent_fd;
      operation->data_parent_owned_ = parent_owned;
      operation->data_name_ = std::move(leaf);
      operation->phase_ = Phase::OPEN_DATA;
      operation->submit_open(operation->data_parent_fd_,
                             operation->data_name_,
                             O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW);
      return;
    }
    operation->meta_parent_fd_ = parent_fd;
    operation->meta_parent_owned_ = parent_owned;
    operation->meta_name_ = std::move(leaf);
    operation->phase_ = Phase::OPEN_META;
    operation->submit_open(operation->meta_parent_fd_,
                           operation->meta_name_,
                           O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW);
  }

  void reset_io(AsyncIoRequest::Kind kind, int fd) noexcept {
    io_ = AsyncIoRequest{};
    io_.kind = kind;
    io_.fd = fd;
    io_.complete = completed;
    io_.context = this;
  }

  bool submit_open(int parent, const std::string& name,
                   unsigned flags) noexcept {
    reset_io(AsyncIoRequest::OPENAT, parent);
    io_.path = name.c_str();
    io_.flags = flags;
    io_.mode = 0600;
    return submit();
  }

  bool submit_statx_fd(int fd) noexcept {
    memset(&status_, 0, sizeof(status_));
    reset_io(AsyncIoRequest::STATX, fd);
    io_.path = "";
    io_.data = &status_;
    io_.flags = AT_EMPTY_PATH;
    io_.mask = STATX_BLOCKS | STATX_SIZE | STATX_TYPE;
    return submit();
  }

  bool submit_existing_header() noexcept {
    memset(&existing_header_, 0, sizeof(existing_header_));
    reset_io(AsyncIoRequest::PREAD, meta_fd_);
    io_.data = &existing_header_;
    io_.length = sizeof(existing_header_);
    io_.input_offset = 0;
    io_.exact = true;
    return submit();
  }

  bool submit_truncate(int fd, uint64_t length) noexcept {
    reset_io(AsyncIoRequest::FTRUNCATE, fd);
    io_.length = size_t(length);
    return submit();
  }

  bool submit_fallocate(int fd, uint64_t length) noexcept {
    reset_io(AsyncIoRequest::FALLOCATE, fd);
    io_.input_offset = 0;
    io_.length = size_t(length);
    return submit();
  }

  bool submit_recovery_punch() noexcept {
    reset_io(AsyncIoRequest::FALLOCATE, data_fd_);
    io_.flags = FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE;
    io_.input_offset = off_t(recovery_offsets_[recovery_index_]);
    io_.length = cache_->config_.block_size;
    return submit();
  }

  bool submit_madvise(int advice) noexcept {
    reset_io(AsyncIoRequest::MADVISE, -1);
    io_.data = mapping_;
    io_.length = mapping_size_;
    io_.flags = unsigned(advice);
    return submit();
  }

  bool submit_close(int fd) noexcept {
    reset_io(AsyncIoRequest::CLOSE, fd);
    return submit();
  }

  bool submit_unlink_data() noexcept {
    reset_io(AsyncIoRequest::UNLINKAT, data_parent_fd_);
    io_.path = data_name_.c_str();
    return submit();
  }

  bool submit() noexcept {
    if (executor_->submit(io_)) {
      accepted_ = true;
      return true;
    }
    const int error = errno == 0 ? EIO : errno;
    if (!accepted_) return reject(error);
    fail(std::make_exception_ptr(std::system_error(
        error, std::generic_category(), "submit asynchronous cache file operation")));
    return false;
  }

  bool reject(int error) noexcept {
    abandon_start();
    errno = error;
    return false;
  }

  static void completed(void* context, ssize_t result) noexcept {
    static_cast<CacheFileOperation*>(context)->advance(result);
  }

  void advance(ssize_t result) noexcept {
    try {
      if (result < 0 && is_cleanup_phase()) {
        cleanup_failed_close();
        return;
      }
      if (result < 0 && recovery_phase()) {
        if (result == -ECANCELED) {
          throw std::system_error(ECANCELED, std::generic_category(),
                                  "cancel cache recovery punch");
        }
        ++recovery_index_;
        start_next_recovery_punch();
        return;
      }
      if (result < 0) {
        throw std::system_error(int(-result), std::generic_category(),
                                "asynchronous cache file operation");
      }
      switch (phase_) {
        case Phase::HIT_STAT:
          finish(CacheAsyncResult{
              .entry = std::move(hit_), .error = {}, .value = true});
          return;

        case Phase::RETIRE_WAIT:
          retiring_->end_async_wait();
          retiring_.reset();
          inspect_registry();
          return;

        case Phase::OPEN_DATA:
          data_fd_ = int(result);
          path_ = CachePathWalker{};
          path_.start(*executor_, cache_->objects_root_fd_, key_, true,
                      path_resolved, this, cache_->name_max_);
          return;

        case Phase::OPEN_META:
          meta_fd_ = int(result);
          if (::flock(meta_fd_, LOCK_EX | LOCK_NB) != 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
              fail(std::make_exception_ptr(CacheRetirementPending{}));
              return;
            }
            cache_throw_errno("flock(asynchronous cache metadata)");
          }
          metadata_locked_ = true;
          phase_ = Phase::META_STAT;
          submit_statx_fd(meta_fd_);
          return;

        case Phase::META_STAT:
          old_meta_blocks_ = status_.stx_blocks;
          existing_mapping_size_ = size_t(status_.stx_size);
          if (existing_mapping_size_ >= mapping_size_) {
            mapping_size_ = existing_mapping_size_;
            mapping_ = ::mmap(nullptr, mapping_size_, PROT_READ | PROT_WRITE,
                              MAP_SHARED, meta_fd_, 0);
            if (mapping_ == MAP_FAILED) {
              mapping_ = nullptr;
              cache_throw_errno("mmap(asynchronous cache metadata)");
            }
            phase_ = Phase::EXISTING_POPULATE;
            submit_madvise(MADV_POPULATE_READ);
            return;
          }
          reset_ = true;
          if (existing_mapping_size_ >= sizeof(CacheMetaHeader)) {
            phase_ = Phase::EXISTING_HEADER_READ;
            submit_existing_header();
            return;
          }
          phase_ = Phase::DATA_STAT;
          submit_statx_fd(data_fd_);
          return;

        case Phase::EXISTING_HEADER_READ: {
          constexpr std::array<char, 8> magic{
              'N', 'G', 'S', '3', 'C', 'A', 'C', 'H'};
          existing_exported_ = result == ssize_t(sizeof(existing_header_)) &&
              existing_header_.magic == magic &&
              existing_header_.version == kCacheMetaVersion &&
              (existing_header_.flags & kCacheMetaExported) != 0;
          phase_ = Phase::DATA_STAT;
          submit_statx_fd(data_fd_);
          return;
        }

        case Phase::EXISTING_POPULATE: {
          auto& header = *static_cast<CacheMetaHeader*>(mapping_);
          existing_exported_ = (header.flags & kCacheMetaExported) != 0;
          reset_ = kind_ == Kind::CREATE_WRITER ||
              !cache_identity_matches(header, identity(), kCacheBitmapUnit);
          phase_ = Phase::DATA_STAT;
          submit_statx_fd(data_fd_);
          return;
        }

        case Phase::DATA_STAT:
          old_data_blocks_ = status_.stx_blocks;
          if (kind_ == Kind::OPEN && status_.stx_size != object_size_) reset_ = true;
          if (!reset_) {
            begin_recovery_punches();
            return;
          }
          if (mapping_ != nullptr) {
            ::munmap(mapping_, mapping_size_);
            mapping_ = nullptr;
          }
          mapping_size_ = kind_ == Kind::CREATE_WRITER
              ? kCacheMetaHeaderSize
              : cache_mapping_size(object_size_, kCacheBitmapUnit);
          allocation_tracking_ = true;
          if (existing_exported_) {
            phase_ = Phase::RECREATE_DATA_UNLINK;
            submit_unlink_data();
            return;
          }
          phase_ = Phase::TRUNCATE_DATA;
          submit_truncate(data_fd_, object_size_);
          return;

        case Phase::RECREATE_DATA_UNLINK:
          cache_->add_allocated(-int64_t(std::min<uint64_t>(
              old_data_blocks_ * 512, INT64_MAX)));
          old_data_blocks_ = 0;
          ignore_data_fd_allocation_ = true;
          phase_ = Phase::RECREATE_DATA_CLOSE;
          submit_close(data_fd_);
          return;

        case Phase::RECREATE_DATA_CLOSE:
          data_fd_ = -1;
          phase_ = Phase::RECREATE_DATA_OPEN;
          submit_open(data_parent_fd_, data_name_,
                      O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW);
          return;

        case Phase::RECREATE_DATA_OPEN:
          data_fd_ = int(result);
          ignore_data_fd_allocation_ = false;
          phase_ = Phase::TRUNCATE_DATA;
          submit_truncate(data_fd_, object_size_);
          return;

        case Phase::TRUNCATE_DATA:
          phase_ = Phase::TRUNCATE_META;
          submit_truncate(meta_fd_, mapping_size_);
          return;

        case Phase::TRUNCATE_META:
          phase_ = Phase::TRUNCATED_META_STAT;
          submit_statx_fd(meta_fd_);
          return;

        case Phase::TRUNCATED_META_STAT:
          new_meta_blocks_ = status_.stx_blocks;
          phase_ = Phase::TRUNCATED_DATA_STAT;
          submit_statx_fd(data_fd_);
          return;

        case Phase::TRUNCATED_DATA_STAT: {
          reconcile_truncated_allocation(
              new_meta_blocks_, status_.stx_blocks);
          const uint64_t reserve =
              cache_round_up(mapping_size_, cache_->config_.page_size);
          capacity_bytes_ = reserve;
          const CacheCapacityOperation::StartResult capacity = capacity_.start(
              *executor_, *cache_, reserve, capacity_completed, this);
          if (capacity == CacheCapacityOperation::StartResult::FAILED) {
            throw std::system_error(
                errno == 0 ? ENOSPC : errno, std::generic_category(),
                "reserve asynchronous cache metadata");
          }
          if (capacity == CacheCapacityOperation::StartResult::RESERVED) {
            reservation_ = reserve;
            start_metadata_allocation();
          }
          return;
        }

        case Phase::ALLOCATE_META:
          phase_ = Phase::ALLOCATED_META_STAT;
          submit_statx_fd(meta_fd_);
          return;

        case Phase::ALLOCATED_META_STAT:
          new_meta_blocks_ = status_.stx_blocks;
          phase_ = Phase::ALLOCATED_DATA_STAT;
          submit_statx_fd(data_fd_);
          return;

        case Phase::ALLOCATED_DATA_STAT: {
          const uint64_t new_data_blocks = status_.stx_blocks;
          reconcile_allocation(new_meta_blocks_, new_data_blocks);
          mapping_ = ::mmap(nullptr, mapping_size_, PROT_READ | PROT_WRITE,
                            MAP_SHARED, meta_fd_, 0);
          if (mapping_ == MAP_FAILED) {
            mapping_ = nullptr;
            cache_throw_errno("mmap(new asynchronous cache metadata)");
          }
          phase_ = Phase::NEW_POPULATE;
          submit_madvise(MADV_POPULATE_WRITE);
          return;
        }

        case Phase::NEW_POPULATE: {
          memset(mapping_, 0, mapping_size_);
          CacheRootHeader& superblock =
              *static_cast<CacheRootHeader*>(cache_->superblock_mapping_);
          std::atomic_ref<uint64_t> next_epoch(superblock.next_epoch);
          const uint64_t epoch =
              next_epoch.fetch_add(1, std::memory_order_relaxed);
          cache_initialize_header(
              *static_cast<CacheMetaHeader*>(mapping_), identity(),
              kCacheBitmapUnit, epoch);
          if (kind_ == Kind::CREATE_WRITER) {
            auto& header = *static_cast<CacheMetaHeader*>(mapping_);
            cache_initialize_write_id(header);
            header.upload_part_size   = cache_->config_.upload_part_size;
            header.checksum_algorithm = cache_->config_.checksum_algorithm;
          }
          publish_entry();
          return;
        }

        case Phase::RECOVERY_STAT_BEFORE:
          recovery_before_blocks_ = status_.stx_blocks;
          phase_ = Phase::RECOVERY_PUNCH;
          submit_recovery_punch();
          return;

        case Phase::RECOVERY_PUNCH:
          phase_ = Phase::RECOVERY_STAT_AFTER;
          submit_statx_fd(data_fd_);
          return;

        case Phase::RECOVERY_STAT_AFTER:
          if (recovery_before_blocks_ > status_.stx_blocks) {
            cache_->add_allocated(-int64_t(std::min<uint64_t>(
                (recovery_before_blocks_ - status_.stx_blocks) * 512,
                INT64_MAX)));
          }
          ++recovery_index_;
          start_next_recovery_punch();
          return;

        case Phase::FAIL_CLOSE_META:
          meta_fd_ = -1;
          continue_failure_cleanup();
          return;

        case Phase::FAIL_CLOSE_DATA:
          data_fd_ = -1;
          continue_failure_cleanup();
          return;

        case Phase::FAIL_CLOSE_META_PARENT:
          meta_parent_fd_ = -1;
          continue_failure_cleanup();
          return;

        case Phase::FAIL_CLOSE_DATA_PARENT:
          data_parent_fd_ = -1;
          complete_now(CacheAsyncResult{
              .entry = {}, .error = failure_, .value = false});
          return;

        case Phase::FINAL_CLOSE_META_PARENT:
          meta_parent_fd_ = -1;
          continue_final_cleanup();
          return;

        case Phase::FINAL_CLOSE_DATA_PARENT:
          data_parent_fd_ = -1;
          complete_now(std::move(final_result_));
          return;
      }
    } catch (...) {
      fail(std::current_exception());
    }
  }

  void publish_entry() {
    auto entry = std::shared_ptr<CacheEntry>(new CacheEntry(
        *cache_, key_, data_fd_, meta_fd_, -1, mapping_, mapping_size_,
        object_size_, false));
    if (kind_ == Kind::CREATE_WRITER) entry->disable_eviction();
    data_fd_ = -1;
    meta_fd_ = -1;
    mapping_ = nullptr;
    if (metadata_locked_) {
      ::flock(entry->meta_fd_, LOCK_UN);
      metadata_locked_ = false;
    }
    std::vector<std::shared_ptr<CacheEntry>> released;
    {
      std::lock_guard guard(cache_->mutex_);
      cache_->entries_.push_back(entry);
      if (kind_ == Kind::OPEN) {
        std::lock_guard entry_guard(entry->mutex_);
        cache_->retain_entry_locked(entry, released);
      }
    }
    finish(CacheAsyncResult{
        .entry = std::move(entry), .error = {}, .value = true});
  }

  bool recovery_phase() const noexcept {
    return phase_ == Phase::RECOVERY_STAT_BEFORE ||
        phase_ == Phase::RECOVERY_PUNCH ||
        phase_ == Phase::RECOVERY_STAT_AFTER;
  }

  void begin_recovery_punches() {
    const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
    if ((header.flags & kCacheMetaExported) == 0 && object_size_ != 0) {
      const auto* words = reinterpret_cast<const uint64_t*>(
          static_cast<const char*>(mapping_) + kCacheMetaHeaderSize);
      const size_t pages = size_t((object_size_ - 1) / kCacheBitmapUnit + 1);
      const size_t blocks = size_t(
          (object_size_ - 1) / cache_->config_.block_size + 1);
      for (size_t block = 0; block < blocks; ++block) {
        const uint64_t offset = uint64_t(block) * cache_->config_.block_size;
        const size_t first = size_t(offset / kCacheBitmapUnit);
        const size_t last = std::min(
            pages, first + cache_->config_.block_size / kCacheBitmapUnit);
        bool had_pending = false;
        bool all_missing = true;
        for (size_t page = first; page < last; ++page) {
          const unsigned shift = unsigned(page % 32) * 2;
          const CachePageState state = CachePageState(
              (words[page / 32] >> shift) & 3);
          had_pending |= state == CACHE_PAGE_READ_PENDING;
          all_missing &= state == CACHE_PAGE_MISSING ||
              state == CACHE_PAGE_READ_PENDING;
        }
        if (had_pending && all_missing) recovery_offsets_.push_back(offset);
      }
    }
    start_next_recovery_punch();
  }

  void start_next_recovery_punch() {
    if (recovery_index_ == recovery_offsets_.size()) {
      publish_entry();
      return;
    }
    phase_ = Phase::RECOVERY_STAT_BEFORE;
    submit_statx_fd(data_fd_);
  }

  static uint64_t allocation_bytes(uint64_t meta_blocks,
                                   uint64_t data_blocks) noexcept {
    if (meta_blocks > UINT64_MAX - data_blocks) return UINT64_MAX;
    const uint64_t blocks = meta_blocks + data_blocks;
    return blocks > UINT64_MAX / 512 ? UINT64_MAX : blocks * 512;
  }

  void reconcile_truncated_allocation(uint64_t meta_blocks,
                                      uint64_t data_blocks) noexcept {
    const uint64_t old_bytes = allocation_bytes(
        old_meta_blocks_, old_data_blocks_);
    const uint64_t truncated_bytes = allocation_bytes(
        meta_blocks, data_blocks);
    if (old_bytes > truncated_bytes) {
      cache_->add_allocated(-int64_t(std::min<uint64_t>(
          old_bytes - truncated_bytes, INT64_MAX)));
    } else if (truncated_bytes > old_bytes) {
      cache_->add_allocated(int64_t(std::min<uint64_t>(
          truncated_bytes - old_bytes, INT64_MAX)));
    }
    // Subsequent allocation reconciliation starts from the blocks that
    // remain after truncate, so the same release is not counted twice.
    old_meta_blocks_ = meta_blocks;
    old_data_blocks_ = data_blocks;
  }

  void reconcile_allocation(uint64_t meta_blocks,
                            uint64_t data_blocks) noexcept {
    const uint64_t old_bytes = allocation_bytes(
        old_meta_blocks_, old_data_blocks_);
    const uint64_t actual_bytes = allocation_bytes(meta_blocks, data_blocks);
    const uint64_t growth = actual_bytes > old_bytes
        ? actual_bytes - old_bytes : 0;
    if (reservation_ != 0) {
      cache_->finish_reservation(reservation_, growth);
      reservation_ = 0;
    } else if (growth != 0) {
      cache_->add_allocated(int64_t(std::min<uint64_t>(growth, INT64_MAX)));
    }
    if (old_bytes > actual_bytes) {
      cache_->add_allocated(-int64_t(std::min<uint64_t>(
          old_bytes - actual_bytes, INT64_MAX)));
    }
    allocation_tracking_ = false;
  }

  void reconcile_failed_allocation() noexcept {
    if (!allocation_tracking_) {
      if (reservation_ != 0) {
        cache_->cancel_reservation(reservation_);
        reservation_ = 0;
      }
      return;
    }
    uint64_t meta_blocks = old_meta_blocks_;
    uint64_t data_blocks = old_data_blocks_;
    struct stat status{};
    if (meta_fd_ >= 0 && ::fstat(meta_fd_, &status) == 0) {
      meta_blocks = uint64_t(status.st_blocks);
    }
    if (ignore_data_fd_allocation_) {
      data_blocks = 0;
    } else if (data_fd_ >= 0 && ::fstat(data_fd_, &status) == 0) {
      data_blocks = uint64_t(status.st_blocks);
    }
    reconcile_allocation(meta_blocks, data_blocks);
  }

  void fail(std::exception_ptr error) noexcept {
    if (!failure_) failure_ = std::move(error);
    if (retiring_) {
      retiring_->end_async_wait();
      retiring_.reset();
    }
    if (mapping_ != nullptr) {
      ::munmap(mapping_, mapping_size_);
      mapping_ = nullptr;
    }
    if (metadata_locked_ && meta_fd_ >= 0) {
      ::flock(meta_fd_, LOCK_UN);
      metadata_locked_ = false;
    }
    reconcile_failed_allocation();
    continue_failure_cleanup();
  }

  bool submit_cleanup_close(int fd, Phase phase) noexcept {
    phase_ = phase;
    reset_io(AsyncIoRequest::CLOSE, fd);
    if (executor_->submit(io_)) return true;
    ::close(fd);
    return false;
  }

  void continue_failure_cleanup() noexcept {
    if (meta_fd_ >= 0) {
      if (submit_cleanup_close(meta_fd_, Phase::FAIL_CLOSE_META)) return;
      meta_fd_ = -1;
    }
    if (data_fd_ >= 0) {
      if (submit_cleanup_close(data_fd_, Phase::FAIL_CLOSE_DATA)) return;
      data_fd_ = -1;
    }
    if (meta_parent_owned_ && meta_parent_fd_ >= 0) {
      meta_parent_owned_ = false;
      if (submit_cleanup_close(meta_parent_fd_,
                               Phase::FAIL_CLOSE_META_PARENT)) return;
      meta_parent_fd_ = -1;
    }
    if (data_parent_owned_ && data_parent_fd_ >= 0) {
      data_parent_owned_ = false;
      if (submit_cleanup_close(data_parent_fd_,
                               Phase::FAIL_CLOSE_DATA_PARENT)) return;
      data_parent_fd_ = -1;
    }
    complete_now(CacheAsyncResult{
        .entry = {}, .error = failure_, .value = false});
  }

  void continue_final_cleanup() noexcept {
    if (meta_parent_owned_ && meta_parent_fd_ >= 0) {
      meta_parent_owned_ = false;
      if (submit_cleanup_close(meta_parent_fd_,
                               Phase::FINAL_CLOSE_META_PARENT)) return;
      meta_parent_fd_ = -1;
    }
    if (data_parent_owned_ && data_parent_fd_ >= 0) {
      data_parent_owned_ = false;
      if (submit_cleanup_close(data_parent_fd_,
                               Phase::FINAL_CLOSE_DATA_PARENT)) return;
      data_parent_fd_ = -1;
    }
    complete_now(std::move(final_result_));
  }

  bool is_cleanup_phase() const noexcept {
    return phase_ == Phase::FAIL_CLOSE_META ||
        phase_ == Phase::FAIL_CLOSE_DATA ||
        phase_ == Phase::FAIL_CLOSE_META_PARENT ||
        phase_ == Phase::FAIL_CLOSE_DATA_PARENT ||
        phase_ == Phase::FINAL_CLOSE_META_PARENT ||
        phase_ == Phase::FINAL_CLOSE_DATA_PARENT;
  }

  void cleanup_failed_close() noexcept {
    switch (phase_) {
      case Phase::FAIL_CLOSE_META:
        ::close(meta_fd_); meta_fd_ = -1; continue_failure_cleanup(); return;
      case Phase::FAIL_CLOSE_DATA:
        ::close(data_fd_); data_fd_ = -1; continue_failure_cleanup(); return;
      case Phase::FAIL_CLOSE_META_PARENT:
        ::close(meta_parent_fd_); meta_parent_fd_ = -1;
        continue_failure_cleanup(); return;
      case Phase::FAIL_CLOSE_DATA_PARENT:
        ::close(data_parent_fd_); data_parent_fd_ = -1;
        complete_now(CacheAsyncResult{
            .entry = {}, .error = failure_, .value = false}); return;
      case Phase::FINAL_CLOSE_META_PARENT:
        ::close(meta_parent_fd_); meta_parent_fd_ = -1;
        continue_final_cleanup(); return;
      case Phase::FINAL_CLOSE_DATA_PARENT:
        ::close(data_parent_fd_); data_parent_fd_ = -1;
        complete_now(std::move(final_result_)); return;
      default: return;
    }
  }

  void finish(std::exception_ptr error) noexcept {
    finish(CacheAsyncResult{
        .entry = {}, .error = std::move(error), .value = false});
  }

  void finish(CacheAsyncResult result) noexcept {
    final_result_ = std::move(result);
    continue_final_cleanup();
  }

  void complete_now(CacheAsyncResult result) noexcept {
    release_key_gate();
    CacheAsyncRequest* request = request_;
    CacheAsyncRequest::Complete callback = request->complete;
    void* context = request->context;
    request->implementation_ = nullptr;
    callback(context, std::move(result));
    delete this;
  }

  IoExecutor* executor_ = nullptr;
  CacheAsyncRequest* request_ = nullptr;
  LocalCache* cache_ = nullptr;
  Kind kind_ = Kind::OPEN;
  Phase phase_ = Phase::OPEN_DATA;
  AsyncIoRequest io_;
  CacheCapacityOperation capacity_;
  CachePathWalker path_;
  struct statx status_{};
  CacheMetaHeader existing_header_{};
  std::shared_ptr<CacheEntry> hit_;
  std::shared_ptr<CacheEntry> retiring_;
  std::exception_ptr failure_;
  std::string key_;
  std::string etag_;
  std::string version_id_;
  std::string data_name_;
  std::string meta_name_;
  std::vector<uint64_t> recovery_offsets_;
  int data_fd_ = -1;
  int meta_fd_ = -1;
  int data_parent_fd_ = -1;
  int meta_parent_fd_ = -1;
  void* mapping_ = nullptr;
  size_t mapping_size_ = 0;
  size_t existing_mapping_size_ = 0;
  uint64_t object_size_ = 0;
  uint64_t maximum_size_ = 0;
  time_t mtime_ = 0;
  uint64_t old_data_blocks_ = 0;
  uint64_t old_meta_blocks_ = 0;
  uint64_t new_meta_blocks_ = 0;
  uint64_t recovery_before_blocks_ = 0;
  size_t recovery_index_ = 0;
  uint64_t reservation_ = 0;
  uint64_t capacity_bytes_ = 0;
  uint64_t notification_ = 0;
  bool reset_ = false;
  bool existing_exported_ = false;
  bool allocation_tracking_ = false;
  bool ignore_data_fd_allocation_ = false;
  bool metadata_locked_ = false;
  bool accepted_ = false;
  bool resolving_data_ = false;
  bool data_parent_owned_ = false;
  bool meta_parent_owned_ = false;
  CacheAsyncResult final_result_;
};

class CacheNamespaceOperation final : private CacheKeyOperation {
 public:
  enum class Kind { REMOVE, RENAME };

  CacheNamespaceOperation(IoExecutor& executor, CacheAsyncRequest& request,
                          LocalCache& cache, Kind kind)
      : CacheKeyOperation(cache, executor), executor_(&executor), request_(&request),
        cache_(&cache), kind_(kind) {}

  bool start(std::string_view key, bool preserve_generation,
             std::string_view new_key = {}, bool wait_for_key = true,
             const std::shared_ptr<CacheEntry>& expected_reclaim = {}) {
    if (request_->complete == nullptr || request_->pending()) {
      errno = EINVAL;
      return false;
    }
    if (key.size() > kCacheKeyCapacity ||
        new_key.size() > kCacheKeyCapacity ||
        (kind_ == Kind::RENAME && new_key.empty())) {
      errno = EOVERFLOW;
      return false;
    }
    old_key_.assign(key);
    new_key_.assign(new_key);
    preserve_generation_ = preserve_generation;
    expected_reclaim_ = expected_reclaim;
    request_->implementation_ = this;
    const GateResult gate = acquire_key_gate(
        old_key_, kind_ == Kind::RENAME ? std::string_view(new_key_)
                                        : std::string_view{},
        wait_for_key);
    if (gate == GateResult::FAILED) {
      request_->implementation_ = nullptr;
      return false;
    }
    if (gate == GateResult::QUEUED) {
      accepted_ = true;
      return true;
    }
    return activate_start();
  }

  bool activate_start() {
    if (expected_reclaim_ && !validate_reclaim_candidate()) {
      phase_ = Phase::DEFER_RESULT;
      result_value_ = false;
      return submit_root_stat();
    }
    if (kind_ == Kind::RENAME && old_key_ == new_key_) {
      phase_ = Phase::DEFER_RESULT;
      result_value_ = true;
      return submit_root_stat();
    }
    inspect_registry();
    if (failure_) {
      phase_ = Phase::DEFER_RESULT;
      return submit_root_stat();
    }
    return start_destination_data_path();
  }

  void abandon_start() noexcept {
    abandon_key_gate();
    if (request_->implementation_ == this) request_->implementation_ = nullptr;
  }

 private:
  bool validate_reclaim_candidate() {
    bool kept = false;
    bool current = false;
    {
      std::lock_guard guard(cache_->mutex_);
      kept = std::any_of(
          cache_->keepalive_.begin(), cache_->keepalive_.end(),
          [&](const auto& slot) {
            return slot.entry.get() == expected_reclaim_.get();
          });
      for (const auto& weak : cache_->entries_) {
        std::shared_ptr<CacheEntry> entry = weak.lock();
        if (entry && entry.get() == expected_reclaim_.get() &&
            entry->key_ == old_key_) {
          current = true;
          break;
        }
      }
    }
    // capacity.reclaim_entry_ and expected_reclaim_ are the only strong
    // references here, plus the optional keepalive.
    if (!current || expected_reclaim_.use_count() != (kept ? 3 : 2)) {
      return false;
    }
    std::lock_guard guard(expected_reclaim_->mutex_);
    const auto& header = *static_cast<const CacheMetaHeader*>(
        expected_reclaim_->mapping_);
    if (expected_reclaim_->stale_ || expected_reclaim_->detached_ ||
        expected_reclaim_->eviction_disabled_ ||
        (header.flags & (kCacheMetaDirty | kCacheMetaExported)) != 0 ||
        !expected_reclaim_->active_claims_.empty() ||
        expected_reclaim_->checksum_ops_ != 0 ||
        expected_reclaim_->pinned_regions_ != 0) {
      return false;
    }
    for (size_t page = 0; page < expected_reclaim_->page_count_; ++page) {
      if (expected_reclaim_->page_state(page) != CACHE_PAGE_MISSING) {
        return false;
      }
    }
    return true;
  }

  void activate_key_gate() noexcept override {
    try {
      (void)activate_start();
    } catch (...) {
      finish(std::current_exception());
    }
  }


  void key_gate_failed(std::exception_ptr error) noexcept override {
    finish(std::move(error));
  }
  enum class Phase {
    DEFER_RESULT,
    DEST_DATA_STAT, DEST_DATA_UNLINK, DEST_META_STAT, DEST_META_UNLINK,
    RENAME_DATA, RENAME_META, RENAME_TARGET_FSYNC,
    RENAME_META_OPEN, RENAME_META_READ, RENAME_META_WRITE,
    RENAME_META_FSYNC, RENAME_META_CLOSE,
    ROLLBACK_OLD_DATA_STAT, ROLLBACK_OLD_DATA_UNLINK,
    ROLLBACK_OLD_META_STAT, ROLLBACK_OLD_META_UNLINK,
    ROLLBACK_NEW_DATA_STAT, ROLLBACK_NEW_DATA_UNLINK,
    ROLLBACK_NEW_META_STAT, ROLLBACK_NEW_META_UNLINK,
    FINAL_CLOSE,
  };
  enum class PathPurpose { DEST_DATA, DEST_META, SOURCE_DATA, SOURCE_META };

  bool start_destination_data_path() {
    path_purpose_ = PathPurpose::DEST_DATA;
    const std::string_view key =
        kind_ == Kind::REMOVE ? std::string_view(old_key_)
                              : std::string_view(new_key_);
    return path_.start(*executor_, cache_->data_root_fd_, key,
                       kind_ == Kind::RENAME, path_resolved, this,
                       cache_->name_max_);
  }

  void start_destination_meta_path() {
    path_ = CachePathWalker{};
    path_purpose_ = PathPurpose::DEST_META;
    const std::string_view key =
        kind_ == Kind::REMOVE ? std::string_view(old_key_)
                              : std::string_view(new_key_);
    path_.start(*executor_, cache_->objects_root_fd_, key,
                kind_ == Kind::RENAME, path_resolved, this,
                cache_->name_max_);
  }

  void start_source_data_path() {
    path_ = CachePathWalker{};
    path_purpose_ = PathPurpose::SOURCE_DATA;
    path_.start(*executor_, cache_->data_root_fd_, old_key_, false,
                path_resolved, this, cache_->name_max_);
  }

  void start_source_meta_path() {
    path_ = CachePathWalker{};
    path_purpose_ = PathPurpose::SOURCE_META;
    path_.start(*executor_, cache_->objects_root_fd_, old_key_, false,
                path_resolved, this, cache_->name_max_);
  }

  static void path_resolved(void* context, bool exists, int parent_fd,
                            bool parent_owned, std::string leaf,
                            std::exception_ptr error) noexcept {
    auto* operation = static_cast<CacheNamespaceOperation*>(context);
    if (error) { operation->finish(std::move(error)); return; }
    switch (operation->path_purpose_) {
      case PathPurpose::DEST_DATA:
        operation->destination_data_parent_fd_ = parent_fd;
        operation->destination_data_parent_owned_ = parent_owned;
        operation->destination_data_name_ = std::move(leaf);
        if (!exists) { operation->start_destination_meta_path(); return; }
        operation->phase_ = Phase::DEST_DATA_STAT;
        operation->submit_path_stat(operation->destination_data_parent_fd_,
                                    operation->destination_data_name_);
        return;
      case PathPurpose::DEST_META:
        operation->destination_meta_parent_fd_ = parent_fd;
        operation->destination_meta_parent_owned_ = parent_owned;
        operation->destination_meta_name_ = std::move(leaf);
        if (!exists) { operation->after_destination_removed(); return; }
        operation->phase_ = Phase::DEST_META_STAT;
        operation->submit_path_stat(operation->destination_meta_parent_fd_,
                                    operation->destination_meta_name_);
        return;
      case PathPurpose::SOURCE_DATA:
        operation->source_data_parent_fd_ = parent_fd;
        operation->source_data_parent_owned_ = parent_owned;
        operation->source_data_name_ = std::move(leaf);
        operation->source_data_exists_ = exists;
        operation->start_source_meta_path();
        return;
      case PathPurpose::SOURCE_META:
        operation->source_meta_parent_fd_ = parent_fd;
        operation->source_meta_parent_owned_ = parent_owned;
        operation->source_meta_name_ = std::move(leaf);
        operation->source_meta_exists_ = exists;
        if (!operation->source_data_exists_ && !exists) {
          operation->finish(true);
          return;
        }
        if (operation->source_data_exists_) {
          operation->phase_ = Phase::RENAME_DATA;
          operation->submit_rename(
              operation->source_data_parent_fd_, operation->source_data_name_,
              operation->destination_data_parent_fd_,
              operation->destination_data_name_);
          return;
        }
        operation->phase_ = Phase::RENAME_META;
        operation->submit_rename(
            operation->source_meta_parent_fd_, operation->source_meta_name_,
            operation->destination_meta_parent_fd_,
            operation->destination_meta_name_);
        return;
    }
  }

  void inspect_registry() {
    const std::string_view destination =
        kind_ == Kind::REMOVE ? std::string_view(old_key_)
                              : std::string_view(new_key_);
    std::vector<std::shared_ptr<CacheEntry>> released;
    std::lock_guard guard(cache_->mutex_);
    for (auto i = cache_->entries_.begin(); i != cache_->entries_.end();) {
      std::shared_ptr<CacheEntry> entry = i->lock();
      if (!entry) {
        i = cache_->entries_.erase(i);
        continue;
      }
      if (entry->key_ == destination) {
        bool busy;
        bool preserved = false;
        {
          std::lock_guard entry_guard(entry->mutex_);
          const auto& header =
              *static_cast<const CacheMetaHeader*>(entry->mapping_);
          preserved = kind_ == Kind::REMOVE && preserve_generation_ &&
              !entry->stale_ && (header.flags & kCacheMetaDirty) == 0;
          entry->detached_ = true;
          if (preserved) {
            entry->eviction_disabled_ = true;
          } else {
            entry->stale_ = true;
            entry->notify_waiters_locked();
          }
          busy = !preserved && (!entry->active_claims_.empty() ||
              entry->checksum_ops_ != 0 || entry->pinned_regions_ != 0 ||
              entry->async_metadata_inflight_);
        }
        cache_->release_keepalive_locked(entry.get(), released);
        if (busy) {
          failure_ = std::make_exception_ptr(CacheRetirementPending{});
          return;
        }
        destination_entry_ = entry;
        result_value_ = preserved;
        i = cache_->entries_.erase(i);
        continue;
      }
      if (kind_ == Kind::RENAME && entry->key_ == old_key_) {
        source_entry_ = entry;
      }
      ++i;
    }
  }

  void reset_io(AsyncIoRequest::Kind kind, int fd) noexcept {
    io_ = AsyncIoRequest{};
    io_.kind = kind;
    io_.fd = fd;
    io_.complete = completed;
    io_.context = this;
  }

  bool submit_root_stat() noexcept {
    memset(&status_, 0, sizeof(status_));
    reset_io(AsyncIoRequest::STATX, cache_->root_fd_);
    io_.path = "";
    io_.data = &status_;
    io_.flags = AT_EMPTY_PATH;
    io_.mask = STATX_TYPE;
    return submit();
  }

  bool submit_path_stat(int root, const std::string& name) noexcept {
    memset(&status_, 0, sizeof(status_));
    reset_io(AsyncIoRequest::STATX, root);
    io_.path = name.c_str();
    io_.data = &status_;
    io_.flags = AT_SYMLINK_NOFOLLOW;
    io_.mask = STATX_BLOCKS | STATX_TYPE;
    return submit();
  }

  bool submit_unlink(int root, const std::string& name) noexcept {
    reset_io(AsyncIoRequest::UNLINKAT, root);
    io_.path = name.c_str();
    return submit();
  }

  bool submit_rename(int source_parent, const std::string& old_name,
                     int destination_parent,
                     const std::string& new_name) noexcept {
    reset_io(AsyncIoRequest::RENAMEAT, source_parent);
    io_.output_fd = destination_parent;
    io_.path = old_name.c_str();
    io_.path2 = new_name.c_str();
    return submit();
  }

  bool submit_open_meta() noexcept {
    reset_io(AsyncIoRequest::OPENAT, destination_meta_parent_fd_);
    io_.path = destination_meta_name_.c_str();
    io_.flags = O_RDWR | O_CLOEXEC | O_NOFOLLOW;
    return submit();
  }

  bool submit_read_header() noexcept {
    reset_io(AsyncIoRequest::PREAD, metadata_fd_);
    io_.data = &header_;
    io_.length = sizeof(header_);
    io_.input_offset = 0;
    io_.exact = true;
    return submit();
  }

  bool submit_write_header() noexcept {
    reset_io(AsyncIoRequest::PWRITE, metadata_fd_);
    io_.data = &header_;
    io_.length = sizeof(header_);
    io_.output_offset = 0;
    io_.exact = true;
    return submit();
  }

  bool submit_fsync(int fd) noexcept {
    reset_io(AsyncIoRequest::FSYNC, fd);
    return submit();
  }

  bool submit_close(int fd) noexcept {
    reset_io(AsyncIoRequest::CLOSE, fd);
    return submit();
  }

  bool submit() noexcept {
    if (executor_->submit(io_)) {
      accepted_ = true;
      return true;
    }
    const int error = errno == 0 ? EIO : errno;
    if (!accepted_) {
      abandon_start();
      errno = error;
      return false;
    }
    finish(make_error(error, "submit asynchronous cache namespace operation"));
    return false;
  }

  static std::exception_ptr make_error(int error, const char* what) noexcept {
    try {
      return std::make_exception_ptr(std::system_error(
          error, std::generic_category(), what));
    } catch (...) {
      return std::current_exception();
    }
  }

  static void completed(void* context, ssize_t result) noexcept {
    static_cast<CacheNamespaceOperation*>(context)->advance(result);
  }

  bool missing_is_ok(ssize_t result) const noexcept {
    return result == -ENOENT &&
        (phase_ == Phase::DEST_DATA_STAT ||
         phase_ == Phase::DEST_DATA_UNLINK ||
         phase_ == Phase::DEST_META_STAT ||
         phase_ == Phase::DEST_META_UNLINK ||
         phase_ == Phase::RENAME_DATA ||
         phase_ == Phase::RENAME_META || rollback_phase());
  }

  bool rollback_phase() const noexcept {
    return phase_ >= Phase::ROLLBACK_OLD_DATA_STAT &&
        phase_ <= Phase::ROLLBACK_NEW_META_UNLINK;
  }

  void advance(ssize_t result) noexcept {
    try {
      if (phase_ == Phase::FINAL_CLOSE) {
        if (result < 0 && closing_fd_ >= 0) ::close(closing_fd_);
        clear_closed_target();
        continue_final_close();
        return;
      }
      if (result < 0 && !missing_is_ok(result)) {
        throw std::system_error(int(-result), std::generic_category(),
                                "asynchronous cache namespace operation");
      }
      switch (phase_) {
        case Phase::DEFER_RESULT:
          if (failure_) finish(failure_);
          else finish(result_value_);
          return;

        case Phase::DEST_DATA_STAT:
          if (result == -ENOENT) {
            start_destination_meta_path();
            return;
          }
          blocks_ = status_.stx_blocks;
          phase_ = Phase::DEST_DATA_UNLINK;
          submit_unlink(destination_data_parent_fd_,
                        destination_data_name_);
          return;

        case Phase::DEST_DATA_UNLINK:
          account_unlink(true);
          start_destination_meta_path();
          return;

        case Phase::DEST_META_STAT:
          if (result == -ENOENT) {
            after_destination_removed();
            return;
          }
          blocks_ = status_.stx_blocks;
          phase_ = Phase::DEST_META_UNLINK;
          submit_unlink(destination_meta_parent_fd_,
                        destination_meta_name_);
          return;

        case Phase::DEST_META_UNLINK:
          account_unlink(false);
          after_destination_removed();
          return;

        case Phase::RENAME_DATA:
          data_moved_ = result >= 0;
          if (!source_meta_exists_) {
            if (!data_moved_) { finish(true); return; }
            throw std::runtime_error("cache rename metadata is missing");
          }
          phase_ = Phase::RENAME_META;
          submit_rename(source_meta_parent_fd_, source_meta_name_,
                        destination_meta_parent_fd_, destination_meta_name_);
          return;

        case Phase::RENAME_META:
          meta_moved_ = result >= 0;
          if (!data_moved_ && !meta_moved_) { finish(true); return; }
          if (!meta_moved_) {
            throw std::runtime_error("cache rename metadata is missing");
          }
          rewrite_renamed_metadata();
          return;

        case Phase::RENAME_TARGET_FSYNC:
          finish(true);
          return;

        case Phase::RENAME_META_OPEN:
          metadata_fd_ = int(result);
          phase_ = Phase::RENAME_META_READ;
          submit_read_header();
          return;

        case Phase::RENAME_META_READ:
          validate_and_rewrite_header(header_);
          phase_ = Phase::RENAME_META_WRITE;
          submit_write_header();
          return;

        case Phase::RENAME_META_WRITE:
          phase_ = Phase::RENAME_META_FSYNC;
          submit_fsync(metadata_fd_);
          return;

        case Phase::RENAME_META_FSYNC:
          phase_ = Phase::RENAME_META_CLOSE;
          submit_close(metadata_fd_);
          return;

        case Phase::RENAME_META_CLOSE:
          metadata_fd_ = -1;
          finish(true);
          return;

        case Phase::ROLLBACK_OLD_DATA_STAT:
          if (result == -ENOENT) { start_rollback_old_meta(); return; }
          blocks_ = status_.stx_blocks;
          phase_ = Phase::ROLLBACK_OLD_DATA_UNLINK;
          submit_unlink(source_data_parent_fd_, source_data_name_);
          return;

        case Phase::ROLLBACK_OLD_DATA_UNLINK:
          if (result >= 0) account_rollback_unlink(true);
          start_rollback_old_meta();
          return;

        case Phase::ROLLBACK_OLD_META_STAT:
          if (result == -ENOENT) { start_rollback_new_data(); return; }
          blocks_ = status_.stx_blocks;
          phase_ = Phase::ROLLBACK_OLD_META_UNLINK;
          submit_unlink(source_meta_parent_fd_, source_meta_name_);
          return;

        case Phase::ROLLBACK_OLD_META_UNLINK:
          if (result >= 0) account_rollback_unlink(false);
          start_rollback_new_data();
          return;

        case Phase::ROLLBACK_NEW_DATA_STAT:
          if (result == -ENOENT) { start_rollback_new_meta(); return; }
          blocks_ = status_.stx_blocks;
          phase_ = Phase::ROLLBACK_NEW_DATA_UNLINK;
          submit_unlink(destination_data_parent_fd_, destination_data_name_);
          return;

        case Phase::ROLLBACK_NEW_DATA_UNLINK:
          if (result >= 0) account_rollback_unlink(true);
          start_rollback_new_meta();
          return;

        case Phase::ROLLBACK_NEW_META_STAT:
          if (result == -ENOENT) { finish(rollback_error_); return; }
          blocks_ = status_.stx_blocks;
          phase_ = Phase::ROLLBACK_NEW_META_UNLINK;
          submit_unlink(destination_meta_parent_fd_, destination_meta_name_);
          return;

        case Phase::ROLLBACK_NEW_META_UNLINK:
          if (result >= 0) account_rollback_unlink(false);
          finish(rollback_error_);
          return;

        case Phase::FINAL_CLOSE:
          return;
      }
    } catch (...) {
      if (rollback_phase()) {
        advance_rollback_after_error();
      } else if (kind_ == Kind::RENAME && (data_moved_ || meta_moved_)) {
        begin_rollback(std::current_exception());
      } else {
        finish(std::current_exception());
      }
    }
  }

  void account_unlink(bool data) noexcept {
    if (destination_entry_) {
      std::lock_guard guard(destination_entry_->mutex_);
      if (data) destination_entry_->unlinked_data_ = true;
      else destination_entry_->unlinked_meta_ = true;
    } else if (blocks_ != 0) {
      cache_->add_allocated(-int64_t(std::min<uint64_t>(
          blocks_ * 512, INT64_MAX)));
    }
    blocks_ = 0;
  }

  void begin_rollback(std::exception_ptr error) noexcept {
    rollback_error_ = std::move(error);
    if (source_entry_) {
      std::lock_guard guard(source_entry_->mutex_);
      source_entry_->stale_ = true;
      source_entry_->detached_ = true;
      source_entry_->notify_waiters_locked();
    }
    start_rollback_old_data();
  }

  void start_rollback_old_data() noexcept {
    if (source_data_parent_fd_ < 0 || source_data_name_.empty()) {
      start_rollback_old_meta();
      return;
    }
    phase_ = Phase::ROLLBACK_OLD_DATA_STAT;
    submit_path_stat(source_data_parent_fd_, source_data_name_);
  }

  void start_rollback_old_meta() noexcept {
    if (source_meta_parent_fd_ < 0 || source_meta_name_.empty()) {
      start_rollback_new_data();
      return;
    }
    phase_ = Phase::ROLLBACK_OLD_META_STAT;
    submit_path_stat(source_meta_parent_fd_, source_meta_name_);
  }

  void start_rollback_new_data() noexcept {
    if (destination_data_parent_fd_ < 0 || destination_data_name_.empty()) {
      start_rollback_new_meta();
      return;
    }
    phase_ = Phase::ROLLBACK_NEW_DATA_STAT;
    submit_path_stat(destination_data_parent_fd_, destination_data_name_);
  }

  void start_rollback_new_meta() noexcept {
    if (destination_meta_parent_fd_ < 0 || destination_meta_name_.empty()) {
      finish(rollback_error_);
      return;
    }
    phase_ = Phase::ROLLBACK_NEW_META_STAT;
    submit_path_stat(destination_meta_parent_fd_, destination_meta_name_);
  }

  void advance_rollback_after_error() noexcept {
    switch (phase_) {
      case Phase::ROLLBACK_OLD_DATA_STAT:
      case Phase::ROLLBACK_OLD_DATA_UNLINK:
        start_rollback_old_meta(); return;
      case Phase::ROLLBACK_OLD_META_STAT:
      case Phase::ROLLBACK_OLD_META_UNLINK:
        start_rollback_new_data(); return;
      case Phase::ROLLBACK_NEW_DATA_STAT:
      case Phase::ROLLBACK_NEW_DATA_UNLINK:
        start_rollback_new_meta(); return;
      case Phase::ROLLBACK_NEW_META_STAT:
      case Phase::ROLLBACK_NEW_META_UNLINK:
        finish(rollback_error_); return;
      default:
        finish(rollback_error_); return;
    }
  }

  void account_rollback_unlink(bool data) noexcept {
    if (source_entry_) {
      std::lock_guard guard(source_entry_->mutex_);
      if (data) source_entry_->unlinked_data_ = true;
      else source_entry_->unlinked_meta_ = true;
    } else if (blocks_ != 0) {
      cache_->add_allocated(-int64_t(std::min<uint64_t>(
          blocks_ * 512, INT64_MAX)));
    }
    blocks_ = 0;
  }

  void after_destination_removed() {
    destination_entry_.reset();
    expected_reclaim_.reset();
    if (kind_ == Kind::REMOVE) { finish(result_value_); return; }
    start_source_data_path();
  }

  void validate_and_rewrite_header(CacheMetaHeader& header) {
    constexpr std::array<char, 8> magic{
        'N', 'G', 'S', '3', 'C', 'A', 'C', 'H'};
    if (header.magic != magic || header.version != kCacheMetaVersion ||
        header.key_length != old_key_.size() ||
        memcmp(header.key.data(), old_key_.data(), old_key_.size()) != 0) {
      throw std::runtime_error("cache rename metadata key mismatch");
    }
    memset(header.key.data(), 0, header.key.size());
    memcpy(header.key.data(), new_key_.data(), new_key_.size());
    header.key_length = uint32_t(new_key_.size());
  }

  void rewrite_renamed_metadata() {
    if (source_entry_) {
      {
        std::lock_guard cache_guard(cache_->mutex_);
        std::lock_guard entry_guard(source_entry_->mutex_);
        auto& header = *static_cast<CacheMetaHeader*>(source_entry_->mapping_);
        validate_and_rewrite_header(header);
        source_entry_->key_ = new_key_;
      }
      phase_ = Phase::RENAME_TARGET_FSYNC;
      submit_fsync(source_entry_->meta_fd_);
      return;
    }
    phase_ = Phase::RENAME_META_OPEN;
    submit_open_meta();
  }

  void release_gates() noexcept {
    release_key_gate();
  }

  void finish(bool value) noexcept {
    finish(CacheAsyncResult{.entry = {}, .error = {}, .value = value});
  }

  void finish(std::exception_ptr error) noexcept {
    finish(CacheAsyncResult{
        .entry = {}, .error = std::move(error), .value = false});
  }

  void finish(CacheAsyncResult result) noexcept {
    final_result_ = std::move(result);
    continue_final_close();
  }

  void continue_final_close() noexcept {
    for (;;) {
      int fd = -1;
      if (metadata_fd_ >= 0) {
        close_target_ = CloseTarget::METADATA;
        fd = metadata_fd_;
      } else if (source_meta_parent_owned_ && source_meta_parent_fd_ >= 0) {
        source_meta_parent_owned_ = false;
        close_target_ = CloseTarget::SOURCE_META_PARENT;
        fd = source_meta_parent_fd_;
      } else if (source_data_parent_owned_ && source_data_parent_fd_ >= 0) {
        source_data_parent_owned_ = false;
        close_target_ = CloseTarget::SOURCE_DATA_PARENT;
        fd = source_data_parent_fd_;
      } else if (destination_meta_parent_owned_ &&
                 destination_meta_parent_fd_ >= 0) {
        destination_meta_parent_owned_ = false;
        close_target_ = CloseTarget::DEST_META_PARENT;
        fd = destination_meta_parent_fd_;
      } else if (destination_data_parent_owned_ &&
                 destination_data_parent_fd_ >= 0) {
        destination_data_parent_owned_ = false;
        close_target_ = CloseTarget::DEST_DATA_PARENT;
        fd = destination_data_parent_fd_;
      } else {
        complete_now(std::move(final_result_));
        return;
      }
      closing_fd_ = fd;
      phase_ = Phase::FINAL_CLOSE;
      reset_io(AsyncIoRequest::CLOSE, fd);
      if (executor_->submit(io_)) return;
      ::close(fd);
      clear_closed_target();
    }
  }

  void clear_closed_target() noexcept {
    switch (close_target_) {
      case CloseTarget::METADATA: metadata_fd_ = -1; break;
      case CloseTarget::SOURCE_META_PARENT: source_meta_parent_fd_ = -1; break;
      case CloseTarget::SOURCE_DATA_PARENT: source_data_parent_fd_ = -1; break;
      case CloseTarget::DEST_META_PARENT: destination_meta_parent_fd_ = -1; break;
      case CloseTarget::DEST_DATA_PARENT: destination_data_parent_fd_ = -1; break;
      case CloseTarget::NONE: break;
    }
    close_target_ = CloseTarget::NONE;
    closing_fd_ = -1;
  }

  void complete_now(CacheAsyncResult result) noexcept {
    expected_reclaim_.reset();
    release_gates();
    CacheAsyncRequest* request = request_;
    CacheAsyncRequest::Complete callback = request->complete;
    void* context = request->context;
    request->implementation_ = nullptr;
    callback(context, std::move(result));
    delete this;
  }

  IoExecutor* executor_ = nullptr;
  CacheAsyncRequest* request_ = nullptr;
  LocalCache* cache_ = nullptr;
  Kind kind_ = Kind::REMOVE;
  Phase phase_ = Phase::DEST_DATA_STAT;
  AsyncIoRequest io_;
  CachePathWalker path_;
  struct statx status_{};
  CacheMetaHeader header_{};
  std::shared_ptr<CacheEntry> destination_entry_;
  std::shared_ptr<CacheEntry> expected_reclaim_;
  std::shared_ptr<CacheEntry> source_entry_;
  std::exception_ptr failure_;
  std::exception_ptr rollback_error_;
  std::string old_key_;
  std::string new_key_;
  enum class CloseTarget {
    NONE, METADATA, SOURCE_META_PARENT, SOURCE_DATA_PARENT,
    DEST_META_PARENT, DEST_DATA_PARENT,
  };
  std::string destination_data_name_;
  std::string destination_meta_name_;
  std::string source_data_name_;
  std::string source_meta_name_;
  int metadata_fd_ = -1;
  int destination_data_parent_fd_ = -1;
  int destination_meta_parent_fd_ = -1;
  int source_data_parent_fd_ = -1;
  int source_meta_parent_fd_ = -1;
  int closing_fd_ = -1;
  uint64_t blocks_ = 0;
  bool preserve_generation_ = false;
  bool result_value_ = false;
  bool data_moved_ = false;
  bool meta_moved_ = false;
  bool accepted_ = false;
  bool source_data_exists_ = false;
  bool source_meta_exists_ = false;
  bool destination_data_parent_owned_ = false;
  bool destination_meta_parent_owned_ = false;
  bool source_data_parent_owned_ = false;
  bool source_meta_parent_owned_ = false;
  PathPurpose path_purpose_ = PathPurpose::DEST_DATA;
  CloseTarget close_target_ = CloseTarget::NONE;
  CacheAsyncResult final_result_;
};

CacheAsyncRequest::~CacheAsyncRequest() {
  if (pending()) abort();
}

CacheEntry::CacheEntry(LocalCache& owner, std::string key, int data_fd,
                       int meta_fd, int dirty_fd, void* mapping,
                       size_t mapping_size, uint64_t size,
                       bool punch_missing)
    : owner_(&owner),
      key_(std::move(key)),
      data_fd_(data_fd),
      meta_fd_(meta_fd),
      dirty_fd_(dirty_fd),
      mapping_(mapping),
      mapping_size_(mapping_size),
      size_(size),
      epoch_(static_cast<CacheMetaHeader*>(mapping)->generation_epoch),
      page_size_(kCacheBitmapUnit),
      block_size_(owner.config().block_size),
      page_count_(size == 0 ? 0 : size_t((size - 1) / page_size_ + 1)),
      bitmap_offset_(kCacheMetaHeaderSize),
      referenced_(size == 0 ? 0 : size_t(
          (size - 1) / block_size_ + 1), 0),
      region_pins_(referenced_.size(), 0) {
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  eviction_disabled_ = (header.flags & kCacheMetaExported) != 0;
  for (size_t block = 0; block < referenced_.size(); ++block) {
    const uint64_t offset = uint64_t(block) * block_size_;
    const size_t first = block_first_page(offset);
    const size_t last  = block_last_page(offset);
    bool had_pending   = false;
    for (size_t page = first; page < last; ++page) {
      if (page_state(page) == CACHE_PAGE_READ_PENDING) {
        set_page_state(page, CACHE_PAGE_MISSING);
        had_pending = true;
      }
    }
    bool all_missing = had_pending;
    for (size_t page = first; page < last && all_missing; ++page) {
      all_missing = page_state(page) == CACHE_PAGE_MISSING;
    }
    if (punch_missing && all_missing &&
        (header.flags & kCacheMetaExported) == 0) {
      owner_->punch_range(data_fd_, offset, block_size_);
    }
  }
}

CacheEntry::~CacheEntry() {
  if (async_metadata_inflight_ || !async_metadata_queue_.empty()) abort();
  if (wait_fd_ >= 0) {
    ::close(wait_fd_);
  }
  uint64_t unlinked_bytes = 0;
  struct stat status{};
  if (unlinked_data_ && data_fd_ >= 0 && ::fstat(data_fd_, &status) == 0) {
    unlinked_bytes += uint64_t(status.st_blocks) * 512;
  }
  if (unlinked_meta_ && meta_fd_ >= 0 && ::fstat(meta_fd_, &status) == 0) {
    unlinked_bytes += uint64_t(status.st_blocks) * 512;
  }
  if (mapping_ != nullptr && mapping_ != MAP_FAILED) {
    ::munmap(mapping_, mapping_size_);
  }
  cache_close_fd(data_fd_);
  cache_close_fd(meta_fd_);
  cache_close_fd(dirty_fd_);
  if (unlinked_bytes != 0) {
    owner_->add_allocated(-int64_t(std::min<uint64_t>(
        unlinked_bytes, INT64_MAX)));
  }
}

int CacheEntry::data_fd() const noexcept { return data_fd_; }

uint64_t CacheEntry::size() const noexcept {
  std::lock_guard guard(mutex_);
  return size_;
}

uint64_t CacheEntry::epoch() const noexcept {
  std::unique_lock guard(mutex_);
  return epoch_;
}

uint64_t CacheEntry::written_end() const noexcept {
  std::lock_guard guard(mutex_);
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  return header.written_end;
}

std::string CacheEntry::etag() const {
  std::lock_guard guard(mutex_);
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  if (header.etag_length > header.etag.size()) {
    throw std::runtime_error("invalid cached ETag");
  }
  return std::string(header.etag.data(), header.etag_length);
}

std::string CacheEntry::version_id() const {
  std::lock_guard guard(mutex_);
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  if (header.version_length > header.version_id.size()) {
    throw std::runtime_error("invalid cached Version ID");
  }
  return std::string(header.version_id.data(), header.version_length);
}

CacheIdentitySnapshot CacheEntry::identity_snapshot() const {
  std::lock_guard guard(mutex_);
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  if (header.etag_length > header.etag.size() ||
      header.version_length > header.version_id.size()) {
    throw std::runtime_error("invalid cached identity");
  }
  return CacheIdentitySnapshot{
      .key = key_,
      .etag = std::string(header.etag.data(), header.etag_length),
      .version_id = std::string(header.version_id.data(), header.version_length),
      .size = size_,
      .epoch = epoch_,
  };
}

std::string CacheEntry::write_id() const {
  std::lock_guard guard(mutex_);
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  if (header.write_id_length > header.write_id.size()) {
    throw std::runtime_error("invalid cached write ID");
  }
  return std::string(header.write_id.data(), header.write_id_length);
}

size_t CacheEntry::page_size() const noexcept { return page_size_; }

size_t CacheEntry::block_size() const noexcept { return block_size_; }

bool CacheEntry::stale() const noexcept {
  std::lock_guard guard(mutex_);
  return stale_;
}

bool CacheEntry::dirty() const noexcept {
  std::lock_guard guard(mutex_);
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  return (header.flags & kCacheMetaDirty) != 0;
}

bool CacheEntry::exported() const noexcept {
  std::lock_guard guard(mutex_);
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  return (header.flags & kCacheMetaExported) != 0;
}

bool CacheEntry::try_export(bool verify) {
  std::lock_guard guard(mutex_);
  auto& header = *static_cast<CacheMetaHeader*>(mapping_);
  if (!owner_->config().unlimited || stale_ || detached_ ||
      (header.flags & kCacheMetaDirty) != 0 ||
      !active_claims_.empty() || checksum_ops_ != 0 || pinned_regions_ != 0 ||
      std::find(checksum_states_.begin(), checksum_states_.end(),
                kPartBad) != checksum_states_.end() ||
      !range_all_state(0, size_t(std::min<uint64_t>(size_, SIZE_MAX)),
                       CACHE_PAGE_CLEAN)) {
    return false;
  }
  if (verify) {
    if (checksum_manifest_ != kChecksumAvailable ||
        (size_ != 0 && checksum_parts_.empty()) ||
        checksum_states_.size() != checksum_parts_.size() ||
        std::find(checksum_states_.begin(), checksum_states_.end(),
                  kPartUnverified) != checksum_states_.end() ||
        std::find_if(checksum_states_.begin(), checksum_states_.end(),
                     [](uint8_t state) { return state != kPartVerified; }) !=
            checksum_states_.end()) {
      return false;
    }
  }
  if ((header.flags & kCacheMetaExported) == 0) {
    header.flags |= kCacheMetaExported;
  }
  eviction_disabled_ = true;
  return true;
}

CachePageState CacheEntry::page_state(size_t page) const noexcept {
  const auto* words = reinterpret_cast<const uint64_t*>(
      static_cast<const char*>(mapping_) + bitmap_offset_);
  const size_t word = page / 32;
  const unsigned shift = unsigned(page % 32) * 2;
  std::atomic_ref<uint64_t> value(
      const_cast<uint64_t&>(words[word]));
  return CachePageState((value.load(std::memory_order_acquire) >> shift) & 3);
}

void CacheEntry::set_page_state(size_t page, CachePageState state) noexcept {
  auto* words = reinterpret_cast<uint64_t*>(
      static_cast<char*>(mapping_) + bitmap_offset_);
  const size_t word = page / 32;
  const unsigned shift = unsigned(page % 32) * 2;
  const uint64_t mask = 3ULL << shift;
  std::atomic_ref<uint64_t> value(words[word]);
  uint64_t current = value.load(std::memory_order_relaxed);
  for (;;) {
    const uint64_t next = (current & ~mask) | (uint64_t(state) << shift);
    if (value.compare_exchange_weak(current, next,
                                    std::memory_order_release,
                                    std::memory_order_relaxed)) {
      return;
    }
  }
}

bool CacheEntry::range_has_state(uint64_t offset, size_t length,
                                 CachePageState state) const noexcept {
  if (length == 0) {
    return false;
  }
  const size_t first = size_t(offset / page_size_);
  const size_t last = size_t((offset + length - 1) / page_size_);
  for (size_t page = first; page <= last; ++page) {
    if (page_state(page) == state) {
      return true;
    }
  }
  return false;
}

bool CacheEntry::range_all_state(uint64_t offset, size_t length,
                                 CachePageState state) const noexcept {
  if (length == 0) {
    return true;
  }
  const size_t first = size_t(offset / page_size_);
  const size_t last  = size_t((offset + length - 1) / page_size_) + 1;
  const auto* words = reinterpret_cast<const uint64_t*>(
      static_cast<const char*>(mapping_) + bitmap_offset_);
  const uint64_t pattern = uint64_t(state) * 0x5555555555555555ULL;
  size_t page = first;
  while (page < last) {
    const size_t word  = page / 32;
    const unsigned bit = unsigned(page % 32) * 2;
    const size_t count = std::min<size_t>(32 - page % 32, last - page);
    uint64_t mask = UINT64_MAX >> ((32 - count) * 2);
    mask <<= bit;
    std::atomic_ref<uint64_t> value(
        const_cast<uint64_t&>(words[word]));
    if ((value.load(std::memory_order_acquire) & mask) !=
        (pattern & mask)) {
      return false;
    }
    page += count;
  }
  return true;
}

bool CacheEntry::range_clean(uint64_t offset, size_t length) const {
  std::lock_guard guard(mutex_);
  return !stale_ && range_all_state(offset, length, CACHE_PAGE_CLEAN);
}

bool CacheEntry::range_available_or_pending(
    uint64_t offset, size_t length) const {
  std::lock_guard guard(mutex_);
  if (stale_ || length == 0 || offset >= size_) return false;
  length = size_t(std::min<uint64_t>(length, size_ - offset));
  const size_t first = size_t(offset / page_size_);
  const size_t last  = size_t((offset + length - 1) / page_size_);
  for (size_t page = first; page <= last; ++page) {
    const CachePageState state = page_state(page);
    if (state != CACHE_PAGE_CLEAN && state != CACHE_PAGE_READ_PENDING) {
      return false;
    }
  }
  return true;
}

bool CacheEntry::range_bad(uint64_t offset, size_t length) const {
  std::lock_guard guard(mutex_);
  if (stale_) {
    return false;
  }
  if (length == 0) {
    return false;
  }
  const size_t first = size_t(offset / page_size_);
  const size_t last = size_t((offset + length - 1) / page_size_);
  for (size_t page = first; page <= last; ++page) {
    if (page_state(page) == CACHE_PAGE_BAD) {
      return true;
    }
  }
  return false;
}

bool CacheEntry::fully_clean() const {
  std::lock_guard guard(mutex_);
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  return !stale_ && (header.flags & kCacheMetaDirty) == 0 &&
      range_all_state(0, size_t(std::min<uint64_t>(size_, SIZE_MAX)),
                      CACHE_PAGE_CLEAN);
}

bool CacheEntry::prepare_read(uint64_t offset, size_t length) {
  {
    std::lock_guard guard(mutex_);
    const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
    if (stale_ || (header.flags & kCacheMetaExported) != 0) return false;
  }
  return owner_->prepare_range(*this, offset, length, false);
}

int cache_async_start_errno(std::exception_ptr error) noexcept {
  try {
    if (error) std::rethrow_exception(error);
  } catch (const std::system_error& system_error) {
    return system_error.code().value();
  } catch (const std::overflow_error&) {
    return EOVERFLOW;
  } catch (const std::bad_alloc&) {
    return ENOMEM;
  } catch (...) {
    return EIO;
  }
  return EIO;
}

template<class Start>
bool cache_start_entry_operation(IoExecutor& executor,
                                 CacheAsyncRequest& request,
                                 CacheEntry& entry,
                                 CacheAsyncOperation::Kind kind,
                                 Start&& start) noexcept {
  CacheAsyncOperation* operation = nullptr;
  try {
    operation = new CacheAsyncOperation(executor, request, entry, kind);
    if (start(*operation)) return true;
    delete operation;
    return false;
  } catch (...) {
    const std::exception_ptr error = std::current_exception();
    if (operation != nullptr) {
      operation->abandon_start();
      delete operation;
    }
    errno = cache_async_start_errno(error);
    return false;
  }
}

bool CacheEntry::prepare_read_async(IoExecutor& executor,
                                    CacheAsyncRequest& request,
                                    uint64_t offset,
                                    size_t length) noexcept {
  return cache_start_entry_operation(
      executor, request, *this, CacheAsyncOperation::Kind::PREPARE_READ,
      [&](CacheAsyncOperation& operation) {
        return operation.start_prepare_read(offset, length);
      });
}

void CacheEntry::begin_write() {
  uint64_t epoch;
  {
    std::lock_guard guard(mutex_);
    if (stale_) {
      throw std::system_error(ESTALE, std::generic_category(),
                              "begin stale cache write");
    }
    if (dirty_fd_ >= 0) {
      return;
    }
    epoch = epoch_;
  }
  const int fd = owner_->create_dirty_marker(key_, epoch);
  std::lock_guard guard(mutex_);
  if (stale_ || epoch_ != epoch) {
    ::close(fd);
    owner_->remove_dirty_marker(key_);
    throw std::system_error(ESTALE, std::generic_category(),
                            "cache generation changed during write");
  }
  if (dirty_fd_ >= 0) {
    ::close(fd);
    return;
  }
  dirty_fd_ = fd;
  auto& header = *static_cast<CacheMetaHeader*>(mapping_);
  header.flags |= kCacheMetaDirty;
  header.write_phase = 1;
}

bool CacheEntry::begin_write_async(IoExecutor& executor,
                                   CacheAsyncRequest& request) noexcept {
  return cache_start_entry_operation(
      executor, request, *this, CacheAsyncOperation::Kind::BEGIN_WRITE,
      [](CacheAsyncOperation& operation) {
        return operation.start_begin_write();
      });
}

void CacheEntry::ensure_write_capacity(uint64_t end) {
  if (end == 0) {
    return;
  }
  const uint64_t unit = owner_->config().upload_part_size;
  if (unit == 0 || end > UINT64_MAX - (unit - 1)) {
    throw std::overflow_error("cache write capacity overflow");
  }
  const uint64_t capacity = (end + unit - 1) / unit * unit;
  const size_t wanted = cache_mapping_size(capacity, page_size_);
  for (;;) {
    size_t old_mapping_size;
    {
      std::lock_guard guard(mutex_);
      old_mapping_size = mapping_size_;
      if (wanted <= old_mapping_size) {
        return;
      }
    }
    const size_t reserve = cache_round_up(
        wanted - old_mapping_size, owner_->config().page_size);
    if (!owner_->reserve_capacity(reserve)) {
      throw std::system_error(ENOSPC, std::generic_category(),
                              "reserve cache write metadata");
    }
    struct stat before{};
    if (::fstat(meta_fd_, &before) != 0) {
      const int error = errno;
      owner_->cancel_reservation(reserve);
      throw std::system_error(error, std::generic_category(),
                              "fstat(cache write metadata)");
    }
    if (::fallocate(meta_fd_, 0, off_t(old_mapping_size),
                    off_t(wanted - old_mapping_size)) != 0) {
      const int error = errno;
      owner_->cancel_reservation(reserve);
      throw std::system_error(error, std::generic_category(),
                              "fallocate(cache write metadata)");
    }
    struct stat after{};
    if (::fstat(meta_fd_, &after) != 0) {
      const int error = errno;
      owner_->cancel_reservation(reserve);
      throw std::system_error(error, std::generic_category(),
                              "fstat(cache write metadata result)");
    }
    const uint64_t old_bytes = uint64_t(before.st_blocks) * 512;
    const uint64_t new_bytes = uint64_t(after.st_blocks) * 512;
    owner_->finish_reservation(
        reserve, new_bytes > old_bytes ? new_bytes - old_bytes : 0);
    std::lock_guard guard(mutex_);
    if (mapping_size_ != old_mapping_size) {
      continue;
    }
    void* expanded = ::mremap(mapping_, old_mapping_size, wanted,
                              MREMAP_MAYMOVE);
    if (expanded == MAP_FAILED) {
      cache_throw_errno("mremap(cache write metadata)");
    }
    mapping_      = expanded;
    mapping_size_ = wanted;
    return;
  }
}

void CacheEntry::prepare_write(uint64_t offset, size_t length) {
  if (length == 0) {
    return;
  }
  {
    std::lock_guard guard(mutex_);
    const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
    if ((header.flags & kCacheMetaExported) != 0) {
      throw std::system_error(ESTALE, std::generic_category(),
                              "write to exported cache generation");
    }
  }
  if (offset > UINT64_MAX - length) {
    throw std::overflow_error("cache write range overflow");
  }
  const uint64_t end  = offset + length;
  const uint64_t unit = owner_->config().upload_part_size;
  ensure_write_capacity(end);
  const size_t first = size_t(offset / unit);
  const size_t last  = size_t((end - 1) / unit);
  if (reserved_units_.size() <= last) {
    reserved_units_.resize(last + 1, 0);
  }
  for (size_t part = first; part <= last; ++part) {
    if (reserved_units_[part] != 0) {
      continue;
    }
    if (!owner_->prepare_range(*this, uint64_t(part) * unit, 1, true)) {
      throw std::system_error(ENOSPC, std::generic_category(),
                              "reserve dirty cache range");
    }
    reserved_units_[part] = 1;
  }
  begin_write();
}

bool CacheEntry::prepare_write_async(IoExecutor& executor,
                                     CacheAsyncRequest& request,
                                     uint64_t offset,
                                     size_t length) noexcept {
  return cache_start_entry_operation(
      executor, request, *this, CacheAsyncOperation::Kind::PREPARE_WRITE,
      [&](CacheAsyncOperation& operation) {
        return operation.start_prepare_write(offset, length);
      });
}

void CacheEntry::publish_dirty(uint64_t offset, size_t length,
                               uint64_t written_end) {
  std::lock_guard guard(mutex_);
  auto& header = *static_cast<CacheMetaHeader*>(mapping_);
  if (stale_ || (header.flags & kCacheMetaDirty) == 0) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "publish stale cache write");
  }
  if (offset != header.written_end || length == 0 ||
      written_end != offset + length) {
    throw std::logic_error("non-sequential cache write publication");
  }
  const size_t pages = size_t((written_end - 1) / page_size_) + 1;
  page_count_ = pages;
  const size_t regions = size_t((written_end - 1) / block_size_ + 1);
  if (referenced_.size() < regions) {
    referenced_.resize(regions, 0);
    region_pins_.resize(regions, 0);
  }
  const size_t first = size_t(offset / page_size_);
  const size_t last  = size_t((written_end - 1) / page_size_);
  for (size_t page = first; page <= last; ++page) {
    set_page_state(page, CACHE_PAGE_CLEAN);
  }
  header.object_size = written_end;
  std::atomic_ref<uint64_t> published(header.written_end);
  published.store(written_end, std::memory_order_release);
  size_ = written_end;
}

void CacheEntry::set_upload_id(std::string_view upload_id) {
  if (upload_id.size() > kCacheUploadIdCapacity) {
    throw std::system_error(EOVERFLOW, std::generic_category(),
                            "multipart upload ID is too large");
  }
  std::lock_guard guard(mutex_);
  auto& header = *static_cast<CacheMetaHeader*>(mapping_);
  memset(header.upload_id.data(), 0, header.upload_id.size());
  memcpy(header.upload_id.data(), upload_id.data(), upload_id.size());
  header.upload_id_length = uint32_t(upload_id.size());
  header.upload_part_size = owner_->config().upload_part_size;
  header.write_phase      = upload_id.empty() ? 1 : 2;
}

bool CacheEntry::set_upload_id_async(IoExecutor& executor,
                                     CacheAsyncRequest& request,
                                     std::string_view upload_id) noexcept {
  return cache_start_entry_operation(
      executor, request, *this, CacheAsyncOperation::Kind::SET_UPLOAD_ID,
      [&](CacheAsyncOperation& operation) {
        return operation.start_set_upload_id(upload_id);
      });
}

std::string CacheEntry::upload_id() const {
  std::lock_guard guard(mutex_);
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  if (header.upload_id_length > header.upload_id.size()) {
    throw std::runtime_error("invalid cached multipart upload ID");
  }
  return std::string(header.upload_id.data(), header.upload_id_length);
}

void CacheEntry::isolate_write() noexcept {
  std::lock_guard guard(mutex_);
  auto& header = *static_cast<CacheMetaHeader*>(mapping_);
  if ((header.flags & kCacheMetaDirty) == 0) {
    return;
  }
  header.write_phase = 3;
}

bool CacheEntry::mark_commit_pending_async(
    IoExecutor& executor, CacheAsyncRequest& request) noexcept {
  return cache_start_entry_operation(
      executor, request, *this,
      CacheAsyncOperation::Kind::MARK_COMMIT_PENDING,
      [](CacheAsyncOperation& operation) {
        return operation.start_mark_commit_pending();
      });
}

void CacheEntry::discard_write() noexcept {
  int marker_fd = -1;
  {
    std::lock_guard guard(mutex_);
    auto& header = *static_cast<CacheMetaHeader*>(mapping_);
    header.flags       &= ~kCacheMetaDirty;
    header.write_phase  = 0;
    stale_              = true;
    detached_           = true;
    marker_fd           = dirty_fd_;
    dirty_fd_           = -1;
  }
  cache_close_fd(marker_fd);
  owner_->remove_dirty_marker(key_);
}

bool CacheEntry::discard_write_async(IoExecutor& executor,
                                     CacheAsyncRequest& request) noexcept {
  return cache_start_entry_operation(
      executor, request, *this, CacheAsyncOperation::Kind::DISCARD_WRITE,
      [](CacheAsyncOperation& operation) {
        return operation.start_discard_write();
      });
}

void CacheEntry::sync_write() {
  std::lock_guard guard(mutex_);
  if (stale_) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "sync stale cache write");
  }
  if (::fdatasync(data_fd_) != 0) {
    cache_throw_errno("fdatasync(cache data)");
  }
  if (::msync(mapping_, mapping_size_, MS_SYNC) != 0) {
    cache_throw_errno("msync(cache metadata)");
  }
  if (dirty_fd_ >= 0 && ::fdatasync(dirty_fd_) != 0) {
    cache_throw_errno("fdatasync(cache dirty marker)");
  }
}

bool CacheEntry::sync_write_async(IoExecutor& executor,
                                  CacheAsyncRequest& request) noexcept {
  return cache_start_entry_operation(
      executor, request, *this, CacheAsyncOperation::Kind::SYNC_WRITE,
      [](CacheAsyncOperation& operation) {
        return operation.start_sync_write();
      });
}

void CacheEntry::commit_write(const CacheIdentity& identity) {
  if (identity.etag.size() > kCacheEtagCapacity ||
      identity.version_id.size() > kCacheVersionCapacity) {
    throw std::system_error(EOVERFLOW, std::generic_category(),
                            "cache write identity is too large");
  }
  {
    std::lock_guard guard(mutex_);
    if (identity.key != key_ || identity.size != size_) {
      throw std::logic_error("cache write commit identity mismatch");
    }
    auto& header = *static_cast<CacheMetaHeader*>(mapping_);
    memset(header.etag.data(), 0, header.etag.size());
    memset(header.version_id.data(), 0, header.version_id.size());
    if (!identity.etag.empty()) {
      memcpy(header.etag.data(), identity.etag.data(), identity.etag.size());
    }
    if (!identity.version_id.empty()) {
      memcpy(header.version_id.data(), identity.version_id.data(),
             identity.version_id.size());
    }
    header.etag_length      = uint32_t(identity.etag.size());
    header.version_length   = uint32_t(identity.version_id.size());
    header.mtime            = identity.mtime;
    header.flags           &= ~kCacheMetaDirty;
    header.write_phase      = 0;
    header.upload_id_length = 0;
    eviction_disabled_ = false;
    if (::msync(mapping_, mapping_size_, MS_SYNC) != 0) {
      cache_throw_errno("msync(cache write commit)");
    }
    cache_close_fd(dirty_fd_);
  }
  owner_->remove_dirty_marker(key_);
}

bool CacheEntry::commit_write_async(IoExecutor& executor,
                                    CacheAsyncRequest& request,
                                    const CacheIdentity& identity) noexcept {
  return cache_start_entry_operation(
      executor, request, *this, CacheAsyncOperation::Kind::COMMIT_WRITE,
      [&](CacheAsyncOperation& operation) {
        return operation.start_commit_write(identity);
      });
}

bool CacheEntry::pin_clean(uint64_t offset, size_t length) {
  std::lock_guard guard(mutex_);
  if (stale_) {
    return false;
  }
  if (length == 0) {
    return true;
  }
  if (!range_all_state(offset, length, CACHE_PAGE_CLEAN) ||
      checksum_blocked_locked(offset, length) != 0) {
    return false;
  }
  const size_t region_size = block_size_;
  const size_t first_region = size_t(offset / region_size);
  const size_t last_region  = std::min(
      region_pins_.size(), size_t((offset + length - 1) / region_size) + 1);
  for (size_t region = first_region; region < last_region; ++region) {
    if (region_pins_[region] == UINT32_MAX) {
      throw std::overflow_error("cache region pin count overflow");
    }
  }
  for (size_t region = first_region; region < last_region; ++region) {
    ++region_pins_[region];
  }
  pinned_regions_ += last_region - first_region;
  for (size_t region = first_region; region < last_region; ++region) {
    referenced_[region] = 1;
  }
  return true;
}

void CacheEntry::pin(uint64_t offset, size_t length) {
  std::lock_guard guard(mutex_);
  if (stale_) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "pin stale cache generation");
  }
  if (length == 0) {
    return;
  }
  const size_t region_size = block_size_;
  const size_t first = size_t(offset / region_size);
  const size_t last = std::min(
      region_pins_.size(), size_t((offset + length - 1) / region_size) + 1);
  for (size_t region = first; region < last; ++region) {
    if (region_pins_[region] == UINT32_MAX) {
      throw std::overflow_error("cache region pin count overflow");
    }
  }
  for (size_t region = first; region < last; ++region) {
    ++region_pins_[region];
  }
  pinned_regions_ += last - first;
}

void CacheEntry::unpin(uint64_t offset, size_t length) noexcept {
  std::lock_guard guard(mutex_);
  if (length == 0) {
    return;
  }
  const size_t region_size = block_size_;
  const size_t first = size_t(offset / region_size);
  const size_t last = std::min(
      region_pins_.size(), size_t((offset + length - 1) / region_size) + 1);
  bool released_region = false;
  for (size_t region = first; region < last; ++region) {
    if (region_pins_[region] != 0) {
      --region_pins_[region];
      --pinned_regions_;
      released_region = true;
    }
  }
  if (released_region) {
    notify_waiters_locked();
  }
}

void CacheEntry::touch(uint64_t offset, size_t length) noexcept {
  std::lock_guard guard(mutex_);
  if (length == 0 || referenced_.empty()) {
    return;
  }
  const size_t region_size = block_size_;
  const size_t first = size_t(offset / region_size);
  const size_t last = std::min(
      referenced_.size(), size_t((offset + length - 1) / region_size) + 1);
  for (size_t region = first; region < last; ++region) {
    referenced_[region] = 1;
  }
}

void CacheEntry::disable_eviction() noexcept {
  std::lock_guard guard(mutex_);
  eviction_disabled_ = true;
}

size_t CacheEntry::block_first_page(uint64_t offset) const noexcept {
  return size_t((offset / block_size_ * block_size_) / page_size_);
}

size_t CacheEntry::block_last_page(uint64_t offset) const noexcept {
  const uint64_t first = offset / block_size_ * block_size_;
  const uint64_t end = std::min<uint64_t>(size_, first + block_size_);
  return end == 0 ? 0 : std::min(
      page_count_, size_t((end - 1) / page_size_) + 1);
}

bool CacheEntry::range_ready_locked(uint64_t offset,
                                    size_t length) const noexcept {
  if (stale_ || length == 0) {
    return true;
  }
  const uint8_t checksum = checksum_blocked_locked(offset, length);
  if (checksum == kPartBad) return true;
  if (checksum == kPartRetrying) return false;
  const size_t first = size_t(offset / page_size_);
  const size_t last = size_t((offset + length - 1) / page_size_);
  bool pending = false;
  for (size_t page = first; page <= last; ++page) {
    const CachePageState state = page_state(page);
    if (state == CACHE_PAGE_BAD) {
      return true;
    }
    if (state == CACHE_PAGE_READ_PENDING) {
      pending = true;
    }
  }
  if (pending) return false;
  const uint64_t first_block = offset / block_size_;
  const uint64_t last_block = (offset + length - 1) / block_size_;
  for (uint64_t block = first_block; block <= last_block; ++block) {
    const uint64_t block_offset = block * block_size_;
    const size_t block_first = block_first_page(block_offset);
    const size_t block_last  = block_last_page(block_offset);
    bool block_incomplete = false;
    for (size_t page = block_first; page < block_last; ++page) {
      const CachePageState state = page_state(page);
      if (state == CACHE_PAGE_READ_PENDING) return false;
      if (state != CACHE_PAGE_CLEAN) block_incomplete = true;
    }
    if (block_incomplete && block < region_pins_.size() &&
        region_pins_[size_t(block)] != 0) {
      return false;
    }
  }
  return true;
}

void CacheEntry::wait_locked(std::unique_lock<std::mutex>& guard) {
  if (wait_fd_ < 0) {
    wait_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE);
    if (wait_fd_ < 0) {
      throw std::system_error(errno, std::generic_category(),
                              "create cache wait event");
    }
  }
  ++waiters_;
  guard.unlock();
  uint64_t wake = 0;
  ssize_t result;
  do {
    result = io_read(wait_fd_, &wake, sizeof(wake));
  } while (result < 0 && errno == EINTR);
  guard.lock();
  --waiters_;
  if (result != ssize_t(sizeof(wake))) {
    throw std::system_error(result < 0 ? errno : EIO,
                            std::generic_category(),
                            "wait for cache state");
  }
}

void CacheEntry::notify_waiters_locked() noexcept {
  if (wait_fd_ < 0 || waiters_ == 0) {
    return;
  }
  const uint64_t wake = waiters_;
  ssize_t result;
  do {
    result = ::write(wait_fd_, &wake, sizeof(wake));
  } while (result < 0 && errno == EINTR);
}

CacheFetchClaim CacheEntry::claim_fetch(uint64_t wanted_offset,
                                        size_t wanted_length,
                                        size_t expansion) {
  std::lock_guard guard(mutex_);
  if (stale_) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "stale cache generation");
  }
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  if (wanted_length == 0 || wanted_offset >= size_ ||
      (header.flags & (kCacheMetaDirty | kCacheMetaExported)) != 0) {
    return {};
  }
  wanted_length = size_t(std::min<uint64_t>(
      wanted_length, size_ - wanted_offset));
  const size_t first = size_t(wanted_offset / page_size_);
  const size_t last  = size_t((wanted_offset + wanted_length - 1) / page_size_);
  size_t missing = first;
  for (; missing <= last; ++missing) {
    const CachePageState state = page_state(missing);
    if (state == CACHE_PAGE_CLEAN) continue;
    if (state != CACHE_PAGE_MISSING) return {};
    break;
  }
  if (missing > last) {
    return {};
  }

  const uint64_t start = uint64_t(missing) * page_size_ /
      block_size_ * block_size_;
  const uint64_t request_end = wanted_offset + wanted_length;
  const uint64_t extent = std::max<size_t>(expansion, block_size_);
  const uint64_t expanded_end = start > UINT64_MAX - extent
      ? UINT64_MAX : start + extent;
  const uint64_t request_block_end = request_end > UINT64_MAX -
          (block_size_ - 1)
      ? UINT64_MAX
      : (request_end + block_size_ - 1) / block_size_ * block_size_;
  uint64_t end = std::min<uint64_t>(
      size_, std::max(request_block_end,
                      expanded_end > UINT64_MAX - (block_size_ - 1)
                          ? UINT64_MAX
                          : (expanded_end + block_size_ - 1) /
                                block_size_ * block_size_));
  for (uint64_t block = start; block < end; block += block_size_) {
    const size_t region = size_t(block / block_size_);
    bool blocked = region < region_pins_.size() && region_pins_[region] != 0;
    const size_t block_first = block_first_page(block);
    const size_t block_last  = block_last_page(block);
    bool all_clean = true;
    for (size_t page = block_first; page < block_last && !blocked; ++page) {
      const CachePageState state = page_state(page);
      blocked = state == CACHE_PAGE_READ_PENDING || state == CACHE_PAGE_BAD;
      all_clean = all_clean && state == CACHE_PAGE_CLEAN;
    }
    if (blocked || (block != start && all_clean)) {
      end = block;
      break;
    }
    if (block > UINT64_MAX - block_size_) break;
  }
  if (end <= start) {
    return {};
  }
  const size_t first_page = block_first_page(start);
  const size_t end_page = std::min(
      page_count_, size_t((end - 1) / page_size_) + 1);
  ActiveFetch active{
      .id = next_claim_id_++,
      .epoch = epoch_,
      .offset = start,
      .length = size_t(end - start),
      .prior_states = {},
  };
  if (active.id == 0) active.id = next_claim_id_++;
  active.prior_states.reserve(end_page - first_page);
  active_claims_.reserve(active_claims_.size() + 1);
  for (size_t page = first_page; page < end_page; ++page) {
    active.prior_states.push_back(page_state(page));
    set_page_state(page, CACHE_PAGE_READ_PENDING);
  }
  const CacheFetchClaim claim{start, epoch_, active.id, active.length};
  active_claims_.push_back(std::move(active));
  return claim;
}

void CacheEntry::wait_for_range(uint64_t offset, size_t length) {
  std::unique_lock guard(mutex_);
  while (!range_ready_locked(offset, length)) {
    wait_locked(guard);
  }
  if (stale_) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "stale cache generation");
  }
}

int CacheEntry::begin_async_wait(uint64_t offset, size_t length) {
  std::lock_guard guard(mutex_);
  if (range_ready_locked(offset, length)) return -1;
  if (wait_fd_ < 0) {
    wait_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE);
    if (wait_fd_ < 0) {
      throw std::system_error(errno, std::generic_category(),
                              "create cache wait event");
    }
  }
  ++waiters_;
  return wait_fd_;
}

void CacheEntry::end_async_wait() noexcept {
  std::lock_guard guard(mutex_);
  if (waiters_ == 0) abort();
  --waiters_;
}

uint8_t CacheEntry::checksum_blocked_locked(uint64_t offset, size_t length) const noexcept {
  if (stale_ || offset >= size_ || length == 0) return 0;
  const uint64_t end = offset + std::min<uint64_t>(length, size_ - offset);
  auto part = std::lower_bound(checksum_parts_.begin(), checksum_parts_.end(), offset,
      [](const CacheChecksumPart& item, uint64_t begin) { return item.offset + item.size <= begin; });
  uint8_t blocked = 0;
  for (; part != checksum_parts_.end() && part->offset < end; ++part) {
    const uint8_t state = checksum_states_[size_t(part - checksum_parts_.begin())];
    if (state == kPartBad) return kPartBad;
    if (state == kPartRetrying) blocked = state;
  }
  return blocked;
}

bool CacheEntry::checksum_failed(uint64_t offset, size_t length) const {
  std::lock_guard guard(mutex_);
  return checksum_blocked_locked(offset, length) == kPartBad;
}

int CacheEntry::begin_checksum_wait(uint64_t offset, size_t length) {
  std::lock_guard guard(mutex_);
  if (checksum_blocked_locked(offset, length) != kPartRetrying) return -1;
  if (wait_fd_ < 0) {
    wait_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE);
    if (wait_fd_ < 0) {
      throw std::system_error(errno, std::generic_category(), "create checksum wait event");
    }
  }
  ++waiters_;
  return wait_fd_;
}

void CacheEntry::publish_clean(const CacheFetchClaim& claim,
                               size_t published, size_t length,
                               bool final) {
  std::lock_guard guard(mutex_);
  const auto active = std::find_if(active_claims_.begin(), active_claims_.end(),
      [&](const ActiveFetch& item) {
        return item.id == claim.id && item.epoch == claim.epoch &&
            item.offset == claim.offset && item.length == claim.length;
      });
  if (stale_ || active == active_claims_.end() || length < published ||
      (length == published && !final)) {
    return;
  }
  const uint64_t progress = claim.offset + std::min(length, claim.length);
  bool changed = false;
  for (uint64_t block = claim.offset + active->published;
       block < claim.offset + claim.length; block += block_size_) {
    const uint64_t block_end = std::min<uint64_t>(size_, block + block_size_);
    if (progress < block_end) break;
    if (block_end == size_ && block_end % block_size_ != 0 && !final) break;
    const size_t first = block_first_page(block);
    const size_t last  = block_last_page(block);
    for (size_t page = first; page < last; ++page) {
      if (page_state(page) == CACHE_PAGE_READ_PENDING) {
        set_page_state(page, CACHE_PAGE_CLEAN);
        changed = true;
      }
    }
    active->published = size_t(block_end - claim.offset);
    if (block > UINT64_MAX - block_size_) break;
  }
  if (changed) notify_waiters_locked();
}

bool CacheEntry::end_fetch_locked(const CacheFetchClaim& claim) noexcept {
  const auto i = std::find_if(active_claims_.begin(), active_claims_.end(),
      [&](const ActiveFetch& active) {
        return active.id == claim.id && active.epoch == claim.epoch &&
            active.offset == claim.offset && active.length == claim.length;
      });
  if (i == active_claims_.end()) {
    return false;
  }
  if (i->verifying) {
    const size_t first = size_t(claim.offset / block_size_);
    const size_t last = size_t((claim.offset + claim.length - 1) / block_size_) + 1;
    for (size_t region = first; region < last; ++region) {
      if (region_pins_[region] != 0) {
        --region_pins_[region];
        --pinned_regions_;
      }
    }
  }
  active_claims_.erase(i);
  return true;
}

void CacheEntry::finish_fetch(const CacheFetchClaim& claim) noexcept {
  std::lock_guard guard(mutex_);
  const auto active = std::find_if(active_claims_.begin(), active_claims_.end(),
      [&](const ActiveFetch& item) {
        return item.id == claim.id && item.epoch == claim.epoch &&
            item.offset == claim.offset && item.length == claim.length;
      });
  if (active != active_claims_.end()) {
    for (uint64_t block = claim.offset + active->published;
         block < claim.offset + claim.length; block += block_size_) {
      const size_t first = block_first_page(block);
      const size_t last  = block_last_page(block);
      bool incomplete = false;
      for (size_t page = first; page < last; ++page) {
        if (page_state(page) == CACHE_PAGE_READ_PENDING) {
          set_page_state(page, CACHE_PAGE_MISSING);
          incomplete = true;
        }
      }
      if (incomplete) owner_->punch_range(data_fd_, block, block_size_);
      if (block > UINT64_MAX - block_size_) break;
    }
    end_fetch_locked(claim);
  }
  notify_waiters_locked();
}

void CacheEntry::rollback_fetch(const CacheFetchClaim& claim) noexcept {
  std::lock_guard guard(mutex_);
  const auto i = std::find_if(active_claims_.begin(), active_claims_.end(),
      [&](const ActiveFetch& active) {
        return active.id == claim.id && active.epoch == claim.epoch &&
            active.offset == claim.offset && active.length == claim.length;
      });
  if (i == active_claims_.end()) return;
  const size_t first = block_first_page(claim.offset);
  for (size_t index = (i->published + page_size_ - 1) / page_size_;
       index < i->prior_states.size(); ++index) {
    set_page_state(first + index, i->prior_states[index]);
  }
  end_fetch_locked(claim);
  notify_waiters_locked();
}

void CacheEntry::fail_fetch(const CacheFetchClaim& claim) noexcept {
  finish_fetch(claim);
}

void CacheEntry::mark_bad(const CacheFetchClaim& claim) noexcept {
  std::lock_guard guard(mutex_);
  const auto active = std::find_if(active_claims_.begin(), active_claims_.end(),
      [&](const ActiveFetch& item) {
        return item.id == claim.id && item.epoch == claim.epoch &&
            item.offset == claim.offset && item.length == claim.length;
      });
  if (!stale_ && active != active_claims_.end()) {
    const size_t first = size_t(claim.offset / page_size_) +
        (active->verifying ? 0 : (active->published + page_size_ - 1) / page_size_);
    const size_t last = std::min(
        page_count_, size_t((claim.offset + claim.length - 1) / page_size_) + 1);
    for (size_t page = first; page < last; ++page) {
      set_page_state(page, CACHE_PAGE_BAD);
    }
  }
  notify_waiters_locked();
}

bool CacheEntry::pin_fetch_verification(const CacheFetchClaim& claim) {
  std::lock_guard guard(mutex_);
  const auto active = std::find_if(active_claims_.begin(), active_claims_.end(),
      [&](const ActiveFetch& item) {
        return item.id == claim.id && item.epoch == claim.epoch &&
            item.offset == claim.offset && item.length == claim.length;
      });
  if (stale_ || active == active_claims_.end()) return false;
  if (active->verifying) return true;
  if (active->published != claim.length || checksum_ops_ != 0 ||
      checksum_manifest_ == kChecksumAvailable ||
      !range_all_state(claim.offset, claim.length, CACHE_PAGE_CLEAN)) return false;
  const uint64_t end = claim.offset + claim.length;
  for (const auto& item : active_claims_) {
    if (&item != &*active && item.offset < end &&
        item.offset + item.length > claim.offset) return false;
  }
  const size_t first = size_t(claim.offset / block_size_);
  const size_t last  = size_t((end - 1) / block_size_) + 1;
  for (size_t region = first; region < last; ++region) {
    if (region_pins_[region] == UINT32_MAX) {
      throw std::overflow_error("cache verification pin count overflow");
    }
  }
  for (size_t region = first; region < last; ++region) ++region_pins_[region];
  pinned_regions_ += last - first;
  active->verifying = true;
  return true;
}

bool CacheEntry::begin_retry_locked(const CacheFetchClaim& claim) {
  const auto active = std::find_if(active_claims_.begin(), active_claims_.end(),
      [&](const ActiveFetch& item) {
        return item.id == claim.id && item.epoch == claim.epoch &&
            item.offset == claim.offset && item.length == claim.length;
      });
  if (stale_ || active == active_claims_.end()) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "retry retired cache fetch");
  }
  if (!active->verifying) throw std::logic_error("cache retry without verification pin");
  if (!active->retrying) {
    const size_t first = size_t(claim.offset / page_size_);
    const size_t last = std::min(
        page_count_, size_t((claim.offset + claim.length - 1) / page_size_) + 1);
    for (size_t page = first; page < last; ++page) {
      set_page_state(page, CACHE_PAGE_READ_PENDING);
    }
    active->published = 0;
    active->retrying = true;
    notify_waiters_locked();
  }
  return retry_pins_ready_locked(claim.offset, claim.length);
}

void CacheEntry::begin_retry(const CacheFetchClaim& claim) {
  std::unique_lock guard(mutex_);
  while (!begin_retry_locked(claim)) wait_locked(guard);
}

int CacheEntry::begin_retry_wait(const CacheFetchClaim& claim) {
  std::lock_guard guard(mutex_);
  if (begin_retry_locked(claim)) return -1;
  if (wait_fd_ < 0) {
    wait_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE);
    if (wait_fd_ < 0) cache_throw_errno("create cache retry wait event");
  }
  ++waiters_;
  return wait_fd_;
}

void CacheEntry::finish_retry(const CacheFetchClaim& claim,
                              bool valid) noexcept {
  if (valid) {
    publish_clean(claim, 0, claim.length, true);
  } else {
    mark_bad(claim);
  }
  finish_fetch(claim);
}

bool CacheEntry::begin_checksum_manifest(bool wait) {
  std::unique_lock guard(mutex_);
  while (!stale_ && checksum_manifest_ == kChecksumLoading) {
    if (!wait) return false;
    wait_locked(guard);
  }
  if (stale_) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "stale cache generation");
  }
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  if ((header.flags & kCacheMetaExported) != 0) {
    if (checksum_manifest_ == kChecksumUnknown) {
      checksum_manifest_ = kChecksumUnavailable;
      fprintf(stderr,
              "warning: read checksum unavailable for immutable exported "
              "cache generation: key=%s; using best-effort ordinary reads "
              "without verification\n", key_.c_str());
    }
    return false;
  }
  if (checksum_manifest_ != kChecksumUnknown) {
    return false;
  }
  // A whole-object fallback verifier owns its data until verification/retry
  // finishes. Do not admit overlapping part verifiers through a new manifest.
  // Keep UNKNOWN so a later read can still load the manifest.
  if (std::any_of(active_claims_.begin(), active_claims_.end(),
                  [](const ActiveFetch& fetch) { return fetch.verifying; })) {
    return false;
  }
  checksum_manifest_ = kChecksumLoading;
  ++checksum_ops_;
  return true;
}

int CacheEntry::begin_manifest_wait() {
  std::lock_guard guard(mutex_);
  if (stale_ || checksum_manifest_ != kChecksumLoading) return -1;
  if (wait_fd_ < 0) {
    wait_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE);
    if (wait_fd_ < 0) {
      throw std::system_error(errno, std::generic_category(), "create manifest wait event");
    }
  }
  ++waiters_;
  return wait_fd_;
}

void CacheEntry::finish_checksum_manifest(
    std::vector<CacheChecksumPart> parts) {
  std::vector<uint8_t> states(parts.size(), kPartUnverified);
  std::lock_guard guard(mutex_);
  bool valid = !stale_ && !parts.empty();
  uint64_t offset = 0;
  for (const CacheChecksumPart& part : parts) {
    if (part.offset != offset || part.size == 0 ||
        offset > size_ || part.size > size_ - offset ||
        part.value.empty()) {
      valid = false;
      break;
    }
    offset += part.size;
  }
  valid = valid && offset == size_;
  if (valid) {
    checksum_parts_  = std::move(parts);
    checksum_states_ = std::move(states);
    checksum_manifest_ = kChecksumAvailable;
  } else {
    checksum_parts_.clear();
    checksum_states_.clear();
    checksum_manifest_ = kChecksumUnavailable;
  }
  if (checksum_ops_ != 0) {
    --checksum_ops_;
  }
  notify_waiters_locked();
}

void CacheEntry::checksum_manifest_unavailable() noexcept {
  std::lock_guard guard(mutex_);
  checksum_parts_.clear();
  checksum_states_.clear();
  checksum_manifest_ = kChecksumUnavailable;
  if (checksum_ops_ != 0) {
    --checksum_ops_;
  }
  notify_waiters_locked();
}

bool CacheEntry::checksum_manifest_available() const noexcept {
  std::lock_guard guard(mutex_);
  return !stale_ && checksum_manifest_ == kChecksumAvailable;
}

CacheChecksumClaim CacheEntry::claim_checksum(uint64_t offset,
                                               size_t length,
                                               bool skip_in_progress) {
  std::lock_guard guard(mutex_);
  if (stale_) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "stale cache generation");
  }
  if (checksum_manifest_ != kChecksumAvailable || length == 0) {
    return {};
  }
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  if ((header.flags & kCacheMetaExported) != 0) return {};
  if (offset >= size_) {
    return {};
  }
  const uint64_t end = offset + std::min<uint64_t>(length, size_ - offset);
  for (size_t i = 0; i < checksum_parts_.size(); ++i) {
    const CacheChecksumPart& part = checksum_parts_[i];
    const uint64_t part_end = part.offset + part.size;
    if (part.offset >= end || part_end <= offset) {
      continue;
    }
    const uint8_t state = checksum_states_[i];
    if (state == kPartBad) {
      CacheChecksumClaim claim;
      claim.action = CACHE_CHECKSUM_BAD;
      return claim;
    }
    if (state == kPartVerifying || state == kPartRetrying) {
      if (skip_in_progress) continue;
      CacheChecksumClaim claim;
      claim.action = CACHE_CHECKSUM_WAIT;
      return claim;
    }
    if (state != kPartUnverified ||
        !range_all_state(part.offset, size_t(part.size),
                         CACHE_PAGE_CLEAN)) {
      continue;
    }
    const size_t region_size = block_size_;
    const size_t first = size_t(part.offset / region_size);
    const size_t last = std::min(
        region_pins_.size(), size_t((part_end - 1) / region_size) + 1);
    bool pin_overflow = false;
    for (size_t region = first; region < last; ++region) {
      if (region_pins_[region] == UINT32_MAX) {
        pin_overflow = true;
        break;
      }
    }
    if (pin_overflow) {
      throw std::overflow_error("cache region pin count overflow");
    }
    CacheChecksumClaim claim{
        .action    = CACHE_CHECKSUM_VERIFY,
        .offset    = part.offset,
        .size      = part.size,
        .epoch     = epoch_,
        .part      = i,
        .algorithm = part.algorithm,
        .value     = part.value,
    };
    for (size_t region = first; region < last; ++region) {
      ++region_pins_[region];
    }
    pinned_regions_ += last - first;
    checksum_states_[i] = kPartVerifying;
    ++checksum_ops_;
    return claim;
  }
  return {};
}

void CacheEntry::wait_for_checksum(uint64_t offset, size_t length) {
  std::unique_lock guard(mutex_);
  const auto ready = [&] {
    if (stale_ || checksum_manifest_ != kChecksumAvailable || length == 0 ||
        offset >= size_) {
      return true;
    }
    const uint64_t end = offset + std::min<uint64_t>(length, size_ - offset);
    for (size_t i = 0; i < checksum_parts_.size(); ++i) {
      const CacheChecksumPart& part = checksum_parts_[i];
      if (part.offset < end && part.offset + part.size > offset &&
          (checksum_states_[i] == kPartVerifying ||
           checksum_states_[i] == kPartRetrying)) {
        return false;
      }
    }
    return true;
  };
  while (!ready()) {
    wait_locked(guard);
  }
  if (stale_) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "stale cache generation");
  }
}

void CacheEntry::checksum_mismatch(
    const CacheChecksumClaim& claim) noexcept {
  std::lock_guard guard(mutex_);
  if (!stale_ && claim.epoch == epoch_ &&
      claim.part < checksum_states_.size() &&
      checksum_states_[claim.part] == kPartVerifying) {
    checksum_states_[claim.part] = kPartRetrying;
  }
  notify_waiters_locked();
}

bool CacheEntry::retry_pins_ready_locked(uint64_t offset,
                                         size_t length) const noexcept {
  const size_t first = size_t(offset / block_size_);
  const size_t last = size_t((offset + length - 1) / block_size_) + 1;
  for (size_t region = first; region < last; ++region) {
    const uint64_t begin = uint64_t(region) * block_size_;
    const uint64_t end   = std::min<uint64_t>(size_, begin + block_size_);
    size_t verification_pins = 0;
    for (size_t i = 0; i < checksum_parts_.size(); ++i) {
      const CacheChecksumPart& part = checksum_parts_[i];
      if ((checksum_states_[i] == kPartVerifying ||
           checksum_states_[i] == kPartRetrying) &&
          part.offset < end && part.offset + part.size > begin) {
        ++verification_pins;
      }
    }
    for (const ActiveFetch& fetch : active_claims_) {
      if (fetch.verifying && fetch.offset < end &&
          fetch.offset + fetch.length > begin) ++verification_pins;
    }
    // Whole-object verification and part verification are mutually exclusive
    // at admission. Disjoint parts can share an eviction block and must not
    // wait for each other's verification pins, only for admitted replies.
    if (region_pins_[region] > verification_pins) return false;
  }
  return true;
}

bool CacheEntry::begin_checksum_retry_locked(const CacheChecksumClaim& claim) {
  if (stale_ || claim.epoch != epoch_ || claim.part >= checksum_states_.size() ||
      (checksum_states_[claim.part] != kPartVerifying &&
       checksum_states_[claim.part] != kPartRetrying)) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "retry retired cache checksum");
  }
  if (checksum_states_[claim.part] == kPartVerifying) {
    checksum_states_[claim.part] = kPartRetrying;
    notify_waiters_locked();
  }
  return retry_pins_ready_locked(claim.offset, size_t(claim.size));
}

void CacheEntry::wait_for_checksum_retry(const CacheChecksumClaim& claim) {
  std::unique_lock guard(mutex_);
  while (!begin_checksum_retry_locked(claim)) wait_locked(guard);
}

int CacheEntry::begin_checksum_retry_wait(const CacheChecksumClaim& claim) {
  std::lock_guard guard(mutex_);
  if (begin_checksum_retry_locked(claim)) return -1;
  if (wait_fd_ < 0) {
    wait_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE);
    if (wait_fd_ < 0) cache_throw_errno("create checksum retry wait event");
  }
  ++waiters_;
  return wait_fd_;
}

void CacheEntry::finish_checksum(const CacheChecksumClaim& claim,
                                 bool valid) noexcept {
  std::lock_guard guard(mutex_);
  if (claim.part >= checksum_states_.size() ||
      claim.epoch != epoch_) {
    return;
  }
  const uint8_t state = checksum_states_[claim.part];
  if (state != kPartVerifying && state != kPartRetrying) {
    return;
  }
  checksum_states_[claim.part] = valid ? kPartVerified : kPartBad;
  const uint64_t end = claim.offset + claim.size;
  const size_t region_size = block_size_;
  const size_t first = size_t(claim.offset / region_size);
  const size_t last = std::min(
      region_pins_.size(), size_t((end - 1) / region_size) + 1);
  for (size_t region = first; region < last; ++region) {
    if (region_pins_[region] != 0) {
      --region_pins_[region];
      --pinned_regions_;
    }
  }
  if (checksum_ops_ != 0) {
    --checksum_ops_;
  }
  notify_waiters_locked();
}

void CacheEntry::abandon_checksum(const CacheChecksumClaim& claim) noexcept {
  std::lock_guard guard(mutex_);
  if (claim.part >= checksum_states_.size() || claim.epoch != epoch_) {
    return;
  }
  const uint8_t state = checksum_states_[claim.part];
  if (state != kPartVerifying && state != kPartRetrying) {
    return;
  }
  checksum_states_[claim.part] = kPartUnverified;
  const uint64_t end = std::min<uint64_t>(size_, claim.offset + claim.size);
  const size_t first_page = size_t(claim.offset / page_size_);
  const size_t last_page = std::min(
      page_count_, size_t((end - 1) / page_size_) + 1);
  for (size_t page = first_page; page < last_page; ++page) {
    if (page_state(page) == CACHE_PAGE_CLEAN) {
      set_page_state(page, CACHE_PAGE_MISSING);
    }
  }
  const size_t region_size = block_size_;
  const size_t first_region = size_t(claim.offset / region_size);
  const size_t last_region = std::min(
      region_pins_.size(), size_t((end - 1) / region_size) + 1);
  for (size_t region = first_region; region < last_region; ++region) {
    if (region_pins_[region] != 0) {
      --region_pins_[region];
      --pinned_regions_;
    }
  }
  if (checksum_ops_ != 0) --checksum_ops_;
  notify_waiters_locked();
}

CacheRetirementPending::CacheRetirementPending()
    : std::system_error(EAGAIN, std::generic_category(), "cache generation retirement pending") {}

void CacheEntry::retire_generation(bool wait) {
  std::unique_lock guard(mutex_);
  if (!stale_) {
    stale_ = true;
    notify_waiters_locked();
  }
  while (!active_claims_.empty() || checksum_ops_ != 0 ||
         pinned_regions_ != 0) {
    if (!wait || io_executor() != nullptr) {
      throw CacheRetirementPending();
    }
    wait_locked(guard);
  }
}

int CacheEntry::begin_retire_wait() {
  std::lock_guard guard(mutex_);
  if (!stale_) throw std::logic_error("cache retirement wait without retirement");
  if (active_claims_.empty() && checksum_ops_ == 0 && pinned_regions_ == 0) return -1;
  if (wait_fd_ < 0) {
    wait_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE);
    if (wait_fd_ < 0) cache_throw_errno("create cache retirement event");
  }
  ++waiters_;
  return wait_fd_;
}

uint64_t CacheEntry::evict_one_region() noexcept {
  try {
    std::unique_lock guard(mutex_);
    const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
    if (owner_->config().unlimited || stale_ || eviction_disabled_ ||
        (header.flags & (kCacheMetaDirty | kCacheMetaExported)) != 0 ||
        referenced_.empty()) {
      return 0;
    }
    const size_t region_size = block_size_;
    for (size_t scan = 0; scan < referenced_.size(); ++scan) {
      const size_t region = clock_hand_++ % referenced_.size();
      if (referenced_[region] != 0) {
        referenced_[region] = 0;
        continue;
      }
      const uint64_t offset = uint64_t(region) * region_size;
      const uint64_t end = std::min<uint64_t>(size_, offset + region_size);
      const size_t first = size_t(offset / page_size_);
      const size_t last = size_t((end - 1) / page_size_) + 1;
      bool evictable = true;
      std::vector<size_t> clean_pages;
      clean_pages.reserve(last - first);
      for (size_t page = first; page < last; ++page) {
        const CachePageState state = page_state(page);
        if (state == CACHE_PAGE_CLEAN) {
          clean_pages.push_back(page);
        }
        if (state != CACHE_PAGE_CLEAN && state != CACHE_PAGE_MISSING) {
          evictable = false;
          break;
        }
      }
      if (region_pins_[region] != 0) {
        evictable = false;
      }
      if (!evictable || clean_pages.empty()) {
        continue;
      }

      struct stat before{};
      if (::fstat(data_fd_, &before) != 0) {
        return 0;
      }
      for (size_t page : clean_pages) {
        set_page_state(page, CACHE_PAGE_MISSING);
      }
      if (::fallocate(data_fd_,
                      FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                      off_t(offset), off_t(region_size)) != 0) {
        for (size_t page : clean_pages) {
          set_page_state(page, CACHE_PAGE_CLEAN);
        }
        return 0;
      }
      struct stat after{};
      if (::fstat(data_fd_, &after) != 0) {
        return 0;
      }
      notify_waiters_locked();
      const uint64_t old_bytes = uint64_t(before.st_blocks) * 512;
      const uint64_t new_bytes = uint64_t(after.st_blocks) * 512;
      return old_bytes > new_bytes ? old_bytes - new_bytes : 0;
    }
  } catch (...) {
  }
  return 0;
}

uint64_t LocalCache::allocated_bytes() const noexcept {
  if (superblock_mapping_ == nullptr) {
    return 0;
  }
  CacheRootHeader& header =
      *static_cast<CacheRootHeader*>(superblock_mapping_);
  std::atomic_ref<uint64_t> allocated(header.allocated_bytes);
  return allocated.load(std::memory_order_relaxed);
}

void LocalCache::add_allocated(int64_t delta) noexcept {
  if (superblock_mapping_ == nullptr || delta == 0) {
    return;
  }
  CacheRootHeader& header =
      *static_cast<CacheRootHeader*>(superblock_mapping_);
  std::atomic_ref<uint64_t> allocated(header.allocated_bytes);
  uint64_t current = allocated.load(std::memory_order_relaxed);
  for (;;) {
    uint64_t next;
    if (delta > 0) {
      const uint64_t increase = uint64_t(delta);
      next = current > UINT64_MAX - increase
          ? UINT64_MAX : current + increase;
    } else {
      const uint64_t decrease = uint64_t(-(delta + 1)) + 1;
      next = current > decrease ? current - decrease : 0;
    }
    if (allocated.compare_exchange_weak(
            current, next, std::memory_order_relaxed,
            std::memory_order_relaxed)) {
      return;
    }
  }
}

uint64_t LocalCache::reserve_floor_bytes() const {
  if (!config_.reserve_is_percent) {
    return config_.reserve_bytes;
  }
  struct statvfs fs{};
  if (::fstatvfs(root_fd_, &fs) != 0) {
    cache_throw_errno("fstatvfs(cache reserve)");
  }
  const uint64_t unit = fs.f_frsize == 0 ? fs.f_bsize : fs.f_frsize;
  const __uint128_t total = __uint128_t(fs.f_blocks) * unit;
  return uint64_t(total * config_.reserve_percent / 100);
}

bool LocalCache::try_reserve_capacity(uint64_t bytes) {
  if (config_.unlimited) {
    std::lock_guard guard(capacity_mutex_);
    pending_reservations_ = pending_reservations_ > UINT64_MAX - bytes
        ? UINT64_MAX : pending_reservations_ + bytes;
    return true;
  }
  std::lock_guard guard(capacity_mutex_);
  struct statvfs fs{};
  if (::fstatvfs(root_fd_, &fs) != 0) {
    cache_throw_errno("fstatvfs(cache capacity)");
  }
  const uint64_t unit = fs.f_frsize == 0 ? fs.f_bsize : fs.f_frsize;
  const __uint128_t available_wide = __uint128_t(fs.f_bavail) * unit;
  const uint64_t available = available_wide > UINT64_MAX
      ? UINT64_MAX : uint64_t(available_wide);
  const uint64_t floor = config_.reserve_is_percent
      ? uint64_t(__uint128_t(fs.f_blocks) * unit *
                 config_.reserve_percent / 100)
      : config_.reserve_bytes;
  const uint64_t allocated = allocated_bytes();
  const bool maximum_ok = config_.maximum_bytes == 0 ||
      (allocated <= config_.maximum_bytes &&
       pending_reservations_ <= config_.maximum_bytes - allocated &&
       bytes <= config_.maximum_bytes - allocated - pending_reservations_);
  const bool free_ok = available >= floor &&
      pending_reservations_ <= available - floor &&
      bytes <= available - floor - pending_reservations_;
  if (!maximum_ok || !free_ok) {
    return false;
  }
  pending_reservations_ += bytes;
  return true;
}

bool LocalCache::reserve_capacity(uint64_t bytes) {
  for (;;) {
    if (try_reserve_capacity(bytes)) {
      return true;
    }
    if (!evict_one()) {
      return false;
    }
  }
}

void LocalCache::cancel_reservation(uint64_t bytes) noexcept {
  std::lock_guard guard(capacity_mutex_);
  pending_reservations_ = pending_reservations_ > bytes
      ? pending_reservations_ - bytes : 0;
}

void LocalCache::finish_reservation(uint64_t reserved,
                                    uint64_t allocated) noexcept {
  std::lock_guard guard(capacity_mutex_);
  pending_reservations_ = pending_reservations_ > reserved
      ? pending_reservations_ - reserved : 0;
  add_allocated(int64_t(std::min<uint64_t>(allocated, INT64_MAX)));
}

std::recursive_mutex& LocalCache::key_mutex(std::string_view key) noexcept {
  uint64_t hash = 1469598103934665603ULL;
  for (char ch : key) {
    hash ^= u_char(ch);
    hash *= 1099511628211ULL;
  }
  return key_mutexes_[hash % key_mutexes_.size()];
}

void LocalCache::release_keepalive_locked(
    CacheEntry* entry,
    std::vector<std::shared_ptr<CacheEntry>>& released) {
  for (auto i = keepalive_.begin(); i != keepalive_.end(); ++i) {
    if (i->entry.get() != entry) continue;
    released.push_back(i->entry);
    keepalive_base_metadata_bytes_ -= i->base_metadata_bytes;
    i->entry.reset();
    keepalive_.erase(i);
    return;
  }
}

void LocalCache::release_key_keepalive_locked(
    std::string_view key,
    std::vector<std::shared_ptr<CacheEntry>>& released) {
  for (auto i = keepalive_.begin(); i != keepalive_.end();) {
    if (i->entry->key_ != key) {
      ++i;
      continue;
    }
    released.push_back(i->entry);
    keepalive_base_metadata_bytes_ -= i->base_metadata_bytes;
    i->entry.reset();
    i = keepalive_.erase(i);
  }
}

void LocalCache::retain_entry_locked(
    const std::shared_ptr<CacheEntry>& entry,
    std::vector<std::shared_ptr<CacheEntry>>& released) {
  for (const KeepaliveSlot& slot : keepalive_) {
    if (slot.entry.get() == entry.get()) return;
  }
  const size_t referenced_bytes =
      entry->referenced_.capacity() * sizeof(entry->referenced_[0]);
  const size_t pin_bytes =
      entry->region_pins_.capacity() * sizeof(entry->region_pins_[0]);
  if (entry->mapping_size_ > kKeepaliveBaseMetadataLimit ||
      referenced_bytes > kKeepaliveBaseMetadataLimit - entry->mapping_size_ ||
      pin_bytes > kKeepaliveBaseMetadataLimit - entry->mapping_size_ -
          referenced_bytes) {
    return;
  }
  const size_t base_metadata_bytes =
      entry->mapping_size_ + referenced_bytes + pin_bytes;
  while (!keepalive_.empty() &&
         (keepalive_.size() >= kKeepaliveEntryLimit ||
          keepalive_base_metadata_bytes_ >
              kKeepaliveBaseMetadataLimit - base_metadata_bytes)) {
    released.push_back(keepalive_.front().entry);
    keepalive_base_metadata_bytes_ -= keepalive_.front().base_metadata_bytes;
    keepalive_.front().entry.reset();
    keepalive_.erase(keepalive_.begin());
  }
  keepalive_.push_back(KeepaliveSlot{
      .entry = entry,
      .base_metadata_bytes = base_metadata_bytes,
  });
  keepalive_base_metadata_bytes_ += base_metadata_bytes;
}

bool LocalCache::reclaim_closed_clean(
    const std::shared_ptr<CacheEntry>& entry) noexcept {
  std::vector<std::shared_ptr<CacheEntry>> released;
  try {
    std::recursive_mutex* selected_key_mutex;
    {
      std::lock_guard guard(mutex_);
      selected_key_mutex = &key_mutex(entry->key_);
    }
    std::unique_lock key_guard(*selected_key_mutex, std::try_to_lock);
    if (!key_guard.owns_lock()) return false;
    std::unique_lock guard(mutex_);
    if (&key_mutex(entry->key_) != selected_key_mutex) return false;
    auto found = entries_.end();
    for (auto i = entries_.begin(); i != entries_.end();) {
      std::shared_ptr<CacheEntry> candidate = i->lock();
      if (!candidate) {
        i = entries_.erase(i);
        continue;
      }
      if (candidate.get() == entry.get()) {
        const bool kept = std::any_of(
            keepalive_.begin(), keepalive_.end(), [&](const auto& slot) {
              return slot.entry.get() == entry.get();
            });
        if (candidate.use_count() != (kept ? 3 : 2)) {
          return false;
        }
        found = i;
        break;
      }
      ++i;
    }
    if (found == entries_.end()) {
      return false;
    }
    {
      std::lock_guard entry_guard(entry->mutex_);
      const auto& header =
          *static_cast<const CacheMetaHeader*>(entry->mapping_);
      if (entry->stale_ || entry->detached_ || entry->eviction_disabled_ ||
          (header.flags & (kCacheMetaDirty | kCacheMetaExported)) != 0 ||
          !entry->active_claims_.empty() || entry->checksum_ops_ != 0 ||
          entry->pinned_regions_ != 0) {
        return false;
      }
      for (size_t page = 0; page < entry->page_count_; ++page) {
        if (entry->page_state(page) != CACHE_PAGE_MISSING) {
          return false;
        }
      }
      entry->detached_ = true;
      entry->stale_    = true;
    }
    release_keepalive_locked(entry.get(), released);
    entries_.erase(found);
    guard.unlock();

    struct statvfs fs{};
    if (::fstatvfs(root_fd_, &fs) != 0) {
      cache_throw_errno("fstatvfs(cache cold reclamation)");
    }
    const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
    std::optional<CacheLeaf> data = cache_find_leaf(
        data_root_fd_, entry->key_, name_max);
    std::optional<CacheLeaf> meta = cache_find_leaf(
        objects_root_fd_, entry->key_, name_max);
    if (!data || !meta) {
      return false;
    }
    struct stat data_path{};
    struct stat meta_path{};
    struct stat data_fd{};
    struct stat meta_fd{};
    if (::fstatat(data->parent.get(), data->name.c_str(), &data_path,
                  AT_SYMLINK_NOFOLLOW) != 0 ||
        ::fstatat(meta->parent.get(), meta->name.c_str(), &meta_path,
                  AT_SYMLINK_NOFOLLOW) != 0 ||
        ::fstat(entry->data_fd_, &data_fd) != 0 ||
        ::fstat(entry->meta_fd_, &meta_fd) != 0 ||
        data_path.st_dev != data_fd.st_dev ||
        data_path.st_ino != data_fd.st_ino ||
        meta_path.st_dev != meta_fd.st_dev ||
        meta_path.st_ino != meta_fd.st_ino) {
      return false;
    }
    if (::unlinkat(data->parent.get(), data->name.c_str(), 0) != 0) {
      cache_throw_errno("unlinkat(cache cold data)");
    }
    entry->unlinked_data_ = true;
    if (::unlinkat(meta->parent.get(), meta->name.c_str(), 0) != 0) {
      cache_throw_errno("unlinkat(cache cold metadata)");
    }
    entry->unlinked_meta_ = true;
    return true;
  } catch (const std::exception& error) {
    if (!cold_scan_warned_.exchange(true, std::memory_order_relaxed)) {
      fprintf(stderr, "warning: local cache cold reclamation failed: %s\n",
              error.what());
    }
  } catch (...) {
    if (!cold_scan_warned_.exchange(true, std::memory_order_relaxed)) {
      fprintf(stderr, "warning: local cache cold reclamation failed\n");
    }
  }
  return false;
}

bool LocalCache::evict_cold() {
  if (config_.unlimited) return false;
  try {
    struct statvfs fs{};
    if (::fstatvfs(root_fd_, &fs) != 0) {
      cache_throw_errno("fstatvfs(cache cold eviction)");
    }
    const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
    return cache_visit_clean_directory(
        objects_root_fd_, data_root_fd_, config_, name_max,
        [this](CacheCleanRecord&& record) {
          try {
            // A caller reserving space can already own another key lock.
            // Cold eviction must never wait for a second key in that order.
            std::unique_lock key_guard(key_mutex(record.key), std::try_to_lock);
            if (!key_guard.owns_lock()) return false;
            {
              std::lock_guard guard(mutex_);
              for (const auto& weak : entries_) {
                const auto entry = weak.lock();
                // Live entries were already examined by evict_one(). Do not
                // reopen/retire a generation from a stale on-disk scan record.
                if (entry && entry->key_ == record.key) return false;
              }
            }
            CacheIdentity identity{
                .key        = record.key,
                .etag       = record.etag,
                .version_id = record.version_id,
                .size       = record.size,
                .mtime      = record.mtime,
            };
            std::shared_ptr<CacheEntry> entry = open(identity);
            if (!entry) {
              return false;
            }
            for (unsigned pass = 0; pass != 2; ++pass) {
              const uint64_t freed = entry->evict_one_region();
              if (freed != 0) {
                add_allocated(-int64_t(
                    std::min<uint64_t>(freed, INT64_MAX)));
                return true;
              }
            }
            return reclaim_closed_clean(entry);
          } catch (...) {
            return false;
          }
        });
  } catch (const std::exception& error) {
    if (!cold_scan_warned_.exchange(true, std::memory_order_relaxed)) {
      fprintf(stderr, "warning: local cache cold scan failed: %s\n",
              error.what());
    }
  } catch (...) {
    if (!cold_scan_warned_.exchange(true, std::memory_order_relaxed)) {
      fprintf(stderr, "warning: local cache cold scan failed\n");
    }
  }
  return false;
}

bool LocalCache::evict_one() {
  if (config_.unlimited) return false;
  std::vector<std::shared_ptr<CacheEntry>> entries;
  {
    std::lock_guard guard(mutex_);
    entries.reserve(entries_.size());
    for (auto i = entries_.begin(); i != entries_.end();) {
      std::shared_ptr<CacheEntry> entry = i->lock();
      if (!entry) {
        i = entries_.erase(i);
        continue;
      }
      entries.push_back(std::move(entry));
      ++i;
    }
  }
  if (!entries.empty()) {
    const size_t begin = clock_entry_.fetch_add(
        1, std::memory_order_relaxed) % entries.size();
    for (unsigned pass = 0; pass != 2; ++pass) {
      for (size_t n = 0; n < entries.size(); ++n) {
        const uint64_t freed =
            entries[(begin + n) % entries.size()]->evict_one_region();
        if (freed != 0) {
          add_allocated(-int64_t(std::min<uint64_t>(freed, INT64_MAX)));
          return true;
        }
      }
    }
    for (size_t n = 0; n < entries.size(); ++n) {
      if (reclaim_closed_clean(entries[(begin + n) % entries.size()])) {
        return true;
      }
    }
  }
  return evict_cold();
}

void LocalCache::punch_range(int fd, uint64_t offset,
                             uint64_t length) noexcept {
  if (length == 0) {
    return;
  }
  struct stat before{};
  if (::fstat(fd, &before) != 0 ||
      ::fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                  off_t(offset), off_t(length)) != 0) {
    return;
  }
  struct stat after{};
  if (::fstat(fd, &after) != 0) {
    return;
  }
  const uint64_t old_bytes = uint64_t(before.st_blocks) * 512;
  const uint64_t new_bytes = uint64_t(after.st_blocks) * 512;
  if (old_bytes > new_bytes) {
    add_allocated(-int64_t(std::min<uint64_t>(
        old_bytes - new_bytes, INT64_MAX)));
  }
}

bool LocalCache::prepare_range(CacheEntry& entry, uint64_t offset,
                               size_t length, bool write) {
  if (length == 0) {
    return true;
  }
  uint64_t reserve = cache_round_up(length, config_.page_size);
  uint64_t start   = offset;
  if (write) {
    const uint64_t unit = config_.upload_part_size;
    start   = offset / unit * unit;
    reserve = unit;
  } else {
    start = offset / config_.block_size * config_.block_size;
    reserve = cache_round_up(length, config_.block_size);
  }
  if (!reserve_capacity(reserve)) {
    return false;
  }

  struct stat before{};
  if (::fstat(entry.data_fd(), &before) != 0) {
    const int error = errno;
    cancel_reservation(reserve);
    throw std::system_error(error, std::generic_category(),
                            "fstat(cache allocation)");
  }
  if (::fallocate(entry.data_fd(), FALLOC_FL_KEEP_SIZE,
                  off_t(start), off_t(reserve)) != 0) {
    const int error = errno;
    cancel_reservation(reserve);
    if (!write && (error == ENOSPC || error == EDQUOT)) {
      return false;
    }
    throw std::system_error(error, std::generic_category(),
                            "fallocate(cache range)");
  }
  struct stat after{};
  if (::fstat(entry.data_fd(), &after) != 0) {
    const int error = errno;
    cancel_reservation(reserve);
    throw std::system_error(error, std::generic_category(),
                            "fstat(cache allocation result)");
  }
  const uint64_t old_bytes = uint64_t(before.st_blocks) * 512;
  const uint64_t new_bytes = uint64_t(after.st_blocks) * 512;
  finish_reservation(reserve,
                     new_bytes > old_bytes ? new_bytes - old_bytes : 0);
  return true;
}

LocalCache::LocalCache(CacheConfig config) : config_(std::move(config)) {
  if (config_.root.empty() || config_.page_size == 0 ||
      config_.block_size == 0 ||
      config_.block_size > 128U * 1024U * 1024U ||
      config_.block_size % kCacheBitmapUnit != 0 ||
      config_.max_prefetch_window_size < config_.page_size ||
      config_.max_prefetch_window_size % config_.page_size != 0) {
    throw std::invalid_argument("invalid local cache configuration");
  }
  std::filesystem::create_directories(config_.root);
  root_fd_ = ::open(config_.root.c_str(),
                    O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (root_fd_ < 0) {
    cache_throw_errno("open(cache root)");
  }
  try {
    cache_secure_directory(root_fd_, "fstat(cache root)");
    lock_fd_ = ::openat(root_fd_, ".ngs3fs.lock",
                        O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (lock_fd_ < 0) {
      cache_throw_errno("open(cache lock)");
    }
    if (::flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
      cache_throw_errno("flock(cache root)");
    }
    superblock_fd_ = ::openat(
        root_fd_, ".ngs3fs.superblock",
        O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (superblock_fd_ < 0) {
      cache_throw_errno("open(cache superblock)");
    }
    struct stat superblock_status{};
    if (::fstat(superblock_fd_, &superblock_status) != 0) {
      cache_throw_errno("fstat(cache superblock)");
    }
    const bool initialize_superblock = superblock_status.st_size == 0;
    if (initialize_superblock &&
        ::ftruncate(superblock_fd_, off_t(kCacheRootHeaderSize)) != 0) {
      cache_throw_errno("ftruncate(cache superblock)");
    }
    if (initialize_superblock &&
        ::fallocate(superblock_fd_, 0, 0,
                    off_t(kCacheRootHeaderSize)) != 0) {
      cache_throw_errno("fallocate(cache superblock)");
    }
    if (!initialize_superblock &&
        superblock_status.st_size != off_t(kCacheRootHeaderSize)) {
      throw std::runtime_error("incompatible cache superblock size");
    }
    superblock_mapping_ = ::mmap(
        nullptr, kCacheRootHeaderSize, PROT_READ | PROT_WRITE,
        MAP_SHARED, superblock_fd_, 0);
    if (superblock_mapping_ == MAP_FAILED) {
      superblock_mapping_ = nullptr;
      cache_throw_errno("mmap(cache superblock)");
    }
    CacheRootHeader& superblock =
        *static_cast<CacheRootHeader*>(superblock_mapping_);
    if (initialize_superblock) {
      cache_initialize_root(superblock, config_);
      if (::msync(superblock_mapping_, kCacheRootHeaderSize, MS_SYNC) != 0) {
        cache_throw_errno("msync(cache superblock)");
      }
    } else if (!cache_root_matches(superblock, config_)) {
      throw std::runtime_error(
          "cache root belongs to a different mount namespace or format");
    }
    UniqueFd data = cache_make_directory(root_fd_, "data");
    UniqueFd meta = cache_make_directory(root_fd_, "meta");
    UniqueFd objects = cache_make_directory(meta.get(), "objects");
    UniqueFd dirty = cache_make_directory(meta.get(), "dirty");
    UniqueFd pending = cache_make_directory(meta.get(), "pending-delete");
    data_root_fd_    = data.release();
    objects_root_fd_ = objects.release();
    dirty_root_fd_   = dirty.release();
    pending_root_fd_ = pending.release();
    probe_filesystem();
    std::atomic_ref<uint64_t> allocated(superblock.allocated_bytes);
    if (initialize_superblock) {
      allocated.store(cache_tree_allocation(root_fd_),
                      std::memory_order_relaxed);
    }
  } catch (...) {
    cache_close_fd(data_root_fd_);
    cache_close_fd(objects_root_fd_);
    cache_close_fd(dirty_root_fd_);
    cache_close_fd(pending_root_fd_);
    if (superblock_mapping_ != nullptr) {
      ::munmap(superblock_mapping_, kCacheRootHeaderSize);
      superblock_mapping_ = nullptr;
    }
    cache_close_fd(superblock_fd_);
    cache_close_fd(lock_fd_);
    cache_close_fd(root_fd_);
    throw;
  }
}

LocalCache::~LocalCache() {
  std::vector<KeepaliveSlot> released;
  {
    std::lock_guard guard(mutex_);
    released.swap(keepalive_);
    keepalive_base_metadata_bytes_ = 0;
    entries_.clear();
  }
  released.clear();
  cache_close_fd(data_root_fd_);
  cache_close_fd(objects_root_fd_);
  cache_close_fd(dirty_root_fd_);
  cache_close_fd(pending_root_fd_);
  if (superblock_mapping_ != nullptr) {
    ::munmap(superblock_mapping_, kCacheRootHeaderSize);
    superblock_mapping_ = nullptr;
  }
  cache_close_fd(superblock_fd_);
  cache_close_fd(lock_fd_);
  cache_close_fd(root_fd_);
}

void LocalCache::probe_filesystem() {
  struct statvfs filesystem{};
  if (::fstatvfs(root_fd_, &filesystem) != 0) {
    cache_throw_errno("fstatvfs(cache filesystem probe)");
  }
  name_max_ = filesystem.f_namemax == 0 ? NAME_MAX : filesystem.f_namemax;

  constexpr char upper[] = ".ngs3fs-case-A";
  constexpr char lower[] = ".ngs3fs-case-a";
  const int upper_fd = ::openat(root_fd_, upper,
                                O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (upper_fd < 0) {
    cache_throw_errno("create(cache case probe)");
  }
  ::close(upper_fd);
  struct stat status{};
  const bool folded = ::fstatat(root_fd_, lower, &status,
                                AT_SYMLINK_NOFOLLOW) == 0;
  ::unlinkat(root_fd_, upper, 0);
  if (folded) {
    throw std::runtime_error("cache filesystem is not case-sensitive");
  }

  constexpr char composed[]   = ".ngs3fs-unicode-\xc3\xa9";
  constexpr char decomposed[] = ".ngs3fs-unicode-e\xcc\x81";
  const int unicode_fd = ::openat(
      root_fd_, composed, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (unicode_fd < 0) {
    cache_throw_errno("create(cache Unicode probe)");
  }
  ::close(unicode_fd);
  const bool normalized = ::fstatat(root_fd_, decomposed, &status,
                                    AT_SYMLINK_NOFOLLOW) == 0;
  ::unlinkat(root_fd_, composed, 0);
  if (normalized) {
    throw std::runtime_error(
        "cache filesystem normalizes Unicode file names");
  }

  constexpr char allocation_probe[] = ".ngs3fs-fallocate-probe";
  const int fd = ::openat(root_fd_, allocation_probe,
                          O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    cache_throw_errno("create(cache fallocate probe)");
  }
  bool ok = true;
  int error = 0;
  if (::fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, off_t(config_.page_size)) != 0 ||
      ::fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                  0, off_t(config_.page_size)) != 0) {
    ok = false;
    error = errno;
  }
  ::close(fd);
  ::unlinkat(root_fd_, allocation_probe, 0);
  if (!ok) {
    throw std::system_error(error, std::generic_category(),
                            "cache filesystem lacks fallocate/punch-hole");
  }
}

int LocalCache::create_dirty_marker(std::string_view key, uint64_t epoch) {
  if (key.size() > kCacheKeyCapacity) {
    throw std::system_error(EOVERFLOW, std::generic_category(),
                            "cache dirty key is too large");
  }
  struct statvfs fs{};
  if (::fstatvfs(root_fd_, &fs) != 0) {
    cache_throw_errno("fstatvfs(cache dirty marker)");
  }
  const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
  CacheLeaf leaf = cache_leaf(dirty_root_fd_, key, name_max);
  if (!reserve_capacity(kCacheMetaHeaderSize)) {
    throw std::system_error(ENOSPC, std::generic_category(),
                            "reserve cache dirty marker");
  }
  const int fd = ::openat(
      leaf.parent.get(), leaf.name.c_str(),
      O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0) {
    cancel_reservation(kCacheMetaHeaderSize);
    cache_throw_errno("open(cache dirty marker)");
  }
  try {
    struct stat before{};
    if (::fstat(fd, &before) != 0) {
      cache_throw_errno("fstat(cache dirty marker)");
    }
    if (::fallocate(fd, 0, 0, off_t(kCacheMetaHeaderSize)) != 0) {
      cache_throw_errno("fallocate(cache dirty marker)");
    }
    CacheDirtyMarker marker{};
    marker.magic            = {'N', 'G', 'S', '3', 'D', 'I', 'R', 'T'};
    marker.version          = kCacheMetaVersion;
    marker.header_size      = kCacheMetaHeaderSize;
    marker.generation_epoch = epoch;
    marker.key_length       = uint32_t(key.size());
    memcpy(marker.key.data(), key.data(), key.size());
    size_t written = 0;
    const auto* bytes = reinterpret_cast<const char*>(&marker);
    while (written != sizeof(marker)) {
      const ssize_t result = ::pwrite(
          fd, bytes + written, sizeof(marker) - written, off_t(written));
      if (result > 0) {
        written += size_t(result);
      } else if (result < 0 && errno == EINTR) {
        continue;
      } else {
        cache_throw_errno("pwrite(cache dirty marker)");
      }
    }
    struct stat after{};
    if (::fstat(fd, &after) != 0) {
      cache_throw_errno("fstat(cache dirty marker result)");
    }
    const uint64_t old_bytes = uint64_t(before.st_blocks) * 512;
    const uint64_t new_bytes = uint64_t(after.st_blocks) * 512;
    finish_reservation(
        kCacheMetaHeaderSize,
        new_bytes > old_bytes ? new_bytes - old_bytes : 0);
    return fd;
  } catch (...) {
    cancel_reservation(kCacheMetaHeaderSize);
    ::close(fd);
    ::unlinkat(leaf.parent.get(), leaf.name.c_str(), 0);
    throw;
  }
}

void LocalCache::remove_dirty_marker(std::string_view key) noexcept {
  try {
    struct statvfs fs{};
    if (::fstatvfs(root_fd_, &fs) != 0) {
      return;
    }
    const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
    CacheLeaf leaf = cache_leaf(dirty_root_fd_, key, name_max);
    struct stat status{};
    const bool measured = ::fstatat(
        leaf.parent.get(), leaf.name.c_str(), &status,
        AT_SYMLINK_NOFOLLOW) == 0;
    if (::unlinkat(leaf.parent.get(), leaf.name.c_str(), 0) != 0 &&
        errno != ENOENT) {
      fprintf(stderr, "warning: cannot remove cache dirty marker: %s\n",
              strerror(errno));
    } else if (measured) {
      add_allocated(-int64_t(std::min<uint64_t>(
          uint64_t(status.st_blocks) * 512, INT64_MAX)));
    }
  } catch (...) {
    fprintf(stderr, "warning: cannot resolve cache dirty marker for removal\n");
  }
}

struct CacheDirtyRecord {
  std::string key;
  uint64_t epoch = 0;
  bool valid     = true;
};

void cache_scan_dirty_directory(int directory,
                                std::vector<CacheDirtyRecord>& records) {
  const int duplicate = ::openat(
      directory, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (duplicate < 0) {
    cache_throw_errno("open(cache dirty directory scan)");
  }
  DIR* stream = ::fdopendir(duplicate);
  if (stream == nullptr) {
    ::close(duplicate);
    cache_throw_errno("fdopendir(cache dirty directory)");
  }
  try {
    for (;;) {
      errno = 0;
      dirent* item = ::readdir(stream);
      if (item == nullptr) {
        if (errno != 0) {
          cache_throw_errno("readdir(cache dirty directory)");
        }
        break;
      }
      if (strcmp(item->d_name, ".") == 0 ||
          strcmp(item->d_name, "..") == 0) {
        continue;
      }
      struct stat status{};
      if (::fstatat(directory, item->d_name, &status,
                    AT_SYMLINK_NOFOLLOW) != 0) {
        cache_throw_errno("fstatat(cache dirty entry)");
      }
      if (S_ISDIR(status.st_mode)) {
        UniqueFd child = cache_open_directory(directory, item->d_name);
        cache_scan_dirty_directory(child.get(), records);
        continue;
      }
      if (!S_ISREG(status.st_mode)) {
        throw std::runtime_error("non-regular cache dirty marker");
      }
      UniqueFd fd(::openat(directory, item->d_name,
                           O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
      if (!fd) {
        cache_throw_errno("open(cache dirty marker scan)");
      }
      CacheDirtyMarker marker{};
      size_t read_bytes = 0;
      while (read_bytes != sizeof(marker)) {
        const ssize_t result = ::pread(
            fd.get(), reinterpret_cast<char*>(&marker) + read_bytes,
            sizeof(marker) - read_bytes, off_t(read_bytes));
        if (result > 0) {
          read_bytes += size_t(result);
        } else if (result < 0 && errno == EINTR) {
          continue;
        } else {
          throw std::runtime_error("short cache dirty marker");
        }
      }
      constexpr std::array<char, 8> magic{
          'N', 'G', 'S', '3', 'D', 'I', 'R', 'T'};
      const bool valid = marker.magic == magic &&
          marker.version == kCacheMetaVersion &&
          marker.header_size == kCacheMetaHeaderSize &&
          marker.generation_epoch != 0;
      if (marker.key_length == 0 || marker.key_length > marker.key.size()) {
        throw std::runtime_error(
            "invalid cache dirty marker without an identifiable key");
      }
      if (!valid) {
        fprintf(stderr,
                "error: isolating malformed cache dirty marker: key=%.*s\n",
                int(marker.key_length), marker.key.data());
      }
      records.push_back(CacheDirtyRecord{
          .key = std::string(marker.key.data(), marker.key_length),
          .epoch = marker.generation_epoch,
          .valid = valid,
      });
    }
  } catch (...) {
    ::closedir(stream);
    throw;
  }
  ::closedir(stream);
}

std::vector<std::shared_ptr<CacheEntry>> LocalCache::recover_dirty(
    std::vector<std::string>* isolated_keys) {
  std::vector<CacheDirtyRecord> records;
  cache_scan_dirty_directory(dirty_root_fd_, records);
  std::vector<std::shared_ptr<CacheEntry>> result;
  result.reserve(records.size());
  std::lock_guard guard(mutex_);
  struct statvfs fs{};
  if (::fstatvfs(root_fd_, &fs) != 0) {
    cache_throw_errno("fstatvfs(cache recovery)");
  }
  const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
  for (const CacheDirtyRecord& record : records) {
    if (!record.valid) {
      if (isolated_keys != nullptr) {
        isolated_keys->push_back(record.key);
      }
      continue;
    }
    CacheLeaf data_leaf = cache_leaf(data_root_fd_, record.key, name_max);
    CacheLeaf meta_leaf = cache_leaf(objects_root_fd_, record.key, name_max);
    CacheLeaf dirty_leaf = cache_leaf(dirty_root_fd_, record.key, name_max);
    int data_fd = -1;
    int meta_fd = -1;
    int dirty_fd = -1;
    void* mapping = MAP_FAILED;
    size_t mapping_size = 0;
    try {
      data_fd = ::openat(data_leaf.parent.get(), data_leaf.name.c_str(),
                         O_RDWR | O_CLOEXEC | O_NOFOLLOW);
      meta_fd = ::openat(meta_leaf.parent.get(), meta_leaf.name.c_str(),
                         O_RDWR | O_CLOEXEC | O_NOFOLLOW);
      dirty_fd = ::openat(dirty_leaf.parent.get(), dirty_leaf.name.c_str(),
                          O_RDWR | O_CLOEXEC | O_NOFOLLOW);
      if (data_fd < 0 || meta_fd < 0 || dirty_fd < 0) {
        cache_throw_errno("open(cache recovery files)");
      }
      advise_cache_data_file(data_fd);
      struct stat meta_status{};
      struct stat data_status{};
      if (::fstat(meta_fd, &meta_status) != 0 ||
          ::fstat(data_fd, &data_status) != 0 ||
          meta_status.st_size < off_t(kCacheMetaHeaderSize)) {
        cache_throw_errno("fstat(cache recovery files)");
      }
      mapping_size = size_t(meta_status.st_size);
      mapping = ::mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE,
                       MAP_SHARED, meta_fd, 0);
      if (mapping == MAP_FAILED) {
        cache_throw_errno("mmap(cache recovery metadata)");
      }
      const auto& header = *static_cast<const CacheMetaHeader*>(mapping);
      if (!cache_recovery_header_matches(
              header, record.key, record.epoch, config_, mapping_size) ||
          uint64_t(std::max<off_t>(data_status.st_size, 0)) <
              header.written_end) {
        throw std::runtime_error("invalid cache recovery metadata");
      }
      auto entry = std::shared_ptr<CacheEntry>(new CacheEntry(
          *this, record.key, data_fd, meta_fd, dirty_fd, mapping,
          mapping_size, header.written_end));
      entry->disable_eviction();
      const size_t units = header.written_end == 0 ? 0 : size_t(
          (header.written_end - 1) / config_.upload_part_size + 1);
      entry->reserved_units_.assign(units, 1);
      data_fd = -1;
      meta_fd = -1;
      dirty_fd = -1;
      mapping = MAP_FAILED;
      entries_.push_back(entry);
      result.push_back(std::move(entry));
    } catch (const std::exception& error) {
      fprintf(stderr,
              "error: isolating invalid cached write: key=%s: %s\n",
              record.key.c_str(), error.what());
      if (isolated_keys != nullptr) {
        isolated_keys->push_back(record.key);
      }
      if (mapping != MAP_FAILED) {
        ::munmap(mapping, mapping_size);
      }
      cache_close_fd(dirty_fd);
      cache_close_fd(meta_fd);
      cache_close_fd(data_fd);
    }
  }
  return result;
}

void LocalCache::create_pending_delete(
    std::string_view key, std::string_view restore_key,
    std::string_view replacement_etag) {
  if (key.empty() || key.size() > kCacheKeyCapacity ||
      restore_key.size() > kCacheKeyCapacity ||
      replacement_etag.size() > kCacheEtagCapacity) {
    throw std::system_error(EOVERFLOW, std::generic_category(),
                            "cache pending-delete key is too large");
  }
  struct statvfs fs{};
  if (::fstatvfs(root_fd_, &fs) != 0) {
    cache_throw_errno("fstatvfs(cache pending delete)");
  }
  const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
  CacheLeaf leaf = cache_leaf(pending_root_fd_, key, name_max);
  if (!reserve_capacity(kCacheMetaHeaderSize)) {
    throw std::system_error(ENOSPC, std::generic_category(),
                            "reserve cache pending-delete marker");
  }
  const int fd = ::openat(
      leaf.parent.get(), leaf.name.c_str(),
      O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0) {
    cancel_reservation(kCacheMetaHeaderSize);
    cache_throw_errno("open(cache pending-delete marker)");
  }
  bool reservation = true;
  try {
    if (::fallocate(fd, 0, 0, off_t(kCacheMetaHeaderSize)) != 0) {
      cache_throw_errno("fallocate(cache pending-delete marker)");
    }
    CachePendingDeleteMarker marker{};
    marker.magic        = {'N', 'G', 'S', '3', 'P', 'E', 'N', 'D'};
    marker.version      = kCacheMetaVersion;
    marker.header_size = kCacheMetaHeaderSize;
    marker.phase        = restore_key.empty() ? 2 : 1;
    marker.key_length   = uint32_t(key.size());
    marker.restore_key_length = uint32_t(restore_key.size());
    marker.replacement_etag_length = uint32_t(replacement_etag.size());
    memcpy(marker.key.data(), key.data(), key.size());
    if (!restore_key.empty()) {
      memcpy(marker.restore_key.data(), restore_key.data(),
             restore_key.size());
    }
    if (!replacement_etag.empty()) {
      memcpy(marker.replacement_etag.data(), replacement_etag.data(),
             replacement_etag.size());
    }
    size_t written = 0;
    while (written != sizeof(marker)) {
      const ssize_t result = ::pwrite(
          fd, reinterpret_cast<const char*>(&marker) + written,
          sizeof(marker) - written, off_t(written));
      if (result > 0) {
        written += size_t(result);
      } else if (result < 0 && errno == EINTR) {
        continue;
      } else {
        cache_throw_errno("pwrite(cache pending-delete marker)");
      }
    }
    struct stat status{};
    if (::fstat(fd, &status) != 0) {
      cache_throw_errno("fstat(cache pending-delete marker)");
    }
    finish_reservation(
        kCacheMetaHeaderSize, uint64_t(status.st_blocks) * 512);
    reservation = false;
    ::close(fd);
  } catch (...) {
    if (reservation) {
      cancel_reservation(kCacheMetaHeaderSize);
    }
    ::close(fd);
    ::unlinkat(leaf.parent.get(), leaf.name.c_str(), 0);
    throw;
  }
}

bool cache_start_marker_operation(
    IoExecutor& executor, CacheAsyncRequest& request, LocalCache& cache,
    CacheMarkerOperation::Kind kind, std::string_view key,
    std::string_view restore_key = {},
    std::string_view replacement_etag = {}) noexcept {
  CacheMarkerOperation* operation = nullptr;
  try {
    operation = new CacheMarkerOperation(executor, request, cache, kind);
    if (operation->start(key, restore_key, replacement_etag)) return true;
    delete operation;
    return false;
  } catch (...) {
    const std::exception_ptr error = std::current_exception();
    if (operation != nullptr) {
      operation->abandon_start();
      delete operation;
    }
    errno = cache_async_start_errno(error);
    return false;
  }
}

bool LocalCache::create_pending_delete_async(
    IoExecutor& executor, CacheAsyncRequest& request,
    std::string_view key, std::string_view restore_key,
    std::string_view replacement_etag) noexcept {
  return cache_start_marker_operation(
      executor, request, *this,
      CacheMarkerOperation::Kind::CREATE_PENDING,
      key, restore_key, replacement_etag);
}

void cache_scan_pending_directory(int directory,
                                  std::vector<CachePendingDelete>& records) {
  const int duplicate = ::openat(
      directory, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (duplicate < 0) {
    cache_throw_errno("open(cache pending-delete directory scan)");
  }
  DIR* stream = ::fdopendir(duplicate);
  if (stream == nullptr) {
    ::close(duplicate);
    cache_throw_errno("fdopendir(cache pending-delete directory)");
  }
  try {
    for (;;) {
      errno = 0;
      dirent* item = ::readdir(stream);
      if (item == nullptr) {
        if (errno != 0) {
          cache_throw_errno("readdir(cache pending-delete directory)");
        }
        break;
      }
      if (strcmp(item->d_name, ".") == 0 ||
          strcmp(item->d_name, "..") == 0) {
        continue;
      }
      struct stat status{};
      if (::fstatat(directory, item->d_name, &status,
                    AT_SYMLINK_NOFOLLOW) != 0) {
        cache_throw_errno("fstatat(cache pending-delete entry)");
      }
      if (S_ISDIR(status.st_mode)) {
        UniqueFd child = cache_open_directory(directory, item->d_name);
        cache_scan_pending_directory(child.get(), records);
        continue;
      }
      if (!S_ISREG(status.st_mode)) {
        throw std::runtime_error("non-regular cache pending-delete marker");
      }
      UniqueFd fd(::openat(directory, item->d_name,
                           O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
      if (!fd) {
        cache_throw_errno("open(cache pending-delete marker scan)");
      }
      CachePendingDeleteMarker marker{};
      size_t read_bytes = 0;
      while (read_bytes != sizeof(marker)) {
        const ssize_t result = ::pread(
            fd.get(), reinterpret_cast<char*>(&marker) + read_bytes,
            sizeof(marker) - read_bytes, off_t(read_bytes));
        if (result > 0) {
          read_bytes += size_t(result);
        } else if (result < 0 && errno == EINTR) {
          continue;
        } else {
          throw std::runtime_error("short cache pending-delete marker");
        }
      }
      constexpr std::array<char, 8> magic{
          'N', 'G', 'S', '3', 'P', 'E', 'N', 'D'};
      if (marker.key_length == 0 || marker.key_length > marker.key.size() ||
          marker.restore_key_length > marker.restore_key.size() ||
          marker.replacement_etag_length >
              marker.replacement_etag.size()) {
        throw std::runtime_error(
            "invalid cache pending-delete marker without a key");
      }
      if (marker.magic != magic || marker.version != kCacheMetaVersion ||
          marker.header_size != kCacheMetaHeaderSize ||
          (marker.phase != 1 && marker.phase != 2) ||
          (marker.phase == 1 && marker.restore_key_length == 0)) {
        fprintf(stderr,
                "error: isolating malformed cache pending-delete marker: "
                "key=%.*s\n",
                int(marker.key_length), marker.key.data());
        continue;
      }
      records.push_back(CachePendingDelete{
          .key = std::string(marker.key.data(), marker.key_length),
          .restore_key = std::string(
              marker.restore_key.data(), marker.restore_key_length),
          .replacement_etag = std::string(
              marker.replacement_etag.data(),
              marker.replacement_etag_length),
          .rollback = marker.phase == 1,
      });
    }
  } catch (...) {
    ::closedir(stream);
    throw;
  }
  ::closedir(stream);
}

std::vector<CachePendingDelete> LocalCache::recover_pending_deletes() {
  std::vector<CachePendingDelete> records;
  cache_scan_pending_directory(pending_root_fd_, records);
  return records;
}

void LocalCache::commit_pending_delete(std::string_view key) {
  struct statvfs fs{};
  if (::fstatvfs(root_fd_, &fs) != 0) {
    cache_throw_errno("fstatvfs(cache pending-delete commit)");
  }
  const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
  std::optional<CacheLeaf> leaf = cache_find_leaf(
      pending_root_fd_, key, name_max);
  if (!leaf) {
    throw std::system_error(ENOENT, std::generic_category(),
                            "cache pending-delete marker");
  }
  UniqueFd fd(::openat(leaf->parent.get(), leaf->name.c_str(),
                       O_RDWR | O_CLOEXEC | O_NOFOLLOW));
  if (!fd) {
    cache_throw_errno("open(cache pending-delete commit)");
  }
  constexpr uint32_t phase = 2;
  const ssize_t result = ::pwrite(
      fd.get(), &phase, sizeof(phase),
      off_t(offsetof(CachePendingDeleteMarker, phase)));
  if (result != ssize_t(sizeof(phase))) {
    if (result < 0) {
      cache_throw_errno("pwrite(cache pending-delete commit)");
    }
    throw std::runtime_error("short cache pending-delete commit");
  }
}

bool LocalCache::commit_pending_delete_async(
    IoExecutor& executor, CacheAsyncRequest& request,
    std::string_view key) noexcept {
  return cache_start_marker_operation(
      executor, request, *this,
      CacheMarkerOperation::Kind::COMMIT_PENDING, key);
}

void LocalCache::finish_pending_delete(std::string_view key) noexcept {
  try {
    struct statvfs fs{};
    if (::fstatvfs(root_fd_, &fs) != 0) {
      cache_throw_errno("fstatvfs(cache pending-delete removal)");
    }
    const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
    std::optional<CacheLeaf> leaf = cache_find_leaf(
        pending_root_fd_, key, name_max);
    if (!leaf) return;
    struct stat status{};
    const bool measured = ::fstatat(
        leaf->parent.get(), leaf->name.c_str(), &status,
        AT_SYMLINK_NOFOLLOW) == 0;
    if (::unlinkat(leaf->parent.get(), leaf->name.c_str(), 0) != 0 &&
        errno != ENOENT) {
      cache_throw_errno("unlinkat(cache pending-delete marker)");
    }
    if (measured) {
      add_allocated(-int64_t(std::min<uint64_t>(
          uint64_t(status.st_blocks) * 512, INT64_MAX)));
    }
  } catch (const std::exception& error) {
    fprintf(stderr,
            "warning: cannot remove cache pending-delete marker: %s\n",
            error.what());
  } catch (...) {
    fprintf(stderr,
            "warning: cannot remove cache pending-delete marker\n");
  }
}

bool LocalCache::finish_pending_delete_async(
    IoExecutor& executor, CacheAsyncRequest& request,
    std::string_view key) noexcept {
  return cache_start_marker_operation(
      executor, request, *this,
      CacheMarkerOperation::Kind::FINISH_PENDING, key);
}

std::shared_ptr<CacheEntry> LocalCache::retiring_entry(
    std::string_view key, const CacheIdentity* reuse, bool preserve_generation) {
  std::vector<std::shared_ptr<CacheEntry>> released;
  std::lock_guard key_guard(key_mutex(key));
  std::lock_guard guard(mutex_);
  for (auto i = entries_.begin(); i != entries_.end();) {
    const auto entry = i->lock();
    if (!entry) { i = entries_.erase(i); continue; }
    ++i;
    if (entry->key_ != key) continue;
    std::lock_guard entry_guard(entry->mutex_);
    const auto& header = *static_cast<const CacheMetaHeader*>(entry->mapping_);
    if (!entry->stale_ &&
        ((reuse != nullptr && !entry->detached_ && cache_identity_matches(header, *reuse, kCacheBitmapUnit)) ||
         (preserve_generation && (header.flags & kCacheMetaDirty) == 0))) continue;
    release_keepalive_locked(entry.get(), released);
    if (!entry->stale_) {
      entry->stale_ = true;
      entry->notify_waiters_locked();
    }
    if (!entry->active_claims_.empty() || entry->checksum_ops_ != 0 || entry->pinned_regions_ != 0) return entry;
  }
  return {};
}

std::shared_ptr<CacheEntry> LocalCache::create_writer(
    const CacheIdentity& base, uint64_t maximum_size, bool wait) {
  const std::string_view key = base.key;
  if (key.size() > kCacheKeyCapacity ||
      base.etag.size() > kCacheEtagCapacity ||
      base.version_id.size() > kCacheVersionCapacity ||
      maximum_size == 0) {
    throw std::invalid_argument("invalid cache writer identity");
  }
  std::vector<std::shared_ptr<CacheEntry>> released;
  std::lock_guard key_guard(key_mutex(key));
  std::unique_lock guard(mutex_);
  for (auto i = entries_.begin(); i != entries_.end();) {
    std::shared_ptr<CacheEntry> entry = i->lock();
    if (!entry) {
      i = entries_.erase(i);
      continue;
    }
    if (entry->key_ == key) {
      release_keepalive_locked(entry.get(), released);
      guard.unlock();
      entry->retire_generation(wait);
      guard.lock();
      std::erase_if(entries_, [&](const auto& weak) {
        const auto current = weak.lock();
        return !current || current.get() == entry.get();
      });
      i = entries_.begin();
      continue;
    }
    ++i;
  }
  guard.unlock();

  struct statvfs fs{};
  if (::fstatvfs(root_fd_, &fs) != 0) {
    cache_throw_errno("fstatvfs(cache writer)");
  }
  const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
  CacheLeaf data_leaf = cache_leaf(data_root_fd_, key, name_max);
  CacheLeaf meta_leaf = cache_leaf(objects_root_fd_, key, name_max);
  if (!reserve_capacity(kCacheMetaHeaderSize)) {
    throw std::system_error(ENOSPC, std::generic_category(),
                            "reserve cache writer metadata");
  }
  bool reservation = true;
  int data_fd = ::openat(data_leaf.parent.get(), data_leaf.name.c_str(),
                         O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (data_fd < 0) {
    cancel_reservation(kCacheMetaHeaderSize);
    cache_throw_errno("open(cache writer data)");
  }
  advise_cache_data_file(data_fd);
  int meta_fd = -1;
  void* mapping = MAP_FAILED;
  try {
    meta_fd = ::openat(meta_leaf.parent.get(), meta_leaf.name.c_str(),
                       O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (meta_fd < 0) {
      cache_throw_errno("open(cache writer metadata)");
    }
    struct stat old_data{};
    struct stat old_meta{};
    if (::fstat(data_fd, &old_data) != 0 ||
        ::fstat(meta_fd, &old_meta) != 0) {
      cache_throw_errno("fstat(cache writer old allocation)");
    }
    bool replace_exported_data = false;
    if (old_meta.st_size >= off_t(sizeof(CacheMetaHeader))) {
      CacheMetaHeader old_header{};
      const ssize_t bytes = ::pread(
          meta_fd, &old_header, sizeof(old_header), 0);
      constexpr std::array<char, 8> magic{
          'N', 'G', 'S', '3', 'C', 'A', 'C', 'H'};
      replace_exported_data = bytes == ssize_t(sizeof(old_header)) &&
          old_header.magic == magic &&
          old_header.version == kCacheMetaVersion &&
          (old_header.flags & kCacheMetaExported) != 0;
    }
    if (replace_exported_data) {
      if (::unlinkat(data_leaf.parent.get(), data_leaf.name.c_str(), 0) != 0) {
        cache_throw_errno("unlinkat(exported cache writer data)");
      }
      // Ever-exported inodes can outlive every daemon handle through a VMA.
      // Charge only the named cache generation; statvfs still accounts for
      // the space retained by unlinked VFS references.
      add_allocated(-int64_t(std::min<uint64_t>(
          uint64_t(old_data.st_blocks) * 512, INT64_MAX)));
      cache_close_fd(data_fd);
      data_fd = ::openat(data_leaf.parent.get(), data_leaf.name.c_str(),
                         O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                         0600);
      if (data_fd < 0) {
        cache_throw_errno("recreate(exported cache writer data)");
      }
      advise_cache_data_file(data_fd);
      memset(&old_data, 0, sizeof(old_data));
    }
    if (::ftruncate(data_fd, 0) != 0 ||
        ::ftruncate(meta_fd, off_t(kCacheMetaHeaderSize)) != 0 ||
        ::fallocate(meta_fd, 0, 0, off_t(kCacheMetaHeaderSize)) != 0) {
      cache_throw_errno("initialize(cache writer files)");
    }
    mapping = ::mmap(nullptr, kCacheMetaHeaderSize,
                     PROT_READ | PROT_WRITE, MAP_SHARED, meta_fd, 0);
    if (mapping == MAP_FAILED) {
      cache_throw_errno("mmap(cache writer metadata)");
    }
    memset(mapping, 0, kCacheMetaHeaderSize);
    CacheRootHeader& superblock =
        *static_cast<CacheRootHeader*>(superblock_mapping_);
    std::atomic_ref<uint64_t> next_epoch(superblock.next_epoch);
    const uint64_t epoch =
        next_epoch.fetch_add(1, std::memory_order_relaxed);
    CacheIdentity write_identity = base;
    write_identity.size  = 0;
    write_identity.mtime = 0;
    cache_initialize_header(
        *static_cast<CacheMetaHeader*>(mapping),
        write_identity,
        kCacheBitmapUnit, epoch);
    cache_initialize_write_id(*static_cast<CacheMetaHeader*>(mapping));
    auto& header = *static_cast<CacheMetaHeader*>(mapping);
    header.upload_part_size   = config_.upload_part_size;
    header.checksum_algorithm = config_.checksum_algorithm;
    struct stat new_data{};
    struct stat new_meta{};
    if (::fstat(data_fd, &new_data) != 0 ||
        ::fstat(meta_fd, &new_meta) != 0) {
      cache_throw_errno("fstat(cache writer new allocation)");
    }
    const uint64_t old_bytes =
        (uint64_t(old_data.st_blocks) + uint64_t(old_meta.st_blocks)) * 512;
    const uint64_t new_bytes =
        (uint64_t(new_data.st_blocks) + uint64_t(new_meta.st_blocks)) * 512;
    finish_reservation(
        kCacheMetaHeaderSize,
        new_bytes > old_bytes ? new_bytes - old_bytes : 0);
    reservation = false;
    if (old_bytes > new_bytes) {
      add_allocated(-int64_t(std::min<uint64_t>(
          old_bytes - new_bytes, INT64_MAX)));
    }
    auto entry = std::shared_ptr<CacheEntry>(new CacheEntry(
        *this, std::string(key), data_fd, meta_fd, -1, mapping,
        kCacheMetaHeaderSize, 0));
    entry->disable_eviction();
    data_fd = -1;
    meta_fd = -1;
    mapping = MAP_FAILED;
    {
      std::lock_guard registry_guard(mutex_);
      entries_.push_back(entry);
    }
    return entry;
  } catch (...) {
    if (reservation) {
      cancel_reservation(kCacheMetaHeaderSize);
    }
    if (mapping != MAP_FAILED) {
      ::munmap(mapping, kCacheMetaHeaderSize);
    }
    cache_close_fd(meta_fd);
    cache_close_fd(data_fd);
    throw;
  }
}

bool LocalCache::remove(std::string_view key,
                        bool preserve_generation, bool* retry,
                        bool wait) noexcept {
  if (retry != nullptr) *retry = false;
  std::vector<std::shared_ptr<CacheEntry>> released;
  try {
    std::lock_guard key_guard(key_mutex(key));
    std::shared_ptr<CacheEntry> target;
    bool preserved = false;
    std::unique_lock guard(mutex_);
    for (auto i = entries_.begin(); i != entries_.end();) {
      std::shared_ptr<CacheEntry> entry = i->lock();
      if (!entry) {
        i = entries_.erase(i);
        continue;
      }
      if (entry->key_ == key) {
        target = std::move(entry);
      }
      ++i;
    }
    release_key_keepalive_locked(key, released);
    if (target) {
      {
        std::lock_guard entry_guard(target->mutex_);
        const auto& header =
            *static_cast<const CacheMetaHeader*>(target->mapping_);
        preserved = preserve_generation && !target->stale_ &&
            (header.flags & kCacheMetaDirty) == 0;
      }
      {
        std::lock_guard entry_guard(target->mutex_);
        target->detached_ = true;
      }
      guard.unlock();
      if (preserved) {
        target->disable_eviction();
      } else {
        target->retire_generation(wait);
      }
    } else {
      guard.unlock();
    }

    // Do not lose a busy generation from the registry if retirement raises
    // EAGAIN. A later retry must still find and wait for its live file users.
    {
      std::lock_guard entries_guard(mutex_);
      std::erase_if(entries_, [&](const auto& weak) {
        const auto current = weak.lock();
        return !current || current->key_ == key;
      });
    }

    struct statvfs fs{};
    if (::fstatvfs(root_fd_, &fs) != 0) {
      cache_throw_errno("fstatvfs(cache removal)");
    }
    const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
    const auto unlink_file = [&](int root, int target_fd,
                                 bool CacheEntry::*unlinked) {
      std::optional<CacheLeaf> leaf = cache_find_leaf(root, key, name_max);
      if (!leaf) {
        return;
      }
      struct stat path_status{};
      if (::fstatat(leaf->parent.get(), leaf->name.c_str(), &path_status,
                    AT_SYMLINK_NOFOLLOW) != 0) {
        if (errno == ENOENT) {
          return;
        }
        cache_throw_errno("fstatat(cache removal)");
      }
      bool target_file = false;
      if (target && target_fd >= 0) {
        struct stat fd_status{};
        target_file = ::fstat(target_fd, &fd_status) == 0 &&
            fd_status.st_dev == path_status.st_dev &&
            fd_status.st_ino == path_status.st_ino;
      }
      if (::unlinkat(leaf->parent.get(), leaf->name.c_str(), 0) != 0) {
        cache_throw_errno("unlinkat(cache removal)");
      }
      if (target_file) {
        target.get()->*unlinked = true;
      } else {
        add_allocated(-int64_t(std::min<uint64_t>(
            uint64_t(path_status.st_blocks) * 512, INT64_MAX)));
      }
    };
    unlink_file(data_root_fd_, target ? target->data_fd_ : -1,
                &CacheEntry::unlinked_data_);
    unlink_file(objects_root_fd_, target ? target->meta_fd_ : -1,
                &CacheEntry::unlinked_meta_);
    return preserved;
  } catch (const CacheRetirementPending&) {
    if (retry != nullptr) *retry = true;
    return false;
  } catch (const std::exception& error) {
    fprintf(stderr, "warning: local cache removal failed: key=%.*s: %s\n",
            int(key.size()), key.data(), error.what());
  } catch (...) {
    fprintf(stderr, "warning: local cache removal failed: key=%.*s\n",
            int(key.size()), key.data());
  }
  return false;
}

bool LocalCache::rename(std::string_view old_key,
                        std::string_view new_key, bool* retry,
                        bool wait) noexcept {
  if (retry != nullptr) *retry = false;
  if (old_key == new_key) {
    return true;
  }
  if (new_key.size() > kCacheKeyCapacity) {
    return false;
  }
  std::recursive_mutex* first  = &key_mutex(old_key);
  std::recursive_mutex* second = &key_mutex(new_key);
  if (std::less<std::recursive_mutex*>{}(second, first)) {
    std::swap(first, second);
  }
  std::unique_lock first_guard(*first);
  std::optional<std::unique_lock<std::recursive_mutex>> second_guard;
  if (second != first) {
    second_guard.emplace(*second);
  }
  bool busy = false;
  remove(new_key, false, &busy, wait);
  if (busy) {
    if (retry != nullptr) *retry = true;
    return false;
  }
  try {
    std::unique_lock guard(mutex_);
    std::shared_ptr<CacheEntry> target;
    for (auto i = entries_.begin(); i != entries_.end();) {
      std::shared_ptr<CacheEntry> entry = i->lock();
      if (!entry) {
        i = entries_.erase(i);
        continue;
      }
      if (entry->key_ == old_key) {
        target = std::move(entry);
        break;
      }
      ++i;
    }
    guard.unlock();

    struct statvfs fs{};
    if (::fstatvfs(root_fd_, &fs) != 0) {
      cache_throw_errno("fstatvfs(cache rename)");
    }
    const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
    const auto move_file = [&](int root) {
      std::optional<CacheLeaf> source = cache_find_leaf(
          root, old_key, name_max);
      if (!source) {
        return false;
      }
      CacheLeaf destination = cache_leaf(root, new_key, name_max);
      if (::renameat(source->parent.get(), source->name.c_str(),
                     destination.parent.get(),
                     destination.name.c_str()) != 0) {
        cache_throw_errno("renameat(cache object)");
      }
      return true;
    };
    const bool data_moved = move_file(data_root_fd_);
    const bool meta_moved = move_file(objects_root_fd_);
    if (!data_moved && !meta_moved) {
      return true;
    }
    if (!meta_moved) {
      throw std::runtime_error("cache rename metadata is missing");
    }

    const auto rewrite = [&](CacheMetaHeader& header) {
      constexpr std::array<char, 8> magic{
          'N', 'G', 'S', '3', 'C', 'A', 'C', 'H'};
      if (header.magic != magic || header.version != kCacheMetaVersion ||
          header.key_length != old_key.size() ||
          header.key_length > header.key.size() ||
          memcmp(header.key.data(), old_key.data(), old_key.size()) != 0) {
        throw std::runtime_error("cache rename metadata key mismatch");
      }
      memset(header.key.data(), 0, header.key.size());
      memcpy(header.key.data(), new_key.data(), new_key.size());
      header.key_length = uint32_t(new_key.size());
    };
    if (target) {
      guard.lock();
      std::lock_guard entry_guard(target->mutex_);
      rewrite(*static_cast<CacheMetaHeader*>(target->mapping_));
      target->key_.assign(new_key);
      guard.unlock();
    } else {
      CacheLeaf destination = cache_leaf(
          objects_root_fd_, new_key, name_max);
      UniqueFd fd(::openat(destination.parent.get(), destination.name.c_str(),
                           O_RDWR | O_CLOEXEC | O_NOFOLLOW));
      if (!fd) {
        cache_throw_errno("open(cache renamed metadata)");
      }
      void* mapping = ::mmap(nullptr, kCacheMetaHeaderSize,
                             PROT_READ | PROT_WRITE, MAP_SHARED, fd.get(), 0);
      if (mapping == MAP_FAILED) {
        cache_throw_errno("mmap(cache renamed metadata)");
      }
      try {
        rewrite(*static_cast<CacheMetaHeader*>(mapping));
      } catch (...) {
        ::munmap(mapping, kCacheMetaHeaderSize);
        throw;
      }
      ::munmap(mapping, kCacheMetaHeaderSize);
    }
    return true;
  } catch (const std::exception& error) {
    fprintf(stderr,
            "warning: local cache rename failed: old=%.*s new=%.*s: %s\n",
            int(old_key.size()), old_key.data(),
            int(new_key.size()), new_key.data(), error.what());
  } catch (...) {
    fprintf(stderr,
            "warning: local cache rename failed: old=%.*s new=%.*s\n",
            int(old_key.size()), old_key.data(),
            int(new_key.size()), new_key.data());
  }
  remove(old_key, false, nullptr, wait);
  remove(new_key, false, nullptr, wait);
  return false;
}

std::shared_ptr<CacheEntry> LocalCache::try_open(
    const CacheIdentity& identity) noexcept {
  if (identity.key.size() > kCacheKeyCapacity ||
      identity.etag.size() > kCacheEtagCapacity ||
      identity.version_id.size() > kCacheVersionCapacity) {
    return {};
  }
  std::unique_lock key_guard(
      key_mutex(identity.key), std::try_to_lock);
  if (!key_guard.owns_lock()) return {};
  std::unique_lock guard(mutex_, std::try_to_lock);
  if (!guard.owns_lock()) return {};
  for (const KeepaliveSlot& slot : keepalive_) {
    if (slot.entry->key_ != identity.key) continue;
    std::unique_lock entry_guard(
        slot.entry->mutex_, std::try_to_lock);
    if (!entry_guard.owns_lock()) return {};
    const auto& header =
        *static_cast<const CacheMetaHeader*>(slot.entry->mapping_);
    if (!slot.entry->detached_ && !slot.entry->stale_ &&
        cache_identity_matches(header, identity, kCacheBitmapUnit)) {
      return slot.entry;
    }
    return {};
  }
  return {};
}

std::shared_ptr<CacheEntry> LocalCache::open(
    const CacheIdentity& identity, bool wait) {
  if (identity.key.size() > kCacheKeyCapacity ||
      identity.etag.size() > kCacheEtagCapacity ||
      identity.version_id.size() > kCacheVersionCapacity) {
    throw std::system_error(EOVERFLOW, std::generic_category(),
                            "S3 identity is too large for cache metadata");
  }
  std::vector<std::shared_ptr<CacheEntry>> released;
  std::lock_guard key_guard(key_mutex(identity.key));
  std::unique_lock guard(mutex_);
  for (auto i = entries_.begin(); i != entries_.end();) {
    std::shared_ptr<CacheEntry> entry = i->lock();
    if (!entry) {
      i = entries_.erase(i);
      continue;
    }
    if (entry->key_ == identity.key) {
      bool matches = false;
      {
        std::lock_guard entry_guard(entry->mutex_);
        matches = !entry->detached_ && !entry->stale_ &&
            cache_identity_matches(
                *static_cast<CacheMetaHeader*>(entry->mapping_), identity,
                kCacheBitmapUnit);
        if (matches) {
          retain_entry_locked(entry, released);
        }
      }
      if (matches) {
        guard.unlock();
        return entry;
      }
      release_keepalive_locked(entry.get(), released);
      guard.unlock();
      entry->retire_generation(wait);
      guard.lock();
      std::erase_if(entries_, [&](const auto& weak) {
        const auto current = weak.lock();
        return !current || current.get() == entry.get();
      });
      i = entries_.begin();
      continue;
    }
    ++i;
  }
  guard.unlock();

  struct statvfs fs{};
  if (::fstatvfs(root_fd_, &fs) != 0) {
    cache_throw_errno("fstatvfs(cache root)");
  }
  const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
  CacheLeaf data_leaf = cache_leaf(data_root_fd_, identity.key, name_max);
  CacheLeaf meta_leaf = cache_leaf(objects_root_fd_, identity.key, name_max);
  int data_fd = ::openat(data_leaf.parent.get(), data_leaf.name.c_str(),
                         O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (data_fd < 0) {
    cache_throw_errno("open(cache data)");
  }
  advise_cache_data_file(data_fd);
  int meta_fd = -1;
  void* mapping = MAP_FAILED;
  const size_t required_mapping_size =
      cache_mapping_size(identity.size, kCacheBitmapUnit);
  size_t mapping_size = required_mapping_size;
  bool reservation = false;
  try {
    meta_fd = ::openat(meta_leaf.parent.get(), meta_leaf.name.c_str(),
                       O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (meta_fd < 0) {
      cache_throw_errno("open(cache metadata)");
    }
    if (::flock(meta_fd, LOCK_EX) != 0) {
      cache_throw_errno("flock(cache metadata)");
    }

    struct stat meta_status{};
    if (::fstat(meta_fd, &meta_status) != 0) {
      cache_throw_errno("fstat(cache metadata)");
    }
    const size_t existing_mapping_size =
        size_t(std::max<off_t>(meta_status.st_size, 0));
    bool existing_exported = false;
    if (existing_mapping_size >= sizeof(CacheMetaHeader)) {
      CacheMetaHeader existing_header{};
      const ssize_t bytes = ::pread(
          meta_fd, &existing_header, sizeof(existing_header), 0);
      constexpr std::array<char, 8> magic{
          'N', 'G', 'S', '3', 'C', 'A', 'C', 'H'};
      existing_exported = bytes == ssize_t(sizeof(existing_header)) &&
          existing_header.magic == magic &&
          existing_header.version == kCacheMetaVersion &&
          (existing_header.flags & kCacheMetaExported) != 0;
    }
    bool reset = existing_mapping_size < required_mapping_size;
    if (!reset) {
      mapping_size = existing_mapping_size;
    }
    if (!reset && mapping_size != 0) {
      mapping = ::mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE,
                       MAP_SHARED, meta_fd, 0);
      if (mapping == MAP_FAILED) {
        cache_throw_errno("mmap(cache metadata)");
      }
      reset = !cache_identity_matches(
          *static_cast<CacheMetaHeader*>(mapping), identity,
          kCacheBitmapUnit);
    }
    struct stat data_status{};
    if (::fstat(data_fd, &data_status) != 0) {
      cache_throw_errno("fstat(cache data)");
    }
    if (uint64_t(std::max<off_t>(data_status.st_size, 0)) != identity.size) {
      reset = true;
    }
    if (reset) {
      struct stat old_data = data_status;
      struct stat old_meta = meta_status;
      const bool replace_exported_data = existing_exported;
      if (mapping != MAP_FAILED) {
        ::munmap(mapping, mapping_size);
        mapping = MAP_FAILED;
      }
      if (replace_exported_data) {
        if (::unlinkat(data_leaf.parent.get(), data_leaf.name.c_str(), 0) != 0) {
          cache_throw_errno("unlinkat(exported cache generation)");
        }
        // The unlinked exported inode belongs to surviving VFS references,
        // not the named cache generation. Its physical space remains visible
        // through statvfs even after its daemon-side CacheEntry disappears.
        add_allocated(-int64_t(std::min<uint64_t>(
            uint64_t(old_data.st_blocks) * 512, INT64_MAX)));
        cache_close_fd(data_fd);
        data_fd = ::openat(
            data_leaf.parent.get(), data_leaf.name.c_str(),
            O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (data_fd < 0) {
          cache_throw_errno("recreate(exported cache generation)");
        }
        advise_cache_data_file(data_fd);
        memset(&old_data, 0, sizeof(old_data));
      }
      mapping_size = required_mapping_size;
      if (::ftruncate(data_fd, 0) != 0 ||
          ::ftruncate(data_fd, off_t(identity.size)) != 0 ||
          ::ftruncate(meta_fd, 0) != 0 ||
          ::ftruncate(meta_fd, off_t(mapping_size)) != 0) {
        cache_throw_errno("ftruncate(cache generation)");
      }
      struct stat truncated_data{};
      struct stat truncated_meta{};
      if (::fstat(data_fd, &truncated_data) != 0 ||
          ::fstat(meta_fd, &truncated_meta) != 0) {
        cache_throw_errno("fstat(truncated cache generation)");
      }
      const uint64_t old_bytes =
          (uint64_t(old_data.st_blocks) + uint64_t(old_meta.st_blocks)) * 512;
      const uint64_t truncated_bytes =
          (uint64_t(truncated_data.st_blocks) +
           uint64_t(truncated_meta.st_blocks)) * 512;
      if (old_bytes > truncated_bytes) {
        add_allocated(-int64_t(std::min<uint64_t>(
            old_bytes - truncated_bytes, INT64_MAX)));
      }
      const uint64_t reserve = cache_round_up(mapping_size, config_.page_size);
      if (!reserve_capacity(reserve)) {
        ::flock(meta_fd, LOCK_UN);
        cache_close_fd(meta_fd);
        cache_close_fd(data_fd);
        return nullptr;
      }
      reservation = true;
      if (::fallocate(meta_fd, 0, 0, off_t(mapping_size)) != 0) {
        const int error = errno;
        struct stat failed_meta{};
        const uint64_t failed_bytes =
            ::fstat(meta_fd, &failed_meta) == 0
                ? uint64_t(failed_meta.st_blocks) * 512 : 0;
        const uint64_t before_bytes =
            uint64_t(truncated_meta.st_blocks) * 512;
        finish_reservation(
            reserve, failed_bytes > before_bytes
                         ? failed_bytes - before_bytes : 0);
        reservation = false;
        if (error == ENOSPC || error == EDQUOT) {
          ::flock(meta_fd, LOCK_UN);
          cache_close_fd(meta_fd);
          cache_close_fd(data_fd);
          return nullptr;
        }
        throw std::system_error(error, std::generic_category(),
                                "fallocate(cache metadata)");
      }
      struct stat allocated_meta{};
      if (::fstat(meta_fd, &allocated_meta) != 0) {
        cache_throw_errno("fstat(allocated cache metadata)");
      }
      const uint64_t before_bytes =
          uint64_t(truncated_meta.st_blocks) * 512;
      const uint64_t after_bytes =
          uint64_t(allocated_meta.st_blocks) * 512;
      finish_reservation(
          reserve, after_bytes > before_bytes ? after_bytes - before_bytes : 0);
      reservation = false;
      mapping = ::mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE,
                       MAP_SHARED, meta_fd, 0);
      if (mapping == MAP_FAILED) {
        cache_throw_errno("mmap(new cache metadata)");
      }
      memset(mapping, 0, mapping_size);
      CacheRootHeader& superblock =
          *static_cast<CacheRootHeader*>(superblock_mapping_);
      std::atomic_ref<uint64_t> next_epoch(superblock.next_epoch);
      const uint64_t epoch =
          next_epoch.fetch_add(1, std::memory_order_relaxed);
      cache_initialize_header(*static_cast<CacheMetaHeader*>(mapping),
                              identity, kCacheBitmapUnit, epoch);
    }

    auto entry = std::shared_ptr<CacheEntry>(new CacheEntry(
        *this, std::string(identity.key), data_fd, meta_fd, -1, mapping,
        mapping_size, identity.size));
    data_fd = -1;
    meta_fd = -1;
    mapping = MAP_FAILED;
    guard.lock();
    entries_.push_back(entry);
    {
      std::lock_guard entry_guard(entry->mutex_);
      retain_entry_locked(entry, released);
    }
    guard.unlock();
    ::flock(entry->meta_fd_, LOCK_UN);
    return entry;
  } catch (...) {
    if (reservation) {
      cancel_reservation(cache_round_up(mapping_size, config_.page_size));
    }
    if (guard.owns_lock()) {
      guard.unlock();
    }
    if (mapping != MAP_FAILED) {
      ::munmap(mapping, mapping_size);
    }
    if (meta_fd >= 0) {
      ::flock(meta_fd, LOCK_UN);
    }
    cache_close_fd(meta_fd);
    cache_close_fd(data_fd);
    throw;
  }
}

bool LocalCache::open_async(IoExecutor& executor, CacheAsyncRequest& request,
                            const CacheIdentity& identity) noexcept {
  if (std::shared_ptr<CacheEntry> hit = try_open(identity)) {
    CacheOpenOperation* operation = nullptr;
    try {
      operation = new CacheOpenOperation(executor, request, std::move(hit));
      if (operation->start()) return true;
      delete operation;
      return false;
    } catch (...) {
      delete operation;
      errno = cache_async_start_errno(std::current_exception());
      return false;
    }
  }
  CacheFileOperation* operation = nullptr;
  try {
    operation = new CacheFileOperation(
        executor, request, *this, CacheFileOperation::Kind::OPEN);
    if (operation->start(identity, 0)) return true;
    delete operation;
    return false;
  } catch (...) {
    const std::exception_ptr error = std::current_exception();
    if (operation != nullptr) operation->abandon_start();
    delete operation;
    errno = cache_async_start_errno(error);
    return false;
  }
}

bool LocalCache::open_async_if_idle(
    IoExecutor& executor, CacheAsyncRequest& request,
    const CacheIdentity& identity) noexcept {
  CacheFileOperation* operation = nullptr;
  try {
    operation = new CacheFileOperation(
        executor, request, *this, CacheFileOperation::Kind::OPEN);
    if (operation->start(identity, 0, false, true)) return true;
    delete operation;
    return false;
  } catch (...) {
    const std::exception_ptr error = std::current_exception();
    if (operation != nullptr) operation->abandon_start();
    delete operation;
    errno = cache_async_start_errno(error);
    return false;
  }
}

bool LocalCache::create_writer_async(IoExecutor& executor,
                                     CacheAsyncRequest& request,
                                     const CacheIdentity& identity,
                                     uint64_t maximum_size) noexcept {
  CacheFileOperation* operation = nullptr;
  try {
    operation = new CacheFileOperation(
        executor, request, *this, CacheFileOperation::Kind::CREATE_WRITER);
    if (operation->start(identity, maximum_size)) return true;
    delete operation;
    return false;
  } catch (...) {
    const std::exception_ptr error = std::current_exception();
    if (operation != nullptr) operation->abandon_start();
    delete operation;
    errno = cache_async_start_errno(error);
    return false;
  }
}

bool LocalCache::remove_async(IoExecutor& executor,
                              CacheAsyncRequest& request,
                              std::string_view key,
                              bool preserve_generation) noexcept {
  CacheNamespaceOperation* operation = nullptr;
  try {
    operation = new CacheNamespaceOperation(
        executor, request, *this, CacheNamespaceOperation::Kind::REMOVE);
    if (operation->start(key, preserve_generation)) return true;
    delete operation;
    return false;
  } catch (...) {
    const std::exception_ptr error = std::current_exception();
    if (operation != nullptr) operation->abandon_start();
    delete operation;
    errno = cache_async_start_errno(error);
    return false;
  }
}

bool LocalCache::reclaim_closed_async(
    IoExecutor& executor, CacheAsyncRequest& request,
    const std::shared_ptr<CacheEntry>& expected) noexcept {
  CacheNamespaceOperation* operation = nullptr;
  try {
    operation = new CacheNamespaceOperation(
        executor, request, *this, CacheNamespaceOperation::Kind::REMOVE);
    if (operation->start(
            expected->key_, false, {}, false, expected)) {
      return true;
    }
    delete operation;
    return false;
  } catch (...) {
    const std::exception_ptr error = std::current_exception();
    if (operation != nullptr) operation->abandon_start();
    delete operation;
    errno = cache_async_start_errno(error);
    return false;
  }
}

bool LocalCache::rename_async(IoExecutor& executor,
                              CacheAsyncRequest& request,
                              std::string_view old_key,
                              std::string_view new_key) noexcept {
  CacheNamespaceOperation* operation = nullptr;
  try {
    operation = new CacheNamespaceOperation(
        executor, request, *this, CacheNamespaceOperation::Kind::RENAME);
    if (operation->start(old_key, false, new_key)) return true;
    delete operation;
    return false;
  } catch (...) {
    const std::exception_ptr error = std::current_exception();
    if (operation != nullptr) operation->abandon_start();
    delete operation;
    errno = cache_async_start_errno(error);
    return false;
  }
}
