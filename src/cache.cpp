#include "cache.hpp"

#include <openssl/sha.h>
#include <openssl/rand.h>

#include <fcntl.h>
#include <dirent.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
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
constexpr uint32_t kCacheRootVersion = 1;
constexpr uint32_t kCacheMetaVersion = 3;
constexpr uint32_t kCacheByteOrder    = 0x01020304;
constexpr uint32_t kCachePathVersion  = 1;
constexpr uint32_t kCacheMetaDirty    = 1;
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
  uint32_t page_size;
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
  uint32_t page_size;
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
      header.page_size == config.page_size &&
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
  header.page_size        = uint32_t(config.page_size);
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
      if (errno != ENOENT) {
        cache_throw_errno("fstatat(cache prefix)");
      }
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
      if (errno == ENOENT) {
        return std::nullopt;
      }
      cache_throw_errno("fstatat(cache lookup)");
    }
    if (leaf) {
      if (S_ISDIR(status.st_mode)) {
        current = cache_open_directory(current.get(), encoded.c_str());
        encoded = kCacheValueName;
        if (::fstatat(current.get(), encoded.c_str(), &status,
                      AT_SYMLINK_NOFOLLOW) != 0) {
          if (errno == ENOENT) {
            return std::nullopt;
          }
          cache_throw_errno("fstatat(cache value lookup)");
        }
      }
      if (!S_ISREG(status.st_mode)) {
        throw std::runtime_error("non-regular cache object path");
      }
      return CacheLeaf{std::move(current), std::move(encoded)};
    }
    if (!S_ISDIR(status.st_mode)) {
      return std::nullopt;
    }
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
                            size_t page_size) noexcept {
  constexpr std::array<char, 8> magic{'N', 'G', 'S', '3', 'C', 'A', 'C', 'H'};
  if (header.magic != magic || header.version != kCacheMetaVersion ||
      header.header_size != kCacheMetaHeaderSize ||
      (header.flags & kCacheMetaDirty) != 0 ||
      header.page_size != page_size || header.object_size != identity.size ||
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
      header.page_size == config.page_size &&
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
                                         config.page_size) &&
      memcmp(header.key.data(), key.data(), key.size()) == 0;
}

void cache_initialize_header(CacheMetaHeader& header,
                             const CacheIdentity& identity,
                             size_t page_size,
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
  header.page_size   = uint32_t(page_size);
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
          header.page_size != config.page_size ||
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
                                          config.page_size);
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

CacheEntry::CacheEntry(LocalCache& owner, std::string key, int data_fd,
                       int meta_fd, int dirty_fd, void* mapping,
                       size_t mapping_size, uint64_t size)
    : owner_(&owner),
      key_(std::move(key)),
      data_fd_(data_fd),
      meta_fd_(meta_fd),
      dirty_fd_(dirty_fd),
      mapping_(mapping),
      mapping_size_(mapping_size),
      size_(size),
      epoch_(static_cast<CacheMetaHeader*>(mapping)->generation_epoch),
      page_size_(owner.config().page_size),
      page_count_(size == 0 ? 0 : size_t((size - 1) / page_size_ + 1)),
      bitmap_offset_(kCacheMetaHeaderSize),
      fetching_(page_count_, 0),
      referenced_(size == 0 ? 0 : size_t(
          (size - 1) / std::max<size_t>(1024U * 1024U, page_size_) + 1), 0),
      region_pins_(referenced_.size(), 0) {}

CacheEntry::~CacheEntry() {
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

std::string CacheEntry::write_id() const {
  std::lock_guard guard(mutex_);
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  if (header.write_id_length > header.write_id.size()) {
    throw std::runtime_error("invalid cached write ID");
  }
  return std::string(header.write_id.data(), header.write_id_length);
}

size_t CacheEntry::page_size() const noexcept { return page_size_; }

bool CacheEntry::stale() const noexcept {
  std::lock_guard guard(mutex_);
  return stale_;
}

bool CacheEntry::dirty() const noexcept {
  std::lock_guard guard(mutex_);
  const auto& header = *static_cast<const CacheMetaHeader*>(mapping_);
  return (header.flags & kCacheMetaDirty) != 0;
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
    if (page_state(page) == CACHE_PAGE_BAD && !fetching_[page]) {
      return true;
    }
  }
  return false;
}

bool CacheEntry::fully_clean() const {
  std::lock_guard guard(mutex_);
  return !stale_ &&
      range_all_state(0, size_t(std::min<uint64_t>(size_, SIZE_MAX)),
                      CACHE_PAGE_CLEAN);
}

bool CacheEntry::prepare_read(uint64_t offset, size_t length) {
  return owner_->prepare_range(*this, offset, length, false);
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
        wanted - old_mapping_size, page_size_);
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
  if (fetching_.size() < pages) {
    fetching_.resize(pages, 0);
  }
  page_count_ = pages;
  const size_t regions = size_t(
      (written_end - 1) /
          std::max<size_t>(1024U * 1024U, page_size_) + 1);
  if (referenced_.size() < regions) {
    referenced_.resize(regions, 0);
    region_pins_.resize(regions, 0);
  }
  const size_t first = size_t(offset / page_size_);
  const size_t last  = size_t((written_end - 1) / page_size_);
  for (size_t page = first; page <= last; ++page) {
    set_page_state(page, CACHE_PAGE_DIRTY);
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
    for (size_t page = 0; page < page_count_; ++page) {
      if (page_state(page) == CACHE_PAGE_DIRTY) {
        set_page_state(page, CACHE_PAGE_CLEAN);
      }
    }
    eviction_disabled_ = false;
    if (::msync(mapping_, mapping_size_, MS_SYNC) != 0) {
      cache_throw_errno("msync(cache write commit)");
    }
    cache_close_fd(dirty_fd_);
  }
  owner_->remove_dirty_marker(key_);
}

bool CacheEntry::pin_clean(uint64_t offset, size_t length) {
  std::lock_guard guard(mutex_);
  if (stale_) {
    return false;
  }
  if (length == 0) {
    return true;
  }
  if (!range_all_state(offset, length, CACHE_PAGE_CLEAN)) {
    return false;
  }
  const size_t region_size = std::max<size_t>(1024U * 1024U, page_size_);
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
  const size_t region_size = std::max<size_t>(1024U * 1024U, page_size_);
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
  const size_t region_size = std::max<size_t>(1024U * 1024U, page_size_);
  const size_t first = size_t(offset / region_size);
  const size_t last = std::min(
      region_pins_.size(), size_t((offset + length - 1) / region_size) + 1);
  for (size_t region = first; region < last; ++region) {
    if (region_pins_[region] != 0) {
      --region_pins_[region];
      --pinned_regions_;
    }
  }
  if (stale_ && pinned_regions_ == 0) {
    condition_.notify_all();
  }
}

void CacheEntry::touch(uint64_t offset, size_t length) noexcept {
  std::lock_guard guard(mutex_);
  if (length == 0 || referenced_.empty()) {
    return;
  }
  const size_t region_size = std::max<size_t>(1024U * 1024U, page_size_);
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

bool CacheEntry::range_ready_locked(uint64_t offset,
                                    size_t length) const noexcept {
  if (stale_) {
    return true;
  }
  if (range_all_state(offset, length, CACHE_PAGE_CLEAN)) {
    return true;
  }
  const size_t first = size_t(offset / page_size_);
  const size_t last = size_t((offset + length - 1) / page_size_);
  for (size_t page = first; page <= last; ++page) {
    if (page_state(page) == CACHE_PAGE_BAD && !fetching_[page]) {
      return true;
    }
  }
  return false;
}

CacheFetchClaim CacheEntry::claim_fetch(uint64_t wanted_offset,
                                        size_t wanted_length,
                                        size_t expansion) {
  std::lock_guard guard(mutex_);
  if (stale_) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "stale cache generation");
  }
  if (wanted_length == 0 || wanted_offset >= size_) {
    return {};
  }
  wanted_length = size_t(std::min<uint64_t>(
      wanted_length, size_ - wanted_offset));
  const size_t first = size_t(wanted_offset / page_size_);
  const size_t last = size_t((wanted_offset + wanted_length - 1) / page_size_);
  size_t missing = first;
  while (missing <= last) {
    if (page_state(missing) == CACHE_PAGE_CLEAN) {
      ++missing;
      continue;
    }
    if (fetching_[missing] || page_state(missing) == CACHE_PAGE_BAD) {
      return {};
    }
    break;
  }
  if (missing > last) {
    return {};
  }

  const uint64_t start = uint64_t(missing) * page_size_;
  const uint64_t request_end = wanted_offset + wanted_length;
  const uint64_t extent = std::max<size_t>(expansion, page_size_);
  const uint64_t expanded_end = start > UINT64_MAX - extent
      ? UINT64_MAX : start + extent;
  const uint64_t wanted_end = std::min<uint64_t>(
      size_, std::max(request_end, expanded_end));
  size_t end_page = size_t((wanted_end - 1) / page_size_) + 1;
  for (size_t page = missing; page < end_page; ++page) {
    if (fetching_[page] || page_state(page) == CACHE_PAGE_BAD) {
      end_page = page;
      break;
    }
  }
  if (end_page == missing) {
    return {};
  }
  const uint64_t end = std::min<uint64_t>(size_, uint64_t(end_page) * page_size_);
  const uint64_t id = next_claim_id_;
  active_claims_.push_back(id);
  ++next_claim_id_;
  for (size_t page = missing; page < end_page; ++page) {
    if (page_state(page) != CACHE_PAGE_CLEAN) {
      fetching_[page] = 1;
    }
  }
  return CacheFetchClaim{start, epoch_, id, size_t(end - start)};
}

void CacheEntry::wait_for_range(uint64_t offset, size_t length) {
  std::unique_lock guard(mutex_);
  condition_.wait(guard, [&] {
    if (range_ready_locked(offset, length)) {
      return true;
    }
    const size_t first = size_t(offset / page_size_);
    const size_t last = size_t((offset + length - 1) / page_size_);
    for (size_t page = first; page <= last; ++page) {
      if (page_state(page) != CACHE_PAGE_CLEAN && !fetching_[page]) {
        return true;
      }
    }
    return false;
  });
  if (stale_) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "stale cache generation");
  }
}

void CacheEntry::publish_clean(const CacheFetchClaim& claim,
                               size_t published, size_t length,
                               bool final) {
  std::lock_guard guard(mutex_);
  if (stale_ || claim.epoch != epoch_ || length < published ||
      (length == published && !final)) {
    return;
  }
  const size_t first = size_t((claim.offset + published) / page_size_);
  const uint64_t end = claim.offset + std::min(length, claim.length);
  size_t page_end = size_t(end / page_size_);
  if (final && end == size_ && end % page_size_ != 0) {
    ++page_end;
  }
  page_end = std::min(page_end, page_count_);
  for (size_t page = first; page < page_end; ++page) {
    set_page_state(page, CACHE_PAGE_CLEAN);
    fetching_[page] = 0;
  }
  if (first != page_end) {
    condition_.notify_all();
  }
}

bool CacheEntry::end_fetch_locked(const CacheFetchClaim& claim) noexcept {
  const auto i = std::find(active_claims_.begin(), active_claims_.end(),
                           claim.id);
  if (i == active_claims_.end()) {
    return false;
  }
  active_claims_.erase(i);
  return true;
}

void CacheEntry::finish_fetch(const CacheFetchClaim& claim) noexcept {
  std::lock_guard guard(mutex_);
  if (claim.epoch == epoch_ && claim.length != 0) {
    const size_t first = size_t(claim.offset / page_size_);
    const size_t last = std::min(
        page_count_, size_t((claim.offset + claim.length - 1) / page_size_) + 1);
    for (size_t page = first; page < last; ++page) {
      fetching_[page] = 0;
    }
  }
  end_fetch_locked(claim);
  condition_.notify_all();
}

void CacheEntry::fail_fetch(const CacheFetchClaim& claim) noexcept {
  std::lock_guard guard(mutex_);
  if (claim.epoch == epoch_ && claim.length != 0) {
    const size_t first = size_t(claim.offset / page_size_);
    const size_t last = std::min(
        page_count_, size_t((claim.offset + claim.length - 1) / page_size_) + 1);
    for (size_t page = first; page < last; ++page) {
      fetching_[page] = 0;
    }
    size_t page = first;
    while (page < last) {
      while (page < last &&
             page_state(page) != CACHE_PAGE_MISSING) {
        ++page;
      }
      const size_t missing = page;
      while (page < last &&
             page_state(page) == CACHE_PAGE_MISSING) {
        ++page;
      }
      if (missing != page) {
        const uint64_t offset = uint64_t(missing) * page_size_;
        const uint64_t end = std::min<uint64_t>(
            size_, uint64_t(page) * page_size_);
        owner_->punch_range(data_fd_, offset, end - offset);
      }
    }
  }
  end_fetch_locked(claim);
  condition_.notify_all();
}

void CacheEntry::mark_bad(const CacheFetchClaim& claim) noexcept {
  std::lock_guard guard(mutex_);
  if (!stale_ && claim.epoch == epoch_ && claim.length != 0) {
    const size_t first = size_t(claim.offset / page_size_);
    const size_t last = std::min(
        page_count_, size_t((claim.offset + claim.length - 1) / page_size_) + 1);
    for (size_t page = first; page < last; ++page) {
      set_page_state(page, CACHE_PAGE_BAD);
      fetching_[page] = 0;
    }
  }
  condition_.notify_all();
}

void CacheEntry::begin_retry(const CacheFetchClaim& claim) {
  std::lock_guard guard(mutex_);
  if (stale_ || claim.epoch != epoch_ || claim.length == 0) {
    return;
  }
  const size_t first = size_t(claim.offset / page_size_);
  const size_t last = std::min(
      page_count_, size_t((claim.offset + claim.length - 1) / page_size_) + 1);
  for (size_t page = first; page < last; ++page) {
    set_page_state(page, CACHE_PAGE_BAD);
    fetching_[page] = 1;
  }
  condition_.notify_all();
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

bool CacheEntry::begin_checksum_manifest() {
  std::unique_lock guard(mutex_);
  condition_.wait(guard, [&] {
    return stale_ || checksum_manifest_ != kChecksumLoading;
  });
  if (stale_) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "stale cache generation");
  }
  if (checksum_manifest_ != kChecksumUnknown) {
    return false;
  }
  checksum_manifest_ = kChecksumLoading;
  ++checksum_ops_;
  return true;
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
  condition_.notify_all();
}

void CacheEntry::checksum_manifest_unavailable() noexcept {
  std::lock_guard guard(mutex_);
  checksum_parts_.clear();
  checksum_states_.clear();
  checksum_manifest_ = kChecksumUnavailable;
  if (checksum_ops_ != 0) {
    --checksum_ops_;
  }
  condition_.notify_all();
}

bool CacheEntry::checksum_manifest_available() const noexcept {
  std::lock_guard guard(mutex_);
  return !stale_ && checksum_manifest_ == kChecksumAvailable;
}

CacheChecksumClaim CacheEntry::claim_checksum(uint64_t offset,
                                               size_t length) {
  std::lock_guard guard(mutex_);
  if (stale_) {
    throw std::system_error(ESTALE, std::generic_category(),
                            "stale cache generation");
  }
  if (checksum_manifest_ != kChecksumAvailable || length == 0) {
    return {};
  }
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
      CacheChecksumClaim claim;
      claim.action = CACHE_CHECKSUM_WAIT;
      return claim;
    }
    if (state != kPartUnverified ||
        !range_all_state(part.offset, size_t(part.size),
                         CACHE_PAGE_CLEAN)) {
      continue;
    }
    const size_t region_size = std::max<size_t>(1024U * 1024U, page_size_);
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
  condition_.wait(guard, [&] {
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
  });
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
  condition_.notify_all();
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
  const size_t region_size = std::max<size_t>(1024U * 1024U, page_size_);
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
  condition_.notify_all();
}

void CacheEntry::retire_generation() {
  std::unique_lock guard(mutex_);
  stale_ = true;
  condition_.notify_all();
  condition_.wait(guard, [&] {
    return active_claims_.empty() && checksum_ops_ == 0 &&
        pinned_regions_ == 0;
  });
}

uint64_t CacheEntry::evict_one_region() noexcept {
  try {
    std::unique_lock guard(mutex_);
    if (stale_ || eviction_disabled_ || referenced_.empty()) {
      return 0;
    }
    const size_t region_size = std::max<size_t>(1024U * 1024U, page_size_);
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
        if ((state != CACHE_PAGE_CLEAN && state != CACHE_PAGE_MISSING) ||
            fetching_[page]) {
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
                      off_t(offset), off_t(end - offset)) != 0) {
        for (size_t page : clean_pages) {
          set_page_state(page, CACHE_PAGE_CLEAN);
        }
        return 0;
      }
      struct stat after{};
      if (::fstat(data_fd_, &after) != 0) {
        return 0;
      }
      condition_.notify_all();
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

bool LocalCache::reserve_capacity(uint64_t bytes) {
  for (;;) {
    {
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
           bytes <= config_.maximum_bytes - allocated -
               pending_reservations_);
      const bool free_ok = available >= floor &&
          pending_reservations_ <= available - floor &&
          bytes <= available - floor - pending_reservations_;
      if (maximum_ok && free_ok) {
        pending_reservations_ += bytes;
        return true;
      }
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

bool LocalCache::reclaim_closed_clean(
    const std::shared_ptr<CacheEntry>& entry) noexcept {
  try {
    std::lock_guard key_guard(key_mutex(entry->key_));
    std::unique_lock guard(mutex_);
    auto found = entries_.end();
    for (auto i = entries_.begin(); i != entries_.end();) {
      std::shared_ptr<CacheEntry> candidate = i->lock();
      if (!candidate) {
        i = entries_.erase(i);
        continue;
      }
      if (candidate.get() == entry.get()) {
        if (candidate.use_count() != 2) {
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
          (header.flags & kCacheMetaDirty) != 0 ||
          !entry->active_claims_.empty() || entry->checksum_ops_ != 0 ||
          entry->pinned_regions_ != 0) {
        return false;
      }
      for (size_t page = 0; page < entry->page_count_; ++page) {
        if (entry->page_state(page) != CACHE_PAGE_MISSING ||
            entry->fetching_[page]) {
          return false;
        }
      }
      entry->detached_ = true;
      entry->stale_    = true;
    }
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
      config_.maximum_fetch_size < config_.page_size ||
      config_.maximum_fetch_size % config_.page_size != 0) {
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

void LocalCache::finish_pending_delete(std::string_view key) noexcept {
  try {
    struct statvfs fs{};
    if (::fstatvfs(root_fd_, &fs) != 0) {
      cache_throw_errno("fstatvfs(cache pending-delete removal)");
    }
    const size_t name_max = fs.f_namemax == 0 ? NAME_MAX : fs.f_namemax;
    std::optional<CacheLeaf> leaf = cache_find_leaf(
        pending_root_fd_, key, name_max);
    if (!leaf) {
      return;
    }
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

std::shared_ptr<CacheEntry> LocalCache::create_writer(
    const CacheIdentity& base, uint64_t maximum_size) {
  const std::string_view key = base.key;
  if (key.size() > kCacheKeyCapacity ||
      base.etag.size() > kCacheEtagCapacity ||
      base.version_id.size() > kCacheVersionCapacity ||
      maximum_size == 0) {
    throw std::invalid_argument("invalid cache writer identity");
  }
  std::lock_guard key_guard(key_mutex(key));
  std::unique_lock guard(mutex_);
  for (auto i = entries_.begin(); i != entries_.end();) {
    std::shared_ptr<CacheEntry> entry = i->lock();
    if (!entry) {
      i = entries_.erase(i);
      continue;
    }
    if (entry->key_ == key) {
      i = entries_.erase(i);
      guard.unlock();
      entry->retire_generation();
      guard.lock();
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
        config_.page_size, epoch);
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
                        bool preserve_generation) noexcept {
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
        i = entries_.erase(i);
        continue;
      }
      ++i;
    }
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
        target->retire_generation();
      }
    } else {
      guard.unlock();
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
                        std::string_view new_key) noexcept {
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
  remove(new_key, false);
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
  remove(old_key, false);
  remove(new_key, false);
  return false;
}

std::shared_ptr<CacheEntry> LocalCache::open(
    const CacheIdentity& identity) {
  if (identity.key.size() > kCacheKeyCapacity ||
      identity.etag.size() > kCacheEtagCapacity ||
      identity.version_id.size() > kCacheVersionCapacity) {
    throw std::system_error(EOVERFLOW, std::generic_category(),
                            "S3 identity is too large for cache metadata");
  }
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
                config_.page_size);
      }
      if (matches) {
        return entry;
      }
      i = entries_.erase(i);
      guard.unlock();
      entry->retire_generation();
      guard.lock();
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
  int meta_fd = -1;
  void* mapping = MAP_FAILED;
  const size_t required_mapping_size =
      cache_mapping_size(identity.size, config_.page_size);
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
          config_.page_size);
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
      if (mapping != MAP_FAILED) {
        ::munmap(mapping, mapping_size);
        mapping = MAP_FAILED;
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
                              identity, config_.page_size, epoch);
    }

    auto entry = std::shared_ptr<CacheEntry>(new CacheEntry(
        *this, std::string(identity.key), data_fd, meta_fd, -1, mapping,
        mapping_size, identity.size));
    data_fd = -1;
    meta_fd = -1;
    mapping = MAP_FAILED;
    guard.lock();
    entries_.push_back(entry);
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
