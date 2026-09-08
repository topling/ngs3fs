#include "cache.hpp"
#include "io.hpp"

#include <fcntl.h>
#include <linux/stat.h>
#include <sys/mman.h>
#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <algorithm>
#include <assert.h>
#include <chrono>
#include <deque>
#include <filesystem>
#include <future>
#include <string.h>

struct CacheTestAccess {
  static std::recursive_mutex& key_mutex(LocalCache& cache, std::string_view key) {
    return cache.key_mutex(key);
  }
  static bool evict_cold(LocalCache& cache) { return cache.evict_cold(); }
  static bool evict_one(LocalCache& cache) { return cache.evict_one(); }
  static uint64_t allocated_bytes(LocalCache& cache) {
    return cache.allocated_bytes();
  }
  static int data_root_fd(LocalCache& cache) { return cache.data_root_fd_; }
  static size_t keepalive_entries(LocalCache& cache) {
    std::lock_guard guard(cache.mutex_);
    return cache.keepalive_.size();
  }
  static size_t keepalive_base_metadata_bytes(LocalCache& cache) {
    std::lock_guard guard(cache.mutex_);
    return cache.keepalive_base_metadata_bytes_;
  }
  static size_t keepalive_entry_limit() {
    return LocalCache::kKeepaliveEntryLimit;
  }
  static size_t keepalive_base_metadata_limit() {
    return LocalCache::kKeepaliveBaseMetadataLimit;
  }
  static void clear_keepalive(LocalCache& cache) {
    std::vector<LocalCache::KeepaliveSlot> released;
    {
      std::lock_guard guard(cache.mutex_);
      released.swap(cache.keepalive_);
      cache.keepalive_base_metadata_bytes_ = 0;
    }
  }
  static void set_maximum_bytes(LocalCache& cache, uint64_t maximum_bytes) {
    cache.config_.maximum_bytes = maximum_bytes;
    cache.config_.unlimited = false;
  }
  static uint64_t pending_reservations(LocalCache& cache) {
    std::lock_guard guard(cache.capacity_mutex_);
    return cache.pending_reservations_;
  }
  static bool reclaim_closed_async(
      LocalCache& cache, IoExecutor& executor, CacheAsyncRequest& request,
      const std::shared_ptr<CacheEntry>& expected) {
    return cache.reclaim_closed_async(executor, request, expected);
  }
};

struct TemporaryDirectory {
  TemporaryDirectory() {
    std::array<char, 64> pattern{};
    constexpr char value[] = "/tmp/ngs3fs-cache-test-XXXXXX";
    memcpy(pattern.data(), value, sizeof(value));
    char* created = ::mkdtemp(pattern.data());
    assert(created != nullptr);
    path = created;
  }

  ~TemporaryDirectory() { std::filesystem::remove_all(path); }

  std::string path;
};

class DeferredIoExecutor : public IoExecutor {
 public:
  bool submit(AsyncIoRequest& request) noexcept override {
    try {
      pending_.push_back(&request);
    } catch (...) {
      errno = ENOMEM;
      return false;
    }
    return true;
  }

  bool cancel(AsyncIoRequest&) noexcept override { return false; }

  void fail_next(AsyncIoRequest::Kind kind, int error) noexcept {
    fail_kind_ = kind;
    fail_error_ = error;
  }

  bool pump_one() {
    auto selected = pending_.begin();
    while (selected != pending_.end() &&
           (*selected)->kind == AsyncIoRequest::READ) {
      pollfd descriptor{.fd = (*selected)->fd, .events = POLLIN};
      if (::poll(&descriptor, 1, 0) > 0) break;
      ++selected;
    }
    if (selected == pending_.end()) return false;
    AsyncIoRequest* request = *selected;
    pending_.erase(selected);
    ssize_t result;
    if (fail_error_ != 0 && request->kind == fail_kind_) {
      result = -fail_error_;
      fail_error_ = 0;
    } else {
      result = perform(*request);
    }
    request->complete(request->context, result);
    return true;
  }

  void drain() {
    size_t operations = 0;
    while (pump_one()) {
      assert(++operations < 256);
    }
  }

 private:
  static ssize_t perform(AsyncIoRequest& request) noexcept {
    int result;
    switch (request.kind) {
      case AsyncIoRequest::READ: {
        const ssize_t bytes = ::read(
            request.fd, request.data, request.length);
        return bytes < 0 ? -errno : bytes;
      }
      case AsyncIoRequest::OPENAT:
        result = ::openat(request.fd, request.path, int(request.flags),
                          mode_t(request.mode));
        return result < 0 ? -errno : result;
      case AsyncIoRequest::CLOSE:
        result = ::close(request.fd);
        return result < 0 ? -errno : result;
      case AsyncIoRequest::STATX:
        result = ::statx(request.fd, request.path, int(request.flags),
                         request.mask,
                         static_cast<struct statx*>(request.data));
        return result < 0 ? -errno : result;
      case AsyncIoRequest::FALLOCATE:
        result = ::fallocate(request.fd, int(request.flags),
                             request.input_offset, off_t(request.length));
        return result < 0 ? -errno : result;
      case AsyncIoRequest::FSYNC:
        result = request.flags != 0
            ? ::fdatasync(request.fd) : ::fsync(request.fd);
        return result < 0 ? -errno : result;
      case AsyncIoRequest::FTRUNCATE:
        result = ::ftruncate(request.fd, off_t(request.length));
        return result < 0 ? -errno : result;
      case AsyncIoRequest::MKDIRAT:
        result = ::mkdirat(request.fd, request.path, mode_t(request.mode));
        return result < 0 ? -errno : result;
      case AsyncIoRequest::MADVISE:
        result = ::madvise(request.data, request.length, int(request.flags));
        return result < 0 ? -errno : result;
      case AsyncIoRequest::PREAD: {
        const ssize_t bytes = ::pread(
            request.fd, request.data, request.length, request.input_offset);
        return bytes < 0 ? -errno : bytes;
      }
      case AsyncIoRequest::PWRITE: {
        const ssize_t bytes = ::pwrite(
            request.fd, request.data, request.length, request.output_offset);
        return bytes < 0 ? -errno : bytes;
      }
      case AsyncIoRequest::UNLINKAT:
        result = ::unlinkat(request.fd, request.path, int(request.flags));
        return result < 0 ? -errno : result;
      case AsyncIoRequest::RENAMEAT:
        result = ::renameat(request.fd, request.path,
                            request.output_fd, request.path2);
        return result < 0 ? -errno : result;
      default:
        return -EOPNOTSUPP;
    }
  }

  std::deque<AsyncIoRequest*> pending_;
  AsyncIoRequest::Kind fail_kind_ = AsyncIoRequest::RECEIVE;
  int fail_error_ = 0;
};

struct AsyncCacheCapture {
  bool done = false;
  CacheAsyncResult result;
  std::vector<unsigned>* order = nullptr;
  unsigned sequence = 0;

  static void completed(void* context, CacheAsyncResult result) noexcept {
    auto& capture = *static_cast<AsyncCacheCapture*>(context);
    capture.done = true;
    capture.result = std::move(result);
    if (capture.order != nullptr) capture.order->push_back(capture.sequence);
  }
};

void write_test_bytes(int fd, uint64_t offset, size_t length) {
  std::array<std::byte, 4096> bytes;
  for (size_t i = 0; i < bytes.size(); ++i) {
    bytes[i] = std::byte(i & 255);
  }
  size_t written = 0;
  while (written != length) {
    const size_t count = std::min(bytes.size(), length - written);
    const ssize_t result = ::pwrite(fd, bytes.data(), count,
                                    off_t(offset + written));
    assert(result > 0);
    written += size_t(result);
  }
}

CacheIdentity test_identity(std::string_view key, std::string_view etag,
                            uint64_t size) {
  return CacheIdentity{
      .key = key,
      .etag = etag,
      .version_id = {},
      .size = size,
      .mtime = 123,
  };
}

int main() {
  assert(cache_encode_component("normal name-中文", 255) ==
         "normal name-中文");
  assert(cache_encode_component(".", 255).starts_with(".~ngs3fs~.b"));
  assert(cache_encode_component(".~ngs3fs~.value", 255)
             .starts_with(".~ngs3fs~.b"));
  assert(cache_encode_component(std::string(400, 'x'), 255)
             .starts_with(".~ngs3fs~.h"));

  TemporaryDirectory directory;
  CacheConfig config{
      .root = directory.path,
      .namespace_id = "test endpoint / test bucket / test prefix",
      .maximum_bytes = 0,
      .reserve_bytes = 0,
      .reserve_percent = 5,
      .max_prefetch_window_size = 8U * 1024U * 1024U,
      .page_size = 4096,
      .block_size = kCacheBitmapUnit,
      .upload_part_size = 8U * 1024U * 1024U,
      .reserve_is_percent = true,
  };
  std::string recovery_write_id;

  {
    TemporaryDirectory async_directory;
    CacheConfig async_config = config;
    async_config.root = async_directory.path;
    LocalCache cache(async_config);
    DeferredIoExecutor executor;

    const CacheIdentity reader_identity = test_identity(
        "async/cache-read", "etag", 2 * kCacheBitmapUnit);
    CacheAsyncRequest open_request;
    AsyncCacheCapture open_capture;
    open_request.complete = AsyncCacheCapture::completed;
    open_request.context = &open_capture;
    assert(cache.open_async(executor, open_request, reader_identity));
    assert(!open_capture.done);
    executor.drain();
    if (open_capture.result.error) {
      try {
        std::rethrow_exception(open_capture.result.error);
      } catch (const std::exception& error) {
        fprintf(stderr, "async cold open failed: %s\n", error.what());
      }
    }
    assert(open_capture.done && !open_capture.result.error);
    std::shared_ptr<CacheEntry> reader = std::move(open_capture.result.entry);
    assert(reader && reader->key() == reader_identity.key);

    CacheAsyncRequest reopen_request;
    AsyncCacheCapture reopen_capture;
    reopen_request.complete = AsyncCacheCapture::completed;
    reopen_request.context = &reopen_capture;
    assert(cache.open_async(executor, reopen_request, reader_identity));
    assert(!reopen_capture.done);
    executor.drain();
    assert(reopen_capture.done && !reopen_capture.result.error);
    assert(reopen_capture.result.entry.get() == reader.get());
    CacheAsyncRequest read_request;
    AsyncCacheCapture read_capture;
    std::vector<unsigned> read_order;
    read_capture.order = &read_order;
    read_capture.sequence = 1;
    read_request.complete = AsyncCacheCapture::completed;
    read_request.context = &read_capture;
    assert(reader->prepare_read_async(
        executor, read_request, 0, kCacheBitmapUnit));
    assert(read_request.pending());
    assert(!read_capture.done);

    CacheAsyncRequest contended_request;
    AsyncCacheCapture contended_capture;
    contended_capture.order = &read_order;
    contended_capture.sequence = 2;
    contended_request.complete = AsyncCacheCapture::completed;
    contended_request.context = &contended_capture;
    assert(reader->prepare_read_async(
        executor, contended_request, kCacheBitmapUnit, kCacheBitmapUnit));
    assert(contended_request.pending());
    assert(!contended_capture.done);

    CacheAsyncRequest rejected_request;
    AsyncCacheCapture rejected_capture;
    rejected_capture.order = &read_order;
    rejected_capture.sequence = 3;
    rejected_request.complete = AsyncCacheCapture::completed;
    rejected_request.context = &rejected_capture;
    assert(reader->mark_commit_pending_async(executor, rejected_request));
    assert(rejected_request.pending());
    assert(!rejected_capture.done);

    executor.drain();
    assert(read_capture.done && !read_capture.result.error);
    assert(read_capture.result.value);
    assert(!read_request.pending());
    assert(contended_capture.done && !contended_capture.result.error);
    assert(contended_capture.result.value);
    assert(!contended_request.pending());
    assert(rejected_capture.done && rejected_capture.result.error);
    assert(!rejected_request.pending());
    assert((read_order == std::vector<unsigned>{1, 2, 3}));

    DeferredIoExecutor other_executor;
    CacheAsyncRequest owner_request;
    AsyncCacheCapture owner_capture;
    owner_request.complete = AsyncCacheCapture::completed;
    owner_request.context = &owner_capture;
    assert(reader->prepare_read_async(
        executor, owner_request, 0, kCacheBitmapUnit));
    CacheAsyncRequest other_owner_request;
    AsyncCacheCapture other_owner_capture;
    other_owner_request.complete = AsyncCacheCapture::completed;
    other_owner_request.context = &other_owner_capture;
    assert(reader->prepare_read_async(
        other_executor, other_owner_request,
        kCacheBitmapUnit, kCacheBitmapUnit));
    other_executor.drain();
    assert(!other_owner_capture.done);
    executor.drain();
    assert(owner_capture.done && !owner_capture.result.error);
    other_executor.drain();
    assert(other_owner_capture.done && !other_owner_capture.result.error);

    const CacheIdentity writer_identity =
        test_identity("async/cache-write", "old", 0);
    CacheAsyncRequest writer_request;
    AsyncCacheCapture writer_capture;
    writer_request.complete = AsyncCacheCapture::completed;
    writer_request.context = &writer_capture;
    assert(cache.create_writer_async(
        executor, writer_request, writer_identity, 8U * 1024U * 1024U));
    assert(!writer_capture.done);
    executor.drain();
    assert(writer_capture.done && !writer_capture.result.error);
    std::shared_ptr<CacheEntry> writer = std::move(writer_capture.result.entry);
    assert(writer && writer->key() == writer_identity.key);
    CacheAsyncRequest prepare_request;
    AsyncCacheCapture prepare_capture;
    std::vector<unsigned> writer_order;
    prepare_capture.order = &writer_order;
    prepare_capture.sequence = 1;
    prepare_request.complete = AsyncCacheCapture::completed;
    prepare_request.context = &prepare_capture;
    assert(writer->prepare_write_async(executor, prepare_request, 0, 4096));
    assert(!prepare_capture.done);

    CacheAsyncRequest upload_request;
    AsyncCacheCapture upload_capture;
    upload_capture.order = &writer_order;
    upload_capture.sequence = 2;
    upload_request.complete = AsyncCacheCapture::completed;
    upload_request.context = &upload_capture;
    std::string queued_upload_id = "async-upload";
    assert(writer->set_upload_id_async(
        executor, upload_request, queued_upload_id));
    assert(!upload_capture.done);
    queued_upload_id = "mutated-after-queue";
    executor.drain();
    assert(prepare_capture.done && !prepare_capture.result.error);
    assert(prepare_capture.result.value && writer->dirty());
    assert(upload_capture.done && !upload_capture.result.error);
    assert(writer->upload_id() == "async-upload");
    assert((writer_order == std::vector<unsigned>{1, 2}));

    write_test_bytes(writer->data_fd(), 0, 4096);
    writer->publish_dirty(0, 4096, 4096);

    CacheAsyncRequest sync_request;
    AsyncCacheCapture sync_capture;
    sync_request.complete = AsyncCacheCapture::completed;
    sync_request.context = &sync_capture;
    assert(writer->sync_write_async(executor, sync_request));
    assert(!sync_capture.done);
    executor.drain();
    assert(sync_capture.done && !sync_capture.result.error);
    assert(sync_capture.result.value);

    CacheAsyncRequest pending_request;
    AsyncCacheCapture pending_capture;
    pending_request.complete = AsyncCacheCapture::completed;
    pending_request.context = &pending_capture;
    assert(writer->mark_commit_pending_async(executor, pending_request));
    assert(!pending_capture.done);
    executor.drain();
    assert(pending_capture.done && !pending_capture.result.error);

    CacheAsyncRequest commit_request;
    AsyncCacheCapture commit_capture;
    commit_request.complete = AsyncCacheCapture::completed;
    commit_request.context = &commit_capture;
    const CacheIdentity committed =
        test_identity("async/cache-write", "new", 4096);
    assert(writer->commit_write_async(executor, commit_request, committed));
    assert(!commit_capture.done);
    executor.drain();
    assert(commit_capture.done && !commit_capture.result.error);
    assert(commit_capture.result.value);
    assert(!writer->dirty());
    assert(writer->etag() == "new");

    CacheAsyncRequest rename_request;
    AsyncCacheCapture rename_capture;
    rename_request.complete = AsyncCacheCapture::completed;
    rename_request.context = &rename_capture;
    assert(cache.rename_async(executor, rename_request,
                              "async/cache-write", "async/cache-renamed"));
    executor.drain();
    assert(rename_capture.done && !rename_capture.result.error);
    assert(rename_capture.result.value);
    assert(writer->key() == "async/cache-renamed");

    CacheAsyncRequest remove_request;
    AsyncCacheCapture remove_capture;
    remove_request.complete = AsyncCacheCapture::completed;
    remove_request.context = &remove_capture;
    assert(cache.remove_async(
        executor, remove_request, "async/cache-renamed", true));
    executor.drain();
    assert(remove_capture.done && !remove_capture.result.error);
    assert(remove_capture.result.value);
    assert(writer->range_clean(0, 4096));

    const CacheIdentity empty_identity =
        test_identity("async/empty-writer", "old", 0);
    CacheAsyncRequest empty_create_request;
    AsyncCacheCapture empty_create_capture;
    empty_create_request.complete = AsyncCacheCapture::completed;
    empty_create_request.context = &empty_create_capture;
    assert(cache.create_writer_async(executor, empty_create_request,
                                     empty_identity, 8U * 1024U * 1024U));
    executor.drain();
    assert(empty_create_capture.done && !empty_create_capture.result.error);
    std::shared_ptr<CacheEntry> empty_writer =
        std::move(empty_create_capture.result.entry);
    CacheAsyncRequest begin_request;
    AsyncCacheCapture begin_capture;
    begin_request.complete = AsyncCacheCapture::completed;
    begin_request.context = &begin_capture;
    assert(empty_writer->begin_write_async(executor, begin_request));
    assert(!begin_capture.done);
    executor.drain();
    assert(begin_capture.done && !begin_capture.result.error);
    assert(empty_writer->dirty());

    CacheAsyncRequest discard_request;
    AsyncCacheCapture discard_capture;
    discard_request.complete = AsyncCacheCapture::completed;
    discard_request.context = &discard_capture;
    assert(empty_writer->discard_write_async(executor, discard_request));
    executor.drain();
    assert(discard_capture.done && !discard_capture.result.error);
    assert(empty_writer->stale());

    CacheAsyncRequest pending_create_request;
    AsyncCacheCapture pending_create_capture;
    pending_create_request.complete = AsyncCacheCapture::completed;
    pending_create_request.context = &pending_create_capture;
    assert(cache.create_pending_delete_async(
        executor, pending_create_request, "async/pending",
        "async/restore", "replacement"));
    executor.drain();
    assert(pending_create_capture.done && !pending_create_capture.result.error);
    std::vector<CachePendingDelete> pending = cache.recover_pending_deletes();
    assert(pending.size() == 1 && pending.front().rollback);

    CacheAsyncRequest pending_commit_request;
    AsyncCacheCapture pending_commit_capture;
    pending_commit_request.complete = AsyncCacheCapture::completed;
    pending_commit_request.context = &pending_commit_capture;
    assert(cache.commit_pending_delete_async(
        executor, pending_commit_request, "async/pending"));
    executor.drain();
    assert(pending_commit_capture.done && !pending_commit_capture.result.error);
    pending = cache.recover_pending_deletes();
    assert(pending.size() == 1 && !pending.front().rollback);

    CacheAsyncRequest pending_finish_request;
    AsyncCacheCapture pending_finish_capture;
    pending_finish_request.complete = AsyncCacheCapture::completed;
    pending_finish_request.context = &pending_finish_capture;
    assert(cache.finish_pending_delete_async(
        executor, pending_finish_request, "async/pending"));
    executor.drain();
    assert(pending_finish_capture.done && !pending_finish_capture.result.error);
    assert(cache.recover_pending_deletes().empty());

    CacheAsyncRequest prefix_request;
    AsyncCacheCapture prefix_capture;
    prefix_request.complete = AsyncCacheCapture::completed;
    prefix_request.context = &prefix_capture;
    CacheAsyncRequest child_request;
    AsyncCacheCapture child_capture;
    child_request.complete = AsyncCacheCapture::completed;
    child_request.context = &child_capture;
    assert(cache.open_async(
        executor, prefix_request, test_identity("promote", "p", 0)));
    assert(cache.open_async(
        executor, child_request,
        test_identity("promote/child", "c", 0)));
    assert(prefix_request.pending() && child_request.pending());
    executor.drain();
    assert(prefix_capture.done && !prefix_capture.result.error);
    assert(child_capture.done && !child_capture.result.error);
    assert(prefix_capture.result.entry->key() == "promote");
    assert(child_capture.result.entry->key() == "promote/child");

    assert(::symlinkat("/tmp", CacheTestAccess::data_root_fd(cache),
                       "unsafe") == 0);
    CacheAsyncRequest unsafe_request;
    AsyncCacheCapture unsafe_capture;
    unsafe_request.complete = AsyncCacheCapture::completed;
    unsafe_request.context = &unsafe_capture;
    assert(cache.open_async(
        executor, unsafe_request,
        test_identity("unsafe/child", "unsafe", 0)));
    executor.drain();
    assert(unsafe_capture.done && unsafe_capture.result.error);
    assert(::unlinkat(CacheTestAccess::data_root_fd(cache), "unsafe", 0) == 0);
  }

  {
    TemporaryDirectory exported_directory;
    CacheConfig exported_config = config;
    exported_config.root = exported_directory.path;
    exported_config.unlimited = true;
    LocalCache cache(exported_config);
    DeferredIoExecutor executor;
    auto old = cache.open(test_identity("async/exported", "old", 4096));
    CacheFetchClaim claim = old->claim_fetch(0, 4096, 4096);
    assert(old->prepare_read(claim.offset, claim.length));
    write_test_bytes(old->data_fd(), 0, claim.length);
    old->publish_clean(claim, 0, claim.length, true);
    old->finish_fetch(claim);
    assert(old->try_export(false));
    struct stat old_status{};
    assert(::fstat(old->data_fd(), &old_status) == 0);

    CacheAsyncRequest replacement_request;
    AsyncCacheCapture replacement_capture;
    replacement_request.complete = AsyncCacheCapture::completed;
    replacement_request.context = &replacement_capture;
    assert(cache.open_async(
        executor, replacement_request,
        test_identity("async/exported", "new", 64 * kCacheBitmapUnit)));
    executor.drain();
    assert(replacement_capture.done && !replacement_capture.result.error);
    auto replacement = std::move(replacement_capture.result.entry);
    struct stat replacement_status{};
    assert(replacement &&
           ::fstat(replacement->data_fd(), &replacement_status) == 0);
    assert(old_status.st_dev != replacement_status.st_dev ||
           old_status.st_ino != replacement_status.st_ino);
    struct stat retained_status{};
    assert(::fstat(old->data_fd(), &retained_status) == 0 &&
           retained_status.st_size == old_status.st_size);
    std::array<std::byte, 16> bytes{};
    assert(::pread(old->data_fd(), bytes.data(), bytes.size(), 0) ==
           ssize_t(bytes.size()));
    for (size_t i = 0; i < bytes.size(); ++i) {
      assert(bytes[i] == std::byte(i));
    }
  }

  {
    TemporaryDirectory budget_directory;
    CacheConfig budget_config = config;
    budget_config.root = budget_directory.path;
    budget_config.unlimited = true;
    LocalCache cache(budget_config);
    DeferredIoExecutor executor;
    constexpr uint64_t old_size = 1024 * 1024;
    auto old = cache.open(test_identity("async/budget", "old", old_size));
    CacheFetchClaim claim = old->claim_fetch(0, old_size, old_size);
    assert(old->prepare_read(claim.offset, claim.length));
    write_test_bytes(old->data_fd(), 0, claim.length);
    old->publish_clean(claim, 0, claim.length, true);
    old->finish_fetch(claim);
    struct stat before_status{};
    assert(::fstat(old->data_fd(), &before_status) == 0);
    const uint64_t before = CacheTestAccess::allocated_bytes(cache);
    CacheTestAccess::set_maximum_bytes(cache, before);

    CacheAsyncRequest reset_request;
    AsyncCacheCapture reset_capture;
    reset_request.complete = AsyncCacheCapture::completed;
    reset_request.context = &reset_capture;
    assert(cache.open_async(
        executor, reset_request,
        test_identity("async/budget", "new", 0)));
    executor.drain();
    if (reset_capture.result.error) {
      try {
        std::rethrow_exception(reset_capture.result.error);
      } catch (const std::exception& error) {
        fprintf(stderr, "same-key asynchronous cache reset failed: %s\n",
                error.what());
      }
    }
    assert(reset_capture.done && !reset_capture.result.error);
    assert(reset_capture.result.entry);
    struct stat after_status{};
    assert(::fstat(old->data_fd(), &after_status) == 0 &&
           after_status.st_size == 0);
    const uint64_t after = CacheTestAccess::allocated_bytes(cache);
    assert(before_status.st_blocks >= after_status.st_blocks);
    const uint64_t freed_data =
        uint64_t(before_status.st_blocks - after_status.st_blocks) * 512;
    assert(after <= before && before - after >= freed_data);
    assert(CacheTestAccess::pending_reservations(cache) == 0);
  }

  {
    TemporaryDirectory cold_directory;
    CacheConfig cold = config;
    cold.root = cold_directory.path;
    LocalCache cache(cold);
    {
      auto entry = cache.open(test_identity("cold-lock-b", "etag", 4096));
      const auto claim = entry->claim_fetch(0, 4096, 4096);
      assert(entry->prepare_read(0, 4096));
      write_test_bytes(entry->data_fd(), 0, 4096);
      entry->publish_clean(claim, 0, 4096, true);
      entry->finish_fetch(claim);
    }
    auto& first = CacheTestAccess::key_mutex(cache, "reserve-lock-a");
    auto& second = CacheTestAccess::key_mutex(cache, "cold-lock-b");
    assert(&first != &second);
    std::unique_lock held(second);
    std::promise<void> started;
    auto entered = started.get_future();
    auto eviction = std::async(std::launch::async, [&] {
      std::lock_guard reserve_key(first);
      started.set_value();
      return CacheTestAccess::evict_cold(cache);
    });
    entered.get();
    const bool ready = eviction.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    held.unlock();
    const bool evicted = eviction.get();
    assert(ready && !evicted);
    CacheTestAccess::clear_keepalive(cache);
    assert(CacheTestAccess::evict_cold(cache));
  }

  {
    TemporaryDirectory async_cold_directory;
    CacheConfig cold = config;
    cold.root = async_cold_directory.path;
    cold.unlimited = true;
    constexpr uint64_t old_size = 1024 * 1024;
    {
      LocalCache cache(cold);
      auto old = cache.open(test_identity(
          "async/cold-old", "old", old_size));
      const CacheFetchClaim claim = old->claim_fetch(0, old_size, old_size);
      assert(old->prepare_read(claim.offset, claim.length));
      write_test_bytes(old->data_fd(), 0, claim.length);
      old->publish_clean(claim, 0, claim.length, true);
      old->finish_fetch(claim);
    }
    {
      LocalCache cache(cold);
      CacheTestAccess::set_maximum_bytes(
          cache, CacheTestAccess::allocated_bytes(cache));
      DeferredIoExecutor executor;
      CacheAsyncRequest request;
      AsyncCacheCapture capture;
      request.complete = AsyncCacheCapture::completed;
      request.context = &capture;
      assert(cache.open_async(
          executor, request,
          test_identity("async/cold-new", "new", 0)));
      executor.drain();
      assert(capture.done && !capture.result.error && capture.result.entry);
      assert(CacheTestAccess::pending_reservations(cache) == 0);
    }
  }

  {
    TemporaryDirectory metadata_directory;
    CacheConfig bounded = config;
    bounded.root = metadata_directory.path;
    bounded.unlimited = true;
    LocalCache cache(bounded);
    for (unsigned i = 0; i < 8; ++i) {
      auto entry = cache.open(test_identity(
          "async/metadata-" + std::to_string(i), "old", 0));
      assert(entry);
    }
    CacheTestAccess::set_maximum_bytes(
        cache, CacheTestAccess::allocated_bytes(cache));
    DeferredIoExecutor executor;
    CacheAsyncRequest request;
    AsyncCacheCapture capture;
    request.complete = AsyncCacheCapture::completed;
    request.context = &capture;
    assert(cache.open_async(
        executor, request,
        test_identity("async/metadata-new", "new", 0)));
    executor.drain();
    assert(capture.done && !capture.result.error && capture.result.entry);
    assert(CacheTestAccess::pending_reservations(cache) == 0);
  }

  {
    TemporaryDirectory cancellation_directory;
    CacheConfig cancellation = config;
    cancellation.root = cancellation_directory.path;
    cancellation.unlimited = true;
    LocalCache cache(cancellation);
    DeferredIoExecutor executor;
    executor.fail_next(AsyncIoRequest::FALLOCATE, ECANCELED);
    CacheAsyncRequest request;
    AsyncCacheCapture capture;
    request.complete = AsyncCacheCapture::completed;
    request.context = &capture;
    assert(cache.create_writer_async(
        executor, request,
        test_identity("async/cancel-reservation", "", 0),
        8U * 1024U * 1024U));
    executor.drain();
    assert(capture.done && capture.result.error);
    assert(CacheTestAccess::pending_reservations(cache) == 0);
  }

  {
    TemporaryDirectory reclaim_identity_directory;
    CacheConfig reclaim = config;
    reclaim.root = reclaim_identity_directory.path;
    LocalCache cache(reclaim);
    auto old = cache.open(test_identity("async/reclaim-race", "old", 0));
    auto replacement = cache.open(
        test_identity("async/reclaim-race", "replacement", 0));
    assert(old && replacement && old.get() != replacement.get());
    DeferredIoExecutor executor;
    CacheAsyncRequest request;
    AsyncCacheCapture capture;
    request.complete = AsyncCacheCapture::completed;
    request.context = &capture;
    assert(CacheTestAccess::reclaim_closed_async(
        cache, executor, request, old));
    executor.drain();
    assert(capture.done && !capture.result.error && !capture.result.value);
    assert(cache.open(test_identity(
               "async/reclaim-race", "replacement", 0)).get() ==
           replacement.get());
  }

  {
    TemporaryDirectory keepalive_directory;
    CacheConfig keepalive_config = config;
    keepalive_config.root = keepalive_directory.path;
    std::weak_ptr<CacheEntry> surviving;
    {
      LocalCache cache(keepalive_config);
      const CacheIdentity identity =
          test_identity("keepalive-state", "etag", 4096);
      std::shared_ptr<CacheEntry> entry = cache.open(identity);
      const CacheFetchClaim fetch = entry->claim_fetch(0, 4096, 4096);
      assert(entry->prepare_read(fetch.offset, fetch.length));
      write_test_bytes(entry->data_fd(), 0, fetch.length);
      entry->publish_clean(fetch, 0, fetch.length, true);
      entry->finish_fetch(fetch);
      assert(entry->begin_checksum_manifest());
      entry->finish_checksum_manifest({
          CacheChecksumPart{0, 4096, 1, "verified"},
      });
      CacheChecksumClaim checksum = entry->claim_checksum(0, 1);
      assert(checksum.action == CACHE_CHECKSUM_VERIFY);
      entry->finish_checksum(checksum, true);
      surviving = entry;
      CacheEntry* address = entry.get();
      entry.reset();
      assert(!surviving.expired());
      assert(!cache.try_open(
          test_identity("keepalive-state", "different", 4096)));
      std::shared_ptr<CacheEntry> reopened = cache.try_open(identity);
      assert(reopened && reopened.get() == address);
      assert(reopened->checksum_manifest_available());
      assert(reopened->claim_checksum(0, 1).action == CACHE_CHECKSUM_NONE);

      std::promise<void> locked;
      std::promise<void> release;
      auto release_signal = release.get_future();
      auto key_holder = std::async(std::launch::async, [&] {
        std::lock_guard guard(
            CacheTestAccess::key_mutex(cache, identity.key));
        locked.set_value();
        release_signal.wait();
      });
      locked.get_future().get();
      assert(!cache.try_open(identity));
      release.set_value();
      key_holder.get();
    }
    assert(surviving.expired());
  }

  {
    TemporaryDirectory keepalive_count_directory;
    CacheConfig keepalive_config = config;
    keepalive_config.root = keepalive_count_directory.path;
    LocalCache cache(keepalive_config);
    std::weak_ptr<CacheEntry> first;
    for (size_t i = 0; i <= CacheTestAccess::keepalive_entry_limit(); ++i) {
      const std::string key = "keepalive-count-" + std::to_string(i);
      std::shared_ptr<CacheEntry> entry =
          cache.open(test_identity(key, "etag", 4096));
      if (i == 0) first = entry;
    }
    assert(first.expired());
    assert(CacheTestAccess::keepalive_entries(cache) ==
           CacheTestAccess::keepalive_entry_limit());
  }

  {
    TemporaryDirectory keepalive_budget_directory;
    CacheConfig keepalive_config = config;
    keepalive_config.root = keepalive_budget_directory.path;
    keepalive_config.block_size = 128U * 1024U * 1024U;
    keepalive_config.unlimited = true;
    LocalCache cache(keepalive_config);
    std::weak_ptr<CacheEntry> first;
    constexpr uint64_t large_size = 32ULL * 1024ULL * 1024ULL * 1024ULL;
    for (size_t i = 0; i != 40; ++i) {
      const std::string key = "keepalive-budget-" + std::to_string(i);
      std::shared_ptr<CacheEntry> entry =
          cache.open(test_identity(key, "etag", large_size));
      if (i == 0) first = entry;
    }
    assert(first.expired());
    assert(CacheTestAccess::keepalive_entries(cache) < 40);
    assert(CacheTestAccess::keepalive_base_metadata_bytes(cache) <=
           CacheTestAccess::keepalive_base_metadata_limit());
  }

  {
    TemporaryDirectory keepalive_reclaim_directory;
    CacheConfig bounded = config;
    bounded.root               = keepalive_reclaim_directory.path;
    bounded.maximum_bytes      = 0;
    bounded.reserve_bytes      = 0;
    bounded.reserve_percent    = 0;
    bounded.reserve_is_percent = false;
    LocalCache cache(bounded);
    std::shared_ptr<CacheEntry> old = cache.open(
        test_identity("keepalive-reclaim-old", "old", 4096));
    assert(old);
    CacheTestAccess::set_maximum_bytes(
        cache, CacheTestAccess::allocated_bytes(cache));
    std::weak_ptr<CacheEntry> reclaimed = old;
    old.reset();
    std::shared_ptr<CacheEntry> replacement = cache.open(
        test_identity("keepalive-reclaim-new", "new", 4096));
    assert(replacement);
    assert(reclaimed.expired());
  }

  {
    TemporaryDirectory keepalive_writer_directory;
    CacheConfig keepalive_config = config;
    keepalive_config.root = keepalive_writer_directory.path;
    std::weak_ptr<CacheEntry> after_destruction;
    {
      LocalCache cache(keepalive_config);
      const CacheIdentity identity =
          test_identity("keepalive-writer", "old", 4096);
      std::shared_ptr<CacheEntry> old = cache.open(identity);
      std::weak_ptr<CacheEntry> retired = old;
      old.reset();
      std::shared_ptr<CacheEntry> writer =
          cache.create_writer(identity, 8192);
      assert(retired.expired());
      std::weak_ptr<CacheEntry> unretained_writer = writer;
      writer.reset();
      assert(unretained_writer.expired());

      const CacheIdentity first_identity =
          test_identity("keepalive-replacement", "first", 4096);
      std::shared_ptr<CacheEntry> first = cache.open(first_identity);
      std::weak_ptr<CacheEntry> replaced = first;
      first.reset();
      std::shared_ptr<CacheEntry> replacement = cache.open(
          test_identity("keepalive-replacement", "second", 4096));
      assert(replaced.expired());
      after_destruction = replacement;
    }
    assert(after_destruction.expired());
  }

  {
    LocalCache cache(config);
    const auto identity = test_identity("目录/file name", "etag-1", 9000);
    std::shared_ptr<CacheEntry> entry = cache.open(identity);
    assert(!entry->range_clean(0, 4096));
    const CacheFetchClaim claim = entry->claim_fetch(100, 200, 1024 * 1024);
    assert(claim.offset == 0);
    assert(claim.length == 9000);
    assert(!entry->claim_fetch(100, 200, 1024 * 1024));
    auto pending_waiter = std::async(std::launch::async, [&] {
      entry->wait_for_range(100, 200);
      return entry->range_clean(100, 200);
    });
    assert(pending_waiter.wait_for(std::chrono::milliseconds(20)) ==
           std::future_status::timeout);
    write_test_bytes(entry->data_fd(), 0, 4096);
    entry->publish_clean(claim, 0, 4096, false);
    assert(pending_waiter.wait_for(std::chrono::milliseconds(20)) ==
           std::future_status::timeout);
    write_test_bytes(entry->data_fd(), 4096, 9000 - 4096);
    entry->publish_clean(claim, 4096, 9000, true);
    assert(pending_waiter.get());
    assert(entry->range_clean(0, 9000));

    assert(entry->pin_fetch_verification(claim));
    entry->begin_retry(claim);
    auto waiter = std::async(std::launch::async, [&] {
      entry->wait_for_range(0, 4096);
      return entry->range_clean(0, 4096);
    });
    assert(waiter.wait_for(std::chrono::milliseconds(20)) ==
           std::future_status::timeout);
    entry->finish_retry(claim, true);
    assert(waiter.get());

    std::shared_ptr<CacheEntry> unaligned = cache.open(
        test_identity("unaligned", "etag-unaligned", 9000));
    const CacheFetchClaim unaligned_claim = unaligned->claim_fetch(
        100, 4096, 4096);
    assert(unaligned_claim.offset == 0);
    assert(unaligned_claim.length == 9000);
    unaligned->fail_fetch(unaligned_claim);

    std::shared_ptr<CacheEntry> partial_pending = cache.open(
        test_identity("partial-pending", "etag-pending", 8192));
    const CacheFetchClaim one_page = partial_pending->claim_fetch(
        0, 4096, 4096);
    assert(one_page.offset == 0 && one_page.length == 8192);
    auto overlap_waiter = std::async(std::launch::async, [&] {
      partial_pending->wait_for_range(0, 8192);
      return partial_pending->range_clean(0, 4096);
    });
    assert(overlap_waiter.wait_for(std::chrono::milliseconds(20)) ==
           std::future_status::timeout);
    write_test_bytes(partial_pending->data_fd(), 0, 8192);
    partial_pending->publish_clean(one_page, 0, 8192, true);
    partial_pending->finish_fetch(one_page);
    assert(overlap_waiter.get());

    std::shared_ptr<CacheEntry> tail = cache.open(
        test_identity("partial-tail", "etag-tail", 4097));
    const CacheFetchClaim tail_claim = tail->claim_fetch(0, 4097, 4097);
    assert(tail->prepare_read(0, 4097));
    write_test_bytes(tail->data_fd(), 0, 4097);
    tail->publish_clean(tail_claim, 0, 4097, false);
    assert(!tail->range_clean(0, 4096));
    assert(!tail->range_clean(4096, 1));
    tail->publish_clean(tail_claim, 4097, 4097, true);
    assert(tail->range_clean(0, 4097));
    tail->finish_fetch(tail_claim);

    std::shared_ptr<CacheEntry> checksummed = cache.open(
        test_identity("checksummed", "etag-checksummed", 12000));
    assert(checksummed->begin_checksum_manifest());
    auto manifest_waiter = std::async(std::launch::async, [&] {
      return checksummed->begin_checksum_manifest();
    });
    assert(manifest_waiter.wait_for(std::chrono::milliseconds(20)) ==
           std::future_status::timeout);
    checksummed->finish_checksum_manifest({
        CacheChecksumPart{0, 5000, 1, "first"},
        CacheChecksumPart{5000, 7000, 1, "second"},
    });
    assert(!manifest_waiter.get());
    const CacheFetchClaim checksum_fetch = checksummed->claim_fetch(
        0, 12000, 12000);
    assert(checksum_fetch.offset == 0 && checksum_fetch.length == 12000);
    assert(checksummed->prepare_read(checksum_fetch.offset,
                                     checksum_fetch.length));
    write_test_bytes(checksummed->data_fd(), 0, 12000);
    checksummed->publish_clean(checksum_fetch, 0, 12000, true);
    checksummed->finish_fetch(checksum_fetch);
    CacheChecksumClaim second_checksum =
        checksummed->claim_checksum(6000, 1);
    assert(second_checksum.action == CACHE_CHECKSUM_VERIFY);
    assert(second_checksum.offset == 5000 && second_checksum.size == 7000);
    checksummed->finish_checksum(second_checksum, false);
    CacheChecksumClaim bad_checksum = checksummed->claim_checksum(6000, 1);
    assert(bad_checksum.action == CACHE_CHECKSUM_BAD);
    CacheChecksumClaim first_checksum =
        checksummed->claim_checksum(0, 1);
    assert(first_checksum.action == CACHE_CHECKSUM_VERIFY);
    checksummed->finish_checksum(first_checksum, true);
    assert(!checksummed->range_bad(4096, 4096));

    std::shared_ptr<CacheEntry> retried = cache.open(
        test_identity("checksum-retry", "etag-retry", 8192));
    const CacheFetchClaim retry_fetch = retried->claim_fetch(
        0, 8192, 8192);
    assert(retried->prepare_read(retry_fetch.offset, retry_fetch.length));
    write_test_bytes(retried->data_fd(), 0, 8192);
    retried->publish_clean(retry_fetch, 0, retry_fetch.length, true);
    retried->finish_fetch(retry_fetch);
    assert(retried->begin_checksum_manifest());
    retried->finish_checksum_manifest({
        CacheChecksumPart{0, 8192, 1, "value"},
    });
    CacheChecksumClaim retry_checksum = retried->claim_checksum(0, 1);
    assert(retry_checksum.action == CACHE_CHECKSUM_VERIFY);
    retried->checksum_mismatch(retry_checksum);
    assert(retried->claim_checksum(4096, 1).action == CACHE_CHECKSUM_WAIT);
    auto checksum_waiter = std::async(std::launch::async, [&] {
      retried->wait_for_checksum(4096, 1);
      return retried->claim_checksum(4096, 1).action;
    });
    assert(checksum_waiter.wait_for(std::chrono::milliseconds(20)) ==
           std::future_status::timeout);
    retried->finish_checksum(retry_checksum, true);
    assert(checksum_waiter.get() == CACHE_CHECKSUM_NONE);

    std::shared_ptr<CacheEntry> retiring = cache.open(
        test_identity("checksum-retire", "etag-old", 4096));
    const CacheFetchClaim retire_fetch = retiring->claim_fetch(
        0, 4096, 4096);
    assert(retiring->prepare_read(retire_fetch.offset, retire_fetch.length));
    write_test_bytes(retiring->data_fd(), 0, 4096);
    retiring->publish_clean(retire_fetch, 0, retire_fetch.length, true);
    retiring->finish_fetch(retire_fetch);
    assert(retiring->begin_checksum_manifest());
    retiring->finish_checksum_manifest({
        CacheChecksumPart{0, 4096, 1, "value"},
    });
    CacheChecksumClaim retire_checksum = retiring->claim_checksum(0, 1);
    assert(retire_checksum.action == CACHE_CHECKSUM_VERIFY);
    auto replacement = std::async(std::launch::async, [&] {
      return cache.open(
          test_identity("checksum-retire", "etag-new", 4096));
    });
    assert(replacement.wait_for(std::chrono::milliseconds(20)) ==
           std::future_status::timeout);
    retiring->finish_checksum(retire_checksum, true);
    assert(replacement.get()->etag() == "etag-new");

    std::shared_ptr<CacheEntry> parent = cache.open(
        test_identity("collision", "etag-a", 4096));
    std::shared_ptr<CacheEntry> child = cache.open(
        test_identity("collision/child", "etag-b", 4096));
    assert(parent->data_fd() >= 0);
    assert(child->data_fd() >= 0);

    std::shared_ptr<CacheEntry> old = cache.open(
        test_identity("generation", "etag-old", 8192));
    const CacheFetchClaim old_claim = old->claim_fetch(0, 8192, 8192);
    write_test_bytes(old->data_fd(), 0, 8192);
    old->publish_clean(old_claim, 0, 8192, true);
    old->finish_fetch(old_claim);
    assert(old->pin_clean(0, 4096));
    assert(old->pin_clean(4096, 4096));
    auto generation_replacement = std::async(std::launch::async, [&] {
      return cache.open(test_identity("generation", "etag-new", 8192));
    });
    assert(generation_replacement.wait_for(std::chrono::milliseconds(20)) ==
           std::future_status::timeout);
    old->unpin(0, 4096);
    assert(generation_replacement.wait_for(std::chrono::milliseconds(20)) ==
           std::future_status::timeout);
    old->unpin(4096, 4096);
    std::shared_ptr<CacheEntry> current = generation_replacement.get();
    assert(old->stale());
    assert(!old->range_clean(0, 4096));
    assert(!current->range_clean(0, 4096));

    {
      const auto namespace_status = [&](std::string_view key) {
        std::array<struct stat, 2> status{};
        const std::string data = config.root + "/data/" + std::string(key);
        const std::string meta =
            config.root + "/meta/objects/" + std::string(key);
        assert(::stat(data.c_str(), &status[0]) == 0);
        assert(::stat(meta.c_str(), &status[1]) == 0);
        return status;
      };
      const auto assert_namespace_unchanged = [](const auto& before,
                                                 const auto& after) {
        for (size_t i = 0; i != before.size(); ++i) {
          assert(after[i].st_dev == before[i].st_dev);
          assert(after[i].st_ino == before[i].st_ino);
          assert(after[i].st_size == before[i].st_size);
          assert(after[i].st_nlink == before[i].st_nlink);
        }
      };
      const CacheIdentity async_old_identity =
          test_identity("async-generation", "etag-old", 2 * kCacheBitmapUnit);
      const CacheIdentity async_new_identity =
          test_identity("async-generation", "etag-new", 2 * kCacheBitmapUnit);
      std::shared_ptr<CacheEntry> async_old = cache.open(async_old_identity);
      const CacheFetchClaim async_claim =
          async_old->claim_fetch(0, 8192, 2 * kCacheBitmapUnit);
      assert(async_claim.offset == 0 &&
             async_claim.length == 2 * kCacheBitmapUnit);
      const bool prepared =
          async_old->prepare_read(async_claim.offset, async_claim.length);
      assert(prepared);
      write_test_bytes(async_old->data_fd(), 0, kCacheBitmapUnit);
      async_old->publish_clean(async_claim, 0, kCacheBitmapUnit, false);
      const bool pinned = async_old->pin_clean(0, 4096);
      assert(pinned);
      struct stat async_before{};
      const int before_status =
          ::fstat(async_old->data_fd(), &async_before);
      assert(before_status == 0);
      const auto async_namespace = namespace_status(async_old_identity.key);

      IoExecutor reactor_marker;
      IoExecutorScope reactor_scope(&reactor_marker, 0);
      std::shared_ptr<CacheEntry> pending = cache.retiring_entry(
          async_old_identity.key, &async_new_identity);
      assert(pending.get() == async_old.get());
      assert(async_old->stale());
      const int first_retire_fd = async_old->begin_retire_wait();
      assert(first_retire_fd >= 0);

      bool open_retry = false;
      try {
        (void)cache.open(async_new_identity);
      } catch (const std::system_error& error) {
        open_retry = error.code().value() == EAGAIN;
      }
      assert(open_retry);
      pending = cache.retiring_entry(async_old_identity.key);
      assert(pending.get() == async_old.get());

      bool writer_retry = false;
      try {
        (void)cache.create_writer(async_new_identity, 16384);
      } catch (const std::system_error& error) {
        writer_retry = error.code().value() == EAGAIN;
      }
      assert(writer_retry);
      {
        // Background local I/O has no executor, but must still hand a busy
        // generation back to its reactor rather than park the worker pool.
        IoExecutorScope worker_scope(nullptr, 0);
        bool worker_open_retry = false;
        bool worker_write_retry = false;
        try { (void)cache.open(async_new_identity, false); }
        catch (const CacheRetirementPending&) { worker_open_retry = true; }
        try { (void)cache.create_writer(async_new_identity, 16384, false); }
        catch (const CacheRetirementPending&) { worker_write_retry = true; }
        assert(worker_open_retry && worker_write_retry);
        bool worker_remove_retry = false;
        const bool worker_removed = cache.remove(
            async_old_identity.key, false, &worker_remove_retry, false);
        assert(!worker_removed && worker_remove_retry);
        assert(cache.retiring_entry(async_old_identity.key).get() ==
               async_old.get());
        assert_namespace_unchanged(
            async_namespace, namespace_status(async_old_identity.key));
      }
      struct stat async_after{};
      const int after_status = ::fstat(async_old->data_fd(), &async_after);
      assert(after_status == 0);
      assert(async_after.st_dev == async_before.st_dev &&
             async_after.st_ino == async_before.st_ino &&
             async_after.st_size == async_before.st_size);

      bool remove_retry = false;
      const bool removed = cache.remove(
          async_old_identity.key, false, &remove_retry);
      assert(!removed && remove_retry);
      pending = cache.retiring_entry(async_old_identity.key);
      assert(pending.get() == async_old.get());

      async_old->finish_fetch(async_claim);
      uint64_t notification = 0;
      const ssize_t first_wake = ::read(
          first_retire_fd, &notification, sizeof(notification));
      assert(first_wake == ssize_t(sizeof(notification)));
      async_old->end_async_wait();
      const int pinned_retire_fd = async_old->begin_retire_wait();
      assert(pinned_retire_fd >= 0);
      async_old->unpin(0, 4096);
      notification = 0;
      const ssize_t pinned_wake = ::read(
          pinned_retire_fd, &notification, sizeof(notification));
      assert(pinned_wake == ssize_t(sizeof(notification)));
      async_old->end_async_wait();
      const int retired = async_old->begin_retire_wait();
      assert(retired == -1);
      std::shared_ptr<CacheEntry> async_current =
          cache.open(async_new_identity);
      assert(async_current->etag() == "etag-new");
      assert(async_current.get() != async_old.get());
      {
        IoExecutorScope worker_scope(nullptr, 0);
        remove_retry = true;
        (void)cache.remove(async_new_identity.key, false, &remove_retry, false);
        assert(!remove_retry);
        assert(!std::filesystem::exists(
            config.root + "/data/async-generation"));
        assert(!std::filesystem::exists(
            config.root + "/meta/objects/async-generation"));
      }

      const CacheIdentity rename_source_identity =
          test_identity("async-rename-source", "source-etag", 4096);
      const CacheIdentity rename_destination_identity =
          test_identity("async-rename-destination", "destination-etag",
                        2 * kCacheBitmapUnit);
      std::shared_ptr<CacheEntry> rename_source =
          cache.open(rename_source_identity);
      std::shared_ptr<CacheEntry> rename_destination =
          cache.open(rename_destination_identity);
      const CacheFetchClaim rename_claim =
          rename_destination->claim_fetch(
              0, 8192, 2 * kCacheBitmapUnit);
      const bool rename_prepared = rename_destination->prepare_read(
          rename_claim.offset, rename_claim.length);
      assert(rename_prepared);
      write_test_bytes(rename_destination->data_fd(), 0, kCacheBitmapUnit);
      rename_destination->publish_clean(
          rename_claim, 0, kCacheBitmapUnit, false);
      const bool rename_pinned = rename_destination->pin_clean(0, 4096);
      assert(rename_pinned);
      const auto source_namespace = namespace_status(rename_source_identity.key);
      const auto destination_namespace =
          namespace_status(rename_destination_identity.key);
      bool rename_retry = false;
      const bool renamed_busy = cache.rename(
          rename_source_identity.key, rename_destination_identity.key,
          &rename_retry);
      assert(!renamed_busy && rename_retry);
      {
        IoExecutorScope worker_scope(nullptr, 0);
        bool worker_rename_retry = false;
        const bool worker_renamed = cache.rename(
            rename_source_identity.key, rename_destination_identity.key,
            &worker_rename_retry, false);
        assert(!worker_renamed && worker_rename_retry);
        assert_namespace_unchanged(
            source_namespace, namespace_status(rename_source_identity.key));
        assert_namespace_unchanged(destination_namespace,
            namespace_status(rename_destination_identity.key));
      }
      std::shared_ptr<CacheEntry> source_still_present =
          cache.open(rename_source_identity);
      assert(source_still_present.get() == rename_source.get());
      pending = cache.retiring_entry(rename_destination_identity.key);
      assert(pending.get() == rename_destination.get());
      rename_destination->finish_fetch(rename_claim);
      rename_destination->unpin(0, 4096);
      rename_retry = true;
      {
        IoExecutorScope worker_scope(nullptr, 0);
        const bool renamed = cache.rename(
            rename_source_identity.key, rename_destination_identity.key,
            &rename_retry, false);
        assert(renamed && !rename_retry);
      }
      const CacheIdentity renamed_identity = test_identity(
          rename_destination_identity.key, "source-etag", 4096);
      std::shared_ptr<CacheEntry> renamed_entry = cache.open(renamed_identity);
      assert(renamed_entry.get() == rename_source.get());
    }

    std::shared_ptr<CacheEntry> writer = cache.create_writer(
        test_identity("written/file", "old-etag", 17),
        80U * 1024U * 1024U);
    writer->prepare_write(0, 4096);
    write_test_bytes(writer->data_fd(), 0, 4096);
    writer->publish_dirty(0, 4096, 4096);
    assert(writer->dirty());
    assert(!writer->fully_clean());
    assert(writer->written_end() == 4096);
    writer->set_upload_id("upload-1");
    assert(writer->upload_id() == "upload-1");
    writer->sync_write();
    writer->commit_write(test_identity(
        "written/file", "written-etag", 4096));
    assert(!writer->dirty());
    assert(writer->fully_clean());
    assert(writer->range_clean(0, 4096));
  }

  const CacheIdentity interrupted_identity = test_identity(
      "interrupted-read", "etag-interrupted", 8192);
  {
    LocalCache cache(config);
    std::shared_ptr<CacheEntry> entry = cache.open(interrupted_identity);
    const CacheFetchClaim claim = entry->claim_fetch(0, 4096, 8192);
    assert(claim.offset == 0 && claim.length == 8192);
    write_test_bytes(entry->data_fd(), 0, 1024);
  }
  {
    LocalCache cache(config);
    std::shared_ptr<CacheEntry> entry = cache.open(interrupted_identity);
    assert(!entry->range_clean(0, 4096));
    const CacheFetchClaim claim = entry->claim_fetch(0, 4096, 8192);
    assert(claim.offset == 0 && claim.length == 8192);
    entry->fail_fetch(claim);
  }

  {
    LocalCache cache(config);
    std::shared_ptr<CacheEntry> entry = cache.open(
        test_identity("目录/file name", "etag-1", 9000));
    assert(entry->range_clean(0, 4096));
    entry.reset();
    entry = cache.open(test_identity("目录/file name", "etag-2", 9000));
    assert(!entry->range_clean(0, 4096));
    entry = cache.open(test_identity(
        "written/file", "written-etag", 4096));
    assert(entry->range_clean(0, 4096));
  }

  {
    TemporaryDirectory namespace_directory;
    CacheConfig names = config;
    names.root = namespace_directory.path;
    LocalCache cache(names);
    const CacheIdentity old_identity = test_identity(
        "old name", "rename-etag", 4096);
    std::shared_ptr<CacheEntry> old = cache.open(old_identity);
    const CacheFetchClaim claim = old->claim_fetch(0, 4096, 4096);
    assert(old->prepare_read(claim.offset, claim.length));
    write_test_bytes(old->data_fd(), 0, 4096);
    old->publish_clean(claim, 0, claim.length, true);
    old->finish_fetch(claim);
    assert(cache.rename("old name", "new name"));
    std::shared_ptr<CacheEntry> renamed = cache.open(
        test_identity("new name", "rename-etag", 4096));
    assert(renamed.get() == old.get());
    assert(renamed->range_clean(0, 4096));
    assert(cache.remove("new name", true));
    assert(old->range_clean(0, 4096));
    std::shared_ptr<CacheEntry> same_identity = cache.open(
        test_identity("new name", "rename-etag", 4096));
    assert(same_identity.get() != old.get());
    assert(!same_identity->range_clean(0, 4096));
    std::shared_ptr<CacheEntry> replacement = cache.open(
        test_identity("new name", "replacement-etag", 4096));
    assert(replacement.get() != old.get());
    assert(!replacement->range_clean(0, 4096));
  }

  {
    LocalCache cache(config);
    std::shared_ptr<CacheEntry> writer = cache.create_writer(
        test_identity("recovery/file", "recovery-old-etag", 19),
        80U * 1024U * 1024U);
    writer->prepare_write(0, 8192);
    write_test_bytes(writer->data_fd(), 0, 8192);
    writer->publish_dirty(0, 8192, 8192);
    writer->set_upload_id("recovery-upload");
    recovery_write_id = writer->write_id();
    assert(recovery_write_id.size() == 32);
  }

  {
    LocalCache cache(config);
    std::vector<std::shared_ptr<CacheEntry>> dirty = cache.recover_dirty();
    assert(dirty.size() == 1);
    assert(dirty.front()->key() == "recovery/file");
    assert(dirty.front()->written_end() == 8192);
    assert(dirty.front()->etag() == "recovery-old-etag");
    assert(dirty.front()->upload_id() == "recovery-upload");
    assert(dirty.front()->write_id() == recovery_write_id);
    dirty.front()->commit_write(test_identity(
        "recovery/file", "recovered-etag", 8192));
  }

  {
    TemporaryDirectory malformed_directory;
    CacheConfig malformed = config;
    malformed.root = malformed_directory.path;
    {
      LocalCache cache(malformed);
      std::shared_ptr<CacheEntry> writer = cache.create_writer(
          test_identity("malformed", "old-etag", 1),
          80U * 1024U * 1024U);
      writer->prepare_write(0, 4096);
      write_test_bytes(writer->data_fd(), 0, 4096);
      writer->publish_dirty(0, 4096, 4096);
    }
    const std::filesystem::path marker =
        std::filesystem::path(malformed.root) / "meta/dirty/malformed";
    const int fd = ::open(marker.c_str(), O_WRONLY | O_CLOEXEC);
    assert(fd >= 0);
    const uint32_t bad_version = 0;
    assert(::pwrite(fd, &bad_version, sizeof(bad_version), 8) ==
           ssize_t(sizeof(bad_version)));
    assert(::close(fd) == 0);

    LocalCache cache(malformed);
    std::vector<std::string> isolated;
    std::vector<std::shared_ptr<CacheEntry>> dirty =
        cache.recover_dirty(&isolated);
    assert(dirty.empty());
    assert(isolated == std::vector<std::string>{"malformed"});
  }

  {
    TemporaryDirectory isolated_directory;
    CacheConfig isolated_config = config;
    isolated_config.root = isolated_directory.path;
    {
      LocalCache cache(isolated_config);
      std::shared_ptr<CacheEntry> writer = cache.create_writer(
          test_identity("failed-write", "old-etag", 1),
          80U * 1024U * 1024U);
      writer->prepare_write(0, 4096);
      write_test_bytes(writer->data_fd(), 0, 4096);
      writer->publish_dirty(0, 4096, 4096);
      writer->isolate_write();
    }
    LocalCache cache(isolated_config);
    std::vector<std::string> isolated;
    assert(cache.recover_dirty(&isolated).empty());
    assert(isolated == std::vector<std::string>{"failed-write"});
  }

  {
    TemporaryDirectory pending_directory;
    CacheConfig pending_config = config;
    pending_config.root = pending_directory.path;
    const std::string pending_key = "hidden/object";
    {
      LocalCache cache(pending_config);
      cache.create_pending_delete(pending_key);
      bool duplicate = false;
      try {
        cache.create_pending_delete(pending_key);
      } catch (const std::system_error& error) {
        duplicate = error.code().value() == EEXIST;
      }
      assert(duplicate);
    }
    {
      LocalCache cache(pending_config);
      const std::vector<CachePendingDelete> recovered =
          cache.recover_pending_deletes();
      assert(recovered.size() == 1);
      assert(recovered[0].key == pending_key);
      assert(!recovered[0].rollback);
      cache.finish_pending_delete(pending_key);
      assert(cache.recover_pending_deletes().empty());
    }
  }

  {
    TemporaryDirectory rollback_directory;
    CacheConfig rollback_config = config;
    rollback_config.root = rollback_directory.path;
    LocalCache cache(rollback_config);
    cache.create_pending_delete(
        "hidden/overwrite", "visible/destination", "\"new-etag\"");
    std::vector<CachePendingDelete> recovered =
        cache.recover_pending_deletes();
    assert(recovered.size() == 1);
    assert(recovered[0].key == "hidden/overwrite");
    assert(recovered[0].restore_key == "visible/destination");
    assert(recovered[0].replacement_etag == "\"new-etag\"");
    assert(recovered[0].rollback);
    cache.commit_pending_delete("hidden/overwrite");
    recovered = cache.recover_pending_deletes();
    assert(recovered.size() == 1);
    assert(!recovered[0].rollback);
    cache.finish_pending_delete("hidden/overwrite");
  }

  {
    TemporaryDirectory malformed_pending_directory;
    CacheConfig malformed_pending = config;
    malformed_pending.root = malformed_pending_directory.path;
    const std::string pending_key = "identifiable-pending";
    {
      LocalCache cache(malformed_pending);
      cache.create_pending_delete(pending_key);
    }
    const std::filesystem::path marker =
        std::filesystem::path(malformed_pending.root) /
        "meta/pending-delete/identifiable-pending";
    const int fd = ::open(marker.c_str(), O_WRONLY | O_CLOEXEC);
    assert(fd >= 0);
    const uint32_t bad_version = 0;
    assert(::pwrite(fd, &bad_version, sizeof(bad_version), 8) ==
           ssize_t(sizeof(bad_version)));
    assert(::close(fd) == 0);
    LocalCache cache(malformed_pending);
    assert(cache.recover_pending_deletes().empty());
  }

  {
    CacheConfig wrong = config;
    wrong.namespace_id = "different bucket";
    bool rejected = false;
    try {
      LocalCache cache(wrong);
    } catch (const std::exception&) {
      rejected = true;
    }
    assert(rejected);
  }

  {
    TemporaryDirectory permissions_directory;
    assert(::chmod(permissions_directory.path.c_str(), 0755) == 0);
    CacheConfig permissions = config;
    permissions.root = permissions_directory.path;
    LocalCache cache(permissions);
    struct stat status{};
    assert(::stat(permissions.root.c_str(), &status) == 0);
    assert((status.st_mode & 0777) == 0700);
    assert(status.st_uid == ::geteuid());
  }

  {
    TemporaryDirectory capacity_directory;
    CacheConfig bounded = config;
    bounded.root               = capacity_directory.path;
    bounded.maximum_bytes      = 2U * 1024U * 1024U;
    bounded.reserve_bytes      = 0;
    bounded.reserve_percent    = 0;
    bounded.reserve_is_percent = false;
    LocalCache cache(bounded);
    std::shared_ptr<CacheEntry> entry = cache.open(
        test_identity("clock", "etag-clock", 2U * 1024U * 1024U));
    CacheFetchClaim first = entry->claim_fetch(
        0, 4096, 1024U * 1024U);
    assert(first.length == 1024U * 1024U);
    assert(entry->prepare_read(first.offset, first.length));
    write_test_bytes(entry->data_fd(), first.offset, first.length);
    entry->publish_clean(first, 0, first.length, true);
    entry->finish_fetch(first);
    entry->touch(first.offset, first.length);

    CacheFetchClaim second = entry->claim_fetch(
        1024U * 1024U, 4096, 1024U * 1024U);
    assert(second.length == 1024U * 1024U);
    assert(entry->prepare_read(second.offset, second.length));
    assert(!entry->range_clean(first.offset, first.length));
    entry->fail_fetch(second);
  }

  {
    TemporaryDirectory full_directory;
    CacheConfig full = config;
    full.root               = full_directory.path;
    full.maximum_bytes      = 4096;
    full.reserve_bytes      = 0;
    full.reserve_percent    = 0;
    full.reserve_is_percent = false;
    LocalCache cache(full);
    assert(!cache.open(test_identity("bypass", "etag", 4096)));
  }

  {
    TemporaryDirectory cold_directory;
    CacheConfig bounded = config;
    bounded.root               = cold_directory.path;
    bounded.maximum_bytes      = 1200U * 1024U;
    bounded.reserve_bytes      = 0;
    bounded.reserve_percent    = 0;
    bounded.reserve_is_percent = false;
    const CacheIdentity first_identity = test_identity(
        "cold-first", "etag-cold-first", 1024U * 1024U);
    {
      LocalCache cache(bounded);
      std::shared_ptr<CacheEntry> first = cache.open(first_identity);
      CacheFetchClaim claim = first->claim_fetch(
          0, 4096, 1024U * 1024U);
      assert(first->prepare_read(claim.offset, claim.length));
      write_test_bytes(first->data_fd(), claim.offset, claim.length);
      first->publish_clean(claim, 0, claim.length, true);
      first->finish_fetch(claim);
      assert(first->fully_clean());
    }
    {
      LocalCache cache(bounded);
      std::shared_ptr<CacheEntry> second = cache.open(test_identity(
          "cold-second", "etag-cold-second", 1024U * 1024U));
      CacheFetchClaim claim = second->claim_fetch(
          0, 4096, 1024U * 1024U);
      assert(second->prepare_read(claim.offset, claim.length));
      second->fail_fetch(claim);
      std::shared_ptr<CacheEntry> first = cache.open(first_identity);
      assert(first);
      assert(!first->range_clean(0, 4096));
    }
  }

  {
    TemporaryDirectory mixed_directory;
    CacheConfig bounded = config;
    bounded.root               = mixed_directory.path;
    bounded.maximum_bytes      = 80U * 1024U;
    bounded.reserve_bytes      = 0;
    bounded.reserve_percent    = 0;
    bounded.reserve_is_percent = false;
    LocalCache cache(bounded);
    const CacheIdentity mixed_identity = test_identity(
        "mixed-region", "etag-mixed", 1024U * 1024U);
    std::shared_ptr<CacheEntry> mixed = cache.open(mixed_identity);
    CacheFetchClaim first = mixed->claim_fetch(0, 4096, 4096);
    assert(first.length == kCacheBitmapUnit);
    assert(mixed->prepare_read(first.offset, first.length));
    write_test_bytes(mixed->data_fd(), first.offset, first.length);
    mixed->publish_clean(first, 0, first.length, true);
    mixed->finish_fetch(first);
    assert(mixed->range_clean(0, 4096));
    mixed.reset();

    std::shared_ptr<CacheEntry> pressure = cache.open(test_identity(
        "mixed-pressure", "etag-pressure", 64U * 1024U));
    CacheFetchClaim second = pressure->claim_fetch(0, 4096, 64U * 1024U);
    assert(pressure->prepare_read(second.offset, second.length));
    pressure->fail_fetch(second);
    mixed = cache.open(mixed_identity);
    assert(mixed);
    assert(!mixed->range_clean(0, 4096));
  }

  {
    TemporaryDirectory block_directory;
    CacheConfig blocks = config;
    blocks.root       = block_directory.path;
    blocks.block_size = 2 * kCacheBitmapUnit;
    LocalCache cache(blocks);
    std::shared_ptr<CacheEntry> entry = cache.open(test_identity(
        "whole-block", "etag-block", 4 * kCacheBitmapUnit));
    assert(entry->page_size() == kCacheBitmapUnit);
    assert(entry->block_size() == 2 * kCacheBitmapUnit);

    CacheFetchClaim claim = entry->claim_fetch(
        kCacheBitmapUnit + 17, 1, 1);
    assert(claim.offset == 0 && claim.length == 2 * kCacheBitmapUnit);
    assert(entry->range_available_or_pending(0, claim.length));
    assert(entry->prepare_read(claim.offset, claim.length));
    write_test_bytes(entry->data_fd(), 0, claim.length);
    entry->publish_clean(claim, 0, kCacheBitmapUnit, false);
    assert(!entry->range_clean(0, 1));
    entry->publish_clean(claim, kCacheBitmapUnit, claim.length, false);
    assert(entry->range_clean(0, claim.length));
    entry->finish_fetch(claim);
    assert(!entry->try_export(false));

    assert(entry->begin_checksum_manifest());
    entry->finish_checksum_manifest({
        CacheChecksumPart{0, kCacheBitmapUnit, 1, "first"},
        CacheChecksumPart{kCacheBitmapUnit, kCacheBitmapUnit, 1, "second"},
        CacheChecksumPart{2 * kCacheBitmapUnit, kCacheBitmapUnit, 1, "third"},
        CacheChecksumPart{3 * kCacheBitmapUnit, kCacheBitmapUnit, 1, "fourth"},
    });
    CacheChecksumClaim abandoned = entry->claim_checksum(
        kCacheBitmapUnit, 1);
    assert(abandoned.action == CACHE_CHECKSUM_VERIFY);
    entry->abandon_checksum(abandoned);
    assert(entry->range_clean(0, kCacheBitmapUnit));
    assert(!entry->range_clean(kCacheBitmapUnit, 1));

    CacheFetchClaim rollback = entry->claim_fetch(
        kCacheBitmapUnit, 1, 1);
    assert(rollback.offset == 0 &&
           rollback.length == 2 * kCacheBitmapUnit);
    entry->rollback_fetch(rollback);
    assert(entry->range_clean(0, kCacheBitmapUnit));
    assert(!entry->range_clean(kCacheBitmapUnit, 1));
    entry->publish_clean(rollback, 0, rollback.length, true);
    entry->finish_fetch(rollback);
    assert(!entry->range_clean(kCacheBitmapUnit, 1));

    assert(entry->pin_clean(0, kCacheBitmapUnit));
    assert(!entry->claim_fetch(kCacheBitmapUnit, 1, 1));
    auto pin_waiter = std::async(std::launch::async, [&] {
      entry->wait_for_range(kCacheBitmapUnit, 1);
      return true;
    });
    assert(pin_waiter.wait_for(std::chrono::milliseconds(20)) ==
           std::future_status::timeout);
    entry->unpin(0, kCacheBitmapUnit);
    assert(pin_waiter.get());

    CacheFetchClaim refill = entry->claim_fetch(kCacheBitmapUnit, 1, 1);
    assert(refill.offset == 0 && refill.length == 2 * kCacheBitmapUnit);
    assert(entry->prepare_read(refill.offset, refill.length));
    write_test_bytes(entry->data_fd(), refill.offset, refill.length);
    entry->publish_clean(refill, 0, refill.length, false);
    entry->finish_fetch(refill);

    std::shared_ptr<CacheEntry> failed = cache.open(test_identity(
        "failed-tail", "etag-failed", 4 * kCacheBitmapUnit));
    CacheFetchClaim expanded = failed->claim_fetch(
        0, 1, 2 * blocks.block_size);
    assert(expanded.offset == 0 &&
           expanded.length == 4 * kCacheBitmapUnit);
    assert(failed->prepare_read(expanded.offset, expanded.length));
    write_test_bytes(failed->data_fd(), expanded.offset, blocks.block_size);
    failed->publish_clean(expanded, 0, blocks.block_size, false);
    failed->fail_fetch(expanded);
    assert(failed->range_clean(0, blocks.block_size));
    assert(!failed->range_clean(blocks.block_size, 1));
    const CacheFetchClaim late = expanded;
    failed->mark_bad(late);
    failed->publish_clean(late, 0, late.length, true);
    failed->finish_fetch(late);
    assert(!failed->range_bad(0, 1));
    assert(!failed->range_clean(blocks.block_size, 1));
  }

  for (bool failed_tail : {false, true}) {
    TemporaryDirectory overlap_directory;
    CacheConfig blocks = config;
    blocks.root       = overlap_directory.path;
    blocks.block_size = 2 * kCacheBitmapUnit;
    LocalCache cache(blocks);
    auto entry = cache.open(test_identity(
        "reclaimed-prefix", "etag-prefix", 2 * blocks.block_size));
    const CacheFetchClaim first = entry->claim_fetch(
        0, 1, 2 * blocks.block_size);
    assert(first.length == 2 * blocks.block_size);
    assert(entry->prepare_read(first.offset, first.length));
    write_test_bytes(entry->data_fd(), 0, blocks.block_size);
    entry->publish_clean(first, 0, blocks.block_size, false);
    assert(CacheTestAccess::evict_one(cache));
    assert(!entry->range_clean(0, 1));

    const CacheFetchClaim replacement = entry->claim_fetch(0, 1, 1);
    assert(replacement.offset == 0 &&
           replacement.length == blocks.block_size);
    assert(entry->prepare_read(replacement.offset, replacement.length));
    write_test_bytes(entry->data_fd(), 0, kCacheBitmapUnit);
    // An older progress callback cannot publish the new owner's partial data.
    entry->publish_clean(first, 0, blocks.block_size, false);
    assert(!entry->range_clean(0, 1));
    if (failed_tail) {
      entry->fail_fetch(first);
    } else {
      write_test_bytes(entry->data_fd(), blocks.block_size, blocks.block_size);
      entry->publish_clean(first, blocks.block_size, first.length, true);
      assert(!entry->pin_fetch_verification(first));
      entry->finish_fetch(first);
    }
    assert(entry->range_available_or_pending(0, replacement.length));
    assert(!entry->range_clean(0, 1));
    std::array<std::byte, 4096> bytes{};
    assert(::pread(entry->data_fd(), bytes.data(), bytes.size(), 0) ==
           ssize_t(bytes.size()));
    for (size_t i = 0; i < bytes.size(); ++i) assert(bytes[i] == std::byte(i));
    write_test_bytes(entry->data_fd(), kCacheBitmapUnit, kCacheBitmapUnit);
    entry->publish_clean(replacement, 0, replacement.length, false);
    entry->finish_fetch(replacement);
    assert(entry->range_clean(0, replacement.length));
  }

  {
    TemporaryDirectory retry_directory;
    CacheConfig blocks = config;
    blocks.root       = retry_directory.path;
    blocks.block_size = 2 * kCacheBitmapUnit;
    LocalCache cache(blocks);
    auto entry = cache.open(test_identity(
        "pinned-whole-retry", "etag-retry", blocks.block_size));
    const auto claim = entry->claim_fetch(0, 1, 1);
    assert(entry->prepare_read(claim.offset, claim.length));
    write_test_bytes(entry->data_fd(), claim.offset, claim.length);
    entry->publish_clean(claim, 0, claim.length, true);
    assert(entry->pin_fetch_verification(claim));
    assert(!entry->begin_checksum_manifest());
    assert(!entry->checksum_manifest_available());
    assert(entry->claim_checksum(0, 1).action == CACHE_CHECKSUM_NONE);
    assert(!CacheTestAccess::evict_one(cache));
    assert(entry->pin_clean(0, 1));
    const int wait_fd = entry->begin_retry_wait(claim);
    assert(wait_fd >= 0);
    assert(!entry->pin_clean(0, 1));
    assert(!entry->claim_fetch(0, 1, 1));
    entry->unpin(0, 1);
    uint64_t wake = 0;
    assert(::read(wait_fd, &wake, sizeof(wake)) == ssize_t(sizeof(wake)));
    entry->end_async_wait();
    assert(entry->begin_retry_wait(claim) == -1);
    write_test_bytes(entry->data_fd(), claim.offset, claim.length);
    entry->finish_retry(claim, true);
    assert(entry->range_clean(0, claim.length));
    assert(entry->begin_checksum_manifest());
    entry->checksum_manifest_unavailable();
    assert(CacheTestAccess::evict_one(cache));
  }

  {
    TemporaryDirectory part_retry_directory;
    CacheConfig blocks = config;
    blocks.root       = part_retry_directory.path;
    blocks.block_size = 2 * kCacheBitmapUnit;
    LocalCache cache(blocks);
    auto entry = cache.open(test_identity(
        "shared-block-parts", "etag-parts", blocks.block_size));
    const auto fetch = entry->claim_fetch(0, 1, 1);
    assert(entry->prepare_read(fetch.offset, fetch.length));
    write_test_bytes(entry->data_fd(), fetch.offset, fetch.length);
    entry->publish_clean(fetch, 0, fetch.length, true);
    assert(entry->begin_checksum_manifest());
    entry->finish_checksum_manifest({
        CacheChecksumPart{0, kCacheBitmapUnit, 1, "first"},
        CacheChecksumPart{kCacheBitmapUnit, kCacheBitmapUnit, 1, "second"},
    });
    assert(!entry->pin_fetch_verification(fetch));
    entry->finish_fetch(fetch);
    const auto first = entry->claim_checksum(0, 1);
    const auto second = entry->claim_checksum(kCacheBitmapUnit, 1);
    assert(first.action == CACHE_CHECKSUM_VERIFY);
    assert(second.action == CACHE_CHECKSUM_VERIFY);
    assert(entry->pin_clean(0, 1));
    const int first_fd = entry->begin_checksum_retry_wait(first);
    const int second_fd = entry->begin_checksum_retry_wait(second);
    assert(first_fd >= 0 && second_fd >= 0);
    assert(!entry->pin_clean(0, 1));
    assert(!entry->pin_clean(kCacheBitmapUnit, 1));
    // Two disjoint verifiers share one block. The final real reply unpin
    // leaves two verification pins, and must wake both retry waiters.
    entry->unpin(0, 1);
    uint64_t wake = 0;
    assert(::read(first_fd, &wake, sizeof(wake)) == ssize_t(sizeof(wake)));
    entry->end_async_wait();
    assert(::read(second_fd, &wake, sizeof(wake)) == ssize_t(sizeof(wake)));
    entry->end_async_wait();
    assert(entry->begin_checksum_retry_wait(first) == -1);
    assert(entry->begin_checksum_retry_wait(second) == -1);
    entry->wait_for_checksum_retry(first);
    entry->wait_for_checksum_retry(second);
    write_test_bytes(entry->data_fd(), 0, blocks.block_size);
    entry->finish_checksum(first, true);
    entry->finish_checksum(second, true);
    assert(entry->pin_clean(0, blocks.block_size));
    entry->unpin(0, blocks.block_size);
    assert(CacheTestAccess::evict_one(cache));
  }

  {
    TemporaryDirectory physical_directory;
    CacheConfig physical = config;
    physical.root = physical_directory.path;
    {
      LocalCache cache(physical);
      assert(cache.open(test_identity("physical-page", "etag", 1))
                 ->page_size() == kCacheBitmapUnit);
    }
    physical.page_size = 8192;
    {
      LocalCache cache(physical);
      assert(cache.open(test_identity("physical-page", "etag", 1))
                 ->page_size() == kCacheBitmapUnit);
    }
  }

  {
    TemporaryDirectory invalid_block_directory;
    CacheConfig invalid = config;
    invalid.root = invalid_block_directory.path;
    invalid.block_size = kCacheBitmapUnit + 1;
    bool rejected = false;
    try {
      LocalCache cache(invalid);
    } catch (const std::invalid_argument&) {
      rejected = true;
    }
    assert(rejected);
  }

  {
    TemporaryDirectory export_directory;
    CacheConfig unlimited = config;
    unlimited.root               = export_directory.path;
    unlimited.maximum_bytes      = 1;
    unlimited.reserve_percent    = 100;
    unlimited.reserve_is_percent = true;
    unlimited.unlimited          = true;
    int old_backing_fd = -1;
    struct stat old_status{};
    {
      LocalCache cache(unlimited);
      std::shared_ptr<CacheEntry> old = cache.open(
          test_identity("exported", "etag-old", 4096));
      CacheFetchClaim claim = old->claim_fetch(0, 1, 1);
      assert(old->prepare_read(claim.offset, claim.length));
      write_test_bytes(old->data_fd(), 0, claim.length);
      old->publish_clean(claim, 0, claim.length, true);
      old->finish_fetch(claim);
      assert(!old->try_export(true));
      assert(old->try_export(false));
      assert(old->exported());
      assert(::fstat(old->data_fd(), &old_status) == 0);
      old_backing_fd = ::dup(old->data_fd());
      assert(old_backing_fd >= 0);

      std::shared_ptr<CacheEntry> verified = cache.open(
          test_identity("verified-export", "etag-verified", 4096));
      CacheFetchClaim verified_fetch = verified->claim_fetch(0, 1, 1);
      assert(verified->prepare_read(
          verified_fetch.offset, verified_fetch.length));
      write_test_bytes(
          verified->data_fd(), verified_fetch.offset, verified_fetch.length);
      verified->publish_clean(
          verified_fetch, 0, verified_fetch.length, true);
      verified->finish_fetch(verified_fetch);
      assert(verified->begin_checksum_manifest());
      verified->finish_checksum_manifest({
          CacheChecksumPart{0, 4096, 1, "verified"},
      });
      CacheChecksumClaim checksum = verified->claim_checksum(0, 1);
      assert(checksum.action == CACHE_CHECKSUM_VERIFY);
      verified->finish_checksum(checksum, true);
      assert(verified->try_export(true));
    }
    {
      LocalCache cache(unlimited);
      std::shared_ptr<CacheEntry> persisted = cache.open(
          test_identity("exported", "etag-old", 4096));
      assert(persisted->exported());
      assert(!persisted->begin_checksum_manifest());
      assert(!persisted->begin_checksum_manifest());
      assert(!persisted->checksum_manifest_available());
      assert(!persisted->try_export(true));
      const uint64_t old_allocated = CacheTestAccess::allocated_bytes(cache);
      std::shared_ptr<CacheEntry> replacement = cache.open(
          test_identity("exported", "etag-new", 4096));
      assert(CacheTestAccess::allocated_bytes(cache) ==
             old_allocated - uint64_t(old_status.st_blocks) * 512);
      struct stat new_status{};
      assert(::fstat(replacement->data_fd(), &new_status) == 0);
      assert(old_status.st_dev != new_status.st_dev ||
             old_status.st_ino != new_status.st_ino);
      std::array<std::byte, 16> bytes{};
      assert(::pread(old_backing_fd, bytes.data(), bytes.size(), 0) ==
             ssize_t(bytes.size()));
      for (size_t i = 0; i < bytes.size(); ++i) {
        assert(bytes[i] == std::byte(i));
      }
      assert(!replacement->exported());

      auto verified = cache.open(
          test_identity("verified-export", "etag-verified", 4096));
      struct stat verified_status{};
      assert(::fstat(verified->data_fd(), &verified_status) == 0);
      const uint64_t writer_allocated = CacheTestAccess::allocated_bytes(cache);
      auto writer = cache.create_writer(
          test_identity("verified-export", "etag-verified", 4096),
          unlimited.upload_part_size);
      // Starting an empty writer can also release the old bitmap's pages.
      assert(CacheTestAccess::allocated_bytes(cache) <=
             writer_allocated - uint64_t(verified_status.st_blocks) * 512);
      assert(::pread(verified->data_fd(), bytes.data(), bytes.size(), 0) ==
             ssize_t(bytes.size()));
      for (size_t i = 0; i < bytes.size(); ++i) assert(bytes[i] == std::byte(i));
    }
    assert(::close(old_backing_fd) == 0);
  }
  return 0;
}
