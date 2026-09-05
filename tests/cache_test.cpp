#include "cache.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <algorithm>
#include <assert.h>
#include <chrono>
#include <filesystem>
#include <future>
#include <string.h>

struct CacheTestAccess {
  static std::recursive_mutex& key_mutex(LocalCache& cache, std::string_view key) {
    return cache.key_mutex(key);
  }
  static bool evict_cold(LocalCache& cache) { return cache.evict_cold(); }
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
      .upload_part_size = 8U * 1024U * 1024U,
      .reserve_is_percent = true,
  };
  std::string recovery_write_id;

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
    assert(CacheTestAccess::evict_cold(cache));
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
    assert(pending_waiter.get());
    assert(entry->range_clean(0, 4096));
    assert(!entry->range_clean(4096, 1));

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
    assert(unaligned_claim.length == 8192);
    unaligned->fail_fetch(unaligned_claim);

    std::shared_ptr<CacheEntry> partial_pending = cache.open(
        test_identity("partial-pending", "etag-pending", 8192));
    const CacheFetchClaim one_page = partial_pending->claim_fetch(
        0, 4096, 4096);
    assert(one_page.offset == 0 && one_page.length == 4096);
    auto overlap_waiter = std::async(std::launch::async, [&] {
      partial_pending->wait_for_range(0, 8192);
      return partial_pending->range_clean(0, 4096);
    });
    assert(overlap_waiter.wait_for(std::chrono::milliseconds(20)) ==
           std::future_status::timeout);
    write_test_bytes(partial_pending->data_fd(), 0, 4096);
    partial_pending->publish_clean(one_page, 0, 4096, true);
    partial_pending->finish_fetch(one_page);
    assert(overlap_waiter.get());

    std::shared_ptr<CacheEntry> tail = cache.open(
        test_identity("partial-tail", "etag-tail", 4097));
    const CacheFetchClaim tail_claim = tail->claim_fetch(0, 4097, 4097);
    assert(tail->prepare_read(0, 4097));
    write_test_bytes(tail->data_fd(), 0, 4097);
    tail->publish_clean(tail_claim, 0, 4097, false);
    assert(tail->range_clean(0, 4096));
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
          test_identity("async-generation", "etag-old", 8192);
      const CacheIdentity async_new_identity =
          test_identity("async-generation", "etag-new", 8192);
      std::shared_ptr<CacheEntry> async_old = cache.open(async_old_identity);
      const CacheFetchClaim async_claim =
          async_old->claim_fetch(0, 8192, 8192);
      assert(async_claim.offset == 0 && async_claim.length == 8192);
      const bool prepared =
          async_old->prepare_read(async_claim.offset, async_claim.length);
      assert(prepared);
      write_test_bytes(async_old->data_fd(), 0, 8192);
      async_old->publish_clean(async_claim, 0, 4096, false);
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
          test_identity("async-rename-destination", "destination-etag", 8192);
      std::shared_ptr<CacheEntry> rename_source =
          cache.open(rename_source_identity);
      std::shared_ptr<CacheEntry> rename_destination =
          cache.open(rename_destination_identity);
      const CacheFetchClaim rename_claim =
          rename_destination->claim_fetch(0, 8192, 8192);
      const bool rename_prepared = rename_destination->prepare_read(
          rename_claim.offset, rename_claim.length);
      assert(rename_prepared);
      write_test_bytes(rename_destination->data_fd(), 0, 8192);
      rename_destination->publish_clean(rename_claim, 0, 4096, false);
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
    assert(first.length == 4096);
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
  return 0;
}
