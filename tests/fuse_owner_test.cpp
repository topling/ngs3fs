#include "../src/fuse.cpp"

#include <sys/mman.h>
#include <sys/socket.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <span>

void require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

ChecksumValue checksum_of(std::span<const std::byte> bytes) {
  DataChecksum checksum(CHECKSUM_XXHASH128);
  checksum.update(bytes);
  return checksum.finish();
}

void require_equal(const ChecksumValue& actual, const ChecksumValue& expected,
                   const char* message) {
  require(actual.integer == expected.integer && actual.base64 == expected.base64,
          message);
}

struct ChecksumResult {
  ChecksumValue value;
  std::exception_ptr error;
  FuseReactor* callback_reactor = nullptr;
  unsigned callbacks = 0;
  bool complete = false;
};

struct AdmissionResult {
  bool activated = false;
  unsigned callbacks = 0;
};

struct InvalidationResult {
  int result = 0;
  unsigned callbacks = 0;
};

void activate_admission(void* context) noexcept {
  static_cast<AdmissionResult*>(context)->activated = true;
}

void run_admission(void* context) noexcept {
  auto* result = static_cast<AdmissionResult*>(context);
  if (result->activated) ++result->callbacks;
}

void invalidation_complete(void* context, int result) noexcept {
  auto* completion = static_cast<InvalidationResult*>(context);
  completion->result = result;
  ++completion->callbacks;
}

void checksum_complete(void* context, ChecksumValue value,
                       std::exception_ptr error) noexcept {
  auto* result = static_cast<ChecksumResult*>(context);
  result->value = std::move(value);
  result->error = std::move(error);
  result->callback_reactor = current_fuse_reactor();
  ++result->callbacks;
  result->complete = true;
}

void require_owner_completion(const ChecksumResult& result,
                              FuseReactor& owner) {
  require(result.complete && result.callbacks == 1,
          "checksum did not complete exactly once");
  require(result.callback_reactor == &owner,
          "checksum completed outside its owner reactor");
}

struct ReactorIoTest {
  FuseReactorGroup group;
  fuse_session* session = nullptr;
  UniqueFd fuse_fd;
  UniqueFd fuse_peer;

  ~ReactorIoTest() {
    group.reactors_.clear();
    if (session != nullptr) fuse_session_destroy(session);
  }

  static void set_current(FuseReactor* reactor) noexcept {
    FuseReactor::current_ = reactor;
  }

  static void configure_workers(FuseReactorGroup& group, unsigned count) {
    group.reactors_.reserve(count + 1);
    for (unsigned i = 0; i <= count; ++i) {
      auto reactor = std::make_unique<FuseReactor>();
      reactor->group_ = &group;
      reactor->reactor_index_ = i;
      reactor->ready_callbacks_.resize(8, nullptr);
      reactor->max_task_count_ = 8;
      group.reactors_.push_back(std::move(reactor));
    }
  }

  static FuseReactor* worker(FuseReactorGroup& group, unsigned index) {
    return group.reactors_[index + 1].get();
  }

  static bool run_ready(FuseReactor& reactor) {
    set_current(&reactor);
    const bool result = reactor.run_ready_callbacks();
    set_current(nullptr);
    return result;
  }

  static bool drain_tasks(FuseReactor& reactor) {
    return reactor.drain_task_pipe();
  }

  static size_t reply_count(FuseReactor& reactor) {
    return reactor.reply_count_;
  }

  static size_t fd_reply_count(FuseReactor& reactor) {
    return reactor.fd_reply_count_;
  }

  static bool fd_reply_uses_splice(FuseReactor& reactor,
                                   unsigned expected_flags) {
    size_t active = 0;
    for (size_t index = 0; index < reactor.reply_pool_size_; ++index) {
      const FuseReactor::Reply& reply = reactor.reply_pool_[index];
      if (!reply.fd_reply) continue;
      ++active;
      if (reply.fd_source_io.kind != AsyncIoRequest::SPLICE ||
          reply.fd_final_splice_flags != expected_flags ||
          reply.fd_pipe[0] < 0 || reply.fd_pipe[1] < 0) {
        return false;
      }
    }
    return active == 1;
  }

  static bool cancel_fd_reply_source(FuseReactor& reactor) {
    for (FuseReactor::IoRequest* request : reactor.io_requests_) {
      if (request->async != nullptr &&
          request->async->complete == FuseReactor::fd_reply_source_done) {
        set_current(&reactor);
        const bool result = reactor.cancel(*request->async);
        set_current(nullptr);
        return result;
      }
    }
    return false;
  }

  static bool fd_reply_source_uses_flags(
      FuseReactor& reactor, int expected_flags) {
    size_t active = 0;
    for (FuseReactor::IoRequest* request : reactor.io_requests_) {
      if (request->async == nullptr ||
          request->async->complete != FuseReactor::fd_reply_source_done) {
        continue;
      }
      ++active;
      if (request->flags != expected_flags) return false;
    }
    return active == 1;
  }

  static void fail_replies(FuseReactor& reactor, int result) {
    set_current(&reactor);
    reactor.fail_replies(result);
    set_current(nullptr);
  }

  bool initialize(const fuse_lowlevel_ops* supplied_operations = nullptr,
                  void* userdata = nullptr, bool fd_reply = true) {
    char name[] = "fuse_owner_test";
    char* argv[]{name};
    fuse_args args = FUSE_ARGS_INIT(1, argv);
    fuse_lowlevel_ops empty_operations{};
    const fuse_lowlevel_ops* operations = supplied_operations != nullptr ?
        supplied_operations : &empty_operations;
    session = fuse_session_new(&args, operations, sizeof(*operations), userdata);
    fuse_opt_free_args(&args);
    if (session == nullptr) return false;

    int sockets[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) != 0) {
      return false;
    }
    fuse_fd.reset(sockets[0]);
    fuse_peer.reset(sockets[1]);
    group.session_ = session;
    auto reactor = std::make_unique<FuseReactor>();
    std::string error;
    if (!reactor->initialize(&group, session, fuse_fd.get(), false, false,
                             32, 0, false, error)) {
      fprintf(stderr, "fuse_owner_test: %s\n", error.c_str());
      return false;
    }
    group.reactors_.push_back(std::move(reactor));
    if (io_uring_enable_rings(&group.reactors_[0]->ring_) != 0) return false;
    group.reactors_[0]->ring_enabled_ = true;
    fuse_custom_io custom_io{};
    custom_io.writev            = FuseReactor::sync_writev;
    custom_io.read              = FuseReactor::sync_read;
    custom_io.writev_async      = FuseReactor::async_writev;
    custom_io.splice_send_async = FuseReactor::async_splice;
    custom_io.async_userdata    = &group;
    custom_io.async_wakeup      = FuseReactor::async_wakeup;
    custom_io.clear_receive     = FuseReactor::clear_receive;
    if (fd_reply) custom_io.fd_reply_async = FuseReactor::fd_reply_async;
    UniqueFd session_io(::dup(fuse_fd.get()));
    if (!session_io ||
        fuse_session_custom_io(session, &custom_io, sizeof(custom_io),
                               session_io.get()) != 0) return false;
    session_io.release();
    struct InitRequest {
      fuse_in_header header{};
      fuse_init_in body{};
    } request;
    request.header.len = sizeof(request);
    request.header.opcode = FUSE_INIT;
    request.header.unique = 1;
    request.header.nodeid = FUSE_ROOT_ID;
    request.body.major = FUSE_KERNEL_VERSION;
    request.body.minor = FUSE_KERNEL_MINOR_VERSION;
    request.body.max_readahead = 1024 * 1024;
    fuse_buf buffer{};
    buffer.size = sizeof(request);
    buffer.mem = &request;
    set_current(group.reactors_[0].get());
    fuse_session_process_buf(session, &buffer);
    set_current(nullptr);
    std::array<char, 512> reply{};
    return ::recv(fuse_peer.get(), reply.data(), reply.size(), 0) >=
        ssize_t(sizeof(fuse_out_header));
  }

  FuseReactor& reactor() { return *group.reactors_[0]; }

  void drive_replies() {
    FuseReactor& owner = reactor();
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::seconds(5);
    while (!owner.io_requests_.empty() || owner.async_pending_ != 0 ||
           owner.fd_reply_count_ != 0 || owner.reply_count_ != 0) {
      require(std::chrono::steady_clock::now() < deadline,
              "owner reply completion timed out");
      set_current(&owner);
      require(owner.run_ready_callbacks(), "run owner reply callback");
      require(io_uring_submit(&owner.ring_) >= 0, "submit owner reply I/O");
      set_current(nullptr);

      io_uring_cqe* cqe = nullptr;
      __kernel_timespec timeout{.tv_sec = 0, .tv_nsec = 100000000};
      const int waited = io_uring_wait_cqe_timeout(&owner.ring_, &cqe, &timeout);
      require(waited == 0 || waited == -ETIME, "wait for owner reply I/O");
      while (io_uring_peek_cqe(&owner.ring_, &cqe) == 0) {
        const uintptr_t tagged = uintptr_t(io_uring_cqe_get_data(cqe));
        const int completion = cqe->res;
        io_uring_cqe_seen(&owner.ring_, cqe);
        set_current(&owner);
        if ((tagged & 7) == 1) {
          owner.complete_io(reinterpret_cast<FuseReactor::IoRequest*>(
                                tagged & ~uintptr_t(7)), completion);
        } else if ((tagged & 7) == 5) {
          owner.complete_async_cancel(reinterpret_cast<FuseReactor::IoRequest*>(
                                          tagged & ~uintptr_t(7)), completion);
        } else {
          require((tagged & 7) == 0, "unexpected owner reply CQ tag");
          owner.complete_reply(reinterpret_cast<FuseReactor::Reply*>(tagged),
                               completion);
        }
        set_current(nullptr);
      }
    }
  }

  void drive(ChecksumResult& result) {
    FuseReactor& owner = reactor();
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::seconds(5);
    while (!result.complete) {
      require(std::chrono::steady_clock::now() < deadline,
              "owner checksum completion timed out");
      const bool ready_progress = owner.callback_count_ != 0 ||
          owner.local_completion_head_ != nullptr ||
          owner.completion_pending_.load(std::memory_order_acquire);
      set_current(&owner);
      require(owner.run_ready_callbacks(), "run owner checksum callback");
      set_current(nullptr);
      if (result.complete) break;

      bool completion_progress = false;
      io_uring_cqe* cqe = nullptr;
      while (io_uring_peek_cqe(&owner.ring_, &cqe) == 0) {
        completion_progress = true;
        const uintptr_t tagged = uintptr_t(io_uring_cqe_get_data(cqe));
        const int completion = cqe->res;
        io_uring_cqe_seen(&owner.ring_, cqe);
        require((tagged & 7) == 1, "unexpected owner checksum CQ tag");
        set_current(&owner);
        owner.complete_io(reinterpret_cast<FuseReactor::IoRequest*>(
                              tagged & ~uintptr_t(7)),
                          completion);
        set_current(nullptr);
      }
      if (result.complete || ready_progress || completion_progress) continue;
      require(owner.async_pending_ != 0 || !owner.io_requests_.empty(),
              "owner checksum stalled without pending I/O");
      require(io_uring_submit(&owner.ring_) >= 0,
              "submit owner checksum I/O");
      __kernel_timespec timeout{.tv_sec = 0, .tv_nsec = 100000000};
      const int waited = io_uring_wait_cqe_timeout(&owner.ring_, &cqe, &timeout);
      require(waited == 0 || waited == -ETIME,
              "wait for owner checksum I/O");
    }
  }
};

void test_http_pool_affinity() {
  FuseReactorGroup group;
  ReactorIoTest::configure_workers(group, 3);
  HttpPool pool(8, 3);
  FuseReactor* worker0 = ReactorIoTest::worker(group, 0);
  FuseReactor* worker1 = ReactorIoTest::worker(group, 1);

  ReactorIoTest::set_current(worker0);
  auto local0 = pool.try_acquire();
  auto local1 = pool.try_acquire();
  auto local2 = pool.try_acquire();
  require(local0 && local1 && local2, "worker did not acquire local slots");
  require(local0.test_slot() % 3 == 0 && local1.test_slot() % 3 == 0 &&
              local2.test_slot() % 3 == 0,
          "worker acquired a remote slot while local capacity was idle");
  auto borrowed = pool.try_acquire();
  require(borrowed && borrowed.test_slot() % 3 != 0,
          "worker did not borrow idle mount-wide capacity");

  local1 = {};
  auto reused = pool.try_acquire();
  require(reused && reused.test_slot() % 3 == 0,
          "worker did not prefer a newly idle local connection");

  ReactorIoTest::set_current(worker1);
  auto other = pool.try_acquire();
  require(other && other.test_slot() % 3 == 1,
          "second worker did not prefer its own idle connection");
  borrowed = {};
  auto returned = pool.try_acquire();
  require(returned && returned.test_slot() % 3 == 1,
          "borrowed connection did not return to its original owner");
  ReactorIoTest::set_current(nullptr);

  HttpPool single(3, 0);
  auto first = single.try_acquire();
  auto second = single.try_acquire();
  require(first.test_slot() == 0 && second.test_slot() == 1,
          "single-reactor fallback changed global round-robin order");

  HttpPool bulk(3, 3);
  ReactorIoTest::set_current(worker0);
  auto bulk0 = bulk.try_acquire_bulk();
  auto bulk1 = bulk.try_acquire_bulk();
  require(bulk0 && bulk1 && !bulk.try_acquire_bulk(),
          "bulk acquisition did not retain one ordinary slot");
  auto ordinary = bulk.try_acquire();
  require(ordinary && ordinary.test_slot() == 2,
          "ordinary acquisition could not use the bulk-reserved slot");
  ReactorIoTest::set_current(nullptr);
}

void test_owner_upload_admission() {
  ReactorIoTest owner0;
  ReactorIoTest owner1;
  if (!owner0.initialize() || !owner1.initialize()) throw std::system_error(
      ENOTSUP, std::generic_category(), "io_uring unavailable");
  FuseReactor& worker0 = owner0.reactor();
  FuseReactor& worker1 = owner1.reactor();
  UploadScheduler scheduler(1, kRequestIoTimeoutMs);
  AdmissionResult first;
  AdmissionResult second;
  FuseReactor::ReactorTask first_task{
      run_admission, run_admission, &first};
  FuseReactor::ReactorTask second_task{
      run_admission, run_admission, &second};

  ReactorIoTest::set_current(&worker0);
  require(worker0.reserve_completion(&first_task),
          "reserve first upload admission");
  scheduler.submit_owner_upload(
      worker0, first_task, activate_admission, &first);
  ReactorIoTest::set_current(&worker1);
  require(worker1.reserve_completion(&second_task),
          "reserve second upload admission");
  scheduler.submit_owner_upload(
      worker1, second_task, activate_admission, &second);
  ReactorIoTest::set_current(nullptr);

  require(scheduler.test_thread_count() == 0,
          "owner-local upload admission started a scheduler thread");
  require(ReactorIoTest::run_ready(worker0) && first.activated &&
              first.callbacks == 1,
          "first owner upload was not admitted locally");
  require(!second.activated && second.callbacks == 0,
          "owner upload exceeded the global admission limit");

  ReactorIoTest::set_current(&worker0);
  scheduler.finish_upload();
  ReactorIoTest::set_current(nullptr);
  require(second.callbacks == 0,
          "foreign upload release executed the waiting owner's continuation");
  require(ReactorIoTest::run_ready(worker1) && second.activated &&
              second.callbacks == 1,
          "released upload slot did not continue on the waiting owner");
  scheduler.finish_upload();
}

void test_reactor_checksum() {
  ReactorIoTest test;
  if (!test.initialize()) throw std::system_error(
      ENOTSUP, std::generic_category(), "io_uring unavailable");
  FuseReactor& owner = test.reactor();

  std::vector<std::byte> bytes(2 * kPreferredIoSize + 37);
  for (size_t i = 0; i < bytes.size(); ++i) {
    bytes[i] = std::byte((i * 29 + 7) & 0xff);
  }
  const ChecksumValue expected = checksum_of(bytes);

  ChecksumResult memory;
  ReactorChecksum memory_checksum(
      owner, CHECKSUM_XXHASH128, checksum_complete, &memory);
  ReactorIoTest::set_current(&owner);
  require(memory_checksum.memory(bytes.data(), bytes.size()),
          "schedule memory checksum");
  ReactorIoTest::set_current(nullptr);
  require(ReactorIoTest::run_ready(owner) && !memory.complete,
          "checksum self-requeue consumed an unbounded callback batch");
  test.drive(memory);
  require_owner_completion(memory, owner);
  require(!memory.error, "memory checksum failed");
  require_equal(memory.value, expected, "memory checksum mismatch");

  struct SelfDeletingChecksum {
    ChecksumResult result;
    std::unique_ptr<ReactorChecksum> operation;
  } empty;
  empty.operation = std::make_unique<ReactorChecksum>(owner, CHECKSUM_XXHASH128,
      [](void* context, ChecksumValue value, std::exception_ptr error) noexcept {
        auto* result = static_cast<SelfDeletingChecksum*>(context);
        result->operation.reset();
        checksum_complete(&result->result, std::move(value), std::move(error));
      }, &empty);
  ReactorIoTest::set_current(&owner);
  require(empty.operation->memory(nullptr, 0), "schedule empty checksum");
  ReactorIoTest::set_current(nullptr);
  test.drive(empty.result);
  require_owner_completion(empty.result, owner);
  require(!empty.operation && !empty.result.error,
          "checksum completion could not release its own operation");
  require_equal(empty.result.value, checksum_of({}), "empty checksum mismatch");

  UniqueFd file(::memfd_create("fuse_owner_checksum", MFD_CLOEXEC));
  require(bool(file), "create checksum file");
  require(::pwrite(file.get(), bytes.data(), bytes.size(), 31) ==
              ssize_t(bytes.size()),
          "write checksum file");
  ChecksumResult file_result;
  ReactorChecksum file_checksum(
      owner, CHECKSUM_XXHASH128, checksum_complete, &file_result);
  ReactorIoTest::set_current(&owner);
  require(file_checksum.file(file.get(), 31, bytes.size()),
          "schedule file checksum");
  ReactorIoTest::set_current(nullptr);
  test.drive(file_result);
  require_owner_completion(file_result, owner);
  require(!file_result.error, "file checksum failed");
  require_equal(file_result.value, expected, "file checksum mismatch");

  ChecksumResult short_file;
  ReactorChecksum short_checksum(
      owner, CHECKSUM_XXHASH128, checksum_complete, &short_file);
  ReactorIoTest::set_current(&owner);
  require(short_checksum.file(file.get(), 31, bytes.size() + 1),
          "schedule short-file checksum");
  ReactorIoTest::set_current(nullptr);
  test.drive(short_file);
  require_owner_completion(short_file, owner);
  require(bool(short_file.error), "short checksum file was accepted");
  try {
    std::rethrow_exception(short_file.error);
  } catch (const std::system_error& error) {
    require(error.code().value() == EIO,
            "short checksum file returned an unexpected error");
  }

  std::array<std::array<std::byte, 37>, 3> segment_bytes{};
  RetainedPart part;
  std::vector<std::byte> retained_bytes;
  for (size_t segment = 0; segment < segment_bytes.size(); ++segment) {
    for (size_t i = 0; i < segment_bytes[segment].size(); ++i) {
      segment_bytes[segment][i] = std::byte(segment * 83 + i);
    }
    PipeSegment value;
    value.pipe = Pipe::create(4096);
    require(::write(value.pipe.write_fd(), segment_bytes[segment].data(),
                    segment_bytes[segment].size()) ==
                ssize_t(segment_bytes[segment].size()),
            "write retained checksum segment");
    value.bytes = segment_bytes[segment].size();
    part.bytes += value.bytes;
    part.segments.push_back(std::move(value));
    retained_bytes.insert(retained_bytes.end(), segment_bytes[segment].begin(),
                          segment_bytes[segment].end());
  }
  ChecksumResult retained;
  ReactorChecksum retained_checksum_operation(
      owner, CHECKSUM_XXHASH128, checksum_complete, &retained);
  ReactorIoTest::set_current(&owner);
  require(retained_checksum_operation.retained(&part),
          "schedule retained checksum");
  ReactorIoTest::set_current(nullptr);
  test.drive(retained);
  require_owner_completion(retained, owner);
  require(!retained.error, "retained checksum failed");
  require_equal(retained.value, checksum_of(retained_bytes),
                "retained checksum duplicated a segment prefix");
  require_equal(retained.value, retained_checksum(CHECKSUM_XXHASH128, &part),
                "retained owner checksum differs from synchronous checksum");
}

void test_page_cache_invalidation_pin() {
  ReactorIoTest test;
  if (!test.initialize()) throw std::system_error(
      ENOTSUP, std::generic_category(), "io_uring unavailable");
  FuseReactor& owner = test.reactor();
  InodeFile item;
  item.open_count.store(1, std::memory_order_relaxed);
  std::shared_ptr<InodeFile> pin(&item, [](InodeFile* value) {
    release_inode_count(value->open_count);
  });
  std::weak_ptr<InodeFile> retained = pin;
  InvalidationResult completion;

  require(async_invalidate_page_cache(
              owner, fuse_ino_t(uintptr_t(&item)), 0, 0,
              invalidation_complete, &completion, std::move(pin)),
          "post pinned page-cache invalidation");
  require(!retained.expired() &&
              item.open_count.load(std::memory_order_relaxed) == 1,
          "posted invalidation released its inode pin");

  require(ReactorIoTest::drain_tasks(owner),
          "drain pinned invalidation task");
  require(ReactorIoTest::run_ready(owner), "start pinned invalidation");
  require(!retained.expired() && ReactorIoTest::reply_count(owner) == 1 &&
              item.open_count.load(std::memory_order_relaxed) == 1,
          "in-flight invalidation released its inode pin");

  ReactorIoTest::fail_replies(owner, -ECANCELED);
  require(retained.expired() && ReactorIoTest::reply_count(owner) == 0 &&
              item.open_count.load(std::memory_order_relaxed) == 0 &&
              completion.callbacks == 1 && completion.result == -ECANCELED,
          "cancelled invalidation retained its inode pin");
}

void test_cache_reclaim_busy_mutation() {
  // This fixture exercises only in-memory inode state. Never consult the
  // developer's credential profile or a cloud instance metadata endpoint.
  require(setenv("AWS_ACCESS_KEY_ID", "unit-test", 1) == 0 &&
              setenv("AWS_SECRET_ACCESS_KEY", "unit-test", 1) == 0,
          "set local-only fixture credentials");
  MountConfig config;
  config.io_engine = IO_ENGINE_LEGACY;
  config.max_connections = 0;
  State state(std::move(config));
  InodeDir& directory = *state.root_item;
  directory.open_count.store(1, std::memory_order_relaxed);
  ListedChild child;
  child.name.assign("reclaim-child");
  install_item(state, FUSE_ROOT_ID, std::move(child));
  require(directory.children.size() == 1,
          "reclaim fixture did not install its child");

  std::unique_lock mutation_guard(directory.children.mutation_mutex);
  require(reclaim_cached_children(state, directory) == 0,
          "reclaim entered a busy namespace mutation");
  // Holding the gate on this same thread makes an accidental blocking lock
  // deadlock deterministically; CTest bounds the test. A wall-time assertion
  // here would also fail on unrelated CI scheduler pauses.
  require(directory.children.size() == 1,
          "busy namespace reclaim changed children");
  mutation_guard.unlock();

  require(reclaim_cached_children(state, directory) == 1 &&
              directory.children.empty(),
          "idle namespace reclaim did not resume normally");

  ReactorIoTest test;
  if (!test.initialize()) throw std::system_error(
      ENOTSUP, std::generic_category(), "io_uring unavailable");
  FuseReactor& owner = test.reactor();
  ListedChild original;
  original.name.assign("changed-child");
  const fuse_ino_t inode = install_item(state, FUSE_ROOT_ID, std::move(original));
  ListedChild changed;
  changed.name.assign("changed-child");
  changed.mtime = 1;
  ReactorIoTest::set_current(&owner);
  require(install_item(state, FUSE_ROOT_ID, std::move(changed)) == inode,
          "listing update replaced a stable inode");
  ReactorIoTest::set_current(nullptr);
  require(reclaim_cached_children(state, directory) == 0 &&
              inode_item(state, inode).open_count.load(std::memory_order_relaxed) == 1,
          "queued listing invalidation did not pin its inode");
  require(ReactorIoTest::run_ready(owner) &&
              ReactorIoTest::reply_count(owner) == 1,
          "start listing invalidation");
  require(reclaim_cached_children(state, directory) == 0,
          "in-flight listing invalidation lost its inode pin");
  ReactorIoTest::fail_replies(owner, -ECANCELED);
  require(reclaim_cached_children(state, directory) == 1 &&
              directory.children.empty(),
          "completed listing invalidation leaked its inode pin");
}

void test_cached_reply_lifetime() {
  char pattern[] = "/tmp/ngs3fs-cached-reply-XXXXXX";
  char* path = ::mkdtemp(pattern);
  require(path != nullptr, "create cached reply fixture");
  struct Cleanup {
    std::filesystem::path path;
    ~Cleanup() { std::filesystem::remove_all(path); }
  } cleanup{path};
  CacheConfig cache_config;
  cache_config.root             = path;
  cache_config.namespace_id     = "cached-reply-unit";
  cache_config.block_size       = kCacheBitmapUnit;
  cache_config.reserve_percent  = 0;
  LocalCache cache(std::move(cache_config));
  MountConfig config;
  config.io_engine       = IO_ENGINE_LEGACY;
  config.max_connections = 0;
  State state(std::move(config));
  FuseReactor reactor;

  for (bool reply_first : {false, true}) {
    OpenHandle handle;
    handle.cache_entry = cache.open({"object", "etag", "", 4096, 0});
    auto* task = new AsyncCachedReadTask(
        state, handle, nullptr, 0, 4096, 0, reactor);
    task->wanted = 4096;
    task->identity.lock();
    handle.cache_entry->pin(0, 4096);
    ++task->refs; // The accepted source-file operation's completion ownership.
    require(handle.request_state.load() == 2,
            "cached reply admitted a second application read");

    if (reply_first) AsyncCachedReadTask::reply_done(task, 0);
    else task->release(); // HTTP finished, but source-file CQE is still pending.

    require(handle.request_state.load() == 2 &&
                !handle.identity_mutex.try_lock(),
            "first completion released handle or identity too early");
    if (reply_first) task->release();
    else AsyncCachedReadTask::reply_done(task, 0);

    require(handle.request_state.load() == 1 &&
                handle.identity_mutex.try_lock(),
            "final completion leaked handle or identity ownership");
    handle.identity_mutex.unlock();
  }
}

struct CachedReplyRequest {
  State* state = nullptr;
  OpenHandle* handle = nullptr;
  FuseReactor* reactor = nullptr;
  std::exception_ptr error;
  int result = -1;
  unsigned callbacks = 0;

  static void init(void*, fuse_conn_info* connection) noexcept {
    fuse_set_feature_flag(connection, FUSE_CAP_SPLICE_WRITE);
    fuse_set_feature_flag(connection, FUSE_CAP_SPLICE_MOVE);
  }

  static void read(fuse_req_t request, fuse_ino_t inode, size_t size,
                   off_t offset, fuse_file_info*) noexcept {
    auto* fixture = static_cast<CachedReplyRequest*>(fuse_req_userdata(request));
    ++fixture->callbacks;
    try {
      auto* task = new AsyncCachedReadTask(
          *fixture->state, *fixture->handle, request, inode, size, offset,
          *fixture->reactor);
      task->wanted = size;
      task->identity.lock();
      if (!fixture->handle->cache_entry->pin_clean(uint64_t(offset), size)) {
        delete task;
        throw std::runtime_error("cached reply fixture could not pin clean range");
      }
      fixture->result = AsyncCachedReadTask::reply_pinned(task);
      task->release();
    } catch (...) {
      fixture->error = std::current_exception();
      fuse_reply_err(request, EIO);
    }
  }
};

enum CachedReplyScenario {
  CACHED_REPLY_SUCCESS,
  CACHED_REPLY_TWO_PAGES_MINUS_ONE,
  CACHED_REPLY_TWO_PAGES,
  CACHED_REPLY_LARGE_SUCCESS,
  CACHED_REPLY_REJECTED,
  CACHED_REPLY_SHORT_SOURCE,
  CACHED_REPLY_CANCEL_SOURCE,
};

void test_cached_reply_connection() {
  const long system_page_size = ::sysconf(_SC_PAGESIZE);
  require(system_page_size > 0, "determine system page size");
  const size_t kSpliceBoundary = 2 * size_t(system_page_size);
  for (CachedReplyScenario scenario : {
           CACHED_REPLY_SUCCESS,
           CACHED_REPLY_TWO_PAGES_MINUS_ONE, CACHED_REPLY_TWO_PAGES,
           CACHED_REPLY_LARGE_SUCCESS,
           CACHED_REPLY_REJECTED,
           CACHED_REPLY_SHORT_SOURCE, CACHED_REPLY_CANCEL_SOURCE}) {
    const bool accepted = scenario != CACHED_REPLY_REJECTED;
    const size_t payload_size =
        scenario == CACHED_REPLY_TWO_PAGES_MINUS_ONE ? kSpliceBoundary - 1 :
        scenario == CACHED_REPLY_TWO_PAGES ? kSpliceBoundary :
        scenario == CACHED_REPLY_LARGE_SUCCESS ? kSpliceBoundary + 1 :
                                                 4096U;
    const bool successful = scenario == CACHED_REPLY_SUCCESS ||
        scenario == CACHED_REPLY_TWO_PAGES_MINUS_ONE ||
        scenario == CACHED_REPLY_TWO_PAGES ||
        scenario == CACHED_REPLY_LARGE_SUCCESS;
    std::vector<char> payload(payload_size);
    for (size_t i = 0; i < payload.size(); ++i) {
      payload[i] = char((i * 17 + 5) & 0xff);
    }
    char pattern[] = "/tmp/ngs3fs-cached-wire-XXXXXX";
    char* path = ::mkdtemp(pattern);
    require(path != nullptr, "create cached wire fixture");
    struct Cleanup {
      std::filesystem::path path;
      ~Cleanup() { std::filesystem::remove_all(path); }
    } cleanup{path};

    CacheConfig cache_config;
    cache_config.root            = path;
    cache_config.namespace_id    =
        scenario == CACHED_REPLY_SUCCESS ? "cached-wire-accepted" :
        scenario == CACHED_REPLY_TWO_PAGES_MINUS_ONE ? "cached-wire-two-pages-minus" :
        scenario == CACHED_REPLY_TWO_PAGES ? "cached-wire-two-pages" :
        scenario == CACHED_REPLY_LARGE_SUCCESS ? "cached-wire-large" :
        scenario == CACHED_REPLY_REJECTED ? "cached-wire-rejected" :
        scenario == CACHED_REPLY_SHORT_SOURCE ? "cached-wire-short" :
                                                 "cached-wire-cancel";
    cache_config.block_size      = kCacheBitmapUnit;
    cache_config.reserve_percent = 0;
    LocalCache cache(std::move(cache_config));
    const CacheIdentity identity{
        "cached-wire-object", accepted ? "accepted" : "rejected", "",
        payload_size, 0};
    OpenHandle handle;
    handle.cache_entry = cache.open(identity);
    const CacheFetchClaim claim = handle.cache_entry->claim_fetch(
        0, payload_size, payload_size);
    require(claim && handle.cache_entry->prepare_read(0, payload_size),
            "prepare cached wire fixture");
    require(::pwrite(handle.cache_entry->data_fd(), payload.data(),
                     payload.size(), 0) == ssize_t(payload.size()),
            "write cached wire fixture");
    handle.cache_entry->publish_clean(claim, 0, payload_size, true);
    handle.cache_entry->finish_fetch(claim);
    if (scenario == CACHED_REPLY_SHORT_SOURCE) {
      require(::ftruncate(handle.cache_entry->data_fd(), 0) == 0,
              "truncate cached reply source fixture");
    }

    MountConfig config;
    config.io_engine       = IO_ENGINE_LEGACY;
    config.max_connections = 0;
    State state(std::move(config));
    CachedReplyRequest fixture;
    fixture.state  = &state;
    fixture.handle = &handle;
    fuse_lowlevel_ops operations{};
    operations.init = CachedReplyRequest::init;
    operations.read = CachedReplyRequest::read;
    ReactorIoTest test;
    require(test.initialize(&operations, &fixture, accepted),
            "initialize cached reply reactor");
    fixture.reactor = &test.reactor();

    struct ReadRequest {
      fuse_in_header header{};
      fuse_read_in body{};
    } request;
    request.header.len    = sizeof(request);
    request.header.opcode = FUSE_READ;
    request.header.unique = 101 + unsigned(scenario);
    request.header.nodeid = 2;
    request.body.size     = uint32_t(payload_size);
    fuse_buf buffer{};
    buffer.size = sizeof(request);
    buffer.mem  = &request;
    ReactorIoTest::set_current(&test.reactor());
    fuse_session_process_buf(test.session, &buffer);
    ReactorIoTest::set_current(nullptr);
    if (fixture.error) std::rethrow_exception(fixture.error);
    require(fixture.callbacks == 1 && fixture.result == 0,
            "cached reply request was not consumed exactly once");

    if (scenario == CACHED_REPLY_CANCEL_SOURCE) {
      require(ReactorIoTest::cancel_fd_reply_source(test.reactor()),
              "cancel cached reply source CQ");
    }

    const auto retiring = cache.retiring_entry(identity.key);
    if (accepted) {
      require(retiring.get() == handle.cache_entry.get() &&
                  handle.request_state.load() == 2 &&
                  !handle.identity_mutex.try_lock() &&
                  ReactorIoTest::fd_reply_count(test.reactor()) == 1 &&
                  ReactorIoTest::fd_reply_uses_splice(
                      test.reactor(), 0) &&
                  ReactorIoTest::fd_reply_source_uses_flags(
                      test.reactor(), SPLICE_F_NONBLOCK),
              "accepted cached reply released ownership before source CQE");
      const int wait_fd = handle.cache_entry->begin_retire_wait();
      require(wait_fd >= 0, "accepted cached reply did not retain range pin");
      test.drive_replies();
      handle.cache_entry->end_async_wait();
      require(handle.cache_entry->begin_retire_wait() == -1,
              "accepted cached reply leaked range pin");
    } else {
      require(!retiring && handle.request_state.load() == 1 &&
                  handle.identity_mutex.try_lock() &&
                  ReactorIoTest::fd_reply_count(test.reactor()) == 0,
              "rejected cached reply retained task or source callback");
      handle.identity_mutex.unlock();
      test.drive_replies();
    }

    require(handle.request_state.load() == 1 &&
                handle.identity_mutex.try_lock(),
            "cached reply final completion leaked handle or identity");
    handle.identity_mutex.unlock();
    fuse_out_header header{};
    require(::recv(test.fuse_peer.get(), &header, sizeof(header), MSG_WAITALL) ==
                ssize_t(sizeof(header)) &&
                header.len >= sizeof(header) &&
                header.len <= sizeof(header) + payload_size,
            "read cached reply wire header");
    std::vector<char> wire_payload(header.len - sizeof(header));
    if (!wire_payload.empty()) {
      require(::recv(test.fuse_peer.get(), wire_payload.data(),
                     wire_payload.size(), MSG_WAITALL) ==
                  ssize_t(wire_payload.size()),
              "read cached reply wire payload");
    }
    const bool error_matches =
        successful ? header.error == 0 :
        scenario == CACHED_REPLY_REJECTED ? header.error == -EOPNOTSUPP :
        scenario == CACHED_REPLY_SHORT_SOURCE ? header.error == -EIO :
        header.error == 0 || header.error == -ECANCELED;
    require(header.unique == request.header.unique &&
                error_matches,
            "cached reply wire header mismatch");
    if (successful ||
        (scenario == CACHED_REPLY_CANCEL_SOURCE && header.error == 0)) {
      require(wire_payload.size() == payload.size(),
              "successful cached reply wire size mismatch");
      require(memcmp(wire_payload.data(), payload.data(), payload.size()) == 0,
              "cached reply wire payload mismatch");
    } else {
      require(wire_payload.empty() &&
                  (scenario != CACHED_REPLY_CANCEL_SOURCE ||
                   header.error == -ECANCELED),
              "failed cached reply wire errno mismatch");
    }
    require(fixture.callbacks == 1 &&
                ReactorIoTest::fd_reply_count(test.reactor()) == 0,
            "cached reply completed more than once or leaked admission");
    char extra = 0;
    errno = 0;
    require(::recv(test.fuse_peer.get(), &extra, 1, MSG_DONTWAIT) == -1 &&
                (errno == EAGAIN || errno == EWOULDBLOCK),
            "cached reply emitted extra wire data");
  }
}


int main() {
  try {
    test_http_pool_affinity();
    test_owner_upload_admission();
    test_reactor_checksum();
    test_page_cache_invalidation_pin();
    test_cache_reclaim_busy_mutation();
    test_cached_reply_lifetime();
    test_cached_reply_connection();
    return 0;
  } catch (const std::system_error& error) {
    if (error.code().value() == ENOTSUP || error.code().value() == EPERM) {
      fprintf(stderr, "fuse_owner_test: skipped: %s\n", error.what());
      return 77;
    }
    fprintf(stderr, "fuse_owner_test: %s\n", error.what());
  } catch (const std::exception& error) {
    fprintf(stderr, "fuse_owner_test: %s\n", error.what());
  }
  ReactorIoTest::set_current(nullptr);
  return 1;
}
