#include "../src/fuse.cpp"

#include <sys/mman.h>
#include <sys/socket.h>

#include <array>
#include <chrono>
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

void activate_admission(void* context) noexcept {
  static_cast<AdmissionResult*>(context)->activated = true;
}

void run_admission(void* context) noexcept {
  auto* result = static_cast<AdmissionResult*>(context);
  if (result->activated) ++result->callbacks;
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

  bool initialize() {
    char name[] = "fuse_owner_test";
    char* argv[]{name};
    fuse_args args = FUSE_ARGS_INIT(1, argv);
    fuse_lowlevel_ops operations{};
    session = fuse_session_new(&args, &operations, sizeof(operations), nullptr);
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
    return true;
  }

  FuseReactor& reactor() { return *group.reactors_[0]; }

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


int main() {
  try {
    test_http_pool_affinity();
    test_owner_upload_admission();
    test_reactor_checksum();
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
