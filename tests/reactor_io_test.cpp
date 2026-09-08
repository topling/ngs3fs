#include "fuse_reactor.hpp"

#include <arpa/inet.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <errno.h>
#include <fcntl.h>
#include <linux/fuse.h>
#include <shared_mutex>
#include <stdexcept>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <thread>

struct ReactorIoTest {
  struct IoResult {
    ssize_t value = -EINPROGRESS;
    bool completed = false;

    static void complete(void* context, ssize_t result) noexcept {
      auto* self = static_cast<IoResult*>(context);
      self->value = result;
      self->completed = true;
    }
  };

  FuseReactorGroup group;
  fuse_session* session = nullptr;
  UniqueFd fake_fuse;
  UniqueFd fake_fuse_peer;
  UniqueFd peer;
  UniqueFd socket;
  UniqueFd fairness_peer;
  UniqueFd fairness_socket;
  UniqueFd file;
  Pipe pipe;
  AsyncIoRequest io;
  AsyncIoRequest fairness_io;
  std::array<AsyncIoRequest, 2> burst_io{};
  std::array<char, 2> burst_data{};
  FuseReactor::ReactorTask burst_ready{};
  AsyncIoRequest shutdown_io;
  std::array<FuseReactor::ReactorTask, 32> tasks{};
  FuseReactor::ReactorTask rejected{};
  FuseReactor::ReactorTask stopped{};
  FuseReactor::ReactorTask worker_completion{};
  std::array<FuseReactor::ReactorTask, 80> cleanup_stages{};
  std::thread worker;
  std::thread notification_drain;
  std::atomic<bool> worker_can_complete{false};
  std::atomic<bool> notification_can_drain{false};
  std::atomic<bool> notification_drain_failed{false};
  bool worker_completed = false;
  std::array<char, 8> data{};
  char fairness_data = 0;
  std::array<char, 8> shutdown_data{};
  std::thread::id owner;
  unsigned phase = 0;
  unsigned cancelled_rounds = 0;
  unsigned processor_bytes = 0;
  unsigned callbacks = 0;
  unsigned posts = 0;
  unsigned cancellations = 0;
  unsigned cleanup_count = 0;
  unsigned notification_callbacks = 0;
  unsigned queued_reply_callbacks = 0;
  unsigned blocked_reply_callbacks = 0;
  unsigned fairness_iterations = 0;
  unsigned fairness_completion_iteration = 0;
  bool fairness_cqe_observed_ready = false;
  std::chrono::steady_clock::time_point fairness_deadline{};
  unsigned burst_completions = 0;
  unsigned burst_replies = 0;
  bool burst_reply_pending = false;
  bool submitting = false;
  bool submitting_notification = false;
  bool shutdown_completed = false;
  bool stopped_called = false;
  bool failed = false;
  bool sqpoll = false;

  ~ReactorIoTest() {
    notification_can_drain.store(true, std::memory_order_release);
    notification_can_drain.notify_one();
    if (notification_drain.joinable()) notification_drain.join();
    if (worker.joinable()) worker.join();
    group.shutdown();
    if (session != nullptr) fuse_session_destroy(session);
  }

  FuseReactor& reactor() { return *group.reactors_.front(); }

  static int dispatch_case() {
    const auto require = [](bool condition, const char* message) {
      if (!condition) throw std::runtime_error(message);
    };
    FuseReactorGroup group;
    require(group.dispatch_reactor(1) == nullptr, "empty dispatch group");
    for (unsigned i = 0; i < 4; ++i) {
      group.reactors_.push_back(std::make_unique<FuseReactor>());
    }
    FuseReactor* first = group.reactors_.front().get();
    first->group_ = &group;
    require(group.dispatch_reactor(0x12345670) == first, "pre-INIT affinity");
    group.initialized_ = true;
    group.running_reactors_.store(3);
    require(group.dispatch_reactor(0x12345670) == first && !group.dispatch_ready_,
            "affinity selected a disabled ring");
    first->receive_active_ = true;
    first->receive_fds_.push_back(-1);
    first->initialization_complete_ = true;
    require(first->submit_receive() && !group.dispatch_ready_ &&
            first->receive_count_ == 0, "post-INIT receive was not gated");
    group.running_reactors_.store(4);
    require(first->submit_receive() && group.dispatch_ready_,
            "all-ready receive did not open the affinity gate");
    first->receive_active_ = false;
    first->receive_fds_.clear();
    for (size_t count : {4, 3, 1}) {
      group.reactors_.resize(count);
      group.running_reactors_.store(unsigned(count));
      group.dispatch_ready_ = false;
      std::array<size_t, 4> counts{};
      for (uint64_t i = 0; i < 16384; ++i) {
        const uint64_t inode = UINT64_C(0x7fff00000000) + i * 16;
        FuseReactor* target = group.dispatch_reactor(inode);
        (void)group.dispatch_reactor(inode + 16);
        require(group.dispatch_reactor(inode) == target,
                "inode affinity changed between requests");
        size_t slot = 0;
        while (slot < count && group.reactors_[slot].get() != target) ++slot;
        require(slot < count, "inode affinity selected an invalid target");
        ++counts[slot];
      }
      if (count == 1) {
        require(counts[0] == 16384,
                "single reactor did not execute its own requests");
      } else {
        require(counts[0] == 0,
                "multi-reactor ingress executed an ordinary inode request");
        const size_t workers = count - 1;
        for (size_t i = 1; i < count; ++i) {
          require(counts[i] > 16384 / workers * 8 / 10 &&
                  counts[i] < 16384 / workers * 12 / 10,
                  "aligned pointer inodes produced an imbalanced worker hash");
        }
      }
      require(group.dispatch_reactor(0) == first, "inode-less control routing");
      FuseReactor* root = group.dispatch_reactor(FUSE_ROOT_ID);
      require(root == group.dispatch_reactor(FUSE_ROOT_ID), "root inode routing");
      require(count == 1 || root != first,
              "multi-reactor root inode routed to ingress");
    }

    group.reactors_.resize(4);
    for (size_t i = 0; i < group.reactors_.size(); ++i) {
      if (!group.reactors_[i]) {
        group.reactors_[i] = std::make_unique<FuseReactor>();
      }
      group.reactors_[i]->group_ = &group;
      group.reactors_[i]->reactor_index_ = i;
    }
    require(!group.reactors_[0]->is_multi_worker() &&
            group.reactors_[0]->worker_count() == 3 &&
            group.reactors_[0]->worker_index() == 0,
            "ingress worker identity is inconsistent");
    for (size_t i = 1; i < group.reactors_.size(); ++i) {
      require(group.reactors_[i]->is_multi_worker() &&
              group.reactors_[i]->worker_count() == 3 &&
              group.reactors_[i]->worker_index() == i - 1,
              "multi-reactor worker identity is inconsistent");
    }

    FuseReactor& local = *group.reactors_[1];
    local.ready_callbacks_.resize(4, nullptr);
    local.max_task_count_ = 4;
    local.wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    require(local.wake_fd_ >= 0, "create owner-local completion wake eventfd");
    unsigned local_callbacks = 0;
    FuseReactor::ReactorTask local_completion{
        [](void* context) noexcept { ++*static_cast<unsigned*>(context); },
        nullptr, &local_callbacks};
    local_completion.completion_owner = &local;
    local.task_count_.store(1, std::memory_order_relaxed);
    FuseReactor::current_ = &local;
    local.complete(&local_completion);
    FuseReactor::current_ = nullptr;
    uint64_t wake = 0;
    errno = 0;
    require(local.local_completion_head_ == &local_completion &&
            !local.completion_pending_.load(std::memory_order_relaxed) &&
            ::read(local.wake_fd_, &wake, sizeof(wake)) < 0 && errno == EAGAIN,
            "owner-local completion used cross-thread notification");
    FuseReactor::current_ = &local;
    require(local.run_ready_callbacks(),
            "owner-local completion callback failed");
    FuseReactor::current_ = nullptr;
    require(local_callbacks == 1 &&
            local.task_count_.load(std::memory_order_relaxed) == 0 &&
            local.local_completion_head_ == nullptr,
            "owner-local completion did not run exactly once");

    Pipe pipe = Pipe::create(4096);
    FuseReactor::Dispatch dispatch;
    dispatch.buffer.flags = FUSE_BUF_IS_FD;
    dispatch.buffer.fd = pipe.read_fd();
    std::array<char, 257> payload;
    for (size_t i = 0; i < payload.size(); ++i) payload[i] = char(i % 127);
    decltype(dispatch.prefix) prefix{};
    prefix.in.len = unsigned(sizeof(prefix) + payload.size());
    prefix.in.opcode = FUSE_WRITE;
    prefix.in.nodeid = UINT64_C(0x7fff12345670);
    prefix.write.fh = 42;
    prefix.write.size = unsigned(payload.size());
    iovec input[]{{&prefix, sizeof(prefix)}, {payload.data(), payload.size()}};
    require(::writev(pipe.write_fd(), input, 2) == ssize_t(prefix.in.len),
            "write request fixture");
    dispatch.buffer.size = prefix.in.len;
    require(FuseReactor::read_dispatch_prefix(&dispatch) == 0 &&
            dispatch.prefix_size == sizeof(prefix) &&
            dispatch.prefix.in.nodeid == prefix.in.nodeid &&
            dispatch.prefix.write.fh == 42, "read FD-backed request prefix");
    std::array<char, 257> received{};
    require(::read(pipe.read_fd(), received.data(), received.size()) ==
                ssize_t(received.size()) && received == payload,
            "prefix routing consumed or changed the WRITE payload");

    prefix.in.len = sizeof(fuse_in_header);
    prefix.in.opcode = FUSE_DESTROY;
    prefix.in.nodeid = 0;
    require(::write(pipe.write_fd(), &prefix, prefix.in.len) ==
                ssize_t(prefix.in.len), "short control request fixture");
    dispatch.buffer.size = prefix.in.len;
    require(FuseReactor::read_dispatch_prefix(&dispatch) == 0 &&
            dispatch.prefix_size == sizeof(fuse_in_header), "short request prefix");
    dispatch.buffer.size = sizeof(fuse_in_header) - 1;
    require(FuseReactor::read_dispatch_prefix(&dispatch) == -EIO,
            "accepted truncated FUSE header");
    prefix.in.len = sizeof(fuse_in_header) + 1;
    dispatch.buffer.size = sizeof(fuse_in_header);
    require(::write(pipe.write_fd(), &prefix, sizeof(fuse_in_header)) ==
                ssize_t(sizeof(fuse_in_header)), "invalid length fixture");
    require(FuseReactor::read_dispatch_prefix(&dispatch) == -EIO,
            "accepted inconsistent FUSE request length");
    return 0;
  }

  static int dispatch_freelist_case() {
    const auto require = [](bool condition, const char* message) {
      if (!condition) throw std::runtime_error(message);
    };
    constexpr size_t node_count = 48;
    constexpr size_t producer_count = 3;
    constexpr size_t rounds = 128;

    FuseReactorGroup group;
    auto owner_value = std::make_unique<FuseReactor>();
    FuseReactor* owner = owner_value.get();
    owner->group_ = &group;
    owner->max_dispatch_count_ = 4;
    owner->wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    require(owner->wake_fd_ >= 0, "create dispatch free-list wake eventfd");
    require(::pipe2(owner->return_pipe_, O_CLOEXEC | O_NONBLOCK) == 0,
            "create dispatch return pipe");
    group.reactors_.push_back(std::move(owner_value));

    std::array<FuseReactor::Dispatch*, node_count> nodes{};
    for (FuseReactor::Dispatch*& node : nodes) {
      node = new FuseReactor::Dispatch;
    }

    owner->recycle_dispatch(nodes[0]);
    owner->recycle_dispatch(nodes[1]);
    owner->recycle_dispatch(nodes[2]);
    require(owner->pop_dispatch() == nodes[2] &&
            owner->pop_dispatch() == nodes[1] &&
            owner->pop_dispatch() == nodes[0] &&
            owner->pop_dispatch() == nullptr,
            "dispatch free list is not LIFO");

    std::barrier phase(producer_count + 1);
    std::atomic<bool> publication_failed{false};
    std::array<std::thread, producer_count> producers;
    for (size_t producer = 0; producer < producer_count; ++producer) {
      producers[producer] = std::thread([&, producer] {
        for (size_t round = 0; round < rounds; ++round) {
          phase.arrive_and_wait();
          for (size_t index = producer; index < nodes.size();
               index += producer_count) {
            owner->recycle_dispatch(nodes[index]);
          }
          phase.arrive_and_wait();
        }
      });
    }
    for (size_t round = 0; round < rounds; ++round) {
      std::array<bool, node_count> seen{};
      phase.arrive_and_wait();
      for (size_t consumed = 0; consumed < nodes.size();) {
        FuseReactor::Dispatch* node = owner->pop_dispatch();
        if (node == nullptr) {
          std::this_thread::yield();
          continue;
        }
        size_t index = 0;
        while (index < nodes.size() && nodes[index] != node) ++index;
        if (index == nodes.size() || seen[index]) {
          publication_failed.store(true, std::memory_order_relaxed);
        } else {
          seen[index] = true;
        }
        ++consumed;
      }
      if (owner->pop_dispatch() != nullptr) {
        publication_failed.store(true, std::memory_order_relaxed);
      }
      for (bool present : seen) {
        if (!present) publication_failed.store(true, std::memory_order_relaxed);
      }
      phase.arrive_and_wait();
    }
    for (std::thread& producer : producers) producer.join();
    require(!publication_failed.load(std::memory_order_relaxed),
            "concurrent dispatch publication lost or duplicated a node");

    FuseReactor::Dispatch* retained = nodes[0];
    require(::pipe2(retained->pipe, O_CLOEXEC | O_NONBLOCK) == 0,
            "create retained dispatch input pipe");
    const int retained_read_fd = retained->pipe[0];
    const int retained_write_fd = retained->pipe[1];
    owner->recycle_dispatch(retained);
    require(owner->pop_dispatch() == retained &&
            retained->pipe[0] == retained_read_fd &&
            retained->pipe[1] == retained_write_fd,
            "dispatch recycle replaced its input pipe");
    const char sent = 'd';
    char received = 0;
    require(::write(retained_write_fd, &sent, 1) == 1 &&
            ::read(retained_read_fd, &received, 1) == 1 && received == sent,
            "recycled dispatch input pipe is not usable");

    FuseReactor target;
    target.group_ = &group;
    retained->owner = owner;
    retained->target = &target;
    retained->processing_complete = true;
    retained->input_tasks.store(2, std::memory_order_relaxed);
    owner->dispatch_count_.store(1, std::memory_order_relaxed);
    target.release_input_dispatch(retained, false);
    require(owner->pop_dispatch() == nullptr &&
            owner->dispatch_count_.load(std::memory_order_relaxed) == 1,
            "dispatch was recycled while retained input was active");
    target.release_input_dispatch(retained, false);
    require(owner->pop_dispatch() == retained &&
            owner->dispatch_count_.load(std::memory_order_relaxed) == 0,
            "final retained-input release did not recycle the dispatch");

    const auto expect_wake = [&](uint64_t expected, const char* message) {
      uint64_t value = 0;
      const ssize_t result = ::read(owner->wake_fd_, &value, sizeof(value));
      require(result == ssize_t(sizeof(value)) && value == expected, message);
    };
    const auto expect_no_wake = [&](const char* message) {
      uint64_t value = 0;
      errno = 0;
      const ssize_t result = ::read(owner->wake_fd_, &value, sizeof(value));
      require(result < 0 && errno == EAGAIN, message);
    };

    retained->owner = owner;
    retained->target = &target;
    retained->input_tasks.store(1, std::memory_order_relaxed);
    owner->dispatch_count_.store(owner->max_dispatch_count_,
                                 std::memory_order_relaxed);
    target.task_count_.store(1, std::memory_order_relaxed);
    require(::write(retained_write_fd, &sent, 1) == 1,
            "write failed MSG_RING input fixture");
    FuseReactor::current_ = owner;
    owner->fail_remote_dispatch(retained, -EBADF);
    FuseReactor::current_ = nullptr;
    require(owner->pop_dispatch() == retained &&
            owner->dispatch_count_.load(std::memory_order_relaxed) ==
                owner->max_dispatch_count_ - 1 &&
            target.task_count_.load(std::memory_order_relaxed) == 0 &&
            retained->input_tasks.load(std::memory_order_relaxed) == 0,
            "MSG_RING failure did not restore dispatch admission");
    expect_no_wake("owner-local MSG_RING failure woke its own ingress");
    errno = 0;
    require(::read(retained_read_fd, &received, 1) < 0 && errno == EAGAIN,
            "MSG_RING failure left request input in the retained pipe");
    owner->error_ = 0;

    group.shutting_down_.store(false, std::memory_order_relaxed);
    owner->dispatch_count_.store(owner->max_dispatch_count_ - 1,
                                 std::memory_order_relaxed);
    owner->dispatch_complete(nodes[1]);
    require(owner->pop_dispatch() == nodes[1],
            "ordinary completion did not publish its dispatch");
    expect_no_wake("unsaturated completion woke the receive owner");

    owner->dispatch_count_.store(owner->max_dispatch_count_,
                                 std::memory_order_relaxed);
    owner->dispatch_complete(nodes[1]);
    require(owner->pop_dispatch() == nullptr &&
            owner->dispatch_count_.load(std::memory_order_relaxed) ==
                owner->max_dispatch_count_,
            "saturated return escaped its pointer-pipe ownership");
    require(owner->drain_return_pipe() &&
            owner->pop_dispatch() == nullptr &&
            owner->pop_returned_dispatch() == nodes[1] &&
            owner->dispatch_count_.load(std::memory_order_relaxed) ==
                owner->max_dispatch_count_ - 1,
            "saturated pointer return touched the shared freelist");
    expect_no_wake("pointer return also wrote the control eventfd");

    group.shutting_down_.store(true, std::memory_order_release);
    owner->dispatch_count_.store(2, std::memory_order_relaxed);
    owner->dispatch_complete(nodes[1]);
    require(owner->pop_dispatch() == nodes[1],
            "shutdown completion did not publish its dispatch");
    expect_no_wake("non-final shutdown completion woke the owner");
    owner->dispatch_count_.store(1, std::memory_order_relaxed);
    owner->dispatch_complete(nodes[1]);
    require(owner->pop_dispatch() == nullptr &&
            owner->dispatch_count_.load(std::memory_order_relaxed) == 1,
            "final shutdown return escaped its pointer-pipe ownership");
    require(owner->drain_return_pipe() &&
            owner->pop_dispatch() == nullptr &&
            owner->pop_returned_dispatch() == nodes[1] &&
            owner->dispatch_count_.load(std::memory_order_relaxed) == 0,
            "final shutdown pointer touched the shared freelist");
    expect_no_wake("final pointer return also wrote the control eventfd");

    ::close(owner->return_pipe_[1]);
    owner->return_pipe_[1] = -1;
    owner->dispatch_count_.store(owner->max_dispatch_count_,
                                 std::memory_order_relaxed);
    owner->dispatch_complete(nodes[1]);
    require(owner->pop_dispatch() == nodes[1] &&
            owner->dispatch_count_.load(std::memory_order_relaxed) ==
                owner->max_dispatch_count_ - 1,
            "unavailable return pipe lost its fallback dispatch");
    expect_wake(1, "return-pipe fallback did not preserve control wake");

    for (FuseReactor::Dispatch* node : nodes) owner->recycle_dispatch(node);
    return 0;
  }

  static int shutdown_msg_ring_case() {
    ReactorIoTest test;
    if (!test.initialize(false)) return test.failed ? 1 : 77;
    const auto require = [](bool condition, const char* message) {
      if (!condition) throw std::runtime_error(message);
    };

    FuseReactor& source = test.reactor();
    require(::pipe2(source.return_pipe_, O_CLOEXEC | O_NONBLOCK) == 0,
            "create shutdown MSG_RING return pipe");
    auto target_value = std::make_unique<FuseReactor>();
    FuseReactor* target = target_value.get();
    std::string error;
    require(target->initialize(&test.group, test.session, test.fake_fuse.get(),
                               false, false, 8, 0, false, error),
            "initialize shutdown MSG_RING target");
    test.group.reactors_.push_back(std::move(target_value));
    require(io_uring_enable_rings(&source.ring_) == 0 &&
            io_uring_enable_rings(&target->ring_) == 0,
            "enable shutdown MSG_RING rings");
    source.ring_enabled_ = true;
    target->ring_enabled_ = true;

    auto* dispatch = new FuseReactor::Dispatch;
    require(::pipe2(dispatch->pipe, O_CLOEXEC | O_NONBLOCK) == 0,
            "create shutdown MSG_RING input pipe");
    dispatch->owner = &source;
    source.dispatch_count_.store(1, std::memory_order_relaxed);
    require(source.start_remote_dispatch(dispatch, target),
            "reserve shutdown MSG_RING dispatch");
    dispatch->task.run = [](void* context) noexcept {
      static_cast<FuseReactor::Dispatch*>(context)->processing_complete = true;
    };

    test.group.shutting_down_.store(true, std::memory_order_release);
    require(!test.group.dispatch_admission_closed_.load(
                std::memory_order_acquire) &&
            target->task_count_.load(std::memory_order_acquire) == 1,
            "peer drained before ingress closed MSG_RING admission");
    require(io_uring_submit(&source.ring_) >= 1,
            "submit shutdown MSG_RING dispatch");
    test.group.dispatch_admission_closed_.store(true,
                                                std::memory_order_release);
    io_uring_cqe* cqe = nullptr;
    require(io_uring_wait_cqe(&target->ring_, &cqe) == 0,
            "wait for shutdown MSG_RING dispatch");
    const uintptr_t tagged = uintptr_t(io_uring_cqe_get_data(cqe));
    io_uring_cqe_seen(&target->ring_, cqe);
    require((tagged & 7) == 3,
            "shutdown MSG_RING returned an incorrect CQ tag");
    auto* received = reinterpret_cast<FuseReactor::Dispatch*>(
        tagged & ~uintptr_t(7));
    require(received == dispatch && target->start_task(&received->task),
            "shutdown MSG_RING did not preserve Dispatch ownership");
    require(target->run_ready_callbacks(),
            "shutdown MSG_RING callback did not complete");

    require(test.group.dispatch_admission_closed_.load(
                std::memory_order_acquire) &&
            target->task_count_.load(std::memory_order_acquire) == 0,
            "shutdown target did not retire its dispatch task");
    require(source.dispatch_count_.load(std::memory_order_acquire) == 1,
            "shutdown pointer return decremented ingress before acquisition");
    require(source.pop_dispatch() == nullptr,
            "shutdown pointer return entered the shared freelist");
    require(source.drain_return_pipe() &&
            source.dispatch_count_.load(std::memory_order_acquire) == 0 &&
            source.pop_dispatch() == nullptr &&
            source.pop_returned_dispatch() == dispatch,
            "shutdown did not drain the admitted MSG_RING dispatch");
    source.recycle_dispatch(dispatch);
    return 0;
  }

  static int msg_ring_batch_case(bool fail, bool shutdown) {
    ReactorIoTest test;
    if (!test.initialize(false)) return test.failed ? 1 : 77;
    const auto require = [](bool condition, const char* message) {
      if (!condition) throw std::runtime_error(message);
    };
    FuseReactor& source = test.reactor();
    require(::pipe2(source.return_pipe_, O_CLOEXEC | O_NONBLOCK) == 0,
            "create batch return pipe");
    auto value = std::make_unique<FuseReactor>();
    FuseReactor* target = value.get();
    std::string error;
    require(target->initialize(&test.group, test.session, test.fake_fuse.get(),
                               false, false, 8, 0, false, error),
            "initialize MSG_RING batch target");
    test.group.reactors_.push_back(std::move(value));
    require(io_uring_enable_rings(&source.ring_) == 0 &&
            io_uring_enable_rings(&target->ring_) == 0,
            "enable MSG_RING batch rings");
    source.ring_enabled_ = true;
    target->ring_enabled_ = true;

    constexpr size_t count = 16;
    std::array<FuseReactor::Dispatch*, count> nodes{};
    std::vector<size_t> order;
    order.reserve(count);
    struct Probe {
      FuseReactor::Dispatch* dispatch;
      std::vector<size_t>* order;
      size_t index;
    };
    std::array<Probe, count> probes{};
    for (size_t i = 0; i < count; ++i) {
      nodes[i] = new FuseReactor::Dispatch;
      require(::pipe2(nodes[i]->pipe, O_CLOEXEC | O_NONBLOCK) == 0,
              "create batch input pipe");
      nodes[i]->owner = &source;
      if (i != 0) nodes[i - 1]->next_remote = nodes[i];
      probes[i] = {nodes[i], &order, i};
    }
    source.dispatch_count_.store(count, std::memory_order_relaxed);
    const unsigned sqes = io_uring_sq_ready(&source.ring_);
    require(!source.start_remote_dispatch_batch(
                nodes.front(), nodes.back(), target, count - 1) &&
            !source.start_remote_dispatch_batch(
                nodes.front(), nodes[count - 2], target, count),
            "malformed batch count or tail was accepted");
    require(target->task_count_.load() == 0 &&
            io_uring_sq_ready(&source.ring_) == sqes,
            "rejected batch changed target admission or SQEs");
    for (auto* node : nodes) {
      require(node->target == nullptr && node->input_tasks.load() == 0 &&
              node->task.run == nullptr,
              "rejected batch partially prepared its nodes");
    }
    // Prepare an invalid destination SQE without closing/reusing the target
    // ring fd. Its failure CQE must own and roll back the whole batch.
    const int fd = target->ring_.ring_fd;
    if (fail) target->ring_.ring_fd = -1;
    const bool accepted = source.start_remote_dispatch_batch(
        nodes.front(), nodes.back(), target, count);
    target->ring_.ring_fd = fd;
    require(accepted && target->task_count_.load() == count,
            "batch admission did not retain every request");
    require(source.remote_dispatch_batches_ == 1 &&
            source.remote_dispatch_requests_ == count,
            "batch emitted one message per request instead of one per owner");
    for (size_t i = 0; i < count; ++i) {
      nodes[i]->task.context = &probes[i];
      nodes[i]->task.run = [](void* context) noexcept {
        auto* probe = static_cast<Probe*>(context);
        probe->order->push_back(probe->index);
        probe->dispatch->processing_complete = true;
      };
    }
    if (shutdown) {
      test.group.shutting_down_.store(true, std::memory_order_release);
      require(!test.group.dispatch_admission_closed_.load(),
              "shutdown closed ingress before its admitted batch was submitted");
    }
    require(io_uring_submit(&source.ring_) == 1,
            "batch did not use exactly one MSG_RING SQE");
    if (shutdown) {
      test.group.dispatch_admission_closed_.store(true, std::memory_order_release);
    }
    io_uring* ring = fail ? &source.ring_ : &target->ring_;
    io_uring_cqe* cqe = nullptr;
    __kernel_timespec timeout{.tv_sec = 2, .tv_nsec = 0};
    require(io_uring_wait_cqe_timeout(ring, &cqe, &timeout) == 0,
            "batch completion timed out");
    const uintptr_t tagged = uintptr_t(io_uring_cqe_get_data(cqe));
    const int result = cqe->res;
    io_uring_cqe_seen(ring, cqe);
    require((tagged & 7) == (fail ? 4 : 3) &&
            reinterpret_cast<FuseReactor::Dispatch*>(tagged & ~uintptr_t(7)) ==
                nodes.front(),
            "batch CQE did not retain its FIFO head");
    struct CurrentScope {
      FuseReactor*& slot;
      FuseReactor* previous;
      ~CurrentScope() { slot = previous; }
    } current{FuseReactor::current_, FuseReactor::current_};
    FuseReactor::current_ = fail ? &source : target;
    if (fail) {
      require(result == -EBADF, "invalid batch target did not fail");
      source.fail_remote_dispatch(nodes.front(), result);
      require(source.error_ == -EBADF && order.empty(),
              "failed batch executed a request or lost its error");
    } else {
      require(result == 0 && target->complete_remote_dispatch(nodes.front()),
              "batch receiver rejected admitted requests");
      for (size_t pass = 0; pass < count && order.size() != count; ++pass) {
        require(target->run_ready_callbacks(), "run MSG_RING batch callbacks");
      }
      require(order.size() == count, "batch omitted request callbacks");
      for (size_t i = 0; i < count; ++i) {
        require(order[i] == i, "batch reordered same-owner requests");
      }
    }
    require(target->task_count_.load(std::memory_order_acquire) == 0,
            "batch leaked target admission");
    require(io_uring_peek_cqe(ring, &cqe) == -EAGAIN,
            "batch emitted extra completion messages");
    require(source.drain_return_pipe() && source.dispatch_count_.load() == 0,
            "batch leaked ingress admission");
    std::array<bool, count> recycled{};
    for (size_t i = 0; i < count; ++i) {
      auto* node = source.pop_returned_dispatch();
      if (node == nullptr) node = source.pop_dispatch();
      const auto found = std::find(nodes.begin(), nodes.end(), node);
      require(found != nodes.end(), "batch lost or returned an unknown Dispatch");
      const size_t index = size_t(found - nodes.begin());
      require(!recycled[index] && node->next_remote == nullptr,
              "batch returned a duplicate Dispatch or retained its chain");
      recycled[index] = true;
    }
    require(source.pop_returned_dispatch() == nullptr &&
            source.pop_dispatch() == nullptr,
            "batch recycled more requests than admitted");
    for (auto* node : nodes) source.recycle_dispatch(node);
    return 0;
  }

  static int metadata_io_case() {
    ReactorIoTest test;
    if (!test.initialize(false)) return test.failed ? 1 : 77;
    const auto require = [](bool condition, const char* message) {
      if (!condition) throw std::runtime_error(message);
    };
    FuseReactor& owner = test.reactor();
    require(io_uring_enable_rings(&owner.ring_) == 0,
            "enable metadata I/O ring");
    owner.ring_enabled_ = true;
    FuseReactor::current_ = &owner;

    bool cancel_completed_operation = false;
    const auto perform = [&](AsyncIoRequest& request) {
      const bool byte_operation =
          request.kind == AsyncIoRequest::PREAD ||
          request.kind == AsyncIoRequest::PWRITE;
      IoResult result;
      request.complete = IoResult::complete;
      request.context = &result;
      require(owner.submit(request), "submit metadata I/O");
      require(io_uring_submit_and_wait(&owner.ring_, 1) >= 0,
              "wait for metadata I/O");
      io_uring_cqe* cqe = nullptr;
      require(io_uring_peek_cqe(&owner.ring_, &cqe) == 0,
              "metadata I/O produced no completion");
      const uintptr_t tagged = uintptr_t(io_uring_cqe_get_data(cqe));
      const int completion = cqe->res;
      io_uring_cqe_seen(&owner.ring_, cqe);
      require((tagged & 7) == 1,
              "metadata I/O used an incorrect completion tag");
      auto* internal = reinterpret_cast<FuseReactor::IoRequest*>(
          tagged & ~uintptr_t(7));
      if (cancel_completed_operation) {
        internal->cancelled = true;
        cancel_completed_operation = false;
      }
      owner.complete_io(internal, completion);
      require(result.completed, "metadata I/O callback did not complete");
      if (byte_operation) {
        require(result.value < 0 ||
                    request.transferred == size_t(result.value),
                "positioned I/O did not retain its byte count");
      } else {
        require(request.transferred == 0,
                "metadata completion was treated as a byte count");
      }
      return result.value;
    };

    char temporary[] = "/tmp/ngs3fs-reactor-XXXXXX";
    require(::mkdtemp(temporary) != nullptr,
            "create metadata I/O directory");
    UniqueFd directory(::open(temporary, O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    require(bool(directory), "open metadata I/O directory");
    struct Cleanup {
      int directory;
      const char* path;
      ~Cleanup() {
        ::unlinkat(directory, "renamed", 0);
        ::unlinkat(directory, "child", AT_REMOVEDIR);
        ::rmdir(path);
      }
    } cleanup{directory.get(), temporary};

    AsyncIoRequest operation{};
    operation.kind = AsyncIoRequest::MKDIRAT;
    operation.fd = directory.get();
    operation.path = "child";
    operation.mode = 0700;
    require(perform(operation) == 0, "io_uring mkdirat failed");

    operation = {};
    operation.kind = AsyncIoRequest::OPENAT;
    operation.fd = directory.get();
    operation.path = "file";
    operation.flags = O_CREAT | O_RDWR | O_CLOEXEC;
    operation.mode = 0600;
    const ssize_t opened = perform(operation);
    require(opened >= 0, "io_uring openat failed");

    operation = {};
    operation.kind = AsyncIoRequest::FALLOCATE;
    operation.fd = int(opened);
    operation.input_offset = 0;
    operation.length = 4096;
    require(perform(operation) == 0, "io_uring fallocate failed");

    operation = {};
    operation.kind = AsyncIoRequest::FSYNC;
    operation.fd = int(opened);
    require(perform(operation) == 0, "io_uring fsync failed");

    struct statx metadata{};
    operation = {};
    operation.kind = AsyncIoRequest::STATX;
    operation.fd = directory.get();
    operation.path = "file";
    operation.data = &metadata;
    operation.mask = STATX_SIZE;
    require(perform(operation) == 0 && metadata.stx_size == 4096,
            "io_uring statx failed");

    char payload[] = "positioned-write";
    operation = {};
    operation.kind = AsyncIoRequest::PWRITE;
    operation.fd = int(opened);
    operation.data = payload;
    operation.length = sizeof(payload);
    operation.output_offset = 17;
    require(perform(operation) == sizeof(payload), "io_uring positioned write failed");
    std::array<char, sizeof(payload)> copied{};
    operation = {};
    operation.kind = AsyncIoRequest::PREAD;
    operation.fd = int(opened);
    operation.data = copied.data();
    operation.length = copied.size();
    operation.input_offset = 17;
    require(perform(operation) == ssize_t(copied.size()) &&
            memcmp(copied.data(), payload, copied.size()) == 0,
            "positioned I/O used the wrong offset field");

    operation = {};
    operation.kind = AsyncIoRequest::FALLOCATE;
    operation.fd = int(opened);
    operation.input_offset = 4096;
    operation.length = 4096;
    operation.flags = FALLOC_FL_KEEP_SIZE;
    require(perform(operation) == 0, "io_uring keep-size allocation failed");
    struct stat allocation{};
    require(::fstat(int(opened), &allocation) == 0 && allocation.st_size == 4096,
            "io_uring fallocate lost KEEP_SIZE");

    operation = {};
    operation.kind = AsyncIoRequest::FALLOCATE;
    operation.fd = int(opened);
    operation.input_offset = 0;
    operation.length = 4096;
    operation.flags = FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE;
    require(perform(operation) == 0, "io_uring hole punch failed");
    copied.fill('x');
    require(::pread(int(opened), copied.data(), copied.size(), 17) == ssize_t(copied.size()),
            "read punched data");
    require(std::all_of(copied.begin(), copied.end(), [](char ch) { return ch == 0; }),
            "io_uring fallocate lost PUNCH_HOLE");

    operation = {};
    operation.kind = AsyncIoRequest::FTRUNCATE;
    operation.fd = int(opened);
    operation.length = 123;
    const ssize_t truncated = perform(operation);
    require(truncated == 0 || truncated == -EINVAL ||
            truncated == -EOPNOTSUPP,
            "io_uring ftruncate returned an unexpected result");

    operation = {};
    operation.kind = AsyncIoRequest::CLOSE;
    operation.fd = int(opened);
    cancel_completed_operation = true;
    require(perform(operation) == 0,
            "successful io_uring close was reported as cancelled");
    errno = 0;
    require(::fcntl(int(opened), F_GETFD) < 0 && errno == EBADF,
            "successful cancelled close retained descriptor ownership");

    operation = {};
    operation.kind = AsyncIoRequest::RENAMEAT;
    operation.fd = directory.get();
    operation.output_fd = directory.get();
    operation.path = "file";
    operation.path2 = "renamed";
    require(perform(operation) == 0, "io_uring renameat failed");

    void* mapping = ::mmap(nullptr, 4096, PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    require(mapping != MAP_FAILED, "create madvise mapping");
    operation = {};
    operation.kind = AsyncIoRequest::MADVISE;
    operation.data = mapping;
    operation.length = 4096;
    operation.flags = MADV_DONTNEED;
    require(perform(operation) == 0, "io_uring madvise failed");
    ::munmap(mapping, 4096);

    operation = {};
    operation.kind = AsyncIoRequest::UNLINKAT;
    operation.fd = directory.get();
    operation.path = "renamed";
    require(perform(operation) == 0, "io_uring unlinkat failed");
    FuseReactor::current_ = nullptr;
    return 0;
  }

  bool check(bool condition, const char* message) noexcept {
    if (!condition) {
      fprintf(stderr, "reactor_io_test: %s\n", message);
      failed = true;
      fuse_session_exit(session);
    }
    return condition;
  }

  bool initialize(bool use_sqpoll = false) {
    sqpoll = use_sqpoll;
    char name[] = "reactor_io_test";
    char* argv[]{name};
    fuse_args args = FUSE_ARGS_INIT(1, argv);
    fuse_lowlevel_ops operations{};
    session = fuse_session_new(&args, &operations, sizeof(operations), nullptr);
    fuse_opt_free_args(&args);
    if (!session) return false;
    int fuse_sockets[2];
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0,
                   fuse_sockets) != 0) return false;
    fake_fuse.reset(fuse_sockets[0]);
    fake_fuse_peer.reset(fuse_sockets[1]);
    int sockets[2];
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) != 0) return false;
    socket.reset(sockets[0]);
    peer.reset(sockets[1]);
    int fairness_sockets[2];
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0,
                   fairness_sockets) != 0) return false;
    fairness_socket.reset(fairness_sockets[0]);
    fairness_peer.reset(fairness_sockets[1]);
    file.reset(memfd_create("reactor_io_test", MFD_CLOEXEC));
    if (!fake_fuse || !file) return false;
    // The two initial CQ-burst PREADs use this byte, independently of the
    // later file-I/O test at offset zero.
    if (::pwrite(file.get(), "b", 1, 4096) != 1) return false;
    pipe = Pipe::create(4096);
    group.session_ = session;
    auto value = std::make_unique<FuseReactor>();
    std::string error;
    if (!value->initialize(&group, session, fake_fuse.get(), false,
                           false, 32, 0, sqpoll, error)) {
      fprintf(stderr, "reactor_io_test: %s\n", error.c_str());
      return false;
    }
    group.reactors_.push_back(std::move(value));
    if (!check((reactor().setup_flags_ & IORING_SETUP_R_DISABLED) != 0,
               "reactor ring was not created disabled")) return false;
    if (!check(bool(reactor().setup_flags_ & IORING_SETUP_SQPOLL) == sqpoll,
               "SQPOLL mode silently changed")) return false;
    if (sqpoll &&
        !check((reactor().setup_flags_ & (IORING_SETUP_COOP_TASKRUN |
                                        IORING_SETUP_DEFER_TASKRUN |
                                        IORING_SETUP_TASKRUN_FLAG |
                                        IORING_SETUP_IOPOLL)) == 0,
               "SQPOLL ring has conflicting task-work or device polling flags")) return false;
    auto disabled_target = std::make_unique<FuseReactor>();
    if (!disabled_target->initialize(&group, session, fake_fuse.get(), false,
                                     false, 32, 0, sqpoll, error)) {
      fprintf(stderr, "reactor_io_test: disabled target: %s\n", error.c_str());
      return false;
    }
    group.reactors_.push_back(std::move(disabled_target));
    group.initialized_ = true;
    group.running_reactors_.store(1, std::memory_order_release);
    if (!check(group.dispatch_reactor(0x12345670) == group.reactors_.front().get() &&
               !group.dispatch_ready_,
               "startup selected a disabled dispatch target")) return false;
    group.running_reactors_.store(0, std::memory_order_release);
    group.initialized_ = false;
    group.reactors_.pop_back();
    fuse_custom_io custom_io{};
    custom_io.writev            = FuseReactor::sync_writev;
    custom_io.read              = FuseReactor::sync_read;
    custom_io.writev_async      = FuseReactor::async_writev;
    custom_io.splice_send_async = FuseReactor::async_splice;
    custom_io.async_userdata    = &group;
    custom_io.async_wakeup      = FuseReactor::async_wakeup;
    custom_io.clear_receive     = FuseReactor::clear_receive;
    const int custom_result = fuse_session_custom_io(
        session, &custom_io, sizeof(custom_io), fake_fuse.get());
    if (custom_result != 0) {
      fprintf(stderr, "reactor_io_test: fuse_session_custom_io: %s\n",
              strerror(-custom_result));
      return false;
    }
    rejected = {noop, cancelled, this};
    stopped = {on_stopped, cancelled, this};
    worker_completion = {on_worker_completion, cancelled, this};
    return true;
  }

  bool submit() noexcept {
    io.complete = completed;
    io.context = this;
    submitting = true;
    const bool accepted = reactor().submit(io);
    submitting = false;
    return check(accepted, "submit rejected a valid request");
  }

  static void noop(void* context) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    test.check(io_executor() == &test.reactor() &&
               current_fuse_reactor() == &test.reactor(), "post ran outside detached reactor scope");
    ++test.posts;
  }

  static void cancelled(void* context) noexcept {
    ++static_cast<ReactorIoTest*>(context)->cancellations;
  }

  static int process(void* context, size_t bytes) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    if (!test.check(io_executor() == &test.reactor(), "processor lacks executor scope")) return -EIO;
    test.processor_bytes += unsigned(bytes);
    if (test.processor_bytes == 3) return 1;
    if (!test.check(::write(test.peer.get(), "x", 1) == 1, "feed parser continuation")) return -EIO;
    return 0;
  }

  bool initialize_fuse_protocol() noexcept {
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
    fuse_session_process_buf(session, &buffer);

    std::array<char, 512> reply{};
    const ssize_t size = recv(fake_fuse_peer.get(), reply.data(), reply.size(),
                              MSG_DONTWAIT);
    return check(size >= ssize_t(sizeof(fuse_out_header)),
                 "minimal FUSE_INIT did not produce a reply");
  }

  static void begin(void* context) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    noop(context);
    if (!test.initialize_fuse_protocol()) return;
    test.burst_ready = {burst_reply, cancelled, &test};
    for (size_t i = 0; i < test.burst_io.size(); ++i) {
      auto& io = test.burst_io[i];
      io.kind = AsyncIoRequest::PREAD;
      io.fd = test.file.get();
      io.input_offset = 4096;
      io.data = &test.burst_data[i];
      io.length = 1;
      io.complete = burst_done;
      io.context = &test;
      if (!test.check(test.reactor().submit(io), "submit CQ burst")) return;
    }
    test.fairness_io.fd = test.fairness_socket.get();
    test.fairness_io.data = &test.fairness_data;
    test.fairness_io.length = 1;
    test.fairness_io.complete = fairness_done;
    test.fairness_io.context = &test;
    if (!test.check(test.reactor().submit(test.fairness_io),
                    "submit fairness receive")) return;
    test.io.kind = AsyncIoRequest::RECEIVE;
    test.io.fd = test.socket.get();
    test.io.data = test.data.data();
    test.io.length = 1;
    test.io.processor = process;
    test.io.processor_context = &test;
    if (!test.check(::write(test.peer.get(), "x", 1) == 1, "feed initial parser byte")) return;
    test.submit();
  }

  static void burst_done(void* context, ssize_t result) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    if (!test.check(result == 1, "CQ burst receive failed") ||
        !test.check(!test.burst_reply_pending,
                    "next I/O CQE overtook a ready FUSE continuation")) return;
    ++test.burst_completions;
    test.burst_reply_pending = true;
    if (!test.check(test.reactor().reserve_completion(&test.burst_ready),
                    "reserve CQ burst reply")) return;
    test.reactor().complete(&test.burst_ready);
  }

  static void burst_reply(void* context) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    test.check(test.burst_reply_pending, "CQ burst reply lost its admission");
    test.burst_reply_pending = false;
    ++test.burst_replies;
  }

  static void fairness_callback(void* context) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    ++test.fairness_iterations;
    if (test.fairness_iterations == 64) {
      test.fairness_deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(2);
      if (!test.check(::write(test.fairness_peer.get(), "f", 1) == 1,
                      "feed fairness receive")) return;
    }
    if (!test.fairness_completion_iteration) {
      unsigned head;
      io_uring_cqe* cqe;
      bool cqe_ready = false;
      io_uring_for_each_cqe(&test.reactor().ring_, head, cqe) {
        if (cqe->user_data == (uintptr_t(&test.fairness_io) | 1)) {
          cqe_ready = true;
          break;
        }
      }
      if (!test.check(!cqe_ready || !test.fairness_cqe_observed_ready,
                      "continuously ready callbacks starved a ready I/O CQE")) {
        return;
      }
      test.fairness_cqe_observed_ready = cqe_ready;
      if (!test.check(test.fairness_iterations < 64 ||
                          std::chrono::steady_clock::now() <
                              test.fairness_deadline,
                      "fairness I/O missed its monotonic completion deadline")) {
        return;
      }
      test.check(test.reactor().post(&test.tasks[1]),
                 "requeue continuously ready callback");
    }
  }

  static void fairness_done(void* context, ssize_t result) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    if (!test.check(std::chrono::steady_clock::now() < test.fairness_deadline,
                    "fairness I/O completed after its monotonic deadline")) {
      return;
    }
    test.check(result == 1 && test.fairness_data == 'f' &&
               std::this_thread::get_id() == test.owner &&
               io_executor() == &test.reactor() &&
               current_fuse_reactor() == &test.reactor(),
               "fairness CQE completed outside its owner");
    test.fairness_completion_iteration = test.fairness_iterations;
  }

  void cancellation_case() noexcept {
    phase = 1;
    io = {};
    io.fd = socket.get();
    io.data = data.data();
    io.length = 1;
    if (submit()) check(reactor().cancel(io), "cancel rejected active receive");
  }

  bool fill_socket(int fd) noexcept {
    const int flags = fcntl(fd, F_GETFL);
    if (!check(flags >= 0 &&
               fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0,
               "make fake FUSE socket nonblocking")) return false;
    std::array<char, 4096> padding{};
    ssize_t result;
    do {
      result = ::write(fd, padding.data(), padding.size());
    } while (result > 0 || (result < 0 && errno == EINTR));
    const bool full = result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
    const bool restored = fcntl(fd, F_SETFL, flags) == 0;
    return check(full && restored, "fill fake FUSE socket");
  }

  bool queue_plain_reply(char value, FuseReactor::NotifyFunction done) noexcept {
    auto* reply = reactor().acquire_reply();
    if (!check(reply != nullptr, "allocate queued reply")) return false;
    reply->length = 1;
    reply->data()[0] = u_char(value);
    reply->output_fd = fairness_socket.get();
    // Exercise the ordinary queued-reply lane without a real kernel request.
    // This private harness completion replaces fuse_reply_async_complete.
    reply->notify_done = done;
    reply->notify_context = this;
    if (!reactor().enqueue_reply(reply)) {
      reactor().release_reply(reply);
      return check(false, "enqueue ordinary reply");
    }
    return true;
  }

  static void queued_reply_done(void* context, int result) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    if (!test.check(result == 0 && test.notification_callbacks == 0,
                    "pending invalidation blocked ordinary reply")) return;
    char value = 0;
    if (!test.check(::recv(test.fairness_peer.get(), &value, 1,
                          MSG_DONTWAIT) == 1 &&
                    value == char('a' + test.queued_reply_callbacks),
                    "ordinary reply lane lost FIFO order")) return;
    if (++test.queued_reply_callbacks == 2) {
      test.notification_can_drain.store(true, std::memory_order_release);
      test.notification_can_drain.notify_one();
    }
  }

  void blocked_reply_case() noexcept {
    if (!fill_socket(fairness_socket.get()) ||
        !queue_plain_reply('z', blocked_reply_done)) return;
    submitting_notification = true;
    const bool accepted = reactor().notify_inval_inode(
        FUSE_ROOT_ID, 0, 4096, notification_done, this);
    submitting_notification = false;
    check(accepted && notification_callbacks == 3,
          "notification behind ordinary reply was rejected or inline");
  }

  static void blocked_reply_done(void* context, int result) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    if (!test.check(result == 0 && test.notification_callbacks == 4,
                    "notification did not bypass blocked ordinary reply")) return;
    ++test.blocked_reply_callbacks;
    test.begin_shutdown_case();
  }

  void begin_shutdown_case() noexcept {
    shutdown_io.fd = socket.get();
    shutdown_io.data = shutdown_data.data();
    shutdown_io.length = shutdown_data.size();
    shutdown_io.complete = shutdown_done;
    shutdown_io.context = this;
    if (!check(reactor().submit(shutdown_io), "submit shutdown receive")) return;
    if (!check(reactor().reserve_completion(&worker_completion),
               "reserve worker completion before shutdown")) return;
    worker = std::thread([this] {
      worker_can_complete.wait(false, std::memory_order_acquire);
      reactor().complete(&worker_completion);
    });
    fuse_session_exit(session);
    check(reactor().post(&stopped),
          "queue callback before shutdown admission closes");
  }

  void notification_case() noexcept {
    if (!fill_socket(fake_fuse.get())) return;
    notification_drain = std::thread([this] {
      notification_can_drain.wait(false, std::memory_order_acquire);
      std::array<char, 65536> discarded{};
      size_t drained = 0;
      ssize_t result = 0;
      do {
        result = recv(fake_fuse_peer.get(), discarded.data(),
                      discarded.size(), MSG_DONTWAIT);
        if (result > 0) drained += size_t(result);
      } while (result > 0 || (result < 0 && errno == EINTR));
      if (drained == 0 ||
          (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
        notification_drain_failed.store(true, std::memory_order_release);
      }
    });

    submitting_notification = true;
    const bool accepted = reactor().notify_inval_inode(
        FUSE_ROOT_ID, 0, 4096, notification_done, this);
    submitting_notification = false;
    if (!check(accepted && notification_callbacks == 0,
               "notification was rejected or completed inline")) return;

    phase = 8;
    io = {};
    io.fd = socket.get();
    io.data = data.data();
    io.length = 1;
    if (!check(::write(peer.get(), "n", 1) == 1,
               "feed read concurrent with notification")) return;
    submit();
  }

  static void notification_done(void* context, int result) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    if (!test.check(!test.submitting_notification,
                    "notification completion was inline") ||
        !test.check(!test.notification_drain_failed.load(
                        std::memory_order_acquire),
                    "unable to drain fake FUSE socket") ||
        !test.check(result == 0, "notification write failed") ||
        !test.check(std::this_thread::get_id() == test.owner &&
                    io_executor() == &test.reactor() &&
                    current_fuse_reactor() == &test.reactor(),
                    "notification completion ran outside owner")) return;
    ++test.notification_callbacks;
    if (test.notification_callbacks == 1) {
      test.submitting_notification = true;
      const bool accepted = test.reactor().notify_inval_inode(
          FUSE_ROOT_ID, 4096, 4096, notification_done, &test);
      test.submitting_notification = false;
      test.check(accepted && test.notification_callbacks == 1,
                 "reentrant notification was rejected or inline");
    } else if (test.notification_callbacks == 2) {
      std::array<char, 4096> discarded{};
      while (::recv(test.fake_fuse_peer.get(), discarded.data(), discarded.size(),
                    MSG_DONTWAIT) > 0) {}
      test.submitting_notification = true;
      const bool accepted = test.reactor().notify_inval_inode(
          FUSE_ROOT_ID, 16384, 8192, notification_done, &test);
      test.submitting_notification = false;
      test.check(accepted && test.notification_callbacks == 2,
                 "inode invalidation was rejected or completed inline");
    } else if (test.notification_callbacks == 3) {
      std::array<char, sizeof(fuse_out_header) + sizeof(fuse_notify_inval_inode_out)> packet{};
      size_t received = 0;
      while (received < packet.size()) {
        const ssize_t count = ::recv(test.fake_fuse_peer.get(), packet.data() + received,
                                     packet.size() - received, MSG_DONTWAIT);
        if (count < 0 && errno == EINTR) continue;
        if (!test.check(count > 0, "invalidation callback preceded complete notification")) return;
        received += size_t(count);
      }
      fuse_out_header header{};
      fuse_notify_inval_inode_out body{};
      memcpy(&header, packet.data(), sizeof(header));
      memcpy(&body, packet.data() + sizeof(header), sizeof(body));
      if (!test.check(header.len == packet.size() && header.unique == 0 &&
                      header.error == FUSE_NOTIFY_INVAL_INODE &&
                      body.ino == FUSE_ROOT_ID && body.off == 16384 &&
                      body.len == 8192, "invalid inode invalidation notification")) return;
      test.blocked_reply_case();
    } else if (test.notification_callbacks == 4) {
      if (!test.check(test.blocked_reply_callbacks == 0,
                      "ordinary reply was not blocked during notification")) return;
      std::array<char, 65536> discarded{};
      size_t drained = 0;
      ssize_t count;
      do {
        count = ::recv(test.fairness_peer.get(), discarded.data(),
                       discarded.size(), MSG_DONTWAIT);
        if (count > 0) drained += size_t(count);
      } while (count > 0 || (count < 0 && errno == EINTR));
      test.check(drained > 0 && count < 0 &&
                 (errno == EAGAIN || errno == EWOULDBLOCK),
                 "unable to drain blocked ordinary reply");
    } else {
      test.check(false, "notification callback ran more than once");
    }
  }

  static void completed(void* context, ssize_t result) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    ++test.callbacks;
    if (!test.check(!test.submitting && !test.io.pending(), "completion was inline or request still pending") ||
        !test.check(std::this_thread::get_id() == test.owner &&
                     io_executor() == &test.reactor() &&
                     current_fuse_reactor() == &test.reactor(), "completion ran on wrong context")) return;
    if (test.phase == 0) {
      if (!test.check(result == 3 && test.io.operations == 3, "processor continuation did not consume three CQEs")) return;
      test.cancellation_case();
    } else if (test.phase == 1) {
      if (!test.check(result == -ECANCELED, "cancelled receive result")) return;
      test.phase = 2;
      test.io = {};
      test.io.fd = test.socket.get();
      test.io.data = test.data.data();
      test.io.length = 1;
      if (!test.check(::write(test.peer.get(), "z", 1) == 1, "feed reused request")) return;
      test.submit();
    } else if (test.phase == 2) {
      if (!test.check(result == 1 && test.data[0] == 'z', "old cancellation affected recycled request")) return;
      if (++test.cancelled_rounds != 32) {
        test.cancellation_case();
        return;
      }
      test.phase = 3;
      test.io = {};
      test.io.fd = test.socket.get();
      test.io.data = test.data.data();
      test.io.length = 1;
      test.io.timeout_ms = 5;
      test.submit();
    } else if (test.phase == 3) {
      if (!test.check(result == -ETIMEDOUT, "receive deadline did not cancel")) return;
      test.phase = 4;
      memcpy(test.data.data(), "abcdefgh", test.data.size());
      test.io = {};
      test.io.kind = AsyncIoRequest::PWRITE;
      test.io.fd = test.file.get();
      test.io.data = test.data.data();
      test.io.length = test.data.size();
      test.io.output_offset = 0;
      test.io.force_async = true;
      test.io.exact = true;
      test.submit();
    } else if (test.phase == 4) {
      if (!test.check(result == 8, "async pwrite result")) return;
      test.phase = 5;
      test.data.fill(0);
      test.io = {};
      test.io.kind = AsyncIoRequest::PREAD;
      test.io.fd = test.file.get();
      test.io.data = test.data.data();
      test.io.length = test.data.size();
      test.io.input_offset = 0;
      test.io.exact = true;
      test.submit();
    } else if (test.phase == 5) {
      if (!test.check(result == 8 && memcmp(test.data.data(), "abcdefgh", 8) == 0, "async pread data")) return;
      test.phase = 6;
      test.io = {};
      test.io.kind = AsyncIoRequest::SPLICE;
      test.io.fd = test.file.get();
      test.io.input_offset = 0;
      test.io.output_fd = test.pipe.write_fd();
      test.io.length = 8;
      test.io.exact = true;
      test.submit();
    } else if (test.phase == 6) {
      if (!test.check(result == 8, "async splice result")) return;
      test.phase = 7;
      test.data.fill(0);
      test.io = {};
      test.io.kind = AsyncIoRequest::READ;
      test.io.fd = test.pipe.read_fd();
      test.io.data = test.data.data();
      test.io.length = 8;
      test.io.exact = true;
      test.submit();
    } else if (test.phase == 7) {
      if (!test.check(result == 8 && memcmp(test.data.data(), "abcdefgh", 8) == 0, "async pipe read data")) return;
      test.notification_case();
    } else {
      if (!test.check(result == 1 && test.data[0] == 'n',
                      "read was blocked by pending notification")) return;
      if (!test.check(test.notification_callbacks == 0,
                      "notification serialized an unrelated read")) return;
      // Both ordinary replies must complete while INVAL remains blocked.
      // Draining INVAL earlier would hide a shared-FIFO dependency cycle.
      // SQPOLL can advance the shared head concurrently; inspect the
      // owner-local preparation tail to count this newly queued reply.
      const unsigned tail = test.reactor().ring_.sq.sqe_tail;
      if (test.queue_plain_reply('a', queued_reply_done)) {
        if (!test.check(test.reactor().ring_.sq.sqe_tail == tail + 1,
                        "ordinary reply was queued behind INVAL")) return;
        test.queue_plain_reply('b', queued_reply_done);
      }
    }
  }

  static void shutdown_done(void* context, ssize_t result) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    test.check(result < 0 && !test.shutdown_io.pending(), "shutdown did not retire active receive");
    test.check(io_executor() == &test.reactor(), "shutdown completion lacks scope");
    test.shutdown_completed = true;
  }

  static void on_stopped(void* context) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    test.stopped_called = true;
    test.check(!test.reactor().post(&test.rejected) && errno == ENOTCONN,
               "shutdown accepted a new callback");
    const unsigned notifications = test.notification_callbacks;
    test.check(!test.reactor().notify_inval_inode(
                   FUSE_ROOT_ID, 0, 4096, notification_done, &test) &&
               errno == ENOTCONN &&
               test.notification_callbacks == notifications,
               "shutdown notification rejection invoked its callback");
    // A previously accepted continuation may still hand local cleanup to a
    // worker. Only unrelated post/IO admission closes during shutdown.
    test.stopped.run = on_cleanup_stage;
    if (test.check(test.reactor().reserve_completion(&test.stopped),
                    "shutdown rejected an admitted cleanup continuation")) {
      test.reactor().complete(&test.stopped);
    }
    for (auto& stage : test.cleanup_stages) {
      stage = {on_cleanup_stage, nullptr, &test};
      if (test.check(test.reactor().reserve_completion(&stage),
                      "cleanup overflow admission failed")) {
        test.reactor().complete(&stage);
      }
    }
    test.worker_can_complete.store(true, std::memory_order_release);
    test.worker_can_complete.notify_one();
  }

  static void on_cleanup_stage(void* context) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    test.check(std::this_thread::get_id() == test.owner,
                 "cleanup continuation left its owner reactor");
    ++test.cleanup_count;
  }

  static void on_worker_completion(void* context) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
    test.check(std::this_thread::get_id() == test.owner &&
                 io_executor() == &test.reactor() && current_fuse_reactor() == &test.reactor(),
               "reserved worker completion lacks owner scope");
    test.worker_completed = true;
  }

  bool reply_shutdown_case() {
    ReactorIoTest test;
    if (!check(test.initialize(sqpoll), "initialize reply shutdown case")) return false;
    FuseReactor& owner = test.reactor();
    if (!check(io_uring_enable_rings(&owner.ring_) == 0,
               "enable reply shutdown ring") ||
        !test.fill_socket(test.fake_fuse.get()) ||
        !test.fill_socket(test.fairness_socket.get())) return false;
    owner.ring_enabled_ = true;
    struct Completion {
      unsigned count = 0;
      int result = 0;
    };
    std::array<Completion, 4> completions{};
    FuseReactor::current_ = &owner;
    FuseReactorReplyScope scope(&owner);
    bool accepted = true;
    for (size_t i = 0; i < completions.size(); ++i) {
      auto* reply = owner.acquire_reply();
      if (reply == nullptr) {
        accepted = false;
        break;
      }
      reply->length = 1;
      reply->data()[0] = 'x';
      reply->notification = i >= 2;
      reply->output_fd = i >= 2 ? test.fake_fuse.get() :
          test.fairness_socket.get();
      reply->notify_context = &completions[i];
      reply->notify_done = [](void* context, int result) noexcept {
        auto& completion = *static_cast<Completion*>(context);
        ++completion.count;
        completion.result = result;
      };
      if (!owner.enqueue_reply(reply)) {
        owner.release_reply(reply);
        accepted = false;
        break;
      }
    }
    const bool submitted = accepted && io_uring_submit(&owner.ring_) == 2;
    // A running io-wq write is not necessarily cancellable. The fake device
    // must eventually release it, just as a real outstanding READ reply or
    // connection teardown releases an INVAL folio wait. Use an admitted
    // shutdown continuation, not a sleep, to make that release deterministic.
    FuseReactor::ReactorTask cleanup{};
    cleanup.context = &test;
    cleanup.run = [](void* context) noexcept {
      auto& value = *static_cast<ReactorIoTest*>(context);
      std::array<char, 65536> discarded{};
      for (int fd : {value.fake_fuse_peer.get(), value.fairness_peer.get()}) {
        ssize_t count;
        do {
          count = ::recv(fd, discarded.data(), discarded.size(), MSG_DONTWAIT);
        } while (count > 0 || (count < 0 && errno == EINTR));
      }
    };
    const bool reserved = owner.reserve_completion(&cleanup);
    if (reserved) owner.complete(&cleanup);
    else {
      accepted = false;
      cleanup.run(cleanup.context);
    }
    test.group.begin_shutdown();
    fuse_session_exit(test.session);
    owner.drain_shutdown();
    const bool drained = !owner.reply_queues_[0].pending &&
        !owner.reply_queues_[1].pending && owner.reply_count_ == 2 &&
        completions[0].count == 1 && completions[0].result <= 0 &&
        completions[2].count == 1 && completions[2].result <= 0 &&
        completions[1].count == 0 && completions[3].count == 0;
    // The two queued tails were never submitted; retire them only after both
    // original CQEs prove the kernel has stopped accessing the lane heads.
    owner.fail_replies(-ENOTCONN);
    FuseReactor::current_ = nullptr;
    bool completed = owner.reply_count_ == 0;
    for (const Completion& completion : completions) {
      completed = completed && completion.count == 1 && completion.result <= 0;
    }
    return check(accepted && submitted && reserved && drained && completed &&
                 completions[1].result == -ENOTCONN &&
                 completions[3].result == -ENOTCONN,
                 "shutdown lost or prematurely freed a reply lane");
  }

  bool shutdown_admission_case() {
    FuseReactorGroup closed_group;
    closed_group.session_ = session;
    std::string error;
    for (unsigned index = 0; index < 2; ++index) {
      auto value = std::make_unique<FuseReactor>();
      if (!value->initialize(&closed_group, session, fake_fuse.get(), false,
                             false, 8, 0, sqpoll, error)) {
        fprintf(stderr, "reactor_io_test: shutdown gate reactor: %s\n",
                error.c_str());
        return false;
      }
      closed_group.reactors_.push_back(std::move(value));
    }

    std::atomic<unsigned> returned{0};
    std::barrier start(3);
    const auto shutdown = [&] {
      start.arrive_and_wait();
      closed_group.begin_shutdown();
      returned.fetch_add(1, std::memory_order_release);
    };
    std::shared_lock admission(closed_group.external_mutex_);
    std::thread first(shutdown);
    std::thread second(shutdown);
    start.arrive_and_wait();
    while (!closed_group.shutting_down_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    if (!check(returned.load(std::memory_order_acquire) == 0,
               "shutdown crossed a held remote-admission gate")) {
      admission.unlock();
      first.join();
      second.join();
      return false;
    }
    admission.unlock();
    first.join();
    second.join();
    if (!check(returned.load(std::memory_order_acquire) == 2,
               "repeated shutdown did not cross its admission barrier")) {
      return false;
    }

    FuseReactor& source = *closed_group.reactors_[0];
    FuseReactor& target = *closed_group.reactors_[1];
    FuseReactor::Dispatch dispatch{};
    const size_t target_tasks = target.task_count_.load(
        std::memory_order_acquire);
    const unsigned source_sqes = io_uring_sq_ready(&source.ring_);
    const bool accepted = source.start_remote_dispatch(&dispatch, &target);
    return check(!accepted &&
                 target.task_count_.load(std::memory_order_acquire) ==
                     target_tasks &&
                 io_uring_sq_ready(&source.ring_) == source_sqes,
                 "closed remote dispatch changed target admission or SQEs");
  }

  bool full_sq_case() {
    ReactorIoTest test;
    if (!check(test.initialize(sqpoll), "initialize full SQ case")) return false;
    FuseReactor& owner = test.reactor();
    if (!check(io_uring_enable_rings(&owner.ring_) == 0,
               "enable full SQ ring")) return false;
    owner.ring_enabled_ = true;
    const unsigned count = io_uring_sq_space_left(&owner.ring_);
    // Only prepare SQEs: the shared tail is not published, so even SQPOLL
    // cannot consume anything before acquire_sqe() takes its full-SQ path.
    for (unsigned i = 0; i < count; ++i) {
      io_uring_sqe* sqe = io_uring_get_sqe(&owner.ring_);
      if (!check(sqe != nullptr, "prepare full SQ")) return false;
      io_uring_prep_nop(sqe);
      io_uring_sqe_set_data64(sqe, i);
    }
    if (!check(io_uring_sq_space_left(&owner.ring_) == 0,
               "SQ was not full before retry")) return false;
    io_uring_sqe* sqe = owner.acquire_sqe();
    if (!check(sqe != nullptr, "full SQ retry failed to obtain a slot")) return false;
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data64(sqe, count);
    std::vector<bool> seen(count + 1, false);
    for (unsigned done = 0; done <= count;) {
      if (!check(io_uring_submit_and_wait(&owner.ring_, 1) >= 0,
                 "submit/wait full SQ operations")) return false;
      io_uring_cqe* cqe = nullptr;
      while (io_uring_peek_cqe(&owner.ring_, &cqe) == 0) {
        const uint64_t id = cqe->user_data;
        if (!check(cqe->res == 0 && id <= count && !seen[size_t(id)],
                   "full SQ returned an erroneous or duplicate completion")) return false;
        seen[size_t(id)] = true;
        ++done;
        io_uring_cqe_seen(&owner.ring_, cqe);
      }
    }
    return true;
  }

  int run() {
    if (!full_sq_case()) return 1;
    errno = 0;
    check(!reactor().notify_inval_inode(
               FUSE_ROOT_ID, 0, 4096, notification_done, this) &&
          errno == EPERM && notification_callbacks == 0,
          "off-owner notification rejection invoked its callback");
    if (failed) return 1;
    for (auto& task : tasks) task = {noop, cancelled, this};
    tasks[0].run = begin;
    tasks[1].run = fairness_callback;
    std::thread producer([&] {
      for (auto& task : tasks) {
        if (!reactor().post(&task)) failed = true;
      }
      if (reactor().post(&rejected) || errno != EAGAIN) failed = true;
    });
    producer.join();
    if (failed) return 1;
    int result = -EIO;
    std::thread event_loop([&] {
      owner = std::this_thread::get_id();
      result = group.run();
    });
    event_loop.join();
    check(reply_shutdown_case(), "reply shutdown regression case failed");
    check(shutdown_admission_case(),
          "shutdown admission regression case failed");
    check(result == 0, "reactor exited with error");
    check(posts == tasks.size() - 1,
          "accepted ordinary callbacks did not all execute");
    check(cancellations == 0, "post rejection took caller-owned cancellation responsibility");
    check(shutdown_completed && stopped_called && worker_completed,
           "shutdown skipped accepted I/O or callbacks");
    check(cleanup_count == cleanup_stages.size() + 1,
           "shutdown skipped overflow cleanup continuations");
    check(notification_callbacks == 4 && queued_reply_callbacks == 2 &&
          blocked_reply_callbacks == 1,
          "accepted notifications did not each complete exactly once");
    check(fairness_completion_iteration >= 64,
          "continuously ready callbacks starved an asynchronous CQE");
    check(burst_completions == burst_io.size() &&
          burst_replies == burst_io.size() && !burst_reply_pending,
          "ready FUSE continuations were not drained between I/O CQEs");
    check(burst_data[0] == 'b' && burst_data[1] == 'b',
          "CQ burst PREAD returned incorrect data");
    return failed ? 1 : 0;
  }
};

int main(int argc, char** argv) {
  const bool sqpoll = argc == 2 && strcmp(argv[1], "--sqpoll") == 0;
  const bool dispatch = argc == 2 && strcmp(argv[1], "--dispatch") == 0;
  const bool dispatch_freelist =
      argc == 2 && strcmp(argv[1], "--dispatch-freelist") == 0;
  const bool metadata_io =
      argc == 2 && strcmp(argv[1], "--metadata-io") == 0;
  if (argc > 1 && !sqpoll && !dispatch && !dispatch_freelist &&
      !metadata_io) return 1;
  try {
    if (dispatch) return ReactorIoTest::dispatch_case();
    if (dispatch_freelist) {
      int result = ReactorIoTest::dispatch_freelist_case();
      if (result != 0) return result;
      result = ReactorIoTest::shutdown_msg_ring_case();
      if (result != 0) return result;
      for (bool shutdown : {false, true}) {
        for (bool fail : {false, true}) {
          result = ReactorIoTest::msg_ring_batch_case(fail, shutdown);
          if (result != 0) return result;
        }
      }
      return 0;
    }
    if (metadata_io) return ReactorIoTest::metadata_io_case();
    ReactorIoTest test;
    if (!test.initialize(sqpoll)) return test.failed ? 1 : 77;
    return test.run();
  } catch (const std::exception& error) {
    fprintf(stderr, "reactor_io_test: %s\n", error.what());
    return 1;
  }
}
