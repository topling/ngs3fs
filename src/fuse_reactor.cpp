#include "fuse_reactor.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <new>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

uint64_t reactor_monotonic_ns() noexcept {
  timespec value{};
  if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
    return 0;
  }
  return uint64_t(value.tv_sec) * 1'000'000'000 +
      uint64_t(value.tv_nsec);
}

thread_local FuseReactor* FuseReactor::current_ = nullptr;
thread_local FuseReactor* FuseReactor::reply_target_ = nullptr;
thread_local FuseReactor::Dispatch* FuseReactor::current_dispatch_ = nullptr;
thread_local int FuseReactor::receive_fd_ = -1;

FuseReactorReplyScope::FuseReactorReplyScope(
    FuseReactor* reactor, int timeout_ms, int receive_fd, bool detached) noexcept
    : previous_(FuseReactor::reply_target_),
      previous_receive_fd_(FuseReactor::receive_fd_),
      previous_dispatch_(FuseReactor::current_dispatch_),
      io_scope_(reactor, timeout_ms) {
  FuseReactor::reply_target_ = reactor;
  FuseReactor::receive_fd_   = receive_fd;
  if (detached) FuseReactor::current_dispatch_ = nullptr;
  if (reactor != nullptr && FuseReactor::current_ == reactor) {
    continuation_owner_ = reactor;
    ++reactor->continuation_depth_;
  }
}

FuseReactorReplyScope::~FuseReactorReplyScope() {
  if (continuation_owner_ != nullptr) --continuation_owner_->continuation_depth_;
  FuseReactor::reply_target_ = previous_;
  FuseReactor::receive_fd_   = previous_receive_fd_;
  FuseReactor::current_dispatch_ =
      static_cast<FuseReactor::Dispatch*>(previous_dispatch_);
}

FuseReactor* current_fuse_reactor() noexcept {
  return FuseReactor::current_ ? FuseReactor::current_ :
      FuseReactor::reply_target_;
}

bool FuseReactor::start_task(ReactorTask* task) noexcept {
  if (callback_count_ == ready_callbacks_.size()) {
    // This task already owns admission (including MSG_RING dispatches). Queue
    // overflow must not discard a request already removed from /dev/fuse.
    task->completion_owner = this;
    complete(task);
    return true;
  }
  const size_t slot = (callback_head_ + callback_count_) % ready_callbacks_.size();
  ready_callbacks_[slot] = task;
  ++callback_count_;
  return true;
}

void FuseReactor::dispatch_entry(void* context) noexcept {
  Dispatch* dispatch = static_cast<Dispatch*>(context);
  FuseReactor* reactor = current_;
  if (reactor == nullptr || dispatch == nullptr) {
    abort();
  }
  fuse_session_process_buf_fd(
      reactor->session_, &dispatch->buffer, dispatch->output_fd);
  dispatch->processing_complete = true;
}

bool FuseReactor::start_dispatch(Dispatch* dispatch) noexcept {
  if (dispatch == nullptr) {
    return false;
  }
  dispatch->input_tasks.store(1, std::memory_order_relaxed);
  dispatch->task = {
      dispatch_entry, nullptr, dispatch, dispatch};
  if (enqueue_task(&dispatch->task)) {
    return true;
  }
  dispatch->task.input_owner = nullptr;
  return false;
}

bool FuseReactor::start_remote_dispatch(
    Dispatch* dispatch, FuseReactor* target) noexcept {
  if (dispatch == nullptr || target == nullptr || target == this) {
    return false;
  }
  // Reserve the destination's lifetime before shutdown can observe it drained.
  // This gate is for actual cross-owner work, not the one-reactor fast path.
  std::shared_lock guard(group_->external_mutex_);
  if (group_->shutting_down_.load(std::memory_order_acquire)) return false;
  target->task_count_.fetch_add(1, std::memory_order_acquire);
  dispatch->input_tasks.store(1, std::memory_order_relaxed);
  dispatch->task = {dispatch_entry, nullptr, dispatch, dispatch};
  dispatch->target = target;
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    target->task_count_.fetch_sub(1, std::memory_order_release);
    return false;
  }
  constexpr uintptr_t kTaskTag = 3;
  io_uring_prep_msg_ring(
      sqe, target->ring_.ring_fd, 0,
      uintptr_t(&dispatch->task) | kTaskTag, 0);
  // Only failures produce a source CQE. It must retain enough ownership
  // information to undo the reservation when the target never receives it.
  constexpr uintptr_t kDispatchFailureTag = 4;
  io_uring_sqe_set_data64(
      sqe, uintptr_t(dispatch) | kDispatchFailureTag);
  sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
  return true;
}

void FuseReactor::release_input_dispatch(
    Dispatch* dispatch, bool drain_input) noexcept {
  if (dispatch == nullptr) {
    return;
  }
  dispatch->input_drain_needed |= drain_input;
  const unsigned previous = dispatch->input_tasks.fetch_sub(
      1, std::memory_order_acq_rel);
  if (previous == 0) {
    abort();
  }
  if (previous != 1) {
    return;
  }
  if (dispatch->input_drain_needed &&
      !drain_receive_pipe(dispatch->pipe[0]) && error_ == 0) {
    error_ = -EIO;
  }
  if (!dispatch->processing_complete) {
    return;
  }
  if (dispatch->owner != this) {
    dispatch->owner->dispatch_complete(dispatch);
    return;
  }
  finish_dispatch(dispatch);
  if (dispatch_count_ == 0) {
    abort();
  }
  --dispatch_count_;
  if (!submit_receive() && error_ == 0) {
    error_ = -EAGAIN;
  }
}

bool FuseReactor::run_ready_callbacks() noexcept {
  if (completion_pending_.load(std::memory_order_acquire)) {
    std::lock_guard guard(completion_mutex_);
    while (completion_head_ != nullptr && callback_count_ < ready_callbacks_.size()) {
      ReactorTask* task = completion_head_;
      completion_head_ = task->completion_next;
      if (!start_task(task)) abort();
      task->completion_next = nullptr;
    }
    if (completion_head_ == nullptr) completion_tail_ = nullptr;
    completion_pending_.store(completion_head_ != nullptr, std::memory_order_release);
  }
  const size_t count = callback_count_;
  for (size_t i = 0; i < count; ++i) {
    ReactorTask* task = ready_callbacks_[callback_head_];
    callback_head_ = (callback_head_ + 1) % ready_callbacks_.size();
    --callback_count_;
    task->completion_owner = nullptr;
    task->completion_queued = false;
    Dispatch* dispatch = static_cast<Dispatch*>(task->input_owner);
    Dispatch* previous_dispatch = current_dispatch_;
    current_dispatch_ = dispatch;
    {
      FuseReactorReplyScope scope(this, group_->io_timeout_ms_,
          dispatch ? dispatch->pipe[0] : -1, dispatch == nullptr);
      task->run(task->context);
    }
    current_dispatch_ = previous_dispatch;
    release_input_dispatch(dispatch, false);
    task_count_.fetch_sub(1, std::memory_order_release);
  }
  return error_ == 0;
}

void* FuseReactor::retain_input() noexcept {
  Dispatch* dispatch = current_dispatch_;
  if (current_ != this || dispatch == nullptr) return nullptr;
  dispatch->input_tasks.fetch_add(1, std::memory_order_acq_rel);
  return dispatch;
}

void FuseReactor::release_input(void* token, bool consumed) noexcept {
  if (current_ != this) abort();
  release_input_dispatch(static_cast<Dispatch*>(token), !consumed);
}

FuseReactor::~FuseReactor() {
  if (ring_ready_) {
    io_uring_queue_exit(&ring_);
    ring_ready_ = false;
  }
  for (Dispatch* dispatch : receiving_) {
    finish_dispatch(dispatch);
  }
  receiving_.clear();
  fail_replies(-ENOTCONN);
  if (external_pipe_[1] >= 0) {
    ::close(external_pipe_[1]);
    external_pipe_[1] = -1;
  }
  if (external_pipe_[0] >= 0) {
    fail_external_replies(-ENOTCONN);
    ::close(external_pipe_[0]);
  }
  if (task_pipe_[1] >= 0) {
    ::close(task_pipe_[1]);
    task_pipe_[1] = -1;
  }
  if (task_pipe_[0] >= 0) {
    ReactorTask* task = nullptr;
    while (::read(task_pipe_[0], &task, sizeof(task)) ==
           ssize_t(sizeof(task))) {
      Dispatch* dispatch = static_cast<Dispatch*>(task->input_owner);
      if (task->cancel != nullptr) {
        task->cancel(task->context);
      }
      if (dispatch != nullptr) {
        const unsigned previous = dispatch->input_tasks.fetch_sub(
            1, std::memory_order_acq_rel);
        if (previous == 0) {
          abort();
        }
      }
      task = nullptr;
    }
    ::close(task_pipe_[0]);
  }
  while (callback_count_ != 0) {
    ReactorTask* task = ready_callbacks_[callback_head_];
    callback_head_ = (callback_head_ + 1) % ready_callbacks_.size();
    --callback_count_;
    Dispatch* dispatch = static_cast<Dispatch*>(task->input_owner);
    if (task->cancel != nullptr) task->cancel(task->context);
    if (dispatch != nullptr) {
      dispatch->input_tasks.fetch_sub(1, std::memory_order_acq_rel);
    }
  }
  ready_callbacks_.clear();
  if (dispatch_pipe_[1] >= 0) {
    ::close(dispatch_pipe_[1]);
    dispatch_pipe_[1] = -1;
  }
  if (dispatch_pipe_[0] >= 0) {
    fail_dispatches();
    ::close(dispatch_pipe_[0]);
  }
  for (Dispatch* dispatch : free_dispatches_) {
    if (dispatch->pipe[0] >= 0) {
      ::close(dispatch->pipe[0]);
    }
    if (dispatch->pipe[1] >= 0) {
      ::close(dispatch->pipe[1]);
    }
    delete dispatch;
  }
  free_dispatches_.clear();
  if (wake_fd_ >= 0) {
    ::close(wake_fd_);
  }
  if (owns_fuse_fd_ && fuse_fd_ >= 0) {
    ::close(fuse_fd_);
  }
  for (int fd : owned_receive_fds_) {
    ::close(fd);
  }
}

bool FuseReactor::initialize(FuseReactorGroup* group,
                             fuse_session* session, int fuse_fd,
                             bool owns_fuse_fd,
                             bool initialization_owner,
                             unsigned depth,
                             unsigned receive_concurrency,
                             std::string& error) {
  if (session == nullptr || fuse_fd < 0 || depth < 8) {
    error = "invalid FUSE reactor configuration";
    return false;
  }
  group_                = group;
  session_              = session;
  fuse_fd_              = fuse_fd;
  owns_fuse_fd_         = owns_fuse_fd;
  initialization_owner_ = initialization_owner;
  receive_concurrency_ = receive_concurrency;
  receive_fds_.reserve(receive_concurrency_);
  owned_receive_fds_.reserve(receive_concurrency_);
  if (receive_concurrency_ != 0) {
    const int flags = fcntl(fuse_fd, F_GETFL);
    if (flags < 0 || fcntl(fuse_fd, F_SETFL, flags | O_NONBLOCK) != 0) {
      error = "fcntl(/dev/fuse, O_NONBLOCK): " +
              std::string(strerror(errno));
      return false;
    }
    receive_fds_.push_back(fuse_fd);
  }
  receive_pending_fds_.resize(receive_fds_.size(), false);
  if (::pipe2(external_pipe_, O_CLOEXEC | O_NONBLOCK) != 0) {
    error = "pipe2(FUSE external replies): " +
            std::string(strerror(errno));
    return false;
  }
  if (::pipe2(dispatch_pipe_, O_CLOEXEC | O_NONBLOCK) != 0) {
    error = "pipe2(FUSE dispatch completion): " +
            std::string(strerror(errno));
    return false;
  }
  if (::pipe2(task_pipe_, O_CLOEXEC | O_NONBLOCK) != 0) {
    error = "pipe2(reactor tasks): " + std::string(strerror(errno));
    return false;
  }
  wake_fd_ = eventfd(0, EFD_CLOEXEC);
  if (wake_fd_ < 0) {
    error = "eventfd(FUSE wakeup): " + std::string(strerror(errno));
    return false;
  }
  io_uring_params params{};
  // initialize() can run on a different thread from run(). Enabling a disabled
  // ring on its owner binds SINGLE_ISSUER to that thread, also for multi-ring
  // mounts; only that owner submits SQEs or advances its completion queue.
  params.flags = IORING_SETUP_R_DISABLED | IORING_SETUP_COOP_TASKRUN |
                 IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN;
  int result = io_uring_queue_init_params(depth, &ring_, &params);
  if (result == -EINVAL) {
    params = {};
    result = io_uring_queue_init_params(depth, &ring_, &params);
  }
  if (result != 0) {
    error = "io_uring_queue_init: " + std::string(strerror(-result));
    return false;
  }
  ring_ready_ = true;
  setup_flags_ = params.flags;
  io_uring_probe* probe = io_uring_get_probe_ring(&ring_);
  if (probe == nullptr) {
    error = "io_uring_get_probe_ring: " + std::string(strerror(errno));
    return false;
  }
  constexpr std::array required_ops{
      IORING_OP_READ,
      IORING_OP_WRITE,
      IORING_OP_SPLICE,
      IORING_OP_POLL_ADD,
      IORING_OP_RECV,
      IORING_OP_SEND,
      IORING_OP_CONNECT,
      IORING_OP_ASYNC_CANCEL,
      IORING_OP_MSG_RING,
  };
  for (unsigned op : required_ops) {
    if (!io_uring_opcode_supported(probe, int(op))) {
      io_uring_free_probe(probe);
      error = "io_uring lacks a required FUSE transport operation";
      return false;
    }
  }
  io_uring_free_probe(probe);
  max_reply_count_ = std::max<size_t>(4, depth / 2);
  max_dispatch_count_ = std::min<size_t>(16, std::max<size_t>(4, depth / 2));
  max_task_count_ = depth;
  reply_pool_size_ = depth;
  reply_pool_.reset(new (std::nothrow) Reply[reply_pool_size_]);
  if (!reply_pool_) {
    error = "unable to allocate FUSE reply pool";
    return false;
  }
  for (size_t index = 0; index < reply_pool_size_; ++index) {
    reply_pool_[index].pooled = true;
    reply_pool_[index].next = index + 1 < reply_pool_size_ ?
        &reply_pool_[index + 1] : nullptr;
  }
  reply_free_ = reply_pool_.get();
  async_pool_.reset(new (std::nothrow) IoRequest[depth]);
  if (!async_pool_) {
    error = "unable to allocate reactor I/O request pool";
    return false;
  }
  for (size_t i = 0; i < depth; ++i) {
    async_pool_[i].next_free = i + 1 < depth ? &async_pool_[i + 1] : nullptr;
  }
  async_free_ = async_pool_.get();
  io_requests_.reserve(size_t(depth) * 2);
  free_dispatches_.reserve(depth);
  receiving_.reserve(depth);
  ready_callbacks_.resize(depth, nullptr);

  return true;
}

bool FuseReactor::submit(AsyncIoRequest& operation) noexcept {
  if (current_ != this) {
    errno = EPERM;
    return false;
  }
  if (operation.implementation_ != nullptr) {
    errno = EBUSY;
    return false;
  }
  if (!ring_ready_ || error_ != 0 ||
      group_->shutting_down_.load(std::memory_order_acquire)) {
    errno = ENOTCONN;
    return false;
  }
  if (operation.complete == nullptr || operation.fd < 0 ||
      operation.length > INT_MAX || operation.timeout_ms < 0 ||
      (operation.kind != AsyncIoRequest::CONNECT &&
       operation.kind != AsyncIoRequest::SPLICE &&
       operation.length != 0 && operation.data == nullptr) ||
      (operation.kind == AsyncIoRequest::CONNECT &&
       (operation.address == nullptr || operation.address_length == 0)) ||
      (operation.kind == AsyncIoRequest::SPLICE && operation.output_fd < 0) ||
      (operation.kind == AsyncIoRequest::PREAD && operation.input_offset < 0) ||
      (operation.kind == AsyncIoRequest::PWRITE && operation.output_offset < 0) ||
      (operation.processor != nullptr &&
       (operation.kind != AsyncIoRequest::RECEIVE || operation.length == 0))) {
    errno = EINVAL;
    return false;
  }
  if (async_free_ == nullptr) {
    errno = EAGAIN;
    return false;
  }
  if (io_uring_sq_space_left(&ring_) == 0) {
    const int result = io_uring_submit(&ring_);
    if (result < 0) {
      errno = -result;
      return false;
    }
  }

  IoRequest* request = async_free_;
  async_free_ = request->next_free;
  switch (operation.kind) {
    case AsyncIoRequest::RECEIVE: request->kind = IO_RECEIVE; break;
    case AsyncIoRequest::READ:    request->kind = IO_READ;    break;
    case AsyncIoRequest::PREAD:   request->kind = IO_PREAD;   break;
    case AsyncIoRequest::SEND:    request->kind = IO_SEND;    break;
    case AsyncIoRequest::WRITE:   request->kind = IO_WRITE;   break;
    case AsyncIoRequest::PWRITE:  request->kind = IO_PWRITE;  break;
    case AsyncIoRequest::SPLICE:  request->kind = IO_SPLICE;  break;
    case AsyncIoRequest::CONNECT: request->kind = IO_CONNECT; break;
    default:
      request->next_free = async_free_;
      async_free_ = request;
      errno = EINVAL;
      return false;
  }
  request->fd                 = operation.fd;
  request->output_fd          = operation.output_fd;
  request->data               = operation.data;
  request->address            = operation.address;
  request->address_length     = operation.address_length;
  request->length             = operation.length;
  request->transferred        = 0;
  request->operations         = 0;
  request->flags              = int(operation.flags);
  request->timeout_ms         = operation.timeout_ms;
  request->input_offset       = operation.input_offset;
  request->output_offset      = operation.output_offset;
  request->has_input_offset   = operation.input_offset >= 0;
  request->has_output_offset  = operation.output_offset >= 0;
  request->force_async        = operation.force_async;
  request->exact              = operation.exact;
  request->timed_out          = false;
  request->cancel_submitted   = false;
  request->receive_processor  = operation.processor;
  request->receive_context    = operation.processor_context;
  request->deadline_ns        = 0;
  request->active_index       = SIZE_MAX;
  request->async              = &operation;
  request->async_result       = -ECANCELED;
  request->cancelled          = false;
  request->cancel_pending     = false;
  request->original_completed = false;
  if (!submit_io_request(request)) {
    request->async = nullptr;
    request->next_free = async_free_;
    async_free_ = request;
    errno = EAGAIN;
    return false;
  }
  operation.transferred = 0;
  operation.operations = 0;
  operation.owner_ = this;
  operation.implementation_ = request;
  ++async_pending_;
  return true;
}

bool FuseReactor::cancel(AsyncIoRequest& operation) noexcept {
  if (current_ != this) {
    errno = EPERM;
    return false;
  }
  if (operation.owner_ != this || operation.implementation_ == nullptr) {
    errno = ENOENT;
    return false;
  }
  IoRequest* request = static_cast<IoRequest*>(operation.implementation_);
  if (request->cancel_submitted) {
    return true;
  }
  if (request->active_index == SIZE_MAX) {
    errno = EALREADY;
    return false;
  }
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    const int result = io_uring_submit(&ring_);
    if (result < 0) {
      errno = -result;
      return false;
    }
    sqe = io_uring_get_sqe(&ring_);
  }
  if (sqe == nullptr) {
    errno = EAGAIN;
    return false;
  }
  request->cancelled        = true;
  request->cancel_submitted = true;
  request->cancel_pending   = true;
  io_uring_prep_cancel64(sqe, uintptr_t(request) | 1, 0);
  io_uring_sqe_set_data64(sqe, uintptr_t(request) | 5);
  if (request->deadline_ns == next_io_deadline_) {
    refresh_io_deadline();
  }
  return true;
}

bool FuseReactor::enqueue_task(ReactorTask* task) noexcept {
  if (task == nullptr || task->run == nullptr) {
    return false;
  }
  size_t count = task_count_.load(std::memory_order_relaxed);
  do {
    if (count >= max_task_count_ && task->input_owner == nullptr) {
      return false;
    }
  } while (!task_count_.compare_exchange_weak(
      count, count + 1, std::memory_order_acquire,
      std::memory_order_relaxed));
  if (current_ == this) {
    if (!start_task(task)) {
      task_count_.fetch_sub(1, std::memory_order_release);
      return false;
    }
    return true;
  }
  std::shared_lock guard(group_->external_mutex_);
  if (group_->shutting_down_.load(std::memory_order_acquire) ||
      task_pipe_[1] < 0) {
    task_count_.fetch_sub(1, std::memory_order_release);
    return false;
  }
  ReactorTask* value = task;
  ssize_t result;
  do {
    result = ::write(task_pipe_[1], &value, sizeof(value));
  } while (result < 0 && errno == EINTR);
  if (result == ssize_t(sizeof(value))) {
    return true;
  }
  task_count_.fetch_sub(1, std::memory_order_release);
  return false;
}

bool FuseReactor::post(ReactorTask* task) noexcept {
  if (task == nullptr || task->run == nullptr || task->input_owner != nullptr ||
      task->completion_owner != nullptr) {
    errno = EINVAL;
    return false;
  }
  if (group_->shutting_down_.load(std::memory_order_acquire) ||
      (current_ == this && (!ring_ready_ || error_ != 0))) {
    errno = ENOTCONN;
    return false;
  }
  if (enqueue_task(task)) return true;
  errno = group_->shutting_down_.load(std::memory_order_acquire)
      ? ENOTCONN : EAGAIN;
  return false;
}

bool FuseReactor::reserve_completion(ReactorTask* task) noexcept {
  if (current_ != this || task == nullptr || task->run == nullptr ||
      task->input_owner != nullptr || task->completion_owner != nullptr) {
    errno = EINVAL;
    return false;
  }
  std::shared_lock guard(group_->external_mutex_);
  if (!ring_ready_ ||
      ((group_->shutting_down_.load(std::memory_order_acquire) || error_ != 0) &&
       continuation_depth_ == 0)) {
    errno = ENOTCONN;
    return false;
  }
  size_t count = task_count_.load(std::memory_order_relaxed);
  do {
    if (count >= max_task_count_ && continuation_depth_ == 0) {
      errno = EAGAIN;
      return false;
    }
  } while (!task_count_.compare_exchange_weak(
      count, count + 1, std::memory_order_acquire, std::memory_order_relaxed));
  task->completion_owner = this;
  return true;
}

void FuseReactor::complete(ReactorTask* task) noexcept {
  if (task == nullptr || task->completion_owner != this) abort();
  {
    std::lock_guard guard(completion_mutex_);
    if (task->completion_queued) abort();
    task->completion_queued = true;
    task->completion_next = nullptr;
    if (completion_tail_ != nullptr) completion_tail_->completion_next = task;
    else completion_head_ = task;
    completion_tail_ = task;
    completion_pending_.store(true, std::memory_order_release);
  }
  // The reservation also keeps wake_fd_ alive while a worker is returning.
  const int saved_errno = errno;
  const uint64_t wake = 1;
  ssize_t result;
  do {
    result = ::write(wake_fd_, &wake, sizeof(wake));
  } while (result < 0 && errno == EINTR);
  errno = saved_errno;
}

ssize_t FuseReactor::sync_writev(int fd, iovec* iov, int count,
                                 void* userdata) noexcept {
  (void)userdata;
  return ::writev(fd, iov, count);
}

ssize_t FuseReactor::sync_read(int fd, void* data, size_t size,
                               void* userdata) noexcept {
  (void)userdata;
  return ::read(fd, data, size);
}

bool FuseReactor::notify_inval_inode(fuse_ino_t inode, off_t offset,
                                      off_t length, NotifyFunction done,
                                      void* context) noexcept {
  if (current_ != this) {
    errno = EPERM;
    return false;
  }
  if (done == nullptr || pending_notify_ != nullptr) {
    errno = EINVAL;
    return false;
  }
  if (!ring_ready_ || error_ != 0 ||
      group_->shutting_down_.load(std::memory_order_acquire)) {
    errno = ENOTCONN;
    return false;
  }
  pending_notify_ = done;
  pending_notify_context_ = context;
  notify_accepted_ = false;
  // Reuse libfuse's encoder and custom-I/O hook. The hook copies this scoped
  // completion into its owned Reply before libfuse's stack buffer disappears.
  const int result = fuse_lowlevel_notify_inval_inode(
      session_, inode, offset, length);
  const bool accepted = notify_accepted_;
  pending_notify_ = nullptr;
  pending_notify_context_ = nullptr;
  notify_accepted_ = false;
  if (accepted) return true;
  errno = result < 0 ? -result : EIO;
  return false;
}

ssize_t FuseReactor::async_writev(int fd, const iovec* iov, int count,
                                  fuse_req_t req, void* userdata) noexcept {
  // Notifications have no request completion callback. A storage worker must
  // finish STORE before publishing its flush completion, not report queue
  // admission as successful cache publication. Do not touch owner-only stats
  // here, and never take this blocking path on a reactor thread.
  if (req == nullptr && current_ == nullptr) {
    ssize_t result;
    do {
      result = ::writev(fd, iov, count);
    } while (result < 0 && errno == EINTR);
    return result;
  }
  auto* group = static_cast<FuseReactorGroup*>(userdata);
  FuseReactor* reactor = group ? group->callback_reactor() : nullptr;
  if (reactor == nullptr || count <= 0) {
    errno = EINVAL;
    return -1;
  }

  // A FUSE reply has a kernel request waiting for it and does not benefit
  // from io-wq. Complete reactor-local replies directly; retain the queued
  // path only for callbacks made by external worker threads.
  if (current_ == reactor && req != nullptr) {
    size_t expected = 0;
    for (int i = 0; i < count; ++i) {
      expected += iov[i].iov_len;
    }
    ssize_t result;
    do {
      result = ::writev(fd, iov, count);
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
      struct OutHeader {
        uint32_t length;
        int32_t error;
        uint64_t unique;
      };
      const auto* header = static_cast<const OutHeader*>(iov[0].iov_base);
      fprintf(stderr,
              "FUSE direct reply failed: fd=%d passed_fd=%d length=%zu "
              "header_length=%u unique=%" PRIu64 " error=%d: %s\n",
              fd, fd, expected, header->length, header->unique,
              header->error, strerror(errno));
    }
    if (result >= 0 && size_t(result) == expected) {
      ++reactor->completed_replies_;
      if (reactor->initialization_owner_ &&
          !reactor->initialization_complete_) {
        reactor->initialization_complete_ = true;
        if (!reactor->initialize_receive_clones()) {
          errno = reactor->error_ == 0 ? EIO : -reactor->error_;
          return -1;
        }
        group->reactor_initialized();
      }
    }
    return result;
  }

  Dispatch* dispatch = current_dispatch_;
  const bool reserved = req != nullptr && dispatch != nullptr &&
      dispatch->owner == reactor &&
      dispatch->reply != nullptr && !dispatch->reply_claimed;
  const bool local = current_ == reactor;
  Reply* reply = reserved ? dispatch->reply :
      (local ? reactor->acquire_reply() : new (std::nothrow) Reply);
  if (reply == nullptr) {
    errno = ENOMEM;
    return -1;
  }
  size_t length = 0;
  for (int index = 0; index < count; ++index) {
    if (iov[index].iov_len > SIZE_MAX - length) {
      if (!reserved) {
        reactor->release_reply(reply);
      }
      errno = EOVERFLOW;
      return -1;
    }
    length += iov[index].iov_len;
  }
  reply->length = length;
  if (length > sizeof(reply->inline_data)) {
    try {
      reply->overflow_data.resize(length);
    } catch (const std::bad_alloc&) {
      if (!reserved) {
        reactor->release_reply(reply);
      }
      errno = ENOMEM;
      return -1;
    }
  }
  size_t offset = 0;
  for (int index = 0; index < count; ++index) {
    memcpy(reply->data() + offset, iov[index].iov_base,
           iov[index].iov_len);
    offset += iov[index].iov_len;
  }
  reply->req       = req;
  reply->kind      = REPLY_WRITE;
  reply->output_fd = fd;
  if (local && req == nullptr && reactor->pending_notify_ != nullptr) {
    reply->notify_done = reactor->pending_notify_;
    reply->notify_context = reactor->pending_notify_context_;
    reactor->pending_notify_ = nullptr;
    reactor->pending_notify_context_ = nullptr;
  }
  if (reserved) {
    dispatch->reply_claimed = true;
  }
  (void)fd;
  if ((local && !reactor->enqueue_reply(reply)) ||
      (!local && !reactor->submit_external_reply(reply))) {
    if (!reserved) {
      reactor->release_reply(reply);
    }
    errno = EAGAIN;
    return -1;
  }
  if (reserved) {
    dispatch->reply_submitted.store(true, std::memory_order_release);
  }
  if (reply->notify_done != nullptr) reactor->notify_accepted_ = true;
  return FUSE_CUSTOM_IO_DEFERRED;
}

ssize_t FuseReactor::async_splice(int input_fd, int output_fd, size_t length,
                                  unsigned flags, fuse_req_t req,
                                  void* userdata) noexcept {
  // As above, worker STORE notifications complete locally before the worker
  // announces publication; reactor-originated notifications remain queued.
  if (req == nullptr && current_ == nullptr) {
    ssize_t result;
    do {
      result = ::splice(input_fd, nullptr, output_fd, nullptr, length, flags);
    } while (result < 0 && errno == EINTR);
    if (result >= 0 && size_t(result) != length) {
      errno = EIO;
      return -1;
    }
    return result;
  }
  auto* group = static_cast<FuseReactorGroup*>(userdata);
  FuseReactor* reactor = group ? group->callback_reactor() : nullptr;
  if (reactor == nullptr || input_fd < 0 || output_fd < 0) {
    errno = EINVAL;
    return -1;
  }

  if (current_ == reactor && req != nullptr) {
    ssize_t result;
    do {
      result = ::splice(
          input_fd, nullptr, output_fd, nullptr, length, flags);
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
      return -1;
    }
    if (size_t(result) != length) {
      errno = EIO;
      return -1;
    }
    ++reactor->completed_replies_;
    if (reactor->initialization_owner_ &&
        !reactor->initialization_complete_) {
      reactor->initialization_complete_ = true;
      if (!reactor->initialize_receive_clones()) {
        errno = reactor->error_ == 0 ? EIO : -reactor->error_;
        return -1;
      }
      group->reactor_initialized();
    }
    return result;
  }

  Dispatch* dispatch = current_dispatch_;
  const bool reserved = req != nullptr && dispatch != nullptr &&
      dispatch->owner == reactor &&
      dispatch->reply != nullptr && !dispatch->reply_claimed;
  const bool local = current_ == reactor;
  Reply* reply = reserved ? dispatch->reply :
      (local ? reactor->acquire_reply() : new (std::nothrow) Reply);
  if (reply == nullptr) {
    errno = ENOMEM;
    return -1;
  }
  reply->req       = req;
  reply->kind      = REPLY_SPLICE;
  reply->length    = length;
  reply->input_fd  = input_fd;
  reply->output_fd = output_fd;
  reply->flags     = flags;
  if (reserved) {
    dispatch->reply_claimed = true;
  }
  if ((local && !reactor->enqueue_reply(reply)) ||
      (!local && !reactor->submit_external_reply(reply))) {
    if (!reserved) {
      reactor->release_reply(reply);
    }
    errno = EAGAIN;
    return -1;
  }
  if (reserved) {
    dispatch->reply_submitted.store(true, std::memory_order_release);
  }
  return FUSE_CUSTOM_IO_DEFERRED;
}

void FuseReactor::async_wakeup(void* userdata) noexcept {
  auto* group = static_cast<FuseReactorGroup*>(userdata);
  if (group == nullptr) {
    return;
  }
  group->wake();
}

void FuseReactor::clear_receive(void* userdata) noexcept {
  (void)userdata;
  FuseReactor* reactor = current_ ? current_ : reply_target_;
  // write_buf may hand its FD-backed payload to an explicit continuation.
  // libfuse's stack-local bufvec then still appears unconsumed on return;
  // the retained owner, not libfuse, drains or consumes that pipe later.
  if (current_dispatch_ != nullptr &&
      current_dispatch_->input_tasks.load(std::memory_order_acquire) > 1) {
    return;
  }
  if (reactor != nullptr && receive_fd_ >= 0) {
    ++reactor->receive_drains_;
    drain_receive_pipe(receive_fd_);
  }
}

void FuseReactorGroup::wake() noexcept {
  const int saved_errno = errno;
  const uint64_t value = 1;
  for (const std::unique_ptr<FuseReactor>& reactor : reactors_) {
    if (reactor->wake_fd_ < 0) {
      continue;
    }
    ssize_t result;
    do {
      result = ::write(reactor->wake_fd_, &value, sizeof(value));
    } while (result < 0 && errno == EINTR);
  }
  errno = saved_errno;
}

bool FuseReactor::initialize_receive_clones() noexcept {
  while (receive_fds_.size() < receive_concurrency_) {
    std::string error;
    const int fd = FuseReactorGroup::clone_fuse_fd(session_, error);
    if (fd < 0) {
      error_ = errno != 0 ? -errno : -EIO;
      return false;
    }
    receive_fds_.push_back(fd);
    owned_receive_fds_.push_back(fd);
    receive_pending_fds_.push_back(false);
  }
  return true;
}

bool FuseReactor::submit_receive() noexcept {
  constexpr uintptr_t kReceiveTag = 2;
  if (!receive_active_ || receive_fds_.empty()) {
    return true;
  }
  while (dispatch_count_ + receive_count_ < max_dispatch_count_ &&
         io_uring_sq_space_left(&ring_) != 0) {
    if (initialization_owner_ && !initialization_complete_ &&
        (!first_receive_ || receive_count_ != 0)) {
      break;
    }
    Dispatch* dispatch = acquire_dispatch();
    if (dispatch == nullptr) {
      return false;
    }
    unsigned receive_index = unsigned(receive_pending_fds_.size());
    for (unsigned i = 0; i < receive_pending_fds_.size(); ++i) {
      if (!receive_pending_fds_[i]) {
        receive_index = i;
        break;
      }
    }
    if (receive_index == receive_pending_fds_.size()) {
      finish_dispatch(dispatch);
      break;
    }
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      finish_dispatch(dispatch);
      return false;
    }
    // /dev/fuse does not implement native io_uring reads.  Submitting a
    // blocking splice makes io_uring create an io-wq worker for every receive,
    // which costs more CPU than the FUSE request itself.  Let the ring wait for
    // readiness, then perform one nonblocking splice on the reactor thread.
    io_uring_prep_poll_add(sqe, receive_fds_[receive_index], POLLIN);
    io_uring_sqe_set_data64(
        sqe, uintptr_t(dispatch) | kReceiveTag);
    dispatch->receive_index = receive_index;
    receive_pending_fds_[receive_index] = true;
    receiving_.push_back(dispatch);
    ++receive_count_;
  }
  return true;
}

bool FuseReactor::complete_receive(
    Dispatch* dispatch, int result) noexcept {
  if (receive_count_ == 0 || dispatch == nullptr) {
    fprintf(stderr, "FUSE receive invariant failed: count=%zu dispatch=%p\n",
            receive_count_, static_cast<void*>(dispatch));
    error_ = -EIO;
    return false;
  }
  --receive_count_;
  if (dispatch->receive_index >= receive_pending_fds_.size() ||
      !receive_pending_fds_[dispatch->receive_index]) {
    fprintf(stderr, "FUSE receive pending invariant failed: index=%u size=%zu\n",
            dispatch->receive_index, receive_pending_fds_.size());
    error_ = -EIO;
    return false;
  }
  receive_pending_fds_[dispatch->receive_index] = false;
  auto found = std::find(receiving_.begin(), receiving_.end(), dispatch);
  if (found == receiving_.end()) {
    fprintf(stderr, "FUSE receive tracking invariant failed\n");
    error_ = -EIO;
    return false;
  }
  *found = receiving_.back();
  receiving_.pop_back();
  if (result < 0) {
    finish_dispatch(dispatch);
    if (result == -EINTR || result == -EAGAIN) {
      return submit_receive();
    }
    if (result == -ENODEV || result == -ENOTCONN ||
        result == -ECANCELED) {
      fuse_session_exit(session_);
    } else {
      error_ = result;
    }
    return false;
  }

  const unsigned receive_index = dispatch->receive_index;
  const auto take = [&](Dispatch* current) {
    const size_t length = std::min(
        fuse_session_bufsize(session_), current->capacity);
    ssize_t received;
    do {
      received = ::splice(
          receive_fds_[receive_index], nullptr,
          current->pipe[1], nullptr, length,
          SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
    } while (received < 0 && errno == EINTR);
    if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return 0;
    }
    if (received <= 0) {
      return received < 0 ? -errno : -ENODEV;
    }
    current->buffer.size  = size_t(received);
    current->buffer.flags = FUSE_BUF_IS_FD;
    current->buffer.fd    = current->pipe[0];
    ++dispatch_count_;
    ++received_requests_;
    first_receive_ = false;
    current->owner     = this;
    current->output_fd = receive_fds_[receive_index];
    FuseReactor* target = group_->next_dispatch_reactor();
    const bool started = target == this
        ? start_dispatch(current)
        : start_remote_dispatch(current, target);
    if (!started) {
      --dispatch_count_;
      return -EAGAIN;
    }
    return 1;
  };

  int received = take(dispatch);
  if (received <= 0) {
    finish_dispatch(dispatch);
    if (received == 0) {
      return submit_receive();
    }
    if (received == -ENODEV || received == -ENOTCONN ||
        received == -ECANCELED) {
      fuse_session_exit(session_);
    } else {
      error_ = received;
    }
    return false;
  }

  if (initialization_owner_ && !initialization_complete_) {
    return submit_receive();
  }
  while (dispatch_count_ < max_dispatch_count_) {
    Dispatch* next = acquire_dispatch();
    if (next == nullptr) {
      return false;
    }
    received = take(next);
    if (received > 0) {
      continue;
    }
    finish_dispatch(next);
    if (received == 0) {
      break;
    }
    if (received == -ENODEV || received == -ENOTCONN ||
        received == -ECANCELED) {
      fuse_session_exit(session_);
    } else {
      error_ = received;
    }
    return false;
  }
  return submit_receive();
}

bool FuseReactor::submit_dispatch_receive() noexcept {
  if (dispatch_pending_ || io_uring_sq_space_left(&ring_) == 0) {
    return true;
  }
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    return true;
  }
  io_uring_prep_poll_add(sqe, dispatch_pipe_[0], POLLIN);
  io_uring_sqe_set_data(sqe, &dispatch_token_);
  dispatch_pending_ = true;
  return true;
}

bool FuseReactor::submit_task_receive() noexcept {
  if (task_pending_ || io_uring_sq_space_left(&ring_) == 0) {
    return true;
  }
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    return true;
  }
  io_uring_prep_poll_add(sqe, task_pipe_[0], POLLIN);
  io_uring_sqe_set_data(sqe, &task_token_);
  task_pending_ = true;
  return true;
}

bool FuseReactor::submit_wakeup() noexcept {
  if (wake_pending_) {
    return true;
  }
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    return false;
  }
  io_uring_prep_read(sqe, wake_fd_, &wake_value_, sizeof(wake_value_), 0);
  io_uring_sqe_set_data(sqe, &wake_token_);
  wake_pending_ = true;
  return true;
}

bool FuseReactor::submit_external_receive() noexcept {
  if (external_pending_ || reply_count_ >= max_reply_count_) {
    return true;
  }
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    return false;
  }
  io_uring_prep_poll_add(sqe, external_pipe_[0], POLLIN);
  io_uring_sqe_set_data(sqe, &external_token_);
  external_pending_ = true;
  return true;
}

bool FuseReactor::submit_io_request(IoRequest* request) noexcept {
  if (io_uring_sq_space_left(&ring_) == 0) {
    if (io_uring_submit(&ring_) < 0) {
      return false;
    }
  }
  io_uring_sqe* operation = io_uring_get_sqe(&ring_);
  if (operation == nullptr) {
    return false;
  }
  switch (request->kind) {
    case IO_RECEIVE:
      io_uring_prep_recv(
          operation, request->fd,
          static_cast<u_char*>(request->data) +
              (request->exact ? request->transferred : 0),
          request->length - (request->exact ? request->transferred : 0),
          request->flags);
      break;
    case IO_READ:
      io_uring_prep_read(
          operation, request->fd,
          static_cast<u_char*>(request->data) + request->transferred,
          unsigned(request->length - request->transferred), UINT64_MAX);
      break;
    case IO_PREAD:
      io_uring_prep_read(
          operation, request->fd,
          static_cast<u_char*>(request->data) + request->transferred,
          unsigned(request->length - request->transferred),
          uint64_t(request->input_offset + off_t(request->transferred)));
      break;
    case IO_SEND:
      io_uring_prep_send(
          operation, request->fd,
          static_cast<u_char*>(request->data) + request->transferred,
          request->length - request->transferred, request->flags);
      break;
    case IO_WRITE:
      io_uring_prep_write(
          operation, request->fd,
          static_cast<u_char*>(request->data) + request->transferred,
          unsigned(request->length - request->transferred), UINT64_MAX);
      break;
    case IO_PWRITE:
      io_uring_prep_write(
          operation, request->fd,
          static_cast<u_char*>(request->data) + request->transferred,
          unsigned(request->length - request->transferred),
          uint64_t(request->output_offset +
                   off_t(request->transferred)));
      break;
    case IO_SPLICE:
      io_uring_prep_splice(
          operation, request->fd,
          request->has_input_offset ?
              request->input_offset + off_t(request->transferred) : -1,
          request->output_fd,
          request->has_output_offset ?
              request->output_offset + off_t(request->transferred) : -1,
          unsigned(request->length - request->transferred),
          unsigned(request->flags));
      break;
    case IO_CONNECT:
      io_uring_prep_connect(operation, request->fd, request->address,
                            request->address_length);
      break;
  }
  constexpr uintptr_t kOperationTag = 1;
  io_uring_sqe_set_data64(
      operation, uintptr_t(request) | kOperationTag);
  if (request->force_async) {
    operation->flags |= IOSQE_ASYNC;
    ++background_file_writes_;
  }
  if (request->deadline_ns == 0 && request->timeout_ms > 0) {
    const uint64_t now = monotonic_now_ns_ != 0
        ? monotonic_now_ns_ : reactor_monotonic_ns();
    if (now != 0) {
      const uint64_t duration =
          uint64_t(request->timeout_ms) * 1'000'000;
      request->deadline_ns = now > UINT64_MAX - duration
          ? UINT64_MAX : now + duration;
    }
  }
  request->active_index = io_requests_.size();
  io_requests_.push_back(request);
  if (!request->cancel_submitted && request->deadline_ns != 0) {
    next_io_deadline_ = std::min(
        next_io_deadline_, request->deadline_ns);
  }
  ++request->operations;
  ++io_operations_;
  return true;
}

bool FuseReactor::submit_external_reply(Reply* reply) noexcept {
  std::shared_lock guard(group_->external_mutex_);
  if (group_->shutting_down_.load(std::memory_order_acquire) ||
      external_pipe_[1] < 0) {
    return false;
  }
  reply->external = true;
  Reply* value = reply;
  ssize_t result;
  do {
    result = ::write(external_pipe_[1], &value, sizeof(value));
  } while (result < 0 && errno == EINTR);
  if (result != ssize_t(sizeof(value))) {
    return false;
  }
  external_submitted_.fetch_add(1, std::memory_order_release);
  return true;
}

FuseReactor::Reply* FuseReactor::acquire_reply() noexcept {
  Reply* reply = reply_free_;
  if (reply != nullptr) {
    reply_free_ = reply->next;
    reply->next = nullptr;
    return reply;
  }
  return new (std::nothrow) Reply;
}

void FuseReactor::release_reply(Reply* reply) noexcept {
  if (!reply->pooled) {
    delete reply;
    return;
  }
  reply->req                = nullptr;
  reply->kind               = REPLY_WRITE;
  reply->overflow_data.clear();
  reply->length             = 0;
  reply->input_fd           = -1;
  reply->output_fd          = -1;
  reply->flags              = 0;
  reply->external           = false;
  reply->notify_done        = nullptr;
  reply->notify_context     = nullptr;
  reply->next               = reply_free_;
  reply_free_               = reply;
}

void FuseReactor::retire_reply(Reply* reply, int result) noexcept {
  const NotifyFunction done = reply->notify_done;
  void* context = reply->notify_context;
  if (reply->input_fd >= 0) {
    ::close(reply->input_fd);
    reply->input_fd = -1;
  }
  if (reply->req != nullptr) {
    fuse_reply_async_complete(reply->req, result);
  }
  release_reply(reply);
  if (done != nullptr) {
    FuseReactorReplyScope scope(this, group_->io_timeout_ms_, -1, true);
    done(context, result);
  }
}

FuseReactor::Dispatch* FuseReactor::acquire_dispatch() noexcept {
  size_t size = fuse_session_bufsize(session_);
  if (size == 0 || size > INT_MAX) {
    error_ = -EOVERFLOW;
    return nullptr;
  }
  if (first_receive_) {
    size = std::min(size, size_t(64U * 1024U));
  }
  Dispatch* dispatch = nullptr;
  if (!free_dispatches_.empty()) {
    dispatch = free_dispatches_.back();
    free_dispatches_.pop_back();
  } else {
    dispatch = new (std::nothrow) Dispatch;
    if (dispatch == nullptr) {
      error_ = -ENOMEM;
      return nullptr;
    }
    if (::pipe2(dispatch->pipe, O_CLOEXEC) != 0) {
      error_ = -errno;
      delete dispatch;
      return nullptr;
    }
    const int capacity = fcntl(dispatch->pipe[0], F_GETPIPE_SZ);
    if (capacity < 0) {
      error_ = -errno;
      ::close(dispatch->pipe[0]);
      ::close(dispatch->pipe[1]);
      delete dispatch;
      return nullptr;
    }
    dispatch->capacity = size_t(capacity);
  }
  if (size <= dispatch->capacity) {
    dispatch->reply = acquire_reply();
    if (dispatch->reply == nullptr) {
      error_ = -ENOMEM;
      recycle_dispatch(dispatch);
      return nullptr;
    }
    return dispatch;
  }
  const int result = fcntl(dispatch->pipe[0], F_SETPIPE_SZ, int(size));
  if (result < 0 || size_t(result) < size) {
    error_ = result < 0 ? -errno : -ENOSPC;
    recycle_dispatch(dispatch);
    return nullptr;
  }
  dispatch->capacity = size_t(result);
  dispatch->reply = acquire_reply();
  if (dispatch->reply == nullptr) {
    error_ = -ENOMEM;
    recycle_dispatch(dispatch);
    return nullptr;
  }
  return dispatch;
}

void FuseReactor::finish_dispatch(Dispatch* dispatch) noexcept {
  if (dispatch == nullptr) {
    return;
  }
  if (dispatch->reply != nullptr &&
      !dispatch->reply_submitted.load(std::memory_order_acquire)) {
    release_reply(dispatch->reply);
  }
  dispatch->reply = nullptr;
  recycle_dispatch(dispatch);
}

void FuseReactor::recycle_dispatch(Dispatch* dispatch) noexcept {
  if (dispatch == nullptr) {
    return;
  }
  dispatch->buffer = {};
  dispatch->task = {};
  dispatch->owner = nullptr;
  dispatch->target = nullptr;
  dispatch->output_fd = -1;
  dispatch->input_tasks.store(0, std::memory_order_relaxed);
  dispatch->processing_complete = false;
  dispatch->input_drain_needed  = false;
  dispatch->reply_claimed = false;
  dispatch->reply_submitted.store(false, std::memory_order_relaxed);
  free_dispatches_.push_back(dispatch);
}

bool FuseReactor::drain_receive_pipe(int fd) noexcept {
  int unread = 0;
  if (fd < 0 || ioctl(fd, FIONREAD, &unread) != 0) {
    return false;
  }
  std::array<u_char, 4096> scratch{};
  while (unread > 0) {
    const size_t size = std::min(size_t(unread), scratch.size());
    ssize_t result;
    do {
      result = ::read(fd, scratch.data(), size);
    } while (result < 0 && errno == EINTR);
    if (result <= 0) {
      return false;
    }
    unread -= int(result);
  }
  return true;
}

void FuseReactor::dispatch_complete(Dispatch* dispatch) noexcept {
  std::shared_lock guard(group_->external_mutex_);
  Dispatch* value = dispatch;
  ssize_t result;
  do {
    result = ::write(dispatch_pipe_[1], &value, sizeof(value));
  } while (result < 0 && errno == EINTR);
  if (result == ssize_t(sizeof(value))) {
    return;
  }
  if (dispatch->pipe[0] >= 0) {
    ::close(dispatch->pipe[0]);
  }
  if (dispatch->pipe[1] >= 0) {
    ::close(dispatch->pipe[1]);
  }
  delete dispatch;
}

bool FuseReactor::drain_dispatch_pipe() noexcept {
  for (;;) {
    std::array<Dispatch*, 32> dispatches{};
    ssize_t result;
    do {
      result = ::read(dispatch_pipe_[0], dispatches.data(),
                      sizeof(dispatches));
    } while (result < 0 && errno == EINTR);
    if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return true;
    }
    if (result == 0 &&
        group_->shutting_down_.load(std::memory_order_acquire)) {
      return true;
    }
    if (result <= 0 || size_t(result) % sizeof(Dispatch*) != 0) {
      error_ = result < 0 ? -errno : -EIO;
      return false;
    }
    const size_t count = size_t(result) / sizeof(Dispatch*);
    if (count > dispatch_count_) {
      fprintf(stderr, "FUSE dispatch accounting failed: completed=%zu active=%zu\n",
              count, dispatch_count_);
      error_ = -EIO;
      return false;
    }
    for (size_t i = 0; i < count; ++i) {
      Dispatch* dispatch = dispatches[i];
      dispatch->processing_complete = true;
      if (dispatch->input_tasks.load(std::memory_order_acquire) == 0) {
        finish_dispatch(dispatch);
        --dispatch_count_;
      }
    }
  }
}

bool FuseReactor::drain_task_pipe() noexcept {
  for (;;) {
    ReactorTask* task = nullptr;
    ssize_t result;
    do {
      result = ::read(task_pipe_[0], &task, sizeof(task));
    } while (result < 0 && errno == EINTR);
    if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return true;
    }
    if (result == 0 &&
        group_->shutting_down_.load(std::memory_order_acquire)) {
      return true;
    }
    if (result != ssize_t(sizeof(task)) || task == nullptr) {
      error_ = result < 0 ? -errno : -EIO;
      return false;
    }
    if (!start_task(task)) {
      if (task->cancel != nullptr) {
        task->cancel(task->context);
      }
      task_count_.fetch_sub(1, std::memory_order_release);
      error_ = -ENOMEM;
      return false;
    }
  }
}

bool FuseReactor::drain_external_pipe() noexcept {
  uint64_t target = external_submitted_.load(std::memory_order_acquire);
  while (external_consumed_ < target) {
    std::array<Reply*, 32> replies{};
    ssize_t result;
    do {
      result = ::read(external_pipe_[0], replies.data(), sizeof(replies));
    } while (result < 0 && errno == EINTR);
    if (result <= 0 || size_t(result) % sizeof(Reply*) != 0) {
      error_ = result < 0 ? -errno : -EIO;
      return false;
    }
    const size_t count = size_t(result) / sizeof(Reply*);
    for (size_t i = 0; i < count; ++i) {
      if (!enqueue_reply(replies[i])) {
        retire_reply(replies[i], -EAGAIN);
        for (++i; i < count; ++i) {
          retire_reply(replies[i], -EAGAIN);
        }
        external_consumed_ += count;
        error_ = -EAGAIN;
        return false;
      }
      ++external_replies_;
    }
    external_consumed_ += count;
    target = external_submitted_.load(std::memory_order_acquire);
  }
  return true;
}

void FuseReactor::complete_io(IoRequest* request, int result) noexcept {
  const bool refresh_deadline = request->deadline_ns == next_io_deadline_;
  if (request->active_index >= io_requests_.size() ||
      io_requests_[request->active_index] != request) {
    error_ = -EIO;
    return;
  }
  IoRequest* moved = io_requests_.back();
  io_requests_[request->active_index] = moved;
  moved->active_index = request->active_index;
  io_requests_.pop_back();
  request->active_index = SIZE_MAX;
  if (refresh_deadline) refresh_io_deadline();
  complete_async_io(request, result);
}

void FuseReactor::complete_async_io(IoRequest* request, int result) noexcept {
  if (result > 0) {
    if (size_t(result) > size_t(SSIZE_MAX) - request->transferred) {
      finish_async_io(request, -EOVERFLOW);
      return;
    }
    request->transferred += size_t(result);
  }
  if (request->timed_out) {
    finish_async_io(request, -ETIMEDOUT);
    return;
  }
  if (request->cancelled) {
    finish_async_io(request, -ECANCELED);
    return;
  }
  if (group_->shutting_down_.load(std::memory_order_acquire)) {
    finish_async_io(request, error_ != 0 ? error_ : -ENOTCONN);
    return;
  }
  if (result == -EINTR ||
      (result > 0 && request->exact &&
       request->transferred < request->length)) {
    if (submit_io_request(request)) {
      return;
    }
    finish_async_io(request, -EAGAIN);
    return;
  }
  if (result < 0) {
    finish_async_io(request, result);
    return;
  }
  if (request->receive_processor != nullptr && result > 0) {
    const size_t length = request->exact ? request->transferred : size_t(result);
    FuseReactorReplyScope scope(this, group_->io_timeout_ms_, -1, true);
    const int processed = request->receive_processor(request->receive_context, length);
    if (processed < 0 || (processed == 0 && request->exact)) {
      finish_async_io(request, processed < 0 ? processed : -EIO);
      return;
    }
    if (processed == 0) {
      if (submit_io_request(request)) {
        return;
      }
      finish_async_io(request, -EAGAIN);
      return;
    }
  }
  finish_async_io(request, ssize_t(request->transferred));
}

void FuseReactor::finish_async_io(IoRequest* request, ssize_t result) noexcept {
  request->original_completed = true;
  request->async_result = result;
  if (!request->cancel_pending) {
    retire_async_io(request);
  }
}

void FuseReactor::complete_async_cancel(IoRequest* request, int result) noexcept {
  request->cancel_pending = false;
  if (result < 0 && result != -ENOENT && result != -EALREADY &&
      result != -ECANCELED && error_ == 0) {
    error_ = result;
  }
  if (request->original_completed) {
    retire_async_io(request);
  }
}

void FuseReactor::retire_async_io(IoRequest* request) noexcept {
  AsyncIoRequest* operation = request->async;
  const auto complete = operation->complete;
  void* context = operation->context;
  const ssize_t result = request->async_result;
  operation->transferred = request->transferred;
  operation->operations = request->operations;
  operation->implementation_ = nullptr;
  operation->owner_ = nullptr;
  request->async = nullptr;
  request->next_free = async_free_;
  async_free_ = request;
  --async_pending_;
  // Release the wrapper before invoking user code: the continuation may
  // destroy its request object or immediately submit the next operation.
  FuseReactorReplyScope scope(this, group_->io_timeout_ms_, -1, true);
  complete(context, result);
}

bool FuseReactor::cancel_expired_io(uint64_t now) noexcept {
  if (next_io_deadline_ > now) {
    return true;
  }
  constexpr uintptr_t kOperationTag = 1;
  uint64_t next = UINT64_MAX;
  for (IoRequest* request : io_requests_) {
    if (request->deadline_ns == 0 || request->cancel_submitted) {
      continue;
    }
    if (request->deadline_ns > now) {
      next = std::min(next, request->deadline_ns);
      continue;
    }
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      next_io_deadline_ = request->deadline_ns;
      return true;
    }
    request->timed_out        = true;
    request->cancel_submitted = true;
    io_uring_prep_cancel64(
        sqe, uintptr_t(request) | kOperationTag, 0);
    if (request->async != nullptr) {
      request->cancel_pending = true;
      io_uring_sqe_set_data64(sqe, uintptr_t(request) | 5);
    } else {
      io_uring_sqe_set_data(sqe, &cancel_token_);
    }
  }
  next_io_deadline_ = next;
  return true;
}

void FuseReactor::refresh_io_deadline() noexcept {
  next_io_deadline_ = UINT64_MAX;
  for (const IoRequest* request : io_requests_) {
    if (!request->cancel_submitted && request->deadline_ns != 0) {
      next_io_deadline_ = std::min(
          next_io_deadline_, request->deadline_ns);
    }
  }
}

void FuseReactor::fail_remote_dispatch(
    Dispatch* dispatch, int result) noexcept {
  if (dispatch == nullptr || dispatch->owner != this ||
      dispatch->target == nullptr || dispatch_count_ == 0) {
    error_ = -EIO;
    return;
  }
  dispatch->target->task_count_.fetch_sub(1, std::memory_order_release);
  dispatch->input_tasks.store(0, std::memory_order_relaxed);
  drain_receive_pipe(dispatch->pipe[0]);
  finish_dispatch(dispatch);
  --dispatch_count_;
  if (error_ == 0) {
    error_ = result < 0 ? result : -EIO;
  }
}

void FuseReactor::drain_shutdown() noexcept {
  receive_active_ = false;
  const int failure = error_ != 0 ? error_ : -ENOTCONN;
  fail_external_replies(failure);

  // A ring close may defer cancellation work. Keep callers and their buffers
  // alive until the original operation CQEs prove kernel access has ended.
  io_uring_sqe* cancel = io_uring_get_sqe(&ring_);
  if (cancel == nullptr) {
    io_uring_submit(&ring_);
    cancel = io_uring_get_sqe(&ring_);
  }
  bool cancel_pending = cancel != nullptr;
  bool wake_cancel_submitted = false;
  if (cancel != nullptr) {
    io_uring_prep_cancel64(
        cancel, 0, IORING_ASYNC_CANCEL_ANY | IORING_ASYNC_CANCEL_ALL);
    io_uring_sqe_set_data(cancel, &shutdown_token_);
  }

  for (;;) {
    drain_task_pipe();
    drain_dispatch_pipe();
    run_ready_callbacks();

    io_uring_cqe* cqe = nullptr;
    while (io_uring_peek_cqe(&ring_, &cqe) == 0) {
      void* data = io_uring_cqe_get_data(cqe);
      const int result = cqe->res;
      io_uring_cqe_seen(&ring_, cqe);
      const uintptr_t tag = uintptr_t(data);
      if ((tag & 7) == 1) {
        complete_io(reinterpret_cast<IoRequest*>(tag & ~uintptr_t(7)), result);
      } else if ((tag & 7) == 5) {
        complete_async_cancel(
            reinterpret_cast<IoRequest*>(tag & ~uintptr_t(7)), result);
      } else if ((tag & 7) == 2) {
        Dispatch* dispatch = reinterpret_cast<Dispatch*>(tag & ~uintptr_t(7));
        auto found = std::find(receiving_.begin(), receiving_.end(), dispatch);
        if (found != receiving_.end()) {
          *found = receiving_.back();
          receiving_.pop_back();
          receive_pending_fds_[dispatch->receive_index] = false;
          --receive_count_;
          finish_dispatch(dispatch);
        }
      } else if ((tag & 7) == 3) {
        ReactorTask* task = reinterpret_cast<ReactorTask*>(tag & ~uintptr_t(7));
        Dispatch* dispatch = static_cast<Dispatch*>(task->input_owner);
        if (!start_task(task)) {
          if (task->cancel != nullptr) task->cancel(task->context);
          task_count_.fetch_sub(1, std::memory_order_release);
          if (dispatch != nullptr) {
            dispatch->processing_complete = true;
            dispatch->owner->dispatch_complete(dispatch);
          }
        }
      } else if ((tag & 7) == 4) {
        fail_remote_dispatch(
            reinterpret_cast<Dispatch*>(tag & ~uintptr_t(7)), result);
      } else if (data == &shutdown_token_) {
        cancel_pending = false;
      } else if (data == &wake_token_) {
        wake_pending_ = false;
      } else if (data == &external_token_) {
        external_pending_ = false;
      } else if (data == &dispatch_token_) {
        dispatch_pending_ = false;
      } else if (data == &task_token_) {
        task_pending_ = false;
      } else if (data != &cancel_token_) {
        complete_reply(static_cast<Reply*>(data), result);
      }
    }

    const bool drained = io_requests_.empty() && async_pending_ == 0 &&
        !reply_pending_ && !cancel_pending &&
        task_count_.load(std::memory_order_acquire) == 0 &&
        dispatch_count_ == 0 && receive_count_ == 0 &&
        callback_count_ == 0;
    if (drained) {
      if (!wake_pending_) break;
      if (!wake_cancel_submitted) {
        io_uring_sqe* wake_cancel = io_uring_get_sqe(&ring_);
        if (wake_cancel != nullptr) {
          io_uring_prep_cancel(wake_cancel, &wake_token_, 0);
          io_uring_sqe_set_data(wake_cancel, &cancel_token_);
          wake_cancel_submitted = true;
        }
      }
    }
    // Remote continuations can still return input ownership after the initial
    // cancellation pass, so keep their completion queues observable.
    submit_dispatch_receive();
    submit_task_receive();
    if (task_count_.load(std::memory_order_acquire) != 0) submit_wakeup();
    if (callback_count_ != 0 || completion_pending_.load(std::memory_order_acquire)) {
      io_uring_submit_and_get_events(&ring_);
      continue;
    }
    __kernel_timespec timeout{.tv_sec = 0, .tv_nsec = 100'000'000};
    io_uring_cqe* waited = nullptr;
    const int result = io_uring_submit_and_wait_timeout(
        &ring_, &waited, 1, &timeout, nullptr);
    if (result < 0 && result != -EINTR && result != -ETIME && error_ == 0) {
      error_ = result;
    }
  }
}

bool FuseReactor::resume_receive() noexcept {
  if (fuse_session_exited(session_)) {
    return true;
  }
  const bool receive_ready = reply_count_ >= max_reply_count_ ||
      submit_receive();
  if (receive_ready && submit_external_receive() &&
      submit_dispatch_receive() &&
      submit_task_receive()) {
    return true;
  }
  if (error_ == 0) {
    error_ = -EAGAIN;
  }
  return false;
}

bool FuseReactor::enqueue_reply(Reply* reply) noexcept {
  if (reply_head_ == nullptr) {
    if (!submit_reply(reply)) {
      return false;
    }
    reply_head_ = reply;
  } else {
    reply_tail_->next = reply;
  }
  reply_tail_ = reply;
  ++reply_count_;
  reply_high_water_ = std::max(reply_high_water_, reply_count_);
  return true;
}

bool FuseReactor::submit_reply(Reply* reply) noexcept {
  if (reply->length == 0 || reply->length > UINT_MAX ||
      reply->output_fd < 0) {
    return false;
  }
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    return false;
  }
  if (reply->kind == REPLY_SPLICE) {
    io_uring_prep_splice(sqe, reply->input_fd, -1,
                         reply->output_fd, -1,
                         unsigned(reply->length), reply->flags);
  } else {
    io_uring_prep_write(sqe, reply->output_fd, reply->data(),
                        unsigned(reply->length), 0);
  }
  // An invalidation can wait for a locked folio whose FUSE_READ still needs an
  // owner reply. Let io-wq perform that wait, never the reactor's submit path.
  if (reply->req == nullptr) sqe->flags |= IOSQE_ASYNC;
  io_uring_sqe_set_data(sqe, reply);
  reply_pending_ = true;
  return true;
}

void FuseReactor::complete_reply(Reply* reply, int result) noexcept {
  int terminal_result = 0;
  if (result < 0 || size_t(result) != reply->length) {
    terminal_result = result < 0 ? result : -EIO;
    if (terminal_result != -ENOENT) {
      error_ = terminal_result;
    }
  }
  reply_head_ = reply->next;
  if (reply_head_ == nullptr) {
    reply_tail_ = nullptr;
  }
  --reply_count_;
  ++completed_replies_;
  reply_pending_ = false;
  if (terminal_result == 0 && initialization_owner_ &&
      !initialization_complete_) {
    initialization_complete_ = true;
    if (!initialize_receive_clones()) {
      retire_reply(reply, terminal_result);
      return;
    }
    group_->reactor_initialized();
  }
  if (!group_->shutting_down_.load(std::memory_order_acquire) &&
      reply_head_ != nullptr && !submit_reply(reply_head_)) {
    error_ = -EAGAIN;
  }
  resume_receive();
  // Finish queue bookkeeping before invoking a notification continuation: it
  // may submit another reply, which must not be submitted twice by this frame.
  retire_reply(reply, terminal_result);
}

void FuseReactor::fail_replies(int result) noexcept {
  while (reply_head_ != nullptr) {
    Reply* reply = reply_head_;
    reply_head_ = reply->next;
    --reply_count_;
    if (reply_head_ == nullptr) reply_tail_ = nullptr;
    retire_reply(reply, result);
  }
  reply_tail_ = nullptr;
  reply_pending_ = false;
}

void FuseReactor::fail_external_replies(int result) noexcept {
  Reply* reply = nullptr;
  while (::read(external_pipe_[0], &reply, sizeof(reply)) ==
         ssize_t(sizeof(reply))) {
    retire_reply(reply, result);
    ++external_consumed_;
    reply = nullptr;
  }
  external_pending_ = false;
}

void FuseReactor::fail_dispatches() noexcept {
  if (dispatch_pipe_[0] < 0) {
    return;
  }
  Dispatch* dispatch = nullptr;
  while (::read(dispatch_pipe_[0], &dispatch, sizeof(dispatch)) ==
         ssize_t(sizeof(dispatch))) {
    if (dispatch->input_tasks.load(std::memory_order_acquire) != 0) {
      abort();
    }
    drain_receive_pipe(dispatch->pipe[0]);
    finish_dispatch(dispatch);
    if (dispatch_count_ != 0) {
      --dispatch_count_;
    }
    dispatch = nullptr;
  }
  dispatch_pending_ = false;
}

int FuseReactor::run() noexcept {
  if (!ring_ready_) return -EINVAL;
  if ((setup_flags_ & IORING_SETUP_R_DISABLED) != 0) {
    const int result = io_uring_enable_rings(&ring_);
    if (result < 0) return result;
  }
  if (!submit_receive() || !submit_wakeup() ||
      !submit_external_receive() ||
      !submit_dispatch_receive() || !submit_task_receive()) {
    return error_ != 0 ? error_ : -EINVAL;
  }
  current_ = this;
  group_->running_reactors_.fetch_add(1, std::memory_order_release);
  while (!fuse_session_exited(session_) && error_ == 0) {
    monotonic_now_ns_ = reactor_monotonic_ns();
    if (!run_ready_callbacks()) {
      break;
    }
    cancel_expired_io(monotonic_now_ns_);
    const uint64_t deadline = next_io_deadline_;
    ++wait_calls_;
    int result;
    if (callback_count_ != 0 || completion_pending_.load(std::memory_order_acquire)) {
      // GETEVENTS with no minimum flushes deferred task work without sleeping.
      // A continuously replenished callback queue must not starve I/O CQEs.
      result = io_uring_submit_and_get_events(&ring_);
    } else if (deadline == UINT64_MAX || monotonic_now_ns_ == 0) {
      result = io_uring_submit_and_wait(&ring_, 1);
    } else {
      const uint64_t remaining = deadline > monotonic_now_ns_
          ? deadline - monotonic_now_ns_ : 1;
      __kernel_timespec timeout{
          .tv_sec = int64_t(remaining / 1'000'000'000),
          .tv_nsec = int64_t(remaining % 1'000'000'000),
      };
      io_uring_cqe* waited = nullptr;
      result = io_uring_submit_and_wait_timeout(
          &ring_, &waited, 1, &timeout, nullptr);
    }
    if (result < 0) {
      if (result == -EINTR || result == -ETIME) {
        continue;
      }
      error_ = result;
      break;
    }
    io_uring_cqe* cqe = nullptr;
    size_t completion_count = 0;
    while (io_uring_peek_cqe(&ring_, &cqe) == 0) {
      ++completion_count;
      void* data = io_uring_cqe_get_data(cqe);
      const int completion = cqe->res;
      io_uring_cqe_seen(&ring_, cqe);
      const uintptr_t tagged = uintptr_t(data);
      if ((tagged & 7) == 3) {
        ReactorTask* task = reinterpret_cast<ReactorTask*>(
            tagged & ~uintptr_t(7));
        Dispatch* dispatch = static_cast<Dispatch*>(task->input_owner);
        if (!start_task(task)) {
          if (task->cancel != nullptr) {
            task->cancel(task->context);
          }
          task_count_.fetch_sub(1, std::memory_order_release);
          if (dispatch != nullptr) {
            dispatch->processing_complete = true;
            dispatch->owner->dispatch_complete(dispatch);
          }
          error_ = -ENOMEM;
          break;
        }
      } else if ((tagged & 7) == 4) {
        fail_remote_dispatch(
            reinterpret_cast<Dispatch*>(tagged & ~uintptr_t(7)), completion);
        break;
      } else if ((tagged & 7) == 5) {
        complete_async_cancel(
            reinterpret_cast<IoRequest*>(tagged & ~uintptr_t(7)), completion);
      } else if ((tagged & 7) == 2) {
        Dispatch* dispatch = reinterpret_cast<Dispatch*>(
            tagged & ~uintptr_t(7));
        if (!complete_receive(dispatch, completion)) {
          break;
        }
      } else if (data == &wake_token_) {
        wake_pending_ = false;
        if (completion < 0 && completion != -EINTR &&
            completion != -ECANCELED) {
          error_ = completion;
        } else if (!fuse_session_exited(session_) && !submit_wakeup()) {
          error_ = -EAGAIN;
        }
      } else if (data == &external_token_) {
        external_pending_ = false;
        if (completion < 0 && completion != -ECANCELED) {
          error_ = completion;
        } else if (!fuse_session_exited(session_) &&
                   (!drain_external_pipe() ||
                    !submit_external_receive())) {
          if (error_ == 0) {
            error_ = -EAGAIN;
          }
        }
      } else if (data == &dispatch_token_) {
        dispatch_pending_ = false;
        if (completion < 0 && completion != -ECANCELED) {
          error_ = completion;
        } else if (!fuse_session_exited(session_) &&
                   (!drain_dispatch_pipe() ||
                    !submit_dispatch_receive() ||
                    !submit_receive())) {
          if (error_ == 0) {
            error_ = -EAGAIN;
          }
        }
      } else if (data == &task_token_) {
        task_pending_ = false;
        if (completion < 0 && completion != -ECANCELED) {
          error_ = completion;
        } else if (!fuse_session_exited(session_) &&
                   (!drain_task_pipe() ||
                    !submit_task_receive())) {
          if (error_ == 0) {
            error_ = -EAGAIN;
          }
        }
      } else if (data == &cancel_token_) {
        // The original operation CQE owns request completion.  Cancellation
        // CQEs carry no request pointer so they cannot outlive stack-backed
        // IoRequest storage.
        if (completion < 0 && completion != -ENOENT &&
            completion != -EALREADY && completion != -ECANCELED) {
          error_ = completion;
        }
      } else {
        if ((tagged & 7) == 1) {
          IoRequest* request = reinterpret_cast<IoRequest*>(
              tagged & ~uintptr_t(7));
          complete_io(request, completion);
        } else {
          complete_reply(static_cast<Reply*>(data), completion);
        }
      }
    }
    if (completion_count != 0) {
      ++completion_batches_;
      completions_ += completion_count;
      completion_batch_high_water_ = std::max(
          completion_batch_high_water_, completion_count);
    }
  }
  group_->begin_shutdown();
  drain_shutdown();
  if (ring_ready_) {
    io_uring_queue_exit(&ring_);
    ring_ready_ = false;
  }
  fail_replies(error_ != 0 ? error_ : -ENOTCONN);
  fail_external_replies(error_ != 0 ? error_ : -ENOTCONN);
  fail_dispatches();
  current_ = nullptr;
  return error_;
}

int FuseReactorGroup::clone_fuse_fd(fuse_session* session,
                                    std::string& error) {
  const int fd = fuse_session_clone_fd(session);
  if (fd < 0) {
    error = "fuse_session_clone_fd: " + std::string(strerror(errno));
    return -1;
  }
  return fd;
}

bool FuseReactorGroup::initialize(fuse_session* session, unsigned count,
                                  unsigned depth,
                                  int io_timeout_ms,
                                  std::string& error) {
  if (session == nullptr || count == 0 ||
      io_timeout_ms <= 0) {
    error = "invalid FUSE reactor group configuration";
    return false;
  }
  session_ = session;
  io_timeout_ms_ = io_timeout_ms;
  const int master_fd = fuse_session_fd(session);
  if (master_fd < 0) {
    error = "invalid FUSE session fd";
    return false;
  }
  try {
    reactors_.reserve(count);
    for (unsigned i = 0; i < count; ++i) {
      auto reactor = std::make_unique<FuseReactor>();
      const bool clone = i != 0;
      const int fd = clone ? clone_fuse_fd(session, error) : master_fd;
      if (fd < 0) {
        return false;
      }
      const unsigned receive_concurrency = i == 0 ? 1U : 0U;
      if (!reactor->initialize(this, session, fd, clone, i == 0, depth,
                               receive_concurrency, error)) {
        return false;
      }
      reactor->reactor_index_ = i;
      reactor->receive_active_ = i == 0;
      reactors_.push_back(std::move(reactor));
    }
  } catch (const std::bad_alloc&) {
    error = "unable to allocate FUSE reactor group";
    return false;
  }

  fuse_custom_io custom_io{};
  custom_io.writev            = FuseReactor::sync_writev;
  custom_io.read              = FuseReactor::sync_read;
  custom_io.writev_async      = FuseReactor::async_writev;
  custom_io.splice_send_async = FuseReactor::async_splice;
  custom_io.async_userdata    = this;
  custom_io.async_wakeup      = FuseReactor::async_wakeup;
  custom_io.clear_receive     = FuseReactor::clear_receive;
  const int result = fuse_session_custom_io(
      session, &custom_io, sizeof(custom_io), master_fd);
  if (result != 0) {
    error = "fuse_session_custom_io: " +
            std::string(strerror(-result));
    return false;
  }
  return true;
}

FuseReactorGroup::~FuseReactorGroup() {
}

FuseReactor* FuseReactorGroup::callback_reactor() noexcept {
  if (shutting_down_.load(std::memory_order_acquire)) {
    return nullptr;
  }
  if (FuseReactor::current_ != nullptr) {
    return FuseReactor::current_;
  }
  if (FuseReactor::reply_target_ != nullptr) {
    return FuseReactor::reply_target_;
  }
  return reactors_.empty() ? nullptr : reactors_.front().get();
}

FuseReactor* FuseReactorGroup::next_dispatch_reactor() noexcept {
  if (reactors_.empty()) {
    return nullptr;
  }
  // FUSE_INIT must be completed by the only reactor that is running yet.
  if (!initialized_) {
    return reactors_.front().get();
  }
  // FUSE_INIT may finish before all event-loop threads have enabled their
  // rings. Never send MSG_RING to a still-disabled destination. This acquire
  // is startup-only; steady-state dispatch needs no extra shared atomic read.
  if (!dispatch_ready_) {
    if (running_reactors_.load(std::memory_order_acquire) != reactors_.size()) {
      return reactors_.front().get();
    }
    dispatch_ready_ = true;
  }
  FuseReactor* reactor = reactors_[next_reactor_].get();
  if (++reactor_dispatches_ == 4) {
    reactor_dispatches_ = 0;
    next_reactor_ = (next_reactor_ + 1) % reactors_.size();
  }
  return reactor;
}

void FuseReactorGroup::begin_shutdown() noexcept {
  if (shutting_down_.exchange(true, std::memory_order_acq_rel)) {
    // Every owner must wait for pre-close reservations, not only the first
    // owner to set the flag. Otherwise a peer could drain and close its ring
    // before an already admitted MSG_RING is accounted for.
    std::unique_lock guard(external_mutex_);
    return;
  }
  fuse_session_exit(session_);
  wake();
  std::unique_lock guard(external_mutex_);
  for (const std::unique_ptr<FuseReactor>& reactor : reactors_) {
    if (reactor->external_pipe_[1] >= 0) {
      ::close(reactor->external_pipe_[1]);
      reactor->external_pipe_[1] = -1;
    }
    // Dispatch workers may still be returning FD-backed input ownership.
    // Keep this queue writable until they have joined; the reactor destructor
    // drains it before recycling the retained dispatch pipes.
    if (reactor->task_pipe_[1] >= 0) {
      ::close(reactor->task_pipe_[1]);
      reactor->task_pipe_[1] = -1;
    }
  }
}

void FuseReactorGroup::reactor_initialized() noexcept {
  {
    std::lock_guard guard(initialization_mutex_);
    initialized_ = true;
  }
  initialization_condition_.notify_one();
}

int FuseReactorGroup::run() {
  if (reactors_.empty()) {
    return -EINVAL;
  }
  if (reactors_.size() == 1) {
    const int result = reactors_.front()->run();
    return result;
  }

  std::atomic<int> result{0};
  const auto run_reactor = [&](size_t index) {
    FuseReactor& reactor = *reactors_[index];
    const int current = reactor.run();
    if (current != 0) {
      int expected = 0;
      result.compare_exchange_strong(expected, current,
                                     std::memory_order_relaxed);
      // An owner can fail before entering its event loop (for example while
      // enabling its ring). Wake sleeping peers as well as closing admission;
      // the one startup wake may already have been consumed by the primary.
      begin_shutdown();
    }
    if (index == 0) {
      {
        std::lock_guard guard(initialization_mutex_);
        primary_stopped_ = true;
      }
      initialization_condition_.notify_one();
    }
  };
  std::vector<std::thread> threads;
  try {
    threads.reserve(reactors_.size());
    threads.emplace_back([&] { run_reactor(0); });
  } catch (const std::system_error& exception) {
    const int error = exception.code().value();
    return error == 0 ? -EAGAIN : -error;
  } catch (...) {
    return -EAGAIN;
  }

  {
    std::unique_lock guard(initialization_mutex_);
    initialization_condition_.wait(guard, [&] {
      return initialized_ || primary_stopped_;
    });
  }
  if (initialized_) {
    for (size_t i = 1; i < reactors_.size(); ++i) {
      reactors_[i]->first_receive_ = false;
      reactors_[i]->initialization_complete_ = true;
    }
    try {
      for (size_t i = 1; i < reactors_.size(); ++i) {
        threads.emplace_back([&, i] { run_reactor(i); });
      }
    } catch (const std::system_error& exception) {
      const int error = exception.code().value();
      result.store(error == 0 ? -EAGAIN : -error,
                   std::memory_order_relaxed);
      fuse_session_exit(session_);
    } catch (...) {
      result.store(-EAGAIN, std::memory_order_relaxed);
      fuse_session_exit(session_);
    }
  } else if (result.load(std::memory_order_relaxed) == 0) {
    result.store(-EIO, std::memory_order_relaxed);
    fuse_session_exit(session_);
  }

  wake();
  for (std::thread& thread : threads) {
    thread.join();
  }
  return result.load(std::memory_order_relaxed);
}

void FuseReactorGroup::report_stats() const noexcept {
  uint64_t received          = 0;
  uint64_t completed         = 0;
  uint64_t external          = 0;
  uint64_t io_operations     = 0;
  uint64_t background_writes = 0;
  uint64_t drains            = 0;
  uint64_t wait_calls        = 0;
  uint64_t completion_batches = 0;
  uint64_t completions       = 0;
  size_t high_water          = 0;
  size_t completion_high_water = 0;
  unsigned setup_flags       = 0;
  for (const std::unique_ptr<FuseReactor>& reactor : reactors_) {
    received          += reactor->received_requests_;
    completed         += reactor->completed_replies_;
    external          += reactor->external_replies_;
    io_operations     += reactor->io_operations_;
    background_writes += reactor->background_file_writes_;
    wait_calls        += reactor->wait_calls_;
    completion_batches += reactor->completion_batches_;
    completions       += reactor->completions_;
    drains            += reactor->receive_drains_.load(
        std::memory_order_relaxed);
    high_water         = std::max(high_water, reactor->reply_high_water_);
    completion_high_water = std::max(
        completion_high_water, reactor->completion_batch_high_water_);
    setup_flags       |= reactor->setup_flags_;
  }
  fprintf(stderr,
          "io_uring FUSE transport stats: reactors=%zu requests=%" PRIu64
          " replies=%" PRIu64 " external_replies=%" PRIu64
          " io_operations=%" PRIu64
          " background_file_writes=%" PRIu64
          " wait_calls=%" PRIu64
          " completion_batches=%" PRIu64
          " completions=%" PRIu64
          " completion_batch_high_water=%zu"
          " receive_drains=%" PRIu64
          " reply_queue_high_water=%zu setup_flags=0x%x\n",
          reactors_.size(), received, completed, external, io_operations,
          background_writes, wait_calls,
          completion_batches, completions,
          completion_high_water, drains, high_water, setup_flags);
}

void FuseReactorGroup::shutdown() noexcept {
  shutting_down_.store(true, std::memory_order_release);
  reactors_.clear();
  session_ = nullptr;
}
