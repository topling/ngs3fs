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
#include <sys/mman.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#if defined(__SANITIZE_ADDRESS__)
extern "C" void __sanitizer_start_switch_fiber(
    void**, const void*, size_t);
extern "C" void __sanitizer_finish_switch_fiber(
    void*, const void**, size_t*);
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
extern "C" void __sanitizer_start_switch_fiber(
    void**, const void*, size_t);
extern "C" void __sanitizer_finish_switch_fiber(
    void*, const void**, size_t*);
#endif
#endif

constexpr size_t kReactorFiberStackSize = 1024U * 1024U;

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
    FuseReactor* reactor, int timeout_ms, int receive_fd) noexcept
    : previous_(FuseReactor::reply_target_),
      previous_receive_fd_(FuseReactor::receive_fd_),
      io_scope_(reactor, timeout_ms) {
  FuseReactor::reply_target_ = reactor;
  FuseReactor::receive_fd_   = receive_fd;
}

FuseReactorReplyScope::~FuseReactorReplyScope() {
  FuseReactor::reply_target_ = previous_;
  FuseReactor::receive_fd_   = previous_receive_fd_;
}

FuseReactor* current_fuse_reactor() noexcept {
  return FuseReactor::current_ ? FuseReactor::current_ :
      FuseReactor::reply_target_;
}

void FuseReactor::fiber_entry() noexcept {
  FuseReactor* reactor = current_;
  Fiber* fiber = reactor ? reactor->active_fiber_ : nullptr;
  if (reactor == nullptr || fiber == nullptr || fiber->task == nullptr) {
    abort();
  }
#if defined(__SANITIZE_ADDRESS__)
  __sanitizer_finish_switch_fiber(
      reactor->scheduler_sanitizer_fake_stack_, nullptr, nullptr);
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
  __sanitizer_finish_switch_fiber(
      reactor->scheduler_sanitizer_fake_stack_, nullptr, nullptr);
#endif
#endif
  ReactorTask* task = fiber->task;
  task->run(task->context);
  fiber->task = nullptr;
  fiber->finished = true;
  reactor->yield_fiber();
  abort();
}

bool FuseReactor::start_task(ReactorTask* task) noexcept {
  Dispatch* input_dispatch = static_cast<Dispatch*>(task->input_owner);
  Fiber* fiber = nullptr;
  if (!free_fibers_.empty()) {
    fiber = free_fibers_.back();
    free_fibers_.pop_back();
  } else {
    fiber = new (std::nothrow) Fiber;
    if (fiber == nullptr) {
      release_input_dispatch(input_dispatch);
      return false;
    }
    fiber->stack_mapping_size =
        fiber_stack_guard_size_ + kReactorFiberStackSize;
    fiber->stack_mapping = ::mmap(
        nullptr, fiber->stack_mapping_size, PROT_NONE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_STACK, -1, 0);
    if (fiber->stack_mapping == MAP_FAILED) {
      fiber->stack_mapping = nullptr;
      delete fiber;
      release_input_dispatch(input_dispatch);
      return false;
    }
    void* stack = static_cast<u_char*>(fiber->stack_mapping) +
        fiber_stack_guard_size_;
    if (::mprotect(stack, kReactorFiberStackSize,
                   PROT_READ | PROT_WRITE) != 0) {
      ::munmap(fiber->stack_mapping, fiber->stack_mapping_size);
      delete fiber;
      release_input_dispatch(input_dispatch);
      return false;
    }
    fibers_.push_back(fiber);
  }
  fiber->task     = task;
  fiber->input_dispatch = input_dispatch;
  fiber->input_consumed = false;
  fiber->queued   = false;
  fiber->finished = false;
  if (!initialize_fiber(fiber)) {
    fiber->task = nullptr;
    release_fiber(fiber);
    return false;
  }
  queue_fiber(fiber);
  return true;
}

bool FuseReactor::initialize_fiber(Fiber* fiber) noexcept {
  if (::getcontext(&fiber->context) != 0) {
    return false;
  }
  fiber->context.uc_stack.ss_sp =
      static_cast<u_char*>(fiber->stack_mapping) +
      fiber_stack_guard_size_;
  fiber->context.uc_stack.ss_size = kReactorFiberStackSize;
  fiber->context.uc_stack.ss_flags = 0;
  fiber->context.uc_link = nullptr;
  ::makecontext(&fiber->context, FuseReactor::fiber_entry, 0);
  return true;
}

void FuseReactor::queue_fiber(Fiber* fiber) noexcept {
  if (fiber == nullptr || fiber->queued || fiber->finished) {
    return;
  }
  fiber->queued = true;
  ready_fibers_.push_back(fiber);
}

void FuseReactor::release_fiber(Fiber* fiber) noexcept {
  if (fiber == nullptr) {
    return;
  }
  fiber->task     = nullptr;
  release_input_dispatch(
      fiber->input_dispatch, !fiber->input_consumed);
  fiber->input_dispatch = nullptr;
  fiber->input_consumed = false;
  fiber->queued   = false;
  fiber->finished = false;
  free_fibers_.push_back(fiber);
}

void FuseReactor::release_input_dispatch(
    Dispatch* dispatch, bool drain_input) noexcept {
  if (dispatch == nullptr) {
    return;
  }
  const unsigned previous = dispatch->input_tasks.fetch_sub(
      1, std::memory_order_acq_rel);
  if (previous == 0) {
    abort();
  }
  if (previous != 1) {
    return;
  }
  if (drain_input && !drain_receive_pipe(dispatch->pipe[0]) && error_ == 0) {
    error_ = -EIO;
  }
  if (!dispatch->processing_complete) {
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

void FuseReactor::yield_fiber() noexcept {
  Fiber* fiber = active_fiber_;
  if (fiber == nullptr) {
    abort();
  }
#if defined(__SANITIZE_ADDRESS__)
  __sanitizer_start_switch_fiber(
      &fiber->sanitizer_fake_stack, nullptr, 0);
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
  __sanitizer_start_switch_fiber(
      &fiber->sanitizer_fake_stack, nullptr, 0);
#endif
#endif
  if (::swapcontext(&fiber->context, &scheduler_context_) != 0) {
    abort();
  }
#if defined(__SANITIZE_ADDRESS__)
  __sanitizer_finish_switch_fiber(
      scheduler_sanitizer_fake_stack_, nullptr, nullptr);
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
  __sanitizer_finish_switch_fiber(
      scheduler_sanitizer_fake_stack_, nullptr, nullptr);
#endif
#endif
}

void FuseReactor::resume_fiber(Fiber* fiber) noexcept {
  active_fiber_ = fiber;
  fiber->queued = false;
  Dispatch* previous_dispatch = current_dispatch_;
  current_dispatch_ = nullptr;
  {
    FuseReactorReplyScope scope(this, group_->io_timeout_ms_, -1);
#if defined(__SANITIZE_ADDRESS__)
    __sanitizer_start_switch_fiber(
        &scheduler_sanitizer_fake_stack_,
        fiber->context.uc_stack.ss_sp, fiber->context.uc_stack.ss_size);
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
    __sanitizer_start_switch_fiber(
        &scheduler_sanitizer_fake_stack_,
        fiber->context.uc_stack.ss_sp, fiber->context.uc_stack.ss_size);
#endif
#endif
    if (::swapcontext(&scheduler_context_, &fiber->context) != 0) {
      error_ = -errno;
      fiber->finished = true;
    }
#if defined(__SANITIZE_ADDRESS__)
    __sanitizer_finish_switch_fiber(
        fiber->sanitizer_fake_stack, nullptr, nullptr);
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
    __sanitizer_finish_switch_fiber(
        fiber->sanitizer_fake_stack, nullptr, nullptr);
#endif
#endif
  }
  current_dispatch_ = previous_dispatch;
  active_fiber_ = nullptr;
  if (!fiber->finished) {
    return;
  }
  release_fiber(fiber);
  task_count_.fetch_sub(1, std::memory_order_release);
}

bool FuseReactor::run_ready_fibers() noexcept {
  while (!ready_fibers_.empty()) {
    Fiber* fiber = ready_fibers_.back();
    ready_fibers_.pop_back();
    resume_fiber(fiber);
  }
  return error_ == 0;
}

void* FuseReactor::task_local_data() const noexcept {
  return in_current_task() ? active_fiber_->local_data : nullptr;
}

void FuseReactor::set_task_local_data(
    void* data, TaskLocalDestructor destructor) noexcept {
  if (!in_current_task()) {
    return;
  }
  if (active_fiber_->local_data != nullptr &&
      active_fiber_->local_destructor != nullptr) {
    active_fiber_->local_destructor(active_fiber_->local_data);
  }
  active_fiber_->local_data       = data;
  active_fiber_->local_destructor = destructor;
}

void FuseReactor::mark_input_consumed() noexcept {
  if (in_current_task() && active_fiber_->input_dispatch != nullptr) {
    active_fiber_->input_consumed = true;
  }
}

FuseReactor::~FuseReactor() {
  if (ring_ready_) {
    io_uring_queue_exit(&ring_);
    ring_ready_ = false;
  }
  fail_replies(-ENOTCONN);
  if (external_pipe_[1] >= 0) {
    ::close(external_pipe_[1]);
    external_pipe_[1] = -1;
  }
  if (external_pipe_[0] >= 0) {
    fail_external_replies(-ENOTCONN);
    ::close(external_pipe_[0]);
  }
  if (io_pipe_[1] >= 0) {
    ::close(io_pipe_[1]);
    io_pipe_[1] = -1;
  }
  if (io_pipe_[0] >= 0) {
    fail_io_requests(-ENOTCONN);
    ::close(io_pipe_[0]);
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
  for (Fiber* fiber : fibers_) {
    if (fiber->task != nullptr && fiber->task->cancel != nullptr) {
      fiber->task->cancel(fiber->task->context);
      fiber->task = nullptr;
    }
    if (fiber->input_dispatch != nullptr) {
      const unsigned previous = fiber->input_dispatch->input_tasks.fetch_sub(
          1, std::memory_order_acq_rel);
      if (previous == 0) {
        abort();
      }
      fiber->input_dispatch = nullptr;
    }
    if (fiber->stack_mapping != nullptr) {
      ::munmap(fiber->stack_mapping, fiber->stack_mapping_size);
    }
    if (fiber->local_data != nullptr &&
        fiber->local_destructor != nullptr) {
      fiber->local_destructor(fiber->local_data);
    }
    delete fiber;
  }
  fibers_.clear();
  free_fibers_.clear();
  ready_fibers_.clear();
  if (dispatch_pipe_[1] >= 0) {
    ::close(dispatch_pipe_[1]);
    dispatch_pipe_[1] = -1;
  }
  if (dispatch_pipe_[0] >= 0) {
    fail_dispatches();
    ::close(dispatch_pipe_[0]);
  }
  finish_dispatch(active_receive_);
  active_receive_ = nullptr;
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
}

bool FuseReactor::initialize(FuseReactorGroup* group,
                             fuse_session* session, int fuse_fd,
                             bool owns_fuse_fd,
                             bool initialization_owner,
                             bool single_reactor,
                             unsigned depth, std::string& error) {
  if (session == nullptr || fuse_fd < 0 || depth < 8) {
    error = "invalid FUSE reactor configuration";
    return false;
  }
  group_                = group;
  session_              = session;
  fuse_fd_              = fuse_fd;
  owns_fuse_fd_         = owns_fuse_fd;
  initialization_owner_ = initialization_owner;
  const long page_result = ::sysconf(_SC_PAGESIZE);
  fiber_stack_guard_size_ =
      page_result > 0 ? size_t(page_result) : 4096;

  if (::pipe2(external_pipe_, O_CLOEXEC | O_NONBLOCK) != 0) {
    error = "pipe2(FUSE external replies): " +
            std::string(strerror(errno));
    return false;
  }
  if (::pipe2(io_pipe_, O_CLOEXEC | O_NONBLOCK) != 0) {
    error = "pipe2(io_uring commands): " +
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
  if (single_reactor) {
    params.flags = IORING_SETUP_SINGLE_ISSUER |
                   IORING_SETUP_COOP_TASKRUN |
                   IORING_SETUP_DEFER_TASKRUN;
  }
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
  max_dispatch_count_ = std::max<size_t>(4, depth / 2);
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
  io_requests_.reserve(depth);
  free_dispatches_.reserve(depth);
  fibers_.reserve(depth);
  free_fibers_.reserve(depth);
  ready_fibers_.reserve(depth);

  return true;
}

int FuseReactor::execute_io(IoRequest& request) noexcept {
  if (current_ == this) {
    if (active_fiber_ == nullptr || error_ != 0 ||
        group_->shutting_down_.load(std::memory_order_acquire)) {
      return -ENOTCONN;
    }
    request.fiber = active_fiber_;
    if (!submit_io_request(&request)) {
      return -EAGAIN;
    }
    yield_fiber();
    return request.operation_result;
  }
  {
    std::shared_lock guard(group_->external_mutex_);
    if (group_->shutting_down_.load(std::memory_order_acquire) ||
        io_pipe_[1] < 0) {
      return -ENOTCONN;
    }
    IoRequest* value = &request;
    ssize_t result;
    do {
      result = ::write(io_pipe_[1], &value, sizeof(value));
    } while (result < 0 && errno == EINTR);
    if (result != ssize_t(sizeof(value))) {
      return result < 0 ? -errno : -EIO;
    }
  }
  bool finished = request.finished.load(std::memory_order_acquire);
  while (!finished) {
    request.finished.wait(false, std::memory_order_acquire);
    finished = request.finished.load(std::memory_order_acquire);
  }
  return request.operation_result;
}

bool FuseReactor::submit_task(ReactorTask* task) noexcept {
  if (task == nullptr || task->run == nullptr) {
    return false;
  }
  size_t count = task_count_.load(std::memory_order_relaxed);
  do {
    if (count >= max_task_count_) {
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

bool FuseReactor::submit_input_task(ReactorTask* task) noexcept {
  Dispatch* dispatch = current_dispatch_;
  if (dispatch == nullptr || current_ == this || task == nullptr ||
      task->input_owner != nullptr) {
    return false;
  }
  dispatch->input_tasks.fetch_add(1, std::memory_order_acq_rel);
  task->input_owner = dispatch;
  if (submit_task(task)) {
    return true;
  }
  task->input_owner = nullptr;
  release_input_dispatch(dispatch);
  return false;
}

ssize_t FuseReactor::receive(int fd, void* data, size_t length,
                             int flags, int timeout_ms) noexcept {
  IoRequest request;
  request.kind       = IO_RECEIVE;
  request.fd         = fd;
  request.data       = data;
  request.length     = length;
  request.flags      = flags;
  request.timeout_ms = timeout_ms;
  const int result = execute_io(request);
  if (result < 0) {
    errno = -result;
    return -1;
  }
  return result;
}

ssize_t FuseReactor::read(int fd, void* data, size_t length,
                          int timeout_ms) noexcept {
  IoRequest request;
  request.kind       = IO_READ;
  request.fd         = fd;
  request.data       = data;
  request.length     = length;
  request.timeout_ms = timeout_ms;
  const int result = execute_io(request);
  if (result < 0) {
    errno = -result;
    return -1;
  }
  return result;
}

ssize_t FuseReactor::pread(int fd, void* data, size_t length,
                           off_t offset, int timeout_ms) noexcept {
  if (length > INT_MAX || offset < 0) {
    errno = EINVAL;
    return -1;
  }
  IoRequest request;
  request.kind         = IO_PREAD;
  request.fd           = fd;
  request.data         = data;
  request.length       = length;
  request.timeout_ms   = timeout_ms;
  request.input_offset = offset;
  const int result = execute_io(request);
  if (result < 0) {
    errno = -result;
    return -1;
  }
  return result;
}

ssize_t FuseReactor::receive_exact(int fd, void* data, size_t length,
                                   int flags, int timeout_ms) noexcept {
  if (length > INT_MAX) {
    errno = EOVERFLOW;
    return -1;
  }
  IoRequest request;
  request.kind       = IO_RECEIVE;
  request.fd         = fd;
  request.data       = data;
  request.length     = length;
  request.flags      = flags;
  request.timeout_ms = timeout_ms;
  request.exact      = true;
  const int result = execute_io(request);
  if (result < 0) {
    errno = -result;
    return -1;
  }
  return result;
}

ssize_t FuseReactor::receive_exact_then(
    int fd, void* data, size_t length, int flags, int timeout_ms,
    ReceiveProcessor processor, void* context) noexcept {
  if (length > INT_MAX || processor == nullptr) {
    errno = EINVAL;
    return -1;
  }
  IoRequest request;
  request.kind              = IO_RECEIVE;
  request.fd                = fd;
  request.data              = data;
  request.length            = length;
  request.flags             = flags;
  request.timeout_ms        = timeout_ms;
  request.exact             = true;
  request.receive_processor = processor;
  request.receive_context   = context;
  const int result = execute_io(request);
  if (result < 0) {
    errno = -result;
    return -1;
  }
  return result;
}

ssize_t FuseReactor::receive_until(
    int fd, void* data, size_t length, int flags, int timeout_ms,
    ReceiveProcessor processor, void* context) noexcept {
  if (length > INT_MAX || length == 0 || processor == nullptr) {
    errno = EINVAL;
    return -1;
  }
  IoRequest request;
  request.kind              = IO_RECEIVE;
  request.fd                = fd;
  request.data              = data;
  request.length            = length;
  request.flags             = flags;
  request.timeout_ms        = timeout_ms;
  request.receive_processor = processor;
  request.receive_context   = context;
  const int result = execute_io(request);
  if (result < 0) {
    errno = -result;
    return -1;
  }
  return result;
}

ssize_t FuseReactor::send(int fd, const void* data, size_t length,
                          int flags, int timeout_ms) noexcept {
  IoRequest request;
  request.kind       = IO_SEND;
  request.fd         = fd;
  request.data       = const_cast<void*>(data);
  request.length     = length;
  request.flags      = flags;
  request.timeout_ms = timeout_ms;
  const int result = execute_io(request);
  if (result < 0) {
    errno = -result;
    return -1;
  }
  return result;
}

ssize_t FuseReactor::send_exact(int fd, const void* data, size_t length,
                                int flags, int timeout_ms) noexcept {
  if (length > INT_MAX) {
    errno = EOVERFLOW;
    return -1;
  }
  IoRequest request;
  request.kind       = IO_SEND;
  request.fd         = fd;
  request.data       = const_cast<void*>(data);
  request.length     = length;
  request.flags      = flags;
  request.timeout_ms = timeout_ms;
  request.exact      = true;
  const int result = execute_io(request);
  if (result < 0) {
    errno = -result;
    return -1;
  }
  return result;
}

ssize_t FuseReactor::pwrite(int fd, const void* data, size_t length,
                            off_t offset, int timeout_ms) noexcept {
  if (length > INT_MAX || offset < 0) {
    errno = EINVAL;
    return -1;
  }
  IoRequest request;
  request.kind              = IO_PWRITE;
  request.fd                = fd;
  request.data              = const_cast<void*>(data);
  request.length            = length;
  request.timeout_ms        = timeout_ms;
  request.output_offset     = offset;
  request.has_output_offset = true;
  request.force_async       = true;
  const int result = execute_io(request);
  if (result < 0) {
    errno = -result;
    return -1;
  }
  return result;
}

ssize_t FuseReactor::splice(int input_fd, off_t* input_offset,
                            int output_fd, off_t* output_offset,
                            size_t length, unsigned flags,
                            int timeout_ms) noexcept {
  IoRequest request;
  request.kind              = IO_SPLICE;
  request.fd                = input_fd;
  request.output_fd         = output_fd;
  request.length            = length;
  request.flags             = int(flags);
  request.timeout_ms        = timeout_ms;
  request.has_input_offset  = input_offset != nullptr;
  request.has_output_offset = output_offset != nullptr;
  // Buffered regular-file writes may otherwise execute inline while the
  // reactor submits the ring.  An explicit destination offset identifies
  // the pipe-to-cache-file direction; force that work onto io-wq.
  request.force_async       = output_offset != nullptr;
  request.input_offset      = input_offset ? *input_offset : -1;
  request.output_offset     = output_offset ? *output_offset : -1;
  const int result = execute_io(request);
  if (result < 0) {
    errno = -result;
    return -1;
  }
  if (input_offset != nullptr) {
    *input_offset += result;
  }
  if (output_offset != nullptr) {
    *output_offset += result;
  }
  return result;
}

ssize_t FuseReactor::splice_exact(
    int input_fd, off_t* input_offset,
    int output_fd, off_t* output_offset,
    size_t length, unsigned flags, int timeout_ms,
    size_t* calls) noexcept {
  if (length > INT_MAX) {
    errno = EOVERFLOW;
    return -1;
  }
  IoRequest request;
  request.kind              = IO_SPLICE;
  request.fd                = input_fd;
  request.output_fd         = output_fd;
  request.length            = length;
  request.flags             = int(flags);
  request.timeout_ms        = timeout_ms;
  request.has_input_offset  = input_offset != nullptr;
  request.has_output_offset = output_offset != nullptr;
  request.force_async       = output_offset != nullptr;
  request.input_offset      = input_offset ? *input_offset : -1;
  request.output_offset     = output_offset ? *output_offset : -1;
  request.exact             = true;
  const int result = execute_io(request);
  if (calls != nullptr) {
    *calls += request.operations;
  }
  if (result < 0) {
    errno = -result;
    return -1;
  }
  if (input_offset != nullptr) {
    *input_offset += result;
  }
  if (output_offset != nullptr) {
    *output_offset += result;
  }
  return result;
}

int FuseReactor::connect(int fd, const sockaddr* address,
                         socklen_t address_length,
                         int timeout_ms) noexcept {
  IoRequest request;
  request.kind           = IO_CONNECT;
  request.fd             = fd;
  request.address        = address;
  request.address_length = address_length;
  request.timeout_ms     = timeout_ms;
  const int result = execute_io(request);
  if (result < 0) {
    errno = -result;
    return -1;
  }
  return result;
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

ssize_t FuseReactor::async_writev(int fd, const iovec* iov, int count,
                                  fuse_req_t req, void* userdata) noexcept {
  auto* group = static_cast<FuseReactorGroup*>(userdata);
  FuseReactor* reactor = group ? group->callback_reactor() : nullptr;
  if (reactor == nullptr || count <= 0) {
    errno = EINVAL;
    return -1;
  }

  Dispatch* dispatch = current_dispatch_;
  const bool reserved = dispatch != nullptr &&
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
  if (reserved) {
    dispatch->reply_claimed = true;
  }
  (void)fd;
  if ((local && (!group->synchronize_external(reactor) ||
                 !reactor->enqueue_reply(reply))) ||
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

ssize_t FuseReactor::async_splice(int input_fd, int output_fd, size_t length,
                                  unsigned flags, fuse_req_t req,
                                  void* userdata) noexcept {
  auto* group = static_cast<FuseReactorGroup*>(userdata);
  FuseReactor* reactor = group ? group->callback_reactor() : nullptr;
  if (reactor == nullptr || input_fd < 0 || output_fd < 0) {
    errno = EINVAL;
    return -1;
  }
  Dispatch* dispatch = current_dispatch_;
  const bool reserved = dispatch != nullptr &&
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
  reply->flags     = flags;
  if (reserved) {
    dispatch->reply_claimed = true;
  }
  if ((local && (!group->synchronize_external(reactor) ||
                 !reactor->enqueue_reply(reply))) ||
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

bool FuseReactor::submit_receive() noexcept {
  if (receive_pending_ || dispatch_count_ >= max_dispatch_count_) {
    return true;
  }
  if (initialization_owner_ && !first_receive_ &&
      !initialization_complete_) {
    return true;
  }
  if (active_receive_ == nullptr) {
    active_receive_ = acquire_dispatch();
    if (active_receive_ == nullptr) {
      return false;
    }
  }
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    return false;
  }
  io_uring_prep_splice(sqe, fuse_fd_, -1,
                       active_receive_->pipe[1], -1,
                       unsigned(std::min(fuse_session_bufsize(session_),
                                         active_receive_->capacity)), 0);
  io_uring_sqe_set_data(sqe, &receive_token_);
  receive_pending_ = true;
  return true;
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

bool FuseReactor::submit_io_receive() noexcept {
  if (io_pending_ ||
      io_uring_sq_space_left(&ring_) == 0) {
    return true;
  }
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    return true;
  }
  io_uring_prep_poll_add(sqe, io_pipe_[0], POLLIN);
  io_uring_sqe_set_data(sqe, &io_token_);
  io_pending_ = true;
  return true;
}

bool FuseReactor::submit_io_request(IoRequest* request) noexcept {
  if (io_uring_sq_space_left(&ring_) == 0) {
    return false;
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
          operation, request->fd, request->data,
          unsigned(request->length), UINT64_MAX);
      break;
    case IO_PREAD:
      io_uring_prep_read(
          operation, request->fd, request->data,
          unsigned(request->length), uint64_t(request->input_offset));
      break;
    case IO_SEND:
      io_uring_prep_send(
          operation, request->fd,
          static_cast<u_char*>(request->data) + request->transferred,
          request->length - request->transferred, request->flags);
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
  io_requests_.push_back(request);
  ++request->operations;
  ++io_operations_;
  if (request->fiber != nullptr) {
    ++task_io_operations_;
  } else {
    ++external_io_operations_;
  }
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
  reply->flags              = 0;
  reply->external           = false;
  reply->next               = reply_free_;
  reply_free_               = reply;
}

void FuseReactor::retire_reply(Reply* reply, int result) noexcept {
  if (reply->input_fd >= 0) {
    ::close(reply->input_fd);
    reply->input_fd = -1;
  }
  if (reply->req != nullptr) {
    fuse_reply_async_complete(reply->req, result);
  }
  if (reply->external) {
    external_completed_.fetch_add(1, std::memory_order_release);
    external_completed_.notify_all();
  }
  release_reply(reply);
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
  dispatch->input_tasks.store(0, std::memory_order_relaxed);
  dispatch->processing_complete = false;
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

bool FuseReactor::drain_io_pipe() noexcept {
  for (;;) {
    if (io_uring_sq_space_left(&ring_) < 2) {
      return true;
    }
    IoRequest* request = nullptr;
    ssize_t result;
    do {
      result = ::read(io_pipe_[0], &request, sizeof(request));
    } while (result < 0 && errno == EINTR);
    if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return true;
    }
    if (result != ssize_t(sizeof(request)) || request == nullptr) {
      error_ = result < 0 ? -errno : -EIO;
      return false;
    }
    if (!submit_io_request(request)) {
      request->operation_result = -EAGAIN;
      request->finished.store(true, std::memory_order_release);
      request->finished.notify_one();
      return true;
    }
  }
}

void FuseReactor::complete_io(IoRequest* request, int result) noexcept {
  request->operation_result = request->timed_out ? -ETIMEDOUT : result;
  auto found = std::find(io_requests_.begin(), io_requests_.end(), request);
  if (found != io_requests_.end()) {
    *found = io_requests_.back();
    io_requests_.pop_back();
  }
  if (request->exact && request->operation_result > 0) {
    request->transferred += size_t(request->operation_result);
    if (request->transferred < request->length) {
      request->operation_result = -ECANCELED;
      if (submit_io_request(request)) {
        return;
      }
      request->operation_result = -EAGAIN;
    } else {
      request->operation_result = int(request->transferred);
    }
  }
  if (request->receive_processor != nullptr &&
      request->operation_result > 0) {
    const size_t processed_bytes = request->exact
        ? request->transferred : size_t(request->operation_result);
    if (!request->exact) {
      request->transferred += processed_bytes;
    }
    const int processed = request->receive_processor(
        request->receive_context, processed_bytes);
    if (processed == 0 && request->exact) {
      request->operation_result = -EIO;
    } else if (processed == 0) {
      request->operation_result = -ECANCELED;
      if (submit_io_request(request)) {
        return;
      }
      request->operation_result = -EAGAIN;
    } else if (processed < 0) {
      request->operation_result = processed;
    } else {
      request->operation_result = int(request->transferred);
    }
  }
  if (request->fiber != nullptr) {
    queue_fiber(request->fiber);
  } else {
    request->finished.store(true, std::memory_order_release);
    request->finished.notify_one();
  }
}

bool FuseReactor::cancel_expired_io(uint64_t now) noexcept {
  constexpr uintptr_t kOperationTag = 1;
  for (IoRequest* request : io_requests_) {
    if (request->deadline_ns == 0 || request->deadline_ns > now ||
        request->cancel_submitted) {
      continue;
    }
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      return true;
    }
    request->timed_out        = true;
    request->cancel_submitted = true;
    io_uring_prep_cancel64(
        sqe, uintptr_t(request) | kOperationTag, 0);
    io_uring_sqe_set_data(sqe, &cancel_token_);
  }
  return true;
}

uint64_t FuseReactor::next_io_deadline() const noexcept {
  uint64_t deadline = UINT64_MAX;
  for (const IoRequest* request : io_requests_) {
    if (!request->cancel_submitted && request->deadline_ns != 0) {
      deadline = std::min(deadline, request->deadline_ns);
    }
  }
  return deadline;
}

void FuseReactor::fail_io_requests(int result) noexcept {
  for (IoRequest* request : io_requests_) {
    request->operation_result = result;
    if (request->fiber != nullptr) {
      queue_fiber(request->fiber);
    } else {
      request->finished.store(true, std::memory_order_release);
      request->finished.notify_one();
    }
  }
  io_requests_.clear();

  if (io_pipe_[0] < 0) {
    return;
  }
  IoRequest* request = nullptr;
  while (::read(io_pipe_[0], &request, sizeof(request)) ==
         ssize_t(sizeof(request))) {
    request->operation_result = result;
    request->finished.store(true, std::memory_order_release);
    request->finished.notify_one();
    request = nullptr;
  }
  io_pending_ = false;
}

bool FuseReactor::resume_receive() noexcept {
  if (fuse_session_exited(session_)) {
    return true;
  }
  const bool receive_ready = reply_count_ >= max_reply_count_ ||
      submit_receive();
  if (receive_ready && submit_external_receive() &&
      submit_io_receive() && submit_dispatch_receive() &&
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
  if (reply->length == 0 || reply->length > UINT_MAX) {
    return false;
  }
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    return false;
  }
  if (reply->kind == REPLY_SPLICE) {
    io_uring_prep_splice(sqe, reply->input_fd, -1, fuse_fd_, -1,
                         unsigned(reply->length), reply->flags);
  } else {
    io_uring_prep_write(sqe, fuse_fd_, reply->data(),
                        unsigned(reply->length), 0);
  }
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
  retire_reply(reply, terminal_result);
  if (terminal_result == 0 && initialization_owner_ &&
      !initialization_complete_) {
    initialization_complete_ = true;
    group_->reactor_initialized();
  }
  if (reply_head_ != nullptr && !submit_reply(reply_head_)) {
    error_ = -EAGAIN;
  }
  resume_receive();
}

void FuseReactor::fail_replies(int result) noexcept {
  while (reply_head_ != nullptr) {
    Reply* reply = reply_head_;
    reply_head_ = reply->next;
    retire_reply(reply, result);
    --reply_count_;
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
  if (!ring_ready_ || !submit_receive() || !submit_wakeup() ||
      !submit_external_receive() || !submit_io_receive() ||
      !submit_dispatch_receive() || !submit_task_receive()) {
    return error_ != 0 ? error_ : -EINVAL;
  }
  current_ = this;
  while (!fuse_session_exited(session_) && error_ == 0) {
    monotonic_now_ns_ = reactor_monotonic_ns();
    if (!run_ready_fibers()) {
      break;
    }
    monotonic_now_ns_ = reactor_monotonic_ns();
    cancel_expired_io(monotonic_now_ns_);
    const uint64_t deadline = next_io_deadline();
    int result;
    if (deadline == UINT64_MAX || monotonic_now_ns_ == 0) {
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
    while (io_uring_peek_cqe(&ring_, &cqe) == 0) {
      void* data = io_uring_cqe_get_data(cqe);
      const int completion = cqe->res;
      io_uring_cqe_seen(&ring_, cqe);
      if (data == &receive_token_) {
        receive_pending_ = false;
        if (completion == -EINTR || completion == -EAGAIN) {
          resume_receive();
          continue;
        }
        if (completion <= 0) {
          if (completion == 0 || completion == -ENODEV ||
              completion == -ENOTCONN) {
            fuse_session_exit(session_);
          } else {
            error_ = completion;
          }
          break;
        }
        Dispatch* dispatch = active_receive_;
        active_receive_        = nullptr;
        dispatch->buffer.size  = size_t(completion);
        dispatch->buffer.flags = FUSE_BUF_IS_FD;
        dispatch->buffer.fd    = dispatch->pipe[0];
        ++dispatch_count_;
        ++received_requests_;
        first_receive_ = false;
        if (!group_->submit_dispatch(this, dispatch)) {
          --dispatch_count_;
          finish_dispatch(dispatch);
          error_ = -ENOTCONN;
          break;
        }
        if (error_ != 0) {
          break;
        }
        resume_receive();
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
      } else if (data == &io_token_) {
        io_pending_ = false;
        if (completion < 0 && completion != -ECANCELED) {
          error_ = completion;
        } else if (!fuse_session_exited(session_) &&
                   (!drain_io_pipe() || !submit_io_receive())) {
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
        const uintptr_t tagged = uintptr_t(data);
        if ((tagged & 3) == 1) {
          IoRequest* request = reinterpret_cast<IoRequest*>(
              tagged & ~uintptr_t(3));
          complete_io(request, completion);
          if (!drain_io_pipe() || !submit_io_receive()) {
            if (error_ == 0) {
              error_ = -EAGAIN;
            }
          }
        } else {
          complete_reply(static_cast<Reply*>(data), completion);
        }
      }
    }
  }
  group_->begin_shutdown();
  drain_task_pipe();
  fail_io_requests(error_ != 0 ? error_ : -ENOTCONN);
  run_ready_fibers();
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
                                  unsigned depth, unsigned worker_count,
                                  int io_timeout_ms,
                                  std::string& error) {
  if (session == nullptr || count == 0 || worker_count == 0 ||
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
      if (!reactor->initialize(this, session, fd, clone, i == 0,
                               count == 1, depth, error)) {
        return false;
      }
      reactors_.push_back(std::move(reactor));
    }
  } catch (const std::bad_alloc&) {
    error = "unable to allocate FUSE reactor group";
    return false;
  }

  try {
    dispatch_threads_.reserve(worker_count);
    for (unsigned i = 0; i < worker_count; ++i) {
      dispatch_threads_.emplace_back([this] { dispatch_worker(); });
    }
  } catch (const std::system_error& exception) {
    error = "unable to start FUSE dispatch worker: " +
            std::string(exception.what());
    stop_dispatch_workers();
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
    stop_dispatch_workers();
    return false;
  }
  return true;
}

FuseReactorGroup::~FuseReactorGroup() {
  stop_dispatch_workers();
}

bool FuseReactorGroup::submit_dispatch(
    FuseReactor* reactor, FuseReactor::Dispatch* dispatch) noexcept {
  {
    std::lock_guard guard(dispatch_mutex_);
    if (dispatch_stopping_) {
      return false;
    }
    try {
      dispatch_queue_.emplace_back(reactor, dispatch);
    } catch (...) {
      return false;
    }
  }
  dispatch_condition_.notify_one();
  return true;
}

void FuseReactorGroup::dispatch_worker() noexcept {
  for (;;) {
    std::pair<FuseReactor*, FuseReactor::Dispatch*> job;
    {
      std::unique_lock guard(dispatch_mutex_);
      dispatch_condition_.wait(guard, [&] {
        return dispatch_stopping_ || !dispatch_queue_.empty();
      });
      if (dispatch_queue_.empty()) {
        return;
      }
      job = dispatch_queue_.front();
      dispatch_queue_.pop_front();
    }
    {
      FuseReactorReplyScope scope(
          job.first, io_timeout_ms_, job.second->pipe[0]);
      FuseReactor::Dispatch* previous = FuseReactor::current_dispatch_;
      FuseReactor::current_dispatch_ = job.second;
      fuse_session_process_buf(session_, &job.second->buffer);
      FuseReactor::current_dispatch_ = previous;
    }
    job.first->dispatch_complete(job.second);
  }
}

void FuseReactorGroup::stop_dispatch_workers() noexcept {
  {
    std::lock_guard guard(dispatch_mutex_);
    dispatch_stopping_ = true;
  }
  dispatch_condition_.notify_all();
  for (std::thread& thread : dispatch_threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  dispatch_threads_.clear();
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

bool FuseReactorGroup::synchronize_external(
    FuseReactor* reactor) noexcept {
  if (shutting_down_.load(std::memory_order_acquire) ||
      reactors_.empty()) {
    return false;
  }
  FuseReactor* primary = reactors_.front().get();
  if (!reactor->drain_external_pipe() || reactor == primary) {
    return reactor->error_ == 0;
  }
  const uint64_t target =
      primary->external_submitted_.load(std::memory_order_acquire);
  uint64_t completed =
      primary->external_completed_.load(std::memory_order_acquire);
  while (completed < target) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      return false;
    }
    primary->external_completed_.wait(
        completed, std::memory_order_acquire);
    completed = primary->external_completed_.load(
        std::memory_order_acquire);
  }
  return true;
}

void FuseReactorGroup::begin_shutdown() noexcept {
  if (shutting_down_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  std::unique_lock guard(external_mutex_);
  for (const std::unique_ptr<FuseReactor>& reactor : reactors_) {
    reactor->external_completed_.fetch_or(
        1ULL << 63, std::memory_order_release);
    reactor->external_completed_.notify_all();
    if (reactor->external_pipe_[1] >= 0) {
      ::close(reactor->external_pipe_[1]);
      reactor->external_pipe_[1] = -1;
    }
    if (reactor->io_pipe_[1] >= 0) {
      ::close(reactor->io_pipe_[1]);
      reactor->io_pipe_[1] = -1;
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
    stop_dispatch_workers();
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
      fuse_session_exit(session_);
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
  stop_dispatch_workers();
  return result.load(std::memory_order_relaxed);
}

void FuseReactorGroup::report_stats() const noexcept {
  uint64_t received          = 0;
  uint64_t completed         = 0;
  uint64_t external          = 0;
  uint64_t io_operations     = 0;
  uint64_t task_io           = 0;
  uint64_t external_io       = 0;
  uint64_t background_writes = 0;
  uint64_t drains            = 0;
  size_t high_water          = 0;
  unsigned setup_flags       = 0;
  for (const std::unique_ptr<FuseReactor>& reactor : reactors_) {
    received          += reactor->received_requests_;
    completed         += reactor->completed_replies_;
    external          += reactor->external_replies_;
    io_operations     += reactor->io_operations_;
    task_io           += reactor->task_io_operations_;
    external_io       += reactor->external_io_operations_;
    background_writes += reactor->background_file_writes_;
    drains            += reactor->receive_drains_.load(
        std::memory_order_relaxed);
    high_water         = std::max(high_water, reactor->reply_high_water_);
    setup_flags       |= reactor->setup_flags_;
  }
  fprintf(stderr,
          "io_uring FUSE transport stats: reactors=%zu requests=%" PRIu64
          " replies=%" PRIu64 " external_replies=%" PRIu64
          " io_operations=%" PRIu64
          " task_io_operations=%" PRIu64
          " external_io_operations=%" PRIu64
          " background_file_writes=%" PRIu64
          " receive_drains=%" PRIu64
          " reply_queue_high_water=%zu setup_flags=0x%x\n",
          reactors_.size(), received, completed, external, io_operations,
          task_io, external_io, background_writes, drains,
          high_water, setup_flags);
}

void FuseReactorGroup::shutdown() noexcept {
  shutting_down_.store(true, std::memory_order_release);
  reactors_.clear();
  session_ = nullptr;
}
