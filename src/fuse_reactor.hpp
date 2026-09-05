#pragma once

#include "io.hpp"

#include <fuse_lowlevel.h>
#include <liburing.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>
class FuseReactorGroup;
class FuseReactor;

class FuseReactorReplyScope {
 public:
  explicit FuseReactorReplyScope(
      FuseReactor* reactor,
      int timeout_ms = kRequestIoTimeoutMs,
      int receive_fd = -1, bool detached = false) noexcept;
  ~FuseReactorReplyScope();

  FuseReactorReplyScope(const FuseReactorReplyScope&) = delete;
  FuseReactorReplyScope& operator=(const FuseReactorReplyScope&) = delete;

 private:
  FuseReactor* continuation_owner_ = nullptr;
  FuseReactor* previous_ = nullptr;
  int previous_receive_fd_ = -1;
  void* previous_dispatch_ = nullptr;
  IoExecutorScope io_scope_;
};

FuseReactor* current_fuse_reactor() noexcept;

class FuseReactor : public IoExecutor {
 public:
  FuseReactor() = default;
  ~FuseReactor();

  FuseReactor(const FuseReactor&) = delete;
  FuseReactor& operator=(const FuseReactor&) = delete;

  bool initialize(FuseReactorGroup* group, fuse_session* session,
                  int fuse_fd, bool owns_fuse_fd,
                  bool initialization_owner,
                  unsigned depth, unsigned receive_concurrency,
                  std::string& error);
  int run() noexcept;

  bool submit(AsyncIoRequest& request) noexcept override;
  bool cancel(AsyncIoRequest& request) noexcept override;

  using NotifyFunction = void (*)(void*, int) noexcept;
  // Owner-only. Accepted notifications complete once, never inline, after the
  // real local write (or shutdown cancellation); rejected calls do not callback.
  // Completion receives zero on success or a negative errno on failure.
  // The callback may destroy its context or submit another notification.
  bool notify_inval_inode(fuse_ino_t inode, off_t offset, off_t length,
                           NotifyFunction done, void* context) noexcept;
  // fd-backed STORE. Keep source storage alive until done; queued admission
  // is not publication. STORE failures are reported to done, not mount-fatal.
  bool notify_store(fuse_ino_t inode, off_t offset, int fd, off_t source_offset,
                    size_t length, NotifyFunction done, void* context) noexcept;

  using TaskFunction = void (*)(void*) noexcept;
  struct ReactorTask {
    TaskFunction run    = nullptr;
    TaskFunction cancel = nullptr;
    void* context       = nullptr;
    void* input_owner   = nullptr;
    // Reserved completion bookkeeping; callers leave these fields untouched.
    FuseReactor* completion_owner = nullptr;
    ReactorTask* completion_next  = nullptr;
    bool completion_queued       = false;
  };

  // Caller-owned continuation, executed once on this reactor.
  // Accepted tasks never execute inline and may delete themselves in run.
  bool post(ReactorTask* task) noexcept;
  // Reserve on the owner before handing work to another thread. A successful
  // reservation keeps the reactor alive until complete(task), even at shutdown.
  // An admitted owner callback may reserve its next cleanup stage at shutdown;
  // ordinary post/submit admission remains closed.
  // complete is thread-safe, cannot fail, and never invokes run inline.
  bool reserve_completion(ReactorTask* task) noexcept;
  void complete(ReactorTask* task) noexcept;
  // Retain FD-backed FUSE input beyond the initial callback. Release on the
  // same reactor after consuming it, or with consumed=false on failure.
  void* retain_input() noexcept;
  void release_input(void* token, bool consumed) noexcept;

 private:
  friend struct ReactorIoTest;
  friend class FuseReactorGroup;
  friend class FuseReactorReplyScope;
  friend FuseReactor* current_fuse_reactor() noexcept;

  enum ReplyKind {
    REPLY_WRITE,
    REPLY_SPLICE,
  };

  struct Reply {
    Reply* next    = nullptr;
    fuse_req_t req = nullptr;
    ReplyKind kind = REPLY_WRITE;
    u_char inline_data[256];
    std::vector<u_char> overflow_data;
    size_t length  = 0;
    int input_fd   = -1;
    int output_fd  = -1;
    unsigned flags = 0;
    bool pooled    = false;
    bool external  = false;
    NotifyFunction notify_done = nullptr;
    void* notify_context = nullptr;
    bool notify_best_effort = false;

    u_char* data() noexcept {
      return length <= sizeof(inline_data) ? inline_data :
          overflow_data.data();
    }
  };

  enum IoKind {
    IO_RECEIVE,
    IO_READ,
    IO_PREAD,
    IO_SEND,
    IO_WRITE,
    IO_PWRITE,
    IO_SPLICE,
    IO_CONNECT,
  };

  struct alignas(8) IoRequest {
    IoKind kind               = IO_RECEIVE;
    int fd                    = -1;
    int output_fd             = -1;
    void* data                = nullptr;
    const sockaddr* address   = nullptr;
    socklen_t address_length  = 0;
    size_t length             = 0;
    size_t transferred        = 0;
    size_t operations         = 0;
    int flags                 = 0;
    int timeout_ms            = 0;
    off_t input_offset        = -1;
    off_t output_offset       = -1;
    bool has_input_offset     = false;
    bool has_output_offset    = false;
    bool force_async          = false;
    bool exact                 = false;
    bool timed_out             = false;
    bool cancel_submitted      = false;
    ReceiveProcessor receive_processor = nullptr;
    void* receive_context      = nullptr;
    uint64_t deadline_ns       = 0;
    size_t active_index        = SIZE_MAX;
    IoRequest* next_free      = nullptr;
    AsyncIoRequest* async     = nullptr;
    ssize_t async_result      = -ECANCELED;
    bool cancelled            = false;
    bool cancel_pending       = false;
    bool original_completed   = false;
  };

  struct Dispatch {
    fuse_buf buffer = {};
    alignas(8) ReactorTask task = {};
    FuseReactor* owner = nullptr;
    FuseReactor* target = nullptr;
    int pipe[2]     = {-1, -1};
    int output_fd   = -1;
    size_t capacity = 0;
    unsigned receive_index = 0;
    Reply* reply    = nullptr;
    std::atomic<bool> reply_submitted{false};
    std::atomic<unsigned> input_tasks{0};
    bool processing_complete = false;
    bool input_drain_needed  = false;
    bool reply_claimed       = false;
  };

  static ssize_t sync_writev(int fd, iovec* iov, int count,
                             void* userdata) noexcept;
  static ssize_t sync_read(int fd, void* data, size_t size,
                           void* userdata) noexcept;
  static ssize_t async_writev(int fd, const iovec* iov, int count,
                              fuse_req_t req, void* userdata) noexcept;
  static ssize_t async_splice(int input_fd, int output_fd, size_t length,
                              unsigned flags, fuse_req_t req,
                              void* userdata) noexcept;
  static void async_wakeup(void* userdata) noexcept;
  static void clear_receive(void* userdata) noexcept;

  bool submit_receive() noexcept;
  bool initialize_receive_clones() noexcept;
  bool complete_receive(Dispatch* dispatch, int result) noexcept;
  bool submit_wakeup() noexcept;
  bool submit_external_receive() noexcept;
  bool submit_dispatch_receive() noexcept;
  bool submit_task_receive() noexcept;
  bool submit_external_reply(Reply* reply) noexcept;
  bool submit_io_request(IoRequest* request) noexcept;
  static bool drain_receive_pipe(int fd) noexcept;
  bool drain_external_pipe() noexcept;
  bool drain_dispatch_pipe() noexcept;
  bool drain_task_pipe() noexcept;
  bool resume_receive() noexcept;
  bool enqueue_reply(Reply* reply) noexcept;
  Reply* acquire_reply() noexcept;
  void release_reply(Reply* reply) noexcept;
  bool submit_reply(Reply* reply) noexcept;
  void retire_reply(Reply* reply, int result) noexcept;
  void complete_reply(Reply* reply, int result) noexcept;
  void fail_replies(int result) noexcept;
  void fail_external_replies(int result) noexcept;
  void complete_io(IoRequest* request, int result) noexcept;
  void complete_async_io(IoRequest* request, int result) noexcept;
  void complete_async_cancel(IoRequest* request, int result) noexcept;
  void finish_async_io(IoRequest* request, ssize_t result) noexcept;
  void retire_async_io(IoRequest* request) noexcept;
  bool cancel_expired_io(uint64_t now) noexcept;
  void refresh_io_deadline() noexcept;
  void drain_shutdown() noexcept;
  void fail_remote_dispatch(Dispatch* dispatch, int result) noexcept;
  void fail_dispatches() noexcept;
  Dispatch* acquire_dispatch() noexcept;
  void finish_dispatch(Dispatch* dispatch) noexcept;
  void recycle_dispatch(Dispatch* dispatch) noexcept;
  void dispatch_complete(Dispatch* dispatch) noexcept;
  bool enqueue_task(ReactorTask* task) noexcept;
  bool start_task(ReactorTask* task) noexcept;
  bool start_dispatch(Dispatch* dispatch) noexcept;
  bool start_remote_dispatch(
      Dispatch* dispatch, FuseReactor* target) noexcept;
  bool run_ready_callbacks() noexcept;
  void release_input_dispatch(Dispatch* dispatch,
                              bool drain_input = true) noexcept;
  static void dispatch_entry(void* context) noexcept;

  FuseReactorGroup* group_         = nullptr;
  fuse_session* session_           = nullptr;
  io_uring ring_                   = {};
  Reply* reply_head_               = nullptr;
  Reply* reply_tail_               = nullptr;
  NotifyFunction pending_notify_  = nullptr;
  void* pending_notify_context_    = nullptr;
  bool pending_notify_best_effort_ = false;
  bool notify_accepted_            = false;
  int external_pipe_[2]            = {-1, -1};
  int dispatch_pipe_[2]            = {-1, -1};
  int task_pipe_[2]                = {-1, -1};
  std::vector<Dispatch*> free_dispatches_;
  std::vector<Dispatch*> receiving_;
  std::vector<ReactorTask*> ready_callbacks_;
  size_t callback_head_ = 0;
  size_t callback_count_ = 0;
  unsigned continuation_depth_ = 0;
  std::mutex completion_mutex_;
  std::atomic<bool> completion_pending_{false};
  ReactorTask* completion_head_ = nullptr;
  ReactorTask* completion_tail_ = nullptr;
  std::unique_ptr<Reply[]> reply_pool_;
  Reply* reply_free_                = nullptr;
  std::atomic<uint64_t> external_submitted_{0};
  std::atomic<size_t> task_count_{0};
  std::vector<IoRequest*> io_requests_;
  std::unique_ptr<IoRequest[]> async_pool_;
  IoRequest* async_free_ = nullptr;
  size_t async_pending_ = 0;
  uint64_t next_io_deadline_          = UINT64_MAX;
  uint64_t external_consumed_      = 0;
  uint64_t wake_value_             = 0;
  uint64_t monotonic_now_ns_       = 0;
  int fuse_fd_                     = -1;
  std::vector<int> receive_fds_;
  std::vector<int> owned_receive_fds_;
  std::vector<bool> receive_pending_fds_;
  int wake_fd_                     = -1;
  int error_                       = 0;
  size_t reply_count_              = 0;
  size_t max_reply_count_          = 0;
  unsigned receive_concurrency_    = 1;
  size_t reactor_index_            = 0;
  uint64_t received_requests_      = 0;
  uint64_t completed_replies_      = 0;
  uint64_t external_replies_       = 0;
  uint64_t io_operations_          = 0;
  uint64_t background_file_writes_ = 0;
  uint64_t wait_calls_             = 0;
  uint64_t completion_batches_     = 0;
  uint64_t completions_            = 0;
  size_t completion_batch_high_water_ = 0;
  std::atomic<uint64_t> receive_drains_{0};
  size_t dispatch_count_           = 0;
  size_t receive_count_            = 0;
  size_t max_dispatch_count_       = 0;
  size_t max_task_count_           = 0;
  size_t reply_pool_size_          = 0;
  size_t reply_high_water_         = 0;
  unsigned setup_flags_            = 0;
  bool ring_ready_                 = false;
  bool owns_fuse_fd_               = false;
  bool initialization_owner_       = false;
  bool initialization_complete_    = false;
  bool external_pending_           = false;
  bool dispatch_pending_           = false;
  bool task_pending_               = false;
  bool reply_pending_              = false;
  bool wake_pending_               = false;
  bool first_receive_              = true;
  bool receive_active_             = false;
  alignas(8) u_char external_token_ = 0;
  alignas(8) u_char dispatch_token_ = 0;
  alignas(8) u_char task_token_     = 0;
  alignas(8) u_char cancel_token_   = 0;
  alignas(8) u_char wake_token_     = 0;
  alignas(8) u_char receive_handoff_token_ = 0;
  alignas(8) u_char shutdown_token_ = 0;

  static thread_local FuseReactor* current_;
  static thread_local FuseReactor* reply_target_;
  static thread_local Dispatch* current_dispatch_;
  static thread_local int receive_fd_;
};

class FuseReactorGroup {
 public:
  FuseReactorGroup() = default;
  ~FuseReactorGroup();

  FuseReactorGroup(const FuseReactorGroup&) = delete;
  FuseReactorGroup& operator=(const FuseReactorGroup&) = delete;

  bool initialize(fuse_session* session, unsigned count, unsigned depth,
                  int io_timeout_ms,
                  std::string& error);
  int run();
  void report_stats() const noexcept;
  void shutdown() noexcept;

 private:
  friend struct ReactorIoTest;
  friend class FuseReactor;

  FuseReactor* callback_reactor() noexcept;
  FuseReactor* next_dispatch_reactor() noexcept;
  void begin_shutdown() noexcept;
  void reactor_initialized() noexcept;
  void wake() noexcept;
  static int clone_fuse_fd(fuse_session* session, std::string& error);

  fuse_session* session_ = nullptr;
  std::vector<std::unique_ptr<FuseReactor>> reactors_;
  std::mutex initialization_mutex_;
  std::condition_variable initialization_condition_;
  std::shared_mutex external_mutex_;
  std::atomic<bool> shutting_down_{false};
  int io_timeout_ms_ = kRequestIoTimeoutMs;
  bool initialized_     = false;
  bool primary_stopped_ = false;
  std::atomic<unsigned> running_reactors_{0};
  bool dispatch_ready_ = false;
  size_t next_reactor_ = 0;
  unsigned reactor_dispatches_ = 0;
};
