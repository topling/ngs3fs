#pragma once

#include "io.hpp"

#include <fuse_lowlevel.h>
#include <liburing.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <deque>
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
      int receive_fd = -1) noexcept;
  ~FuseReactorReplyScope();

  FuseReactorReplyScope(const FuseReactorReplyScope&) = delete;
  FuseReactorReplyScope& operator=(const FuseReactorReplyScope&) = delete;

 private:
  FuseReactor* previous_ = nullptr;
  int previous_receive_fd_ = -1;
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
                  bool initialization_owner, bool single_reactor,
                  unsigned depth,
                  std::string& error);
  int run() noexcept;

  ssize_t receive(int fd, void* data, size_t length,
                  int flags, int timeout_ms) noexcept override;
  ssize_t receive_exact(int fd, void* data, size_t length,
                        int flags, int timeout_ms) noexcept override;
  ssize_t receive_exact_then(
      int fd, void* data, size_t length, int flags, int timeout_ms,
      ReceiveProcessor processor, void* context) noexcept override;
  ssize_t receive_until(int fd, void* data, size_t length,
                        int flags, int timeout_ms,
                        ReceiveProcessor processor,
                        void* context) noexcept override;
  ssize_t send(int fd, const void* data, size_t length,
               int flags, int timeout_ms) noexcept override;
  ssize_t send_exact(int fd, const void* data, size_t length,
                     int flags, int timeout_ms) noexcept override;
  ssize_t splice(int input_fd, off_t* input_offset,
                 int output_fd, off_t* output_offset,
                 size_t length, unsigned flags,
                 int timeout_ms) noexcept override;
  ssize_t splice_exact(int input_fd, off_t* input_offset,
                       int output_fd, off_t* output_offset,
                       size_t length, unsigned flags,
                       int timeout_ms,
                       size_t* calls) noexcept override;
  int connect(int fd, const sockaddr* address,
              socklen_t address_length,
              int timeout_ms) noexcept override;

 private:
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
    unsigned flags = 0;
    bool pooled    = false;
    bool external  = false;

    u_char* data() noexcept {
      return length <= sizeof(inline_data) ? inline_data :
          overflow_data.data();
    }
  };

  enum IoKind {
    IO_RECEIVE,
    IO_SEND,
    IO_SPLICE,
    IO_CONNECT,
  };

  struct alignas(8) IoRequest {
    // CQEs can complete before an exact receive or its processor is done.
    // Publish finished only after all reactor-side continuation work ends.
    std::atomic<bool> finished{false};
    unsigned cqe_completed      = 0;
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
    unsigned expected         = 2;
    off_t input_offset        = -1;
    off_t output_offset       = -1;
    bool has_input_offset     = false;
    bool has_output_offset    = false;
    bool exact                 = false;
    ReceiveProcessor receive_processor = nullptr;
    void* receive_context      = nullptr;
    int operation_result      = -ECANCELED;
    int timeout_result        = -ECANCELED;
    __kernel_timespec timeout = {};
  };

  struct Dispatch {
    fuse_buf buffer = {};
    int pipe[2]     = {-1, -1};
    size_t capacity = 0;
    Reply* reply    = nullptr;
    std::atomic<bool> reply_submitted{false};
    bool reply_claimed = false;
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
  bool submit_wakeup() noexcept;
  bool submit_external_receive() noexcept;
  bool submit_io_receive() noexcept;
  bool submit_dispatch_receive() noexcept;
  bool submit_external_reply(Reply* reply) noexcept;
  bool submit_io_request(IoRequest* request) noexcept;
  bool resize_receive_pipe() noexcept;
  static bool drain_receive_pipe(int fd) noexcept;
  bool drain_external_pipe() noexcept;
  bool drain_io_pipe() noexcept;
  bool drain_dispatch_pipe() noexcept;
  bool resume_receive() noexcept;
  bool enqueue_reply(Reply* reply) noexcept;
  Reply* acquire_reply() noexcept;
  void release_reply(Reply* reply) noexcept;
  bool submit_reply(Reply* reply) noexcept;
  void retire_reply(Reply* reply, int result) noexcept;
  void complete_reply(Reply* reply, int result) noexcept;
  void fail_replies(int result) noexcept;
  void fail_external_replies(int result) noexcept;
  void complete_io(IoRequest* request, bool timeout,
                   int result) noexcept;
  void fail_io_requests(int result) noexcept;
  void fail_dispatches() noexcept;
  int execute_io(IoRequest& request) noexcept;
  Dispatch* acquire_dispatch() noexcept;
  void finish_dispatch(Dispatch* dispatch) noexcept;
  void recycle_dispatch(Dispatch* dispatch) noexcept;
  void dispatch_complete(Dispatch* dispatch) noexcept;

  FuseReactorGroup* group_         = nullptr;
  fuse_session* session_           = nullptr;
  io_uring ring_                   = {};
  Reply* reply_head_               = nullptr;
  Reply* reply_tail_               = nullptr;
  int external_pipe_[2]            = {-1, -1};
  int io_pipe_[2]                  = {-1, -1};
  int dispatch_pipe_[2]            = {-1, -1};
  Dispatch* active_receive_        = nullptr;
  std::vector<Dispatch*> free_dispatches_;
  std::unique_ptr<Reply[]> reply_pool_;
  Reply* reply_free_                = nullptr;
  std::atomic<uint64_t> external_submitted_{0};
  std::atomic<uint64_t> external_completed_{0};
  std::vector<IoRequest*> io_requests_;
  uint64_t external_consumed_      = 0;
  uint64_t wake_value_             = 0;
  int fuse_fd_                     = -1;
  int wake_fd_                     = -1;
  int error_                       = 0;
  size_t reply_count_              = 0;
  size_t max_reply_count_          = 0;
  uint64_t received_requests_      = 0;
  uint64_t completed_replies_      = 0;
  uint64_t external_replies_       = 0;
  uint64_t io_operations_          = 0;
  std::atomic<uint64_t> receive_drains_{0};
  size_t dispatch_count_           = 0;
  size_t max_dispatch_count_       = 0;
  size_t reply_pool_size_          = 0;
  size_t reply_high_water_         = 0;
  unsigned setup_flags_            = 0;
  bool ring_ready_                 = false;
  bool owns_fuse_fd_               = false;
  bool initialization_owner_       = false;
  bool initialization_complete_    = false;
  bool receive_pending_            = false;
  bool external_pending_           = false;
  bool io_pending_                 = false;
  bool dispatch_pending_           = false;
  bool reply_pending_              = false;
  bool wake_pending_               = false;
  bool first_receive_              = true;
  u_char receive_token_            = 0;
  u_char external_token_           = 0;
  u_char io_token_                 = 0;
  u_char dispatch_token_           = 0;
  u_char wake_token_               = 0;

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
                  unsigned worker_count, int io_timeout_ms,
                  std::string& error);
  int run();
  void report_stats() const noexcept;
  void shutdown() noexcept;

 private:
  friend class FuseReactor;

  FuseReactor* callback_reactor() noexcept;
  void begin_shutdown() noexcept;
  bool synchronize_external(FuseReactor* reactor) noexcept;
  void reactor_initialized() noexcept;
  void wake() noexcept;
  bool submit_dispatch(FuseReactor* reactor,
                       FuseReactor::Dispatch* dispatch) noexcept;
  void dispatch_worker() noexcept;
  void stop_dispatch_workers() noexcept;
  static int clone_fuse_fd(fuse_session* session, std::string& error);

  fuse_session* session_ = nullptr;
  std::vector<std::unique_ptr<FuseReactor>> reactors_;
  std::mutex initialization_mutex_;
  std::condition_variable initialization_condition_;
  std::shared_mutex external_mutex_;
  std::mutex dispatch_mutex_;
  std::condition_variable dispatch_condition_;
  std::deque<std::pair<FuseReactor*, FuseReactor::Dispatch*>>
      dispatch_queue_;
  std::vector<std::thread> dispatch_threads_;
  std::atomic<bool> shutting_down_{false};
  int io_timeout_ms_ = kRequestIoTimeoutMs;
  bool dispatch_stopping_ = false;
  bool initialized_     = false;
  bool primary_stopped_ = false;
};
