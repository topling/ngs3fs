#include "fuse_reactor.hpp"

#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <barrier>
#include <errno.h>
#include <fcntl.h>
#include <linux/fuse.h>
#include <shared_mutex>
#include <stdio.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <thread>

struct ReactorIoTest {
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
  unsigned burst_completions = 0;
  unsigned burst_replies = 0;
  bool burst_reply_pending = false;
  bool submitting = false;
  bool submitting_notification = false;
  bool shutdown_completed = false;
  bool stopped_called = false;
  bool failed = false;

  ~ReactorIoTest() {
    notification_can_drain.store(true, std::memory_order_release);
    notification_can_drain.notify_one();
    if (notification_drain.joinable()) notification_drain.join();
    if (worker.joinable()) worker.join();
    group.shutdown();
    if (session != nullptr) fuse_session_destroy(session);
  }

  FuseReactor& reactor() { return *group.reactors_.front(); }

  bool check(bool condition, const char* message) noexcept {
    if (!condition) {
      fprintf(stderr, "reactor_io_test: %s\n", message);
      failed = true;
      fuse_session_exit(session);
    }
    return condition;
  }

  bool initialize() {
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
                           false, 32, 0, error)) {
      fprintf(stderr, "reactor_io_test: %s\n", error.c_str());
      return false;
    }
    group.reactors_.push_back(std::move(value));
    if (!check((reactor().setup_flags_ & IORING_SETUP_R_DISABLED) != 0,
               "reactor ring was not created disabled")) return false;
    auto disabled_target = std::make_unique<FuseReactor>();
    if (!disabled_target->initialize(&group, session, fake_fuse.get(), false,
                                     false, 32, 0, error)) {
      fprintf(stderr, "reactor_io_test: disabled target: %s\n", error.c_str());
      return false;
    }
    group.reactors_.push_back(std::move(disabled_target));
    group.initialized_ = true;
    group.running_reactors_.store(1, std::memory_order_release);
    if (!check(group.next_dispatch_reactor() == group.reactors_.front().get() &&
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
    if (test.fairness_iterations == 64 &&
        !test.check(::write(test.fairness_peer.get(), "f", 1) == 1,
                    "feed fairness receive")) return;
    if (!test.fairness_completion_iteration) {
      if (!test.check(test.fairness_iterations < 1024,
                      "continuously ready callbacks starved an I/O CQE")) return;
      test.check(test.reactor().post(&test.tasks[1]),
                 "requeue continuously ready callback");
    }
  }

  static void fairness_done(void* context, ssize_t result) noexcept {
    auto& test = *static_cast<ReactorIoTest*>(context);
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
      const unsigned pending = io_uring_sq_ready(&test.reactor().ring_);
      if (test.queue_plain_reply('a', queued_reply_done)) {
        if (!test.check(io_uring_sq_ready(&test.reactor().ring_) == pending + 1,
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
    if (!check(test.initialize(), "initialize reply shutdown case")) return false;
    FuseReactor& owner = test.reactor();
    if (!check(io_uring_enable_rings(&owner.ring_) == 0,
               "enable reply shutdown ring") ||
        !test.fill_socket(test.fake_fuse.get()) ||
        !test.fill_socket(test.fairness_socket.get())) return false;
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
                             false, 8, 0, error)) {
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

  int run() {
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
    check(fairness_completion_iteration >= 64 &&
          fairness_completion_iteration < 1024,
          "continuously ready callbacks starved an asynchronous CQE");
    check(burst_completions == burst_io.size() &&
          burst_replies == burst_io.size() && !burst_reply_pending,
          "ready FUSE continuations were not drained between I/O CQEs");
    check(burst_data[0] == 'b' && burst_data[1] == 'b',
          "CQ burst PREAD returned incorrect data");
    return failed ? 1 : 0;
  }
};

int main() {
  try {
    ReactorIoTest test;
    if (!test.initialize()) return 77;
    return test.run();
  } catch (const std::exception& error) {
    fprintf(stderr, "reactor_io_test: %s\n", error.what());
    return 1;
  }
}
