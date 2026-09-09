#include <fuse_lowlevel.h>

#include <algorithm>
#include <array>
#include <errno.h>
#include <fcntl.h>
#include <linux/fuse.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <vector>

constexpr size_t kPrefixSize =
    sizeof(fuse_in_header) + sizeof(fuse_write_in);

struct Test {
  fuse_session* session = nullptr;
  int reply_peer = -1;
  unsigned clear_calls = 0;
  unsigned write_calls = 0;
  unsigned setxattr_calls = 0;
  fuse_req_t pending_reply = nullptr;
  unsigned fd_calls = 0;
  unsigned fd_case = 0;
  bool failed = false;

  ~Test() {
    if (session != nullptr) fuse_session_destroy(session);
    if (reply_peer >= 0) close(reply_peer);
  }

  bool check(bool condition, const char* message) {
    if (!condition) {
      fprintf(stderr, "libfuse_prefix_test: %s\n", message);
      failed = true;
    }
    return condition;
  }

  static ssize_t writev_callback(int fd, iovec* iov, int count, void*) {
    return ::writev(fd, iov, count);
  }

  static ssize_t read_callback(int fd, void* data, size_t size, void*) {
    return ::read(fd, data, size);
  }

  static ssize_t splice_callback(int in, off_t* offin, int out, off_t* offout,
                                  size_t size, unsigned flags, void*) {
    return ::splice(in, offin, out, offout, size, flags);
  }

  static void clear_receive(void* userdata) {
    ++static_cast<Test*>(userdata)->clear_calls;
  }

  static void init(void* userdata, fuse_conn_info* connection) {
    (void)userdata;
    fuse_set_feature_flag(connection, FUSE_CAP_SPLICE_WRITE);
    fuse_set_feature_flag(connection, FUSE_CAP_SPLICE_MOVE);
  }

  static ssize_t fd_reply(int fd, const iovec* header, int count,
                          int source_fd, off_t offset, size_t length,
                          unsigned flags, fuse_req_t req,
                          void* userdata) {
    auto& test = *static_cast<Test*>(userdata);
    ++test.fd_calls;
    test.check(fd == fuse_session_fd(test.session) && source_fd == 17 &&
                   offset == 31 && length == 123,
               "fd reply source or destination changed");
    test.check(count == 1 && header[0].iov_len == sizeof(fuse_out_header) &&
                   flags == SPLICE_F_MOVE,
               "fd reply changed header or transfer flags");
    const auto* out = static_cast<const fuse_out_header*>(header[0].iov_base);
    test.check(out->len == sizeof(*out) + length && out->error == 0 &&
                   out->unique == 10 + test.fd_case,
               "fd reply header does not match request");
    if (test.fd_case == 1) {
      errno = EAGAIN;
      return -1;
    }
    test.pending_reply = req;
    return FUSE_CUSTOM_IO_DEFERRED;
  }

  static void read_file(fuse_req_t req, fuse_ino_t, size_t, off_t,
                         fuse_file_info*) {
    auto& test = *static_cast<Test*>(fuse_req_userdata(req));
    const off_t offset = test.fd_case == 3 ? INT64_MAX : 31;
    const size_t length = test.fd_case == 2 ? 0 : 123;
    const auto flags = test.fd_case == 5 ? FUSE_BUF_NO_SPLICE :
                                           FUSE_BUF_SPLICE_MOVE;
    const int result = fuse_reply_fd_async(req, 17, offset, length, flags);
    test.check(result == 0,
               "fd reply entry did not enforce splice-only semantics");
  }

  static void write_buf(fuse_req_t req, fuse_ino_t nodeid,
                        fuse_bufvec* source, off_t offset,
                        fuse_file_info*) {
    auto& test = *static_cast<Test*>(fuse_req_userdata(req));
    std::array<char, 32> payload{};
    fuse_bufvec destination{};
    destination.count = 1;
    destination.buf[0].size = payload.size();
    destination.buf[0].mem = payload.data();
    const ssize_t copied = fuse_buf_copy(
        &destination, source, static_cast<fuse_buf_copy_flags>(0));
    ++test.write_calls;
    test.check(nodeid == 0x1234, "WRITE nodeid changed");
    test.check(offset == 77, "WRITE offset changed");
    test.check(copied == 13, "WRITE payload length changed");
    test.check(memcmp(payload.data(), "prefix-write!", 13) == 0,
               "WRITE payload changed");
    fuse_reply_write(req, copied < 0 ? 0 : static_cast<size_t>(copied));
  }

  static void setxattr(fuse_req_t req, fuse_ino_t nodeid, const char* name,
                       const char* value, size_t size, int flags) {
    auto& test = *static_cast<Test*>(fuse_req_userdata(req));
    ++test.setxattr_calls;
    test.check(nodeid == 0x5678, "SETXATTR nodeid changed");
    test.check(strcmp(name, "user.prefix") == 0,
               "SETXATTR name changed");
    test.check(size == 48, "SETXATTR value length changed");
    test.check(flags == 7, "SETXATTR flags changed");
    test.check(memcmp(value, "control-value", 13) == 0,
               "SETXATTR value changed");
    fuse_reply_err(req, 0);
  }

  bool initialize() {
    fuse_lowlevel_ops operations{};
    operations.init = init;
    operations.write_buf = write_buf;
    operations.setxattr = setxattr;
    operations.read = read_file;
    char name[] = "libfuse_prefix_test";
    char* argv[]{name};
    fuse_args args = FUSE_ARGS_INIT(1, argv);
    session = fuse_session_new(&args, &operations, sizeof(operations), this);
    fuse_opt_free_args(&args);
    if (!check(session != nullptr, "fuse_session_new failed")) return false;

    int sockets[2];
    if (!check(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) == 0,
               "socketpair failed")) return false;
    reply_peer = sockets[1];
    fuse_custom_io io{};
    io.writev = writev_callback;
    io.read = read_callback;
    io.splice_send = splice_callback;
    io.async_userdata = this;
    io.clear_receive = clear_receive;
    io.fd_reply_async = fd_reply;
    if (!check(fuse_session_custom_io(session, &io, sizeof(io), sockets[0]) == 0,
               "fuse_session_custom_io failed")) {
      close(sockets[0]);
      return false;
    }

    struct InitRequest {
      fuse_in_header header{};
      fuse_init_in body{};
    } request;
    request.header.len = sizeof(request);
    request.header.opcode = FUSE_INIT;
    request.header.unique = 1;
    request.body.major = FUSE_KERNEL_VERSION;
    request.body.minor = FUSE_KERNEL_MINOR_VERSION;
    request.body.max_readahead = 1024 * 1024;
    fuse_buf buffer{};
    buffer.size = sizeof(request);
    buffer.mem = &request;
    fuse_session_process_buf(session, &buffer);
    return drain_reply("FUSE_INIT reply missing");
  }

  bool drain_reply(const char* message) {
    std::array<char, 512> reply{};
    return check(::read(reply_peer, reply.data(), reply.size()) >=
                     static_cast<ssize_t>(sizeof(fuse_out_header)),
                 message);
  }

  bool process_prefixed(const std::vector<unsigned char>& request) {
    int fds[2];
    if (!check(pipe2(fds, O_CLOEXEC) == 0, "pipe2 failed")) return false;
    const ssize_t written =
        ::write(fds[1], request.data(), request.size());
    close(fds[1]);
    if (!check(written == static_cast<ssize_t>(request.size()),
               "request pipe write failed")) {
      close(fds[0]);
      return false;
    }
    alignas(fuse_write_in) std::array<unsigned char, kPrefixSize> prefix{};
    const size_t prefix_size = std::min(prefix.size(), request.size());
    const ssize_t consumed = ::read(fds[0], prefix.data(), prefix_size);
    if (!check(consumed == static_cast<ssize_t>(prefix_size),
               "request prefix read failed")) {
      close(fds[0]);
      return false;
    }
    fuse_buf buffer{};
    buffer.size = request.size();
    buffer.flags = FUSE_BUF_IS_FD;
    buffer.fd = fds[0];
    fuse_session_process_buf_prefix_fd(session, &buffer, prefix.data(),
                                       prefix_size, fuse_session_fd(session));
    close(fds[0]);
    return true;
  }

  bool write_case() {
    const char payload[] = "prefix-write!";
    std::vector<unsigned char> request(kPrefixSize + sizeof(payload) - 1);
    auto* header = reinterpret_cast<fuse_in_header*>(request.data());
    auto* input = reinterpret_cast<fuse_write_in*>(request.data() +
                                                    sizeof(*header));
    header->len = static_cast<uint32_t>(request.size());
    header->opcode = FUSE_WRITE;
    header->unique = 2;
    header->nodeid = 0x1234;
    input->offset = 77;
    input->size = sizeof(payload) - 1;
    memcpy(request.data() + kPrefixSize, payload, sizeof(payload) - 1);
    return process_prefixed(request) && drain_reply("WRITE reply missing") &&
           check(write_calls == 1, "WRITE callback missing") &&
           check(clear_calls == 0, "consumed WRITE unexpectedly cleared input");
  }

  bool large_control_case() {
    constexpr char name[] = "user.prefix";
    std::array<char, 48> value{};
    memcpy(value.data(), "control-value", 13);
    const size_t body_size = FUSE_COMPAT_SETXATTR_IN_SIZE + sizeof(name) +
                             value.size();
    std::vector<unsigned char> request(sizeof(fuse_in_header) + body_size);
    auto* header = reinterpret_cast<fuse_in_header*>(request.data());
    auto* input = reinterpret_cast<fuse_setxattr_in*>(request.data() +
                                                      sizeof(*header));
    header->len = static_cast<uint32_t>(request.size());
    header->opcode = FUSE_SETXATTR;
    header->unique = 3;
    header->nodeid = 0x5678;
    input->size = value.size();
    input->flags = 7;
    auto* payload = request.data() + sizeof(*header) +
                    FUSE_COMPAT_SETXATTR_IN_SIZE;
    memcpy(payload, name, sizeof(name));
    memcpy(payload + sizeof(name), value.data(), value.size());
    return check(request.size() > kPrefixSize,
                 "SETXATTR request did not cross prefix") &&
           process_prefixed(request) && drain_reply("SETXATTR reply missing") &&
           check(setxattr_calls == 1, "SETXATTR callback missing") &&
           check(clear_calls == 0, "SETXATTR unexpectedly cleared input");
  }

  bool malformed_case() {
    std::array<unsigned char, kPrefixSize> prefix{};
    auto* header = reinterpret_cast<fuse_in_header*>(prefix.data());
    header->len = prefix.size();
    header->opcode = FUSE_WRITE;
    int fds[2];
    if (!check(pipe2(fds, O_CLOEXEC) == 0, "malformed pipe2 failed"))
      return false;
    close(fds[1]);
    fuse_buf buffer{};
    buffer.size = prefix.size();
    buffer.flags = FUSE_BUF_IS_FD;
    buffer.fd = fds[0];
    fuse_session_process_buf_prefix_fd(session, &buffer, prefix.data(),
                                       prefix.size() - 1,
                                       fuse_session_fd(session));
    close(fds[0]);
    return check(clear_calls == 1, "malformed prefix did not clear input") &&
           check(write_calls == 1, "malformed prefix reached WRITE callback");
  }

  bool fd_reply_cases() {
    for (const unsigned scenario : {0u, 1u, 2u, 3u, 5u, 4u}) {
      fd_case = scenario;
      struct ReadRequest {
        fuse_in_header header{};
        fuse_read_in body{};
      } request;
      request.header.len    = sizeof(request);
      request.header.opcode = FUSE_READ;
      request.header.unique = 10 + fd_case;
      request.header.nodeid = 0x1234;
      request.body.size     = 123;
      fuse_buf buffer{};
      buffer.size = sizeof(request);
      buffer.mem  = &request;
      fuse_session_process_buf(session, &buffer);
      fuse_out_header reply{};
      if (fd_case == 0 || fd_case == 4) {
        if (!check(pending_reply != nullptr, "fd reply was not deferred")) return false;
        const ssize_t early = recv(reply_peer, &reply, sizeof(reply), MSG_DONTWAIT);
        if (!check(early == -1 && (errno == EAGAIN || errno == EWOULDBLOCK),
                   "deferred source emitted a premature reply")) return false;
        fuse_req_t req = pending_reply;
        pending_reply = nullptr;
        if (fd_case == 4) {
          return check(fuse_reply_async_complete(req, -EPIPE) == 0 &&
                           fuse_session_exited(session),
                       "final transport failure did not exit the session") &&
                 check(fd_calls == 3, "invalid source ranges reached the hook");
        }
        if (!check(fuse_reply_async_error(req, EIO) == 0,
                   "source error did not send a normal reply")) return false;
      }
      const int error = fd_case == 0 ? EIO :
          fd_case == 1 ? EAGAIN : fd_case == 5 ? EOPNOTSUPP : EINVAL;
      if (!check(recv(reply_peer, &reply, sizeof(reply), MSG_WAITALL) == sizeof(reply) &&
                     reply.len == sizeof(reply) && reply.unique == 10 + fd_case &&
                     reply.error == -error && !fuse_session_exited(session),
                 "source/admission error changed session or reply semantics")) return false;
    }
    return false;
  }
};

int main() {
  Test test;
  if (!test.initialize() || !test.write_case() ||
      !test.large_control_case() || !test.malformed_case() ||
      !test.fd_reply_cases()) {
    return 1;
  }
  return test.failed ? 1 : 0;
}
