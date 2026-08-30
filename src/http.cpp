#include "http.hpp"

#include <llhttp.h>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <errno.h>
#include <ctype.h>
#include <stdint.h>
#include <string.h>
#include <charconv>
#include <iostream>
#include <memory>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

struct SslContextDeleter {
  void operator()(SSL_CTX* context) const noexcept {
    SSL_CTX_free(context);
  }
};

struct SslDeleter {
  void operator()(SSL* ssl) const noexcept {
    SSL_free(ssl);
  }
};

class TlsTunnel {
 public:
  TlsTunnel(std::unique_ptr<SSL_CTX, SslContextDeleter> context,
            std::unique_ptr<SSL, SslDeleter> ssl,
            UniqueFd remote, UniqueFd local)
      : context_(std::move(context)),
        ssl_(std::move(ssl)),
        remote_(std::move(remote)),
        local_(std::move(local)),
        thread_([this](std::stop_token stop) { run(stop); }) {}

  TlsTunnel(const TlsTunnel&) = delete;
  TlsTunnel& operator=(const TlsTunnel&) = delete;

 private:
  static bool would_block(SSL* ssl, int result) noexcept {
    const int error = SSL_get_error(ssl, result);
    return error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE;
  }

  static void make_nonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
      throw std::system_error(errno, std::generic_category(),
                              "fcntl(O_NONBLOCK TLS tunnel)");
    }
  }

  void run(std::stop_token stop) noexcept {
    try {
      make_nonblocking(remote_.get());
      make_nonblocking(local_.get());
      std::array<std::byte, 64U * 1024U> clear_to_tls;
      std::array<std::byte, 64U * 1024U> tls_to_clear;
      size_t clear_begin = 0;
      size_t clear_end   = 0;
      size_t tls_begin   = 0;
      size_t tls_end     = 0;
      bool local_eof     = false;
      while (!stop.stop_requested()) {
        bool progress = false;
        if (tls_begin != tls_end) {
          const ssize_t written = ::write(
              local_.get(), tls_to_clear.data() + tls_begin,
              tls_end - tls_begin);
          if (written > 0) {
            tls_begin += size_t(written);
            progress = true;
            if (tls_begin == tls_end) {
              tls_begin = tls_end = 0;
            }
          } else if (written < 0 && errno != EINTR && errno != EAGAIN &&
                     errno != EWOULDBLOCK) {
            break;
          }
        }
        if (clear_begin != clear_end) {
          const int written = SSL_write(
              ssl_.get(), clear_to_tls.data() + clear_begin,
              int(clear_end - clear_begin));
          if (written > 0) {
            clear_begin += size_t(written);
            progress = true;
            if (clear_begin == clear_end) {
              clear_begin = clear_end = 0;
            }
          } else if (!would_block(ssl_.get(), written)) {
            break;
          }
        }
        if (!local_eof && clear_end == 0) {
          const ssize_t read_bytes =
              ::read(local_.get(), clear_to_tls.data(), clear_to_tls.size());
          if (read_bytes > 0) {
            clear_end = size_t(read_bytes);
            progress = true;
          } else if (read_bytes == 0) {
            local_eof = true;
          } else if (errno != EINTR && errno != EAGAIN &&
                     errno != EWOULDBLOCK) {
            break;
          }
        }
        if (tls_end == 0) {
          const int read_bytes = SSL_read(
              ssl_.get(), tls_to_clear.data(), int(tls_to_clear.size()));
          if (read_bytes > 0) {
            tls_end = size_t(read_bytes);
            progress = true;
          } else {
            const int error = SSL_get_error(ssl_.get(), read_bytes);
            if (error == SSL_ERROR_ZERO_RETURN) {
              break;
            }
            if (error != SSL_ERROR_WANT_READ &&
                error != SSL_ERROR_WANT_WRITE) {
              break;
            }
          }
        }
        if (local_eof && clear_end == 0) {
          break;
        }
        if (progress) {
          continue;
        }
        const short local_events = short(
            ((!local_eof && clear_end == 0) ? POLLIN : 0) |
            ((tls_begin != tls_end) ? POLLOUT : 0));
        const short remote_events = short(
            POLLIN | ((clear_begin != clear_end) ? POLLOUT : 0));
        pollfd descriptors[2]{
            {.fd = local_.get(), .events = local_events, .revents = 0},
            {.fd = remote_.get(), .events = remote_events, .revents = 0},
        };
        const int result = ::poll(descriptors, 2, 100);
        if (result < 0 && errno != EINTR) {
          break;
        }
      }
      SSL_shutdown(ssl_.get());
      ::shutdown(local_.get(), SHUT_RDWR);
    } catch (const std::exception& error) {
      ::shutdown(local_.get(), SHUT_RDWR);
      std::cerr << "TLS tunnel stopped: " << error.what() << '\n';
    }
  }

  std::unique_ptr<SSL_CTX, SslContextDeleter> context_;
  std::unique_ptr<SSL, SslDeleter> ssl_;
  UniqueFd remote_;
  UniqueFd local_;
  std::jthread thread_;
};

struct TlsConnection {
  UniqueFd socket;
  std::shared_ptr<TlsTunnel> tunnel;
  bool http2 = false;
};

TlsConnection connect_tls(std::string_view host, uint16_t port) {
  UniqueFd remote = connect_tcp(host, port);
  std::unique_ptr<SSL_CTX, SslContextDeleter> context(
      SSL_CTX_new(TLS_client_method()));
  if (!context) {
    throw std::runtime_error("SSL_CTX_new failed");
  }
  SSL_CTX_set_min_proto_version(context.get(), TLS1_2_VERSION);
  SSL_CTX_set_verify(context.get(), SSL_VERIFY_PEER, nullptr);
  if (SSL_CTX_set_default_verify_paths(context.get()) != 1) {
    throw std::runtime_error("unable to load system TLS trust store");
  }
  std::unique_ptr<SSL, SslDeleter> ssl(SSL_new(context.get()));
  if (!ssl) {
    throw std::runtime_error("SSL_new failed");
  }
  const std::string server_name(host);
  in_addr ipv4{};
  in6_addr ipv6{};
  if (::inet_pton(AF_INET, server_name.c_str(), &ipv4) == 1 ||
      ::inet_pton(AF_INET6, server_name.c_str(), &ipv6) == 1) {
    if (X509_VERIFY_PARAM_set1_ip_asc(
            SSL_get0_param(ssl.get()), server_name.c_str()) != 1) {
      throw std::runtime_error("unable to configure TLS IP verification");
    }
  } else {
    if (SSL_set_tlsext_host_name(ssl.get(), server_name.c_str()) != 1 ||
        SSL_set1_host(ssl.get(), server_name.c_str()) != 1) {
      throw std::runtime_error("unable to configure TLS host verification");
    }
  }
  constexpr unsigned char protocols[] = {
      2, 'h', '2', 8, 'h', 't', 't', 'p', '/', '1', '.', '1'};
  if (SSL_set_alpn_protos(ssl.get(), protocols, sizeof(protocols)) != 0 ||
      SSL_set_fd(ssl.get(), remote.get()) != 1 ||
      SSL_connect(ssl.get()) != 1) {
    const unsigned long error = ERR_get_error();
    throw std::runtime_error(
        "TLS handshake failed: " +
        std::string(error == 0 ? "unknown error"
                               : ERR_error_string(error, nullptr)));
  }
  const unsigned char* selected = nullptr;
  unsigned selected_length = 0;
  SSL_get0_alpn_selected(ssl.get(), &selected, &selected_length);
  const std::string_view protocol(
      reinterpret_cast<const char*>(selected), selected_length);
  if (protocol != "h2" && protocol != "http/1.1") {
    throw std::runtime_error("TLS server did not negotiate h2 or http/1.1");
  }

  int pair[2] = {-1, -1};
  if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, pair) != 0) {
    throw std::system_error(errno, std::generic_category(), "socketpair(TLS)");
  }
  UniqueFd application(pair[0]);
  UniqueFd tunnel_end(pair[1]);
  auto tunnel = std::make_shared<TlsTunnel>(
      std::move(context), std::move(ssl), std::move(remote),
      std::move(tunnel_end));
  return TlsConnection{
      .socket = std::move(application),
      .tunnel = std::move(tunnel),
      .http2 = protocol == "h2",
  };
}

// HTTP/2 external DATA ingress.
constexpr size_t kFrameHeaderSize = 9;

void require_nghttp2(nghttp2_ssize result, size_t expected,
                     const char* operation) {
  if (result >= 0 && static_cast<size_t>(result) == expected) {
    return;
  }
  if (result < 0) {
    throw std::runtime_error(std::string(operation) + ": " +
                             nghttp2_strerror(static_cast<int>(result)));
  }
  throw std::runtime_error(std::string(operation) +
                           ": incomplete frame consumption");
}

ExternalDataIngress::ExternalDataIngress(size_t shadow_span_bytes)
    : shadow_span_bytes_(shadow_span_bytes) {
  static_assert(kDefaultShadowSpanSize == kPreferredIoSize);
  if (shadow_span_bytes_ == 0) {
    throw std::invalid_argument("shadow span must not be empty");
  }

  void* const mapping = ::mmap(nullptr, shadow_span_bytes_, PROT_NONE,
                               MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (mapping == MAP_FAILED) {
    throw std::system_error(errno, std::generic_category(),
                            "mmap(shadow DATA span)");
  }
  shadow_span_ = static_cast<const uint8_t*>(mapping);
}

ExternalDataIngress::~ExternalDataIngress() {
  if (shadow_span_ != nullptr) {
    ::munmap(const_cast<uint8_t*>(shadow_span_), shadow_span_bytes_);
  }
}

bool ExternalDataIngress::capture_frame_header(
    const nghttp2_frame_hd& header) noexcept {
  if (header_ready_) {
    return false;
  }
  header_ = header;
  header_ready_ = true;
  return true;
}

void ExternalDataIngress::reset() noexcept {
  payload_.clear();
  shadow_advance_active_ = false;
  header_                = {};
  header_ready_          = false;
}

void ExternalDataIngress::advance_shadow_payload(
    nghttp2_session* session, size_t payload_bytes) {
  if (shadow_advance_active_) {
    throw std::logic_error("nested shadow DATA advancement");
  }

  shadow_advance_active_ = true;
  try {
    size_t remaining = payload_bytes;
    while (remaining != 0) {
      const size_t chunk = std::min(remaining, shadow_span_bytes_);
      require_nghttp2(
          nghttp2_session_mem_recv2(session, shadow_span_, chunk), chunk,
          "nghttp2_session_mem_recv2(shadow DATA payload)");
      remaining -= chunk;
    }
  } catch (...) {
    shadow_advance_active_ = false;
    throw;
  }
  shadow_advance_active_ = false;
}

ReceiveResult ExternalDataIngress::receive_one(
    nghttp2_session* session, int socket_fd,
    int data_pipe_write_fd, size_t max_payload_bytes,
    int32_t expected_stream_id,
    std::span<const std::byte> frame_prefix) {
  if (session == nullptr || socket_fd < 0 ||
      (data_pipe_write_fd < 0 && max_payload_bytes != 0) ||
      expected_stream_id < 0 || header_ready_ ||
      frame_prefix.size() > kFrameHeaderSize) {
    throw std::invalid_argument("invalid external DATA ingress argument");
  }

  std::array<std::byte, kFrameHeaderSize> wire_header{};
  std::copy(frame_prefix.begin(), frame_prefix.end(), wire_header.begin());
  read_all(socket_fd, std::span(wire_header).subspan(frame_prefix.size()));
  require_nghttp2(
      nghttp2_session_mem_recv2(
          session, reinterpret_cast<const uint8_t*>(wire_header.data()),
          wire_header.size()),
      wire_header.size(), "nghttp2_session_mem_recv(frame header)");
  if (!header_ready_) {
    throw std::runtime_error("nghttp2 did not report the frame header");
  }

  ReceiveResult result{.header = header_};
  header_ready_ = false;
  if (result.header.type == NGHTTP2_DATA &&
      (result.header.flags & NGHTTP2_FLAG_PADDED) == 0 &&
      result.header.stream_id == expected_stream_id) {
    if (result.header.length > max_payload_bytes) {
      throw std::runtime_error("HTTP/2 DATA exceeds the requested body size");
    }
    if (result.header.length != 0) {
      result.spliced_payload = splice_exact(
          socket_fd, data_pipe_write_fd, result.header.length,
          SPLICE_F_MOVE | SPLICE_F_MORE);
      advance_shadow_payload(session, result.spliced_payload);
    }
    return result;
  }

  payload_.resize(result.header.length);
  if (!payload_.empty()) {
    read_all(socket_fd, payload_);
    require_nghttp2(
        nghttp2_session_mem_recv2(
            session, reinterpret_cast<const uint8_t*>(payload_.data()),
            payload_.size()),
        payload_.size(), "nghttp2_session_mem_recv(frame payload)");
  }
  result.copied_payload = payload_.size();
  return result;
}

// HTTP/2 client.
constexpr uint32_t kInitialReceiveWindow = 4U * 1024U * 1024U;

struct Http2SessionDeleter {
  void operator()(nghttp2_session* value) const {
    nghttp2_session_del(value);
  }
};

struct Http2CallbacksDeleter {
  void operator()(nghttp2_session_callbacks* value) const {
    nghttp2_session_callbacks_del(value);
  }
};

struct Http2OptionDeleter {
  void operator()(nghttp2_option* value) const {
    nghttp2_option_del(value);
  }
};

nghttp2_nv make_header(std::string_view name, std::string_view value) {
  return nghttp2_nv{
      .name = reinterpret_cast<uint8_t*>(
          const_cast<char*>(name.data())),
      .value = reinterpret_cast<uint8_t*>(
          const_cast<char*>(value.data())),
      .namelen = name.size(),
      .valuelen = value.size(),
      .flags = NGHTTP2_NV_FLAG_NONE,
  };
}

ssostr<32> range_header_value(uint64_t offset, uint64_t end) {
  std::array<char, 6 + 20 + 1 + 20> value;
  char* p = std::copy_n("bytes=", 6, value.data());
  const auto offset_result =
      std::to_chars(p, value.data() + value.size(), offset);
  if (offset_result.ec != std::errc{}) {
    throw std::runtime_error("unable to format HTTP range offset");
  }
  p = offset_result.ptr;
  *p++ = '-';
  const auto end_result =
      std::to_chars(p, value.data() + value.size(), end);
  if (end_result.ec != std::errc{}) {
    throw std::runtime_error("unable to format HTTP range end");
  }
  p = end_result.ptr;
  return ssostr<32>(value.data(), size_t(p - value.data()));
}

[[noreturn]] void throw_nghttp2(int error, const char* operation) {
  throw std::runtime_error(std::string(operation) + ": " +
                           nghttp2_strerror(error));
}

uint64_t http2_monotonic_ns() {
  timespec value{};
  if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
    throw std::system_error(errno, std::generic_category(), "clock_gettime");
  }
  return static_cast<uint64_t>(value.tv_sec) * 1'000'000'000ULL +
         static_cast<uint64_t>(value.tv_nsec);
}

class Http2Client final : public HttpClient {
 public:
  explicit Http2Client(UniqueFd connected_socket,
                       std::string request_authority,
                       std::string reconnect_host = {},
                       uint16_t reconnect_port = 0,
                       std::shared_ptr<TlsTunnel> connected_tunnel = {});

  RangeResponse get_range(std::string_view path, uint64_t offset,
                          size_t length, Pipe& destination,
                          std::span<const Header> headers,
                          bool capture_headers,
                          bool measure_transport) override;

  RangeResponse put_from_fd(std::string_view path,
                            std::span<const Header> headers,
                            int source_fd, uint64_t source_offset,
                            size_t length) override;

  void begin_upload(std::string_view method, std::string_view path,
                    std::span<const Header> headers,
                    size_t content_length) override;

  void upload_bytes(std::span<const std::byte> bytes) override;

  void upload_from_fd(int fd, uint64_t offset, size_t length,
                      bool seek) override;

  Response finish_upload(size_t max_response_body) override;

  void cancel_upload() noexcept override;

  RangeResponse request_no_body(std::string_view method,
                                std::string_view path,
                                std::span<const Header> headers,
                                size_t max_response_body) override;

  bool probe_server();
  void consume(const RangeResponse& response) override;

 private:
  struct UploadSource {
    int fd = -1;
    uint64_t offset = 0;
    const std::byte* data = nullptr;
    size_t remaining = 0;
    size_t sent = 0;
    bool seek = true;
    bool end_stream = true;
  };

  struct ActiveRequest {
    int stream_id = -1;
    Pipe* destination = nullptr;
    RangeResponse response;
    bool closed = false;
    uint32_t close_error = NGHTTP2_NO_ERROR;
    size_t max_body_bytes = 0;
    size_t shadow_callback_bytes = 0;
    UploadSource* upload = nullptr;
    bool capture_headers = true;
    bool measure_transport = false;
  };

  std::shared_ptr<TlsTunnel> tunnel;
  UniqueFd socket;
  std::string authority;
  std::string host;
  uint16_t port = 0;
  std::unique_ptr<nghttp2_session_callbacks, Http2CallbacksDeleter> callbacks;
  std::unique_ptr<nghttp2_option, Http2OptionDeleter> option;
  std::unique_ptr<nghttp2_session, Http2SessionDeleter> session;
  std::vector<nghttp2_nv> request_headers;
  ExternalDataIngress ingress;
  ActiveRequest* active = nullptr;
  std::unique_ptr<ActiveRequest> upload_request;
  Pipe upload_response;
  size_t upload_length = kUnknownBodyLength;
  size_t upload_sent = 0;
  uint32_t maximum_frame_size = 16U * 1024U;
  int callback_errno = 0;
  bool reconnect_required = false;
  bool tls = false;

  void initialize_session() {
    nghttp2_session_callbacks* raw_callbacks = nullptr;
    int result = nghttp2_session_callbacks_new(&raw_callbacks);
    if (result != 0) {
      throw_nghttp2(result, "nghttp2_session_callbacks_new");
    }
    callbacks.reset(raw_callbacks);
    nghttp2_session_callbacks_set_send_callback(callbacks.get(), on_send);
    nghttp2_session_callbacks_set_send_data_callback(callbacks.get(),
                                                      on_send_data);
    nghttp2_session_callbacks_set_on_begin_frame_callback(
        callbacks.get(), on_begin_frame);
    nghttp2_session_callbacks_set_on_header_callback(callbacks.get(),
                                                      on_header);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
        callbacks.get(), on_data_chunk);
    nghttp2_session_callbacks_set_on_stream_close_callback(
        callbacks.get(), on_stream_close);

    nghttp2_option* raw_option = nullptr;
    result = nghttp2_option_new(&raw_option);
    if (result != 0) {
      throw_nghttp2(result, "nghttp2_option_new");
    }
    option.reset(raw_option);
    nghttp2_option_set_no_auto_window_update(option.get(), 1);

    nghttp2_session* raw_session = nullptr;
    result = nghttp2_session_client_new2(
        &raw_session, callbacks.get(), this, option.get());
    if (result != 0) {
      throw_nghttp2(result, "nghttp2_session_client_new2");
    }
    session.reset(raw_session);

    const std::array settings{
        nghttp2_settings_entry{
            .settings_id = NGHTTP2_SETTINGS_ENABLE_PUSH,
            .value = 0,
        },
        nghttp2_settings_entry{
            .settings_id = NGHTTP2_SETTINGS_MAX_FRAME_SIZE,
            .value = maximum_frame_size,
        },
        nghttp2_settings_entry{
            .settings_id = NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE,
            .value = kInitialReceiveWindow,
        },
    };
    result = nghttp2_submit_settings(session.get(), NGHTTP2_FLAG_NONE,
                                     settings.data(), settings.size());
    if (result != 0) {
      throw_nghttp2(result, "nghttp2_submit_settings");
    }

    // SETTINGS_INITIAL_WINDOW_SIZE applies only to streams. Grow the
    // connection receive window separately so a request can fill one of our
    // preferred-size pipes before FUSE consumes and returns credit.
    result = nghttp2_submit_window_update(
        session.get(), NGHTTP2_FLAG_NONE, 0,
        kInitialReceiveWindow - NGHTTP2_INITIAL_WINDOW_SIZE);
    if (result != 0) {
      throw_nghttp2(result, "nghttp2_submit_window_update(connection)");
    }
  }

  static int on_header(nghttp2_session*, const nghttp2_frame* frame,
                       const uint8_t* name, size_t name_length,
                       const uint8_t* value, size_t value_length,
                       uint8_t, void* user_data) {
    auto& self = *static_cast<Http2Client*>(user_data);
    if (self.active == nullptr ||
        frame->hd.stream_id != self.active->stream_id) {
      return 0;
    }

    constexpr std::string_view status_name = ":status";
    if (name_length == status_name.size() &&
        memcmp(name, status_name.data(), name_length) == 0) {
      int status = 0;
      const char* first = reinterpret_cast<const char*>(value);
      const auto parsed = std::from_chars(
          first, first + value_length, status);
      if (parsed.ec == std::errc{}) {
        self.active->response.status = status;
      }
    } else if (self.active->capture_headers) {
      ssostr<32> key(reinterpret_cast<const char*>(name), name_length);
      ssostr<32> header_value(
          reinterpret_cast<const char*>(value), value_length);
      self.active->response.headers.insert_or_assign(
          std::move(key), std::move(header_value));
    }
    return 0;
  }

  static ssize_t on_send(nghttp2_session*, const uint8_t* data,
                         size_t length, int, void* user_data) {
    auto& self = *static_cast<Http2Client*>(user_data);
    try {
      if (self.active != nullptr && self.active->measure_transport &&
          self.active->response.wire_start_ns == 0) {
        self.active->response.wire_start_ns = http2_monotonic_ns();
      }
      send_all(
          self.socket.get(),
          std::span(reinterpret_cast<const std::byte*>(data), length));
      return static_cast<ssize_t>(length);
    } catch (const std::system_error& error) {
      self.callback_errno = error.code().value();
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    } catch (...) {
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
  }

  static int on_begin_frame(nghttp2_session*, const nghttp2_frame_hd* header,
                            void* user_data) {
    auto& self = *static_cast<Http2Client*>(user_data);
    return self.ingress.capture_frame_header(*header)
               ? 0
               : NGHTTP2_ERR_CALLBACK_FAILURE;
  }

  static ssize_t read_upload(nghttp2_session*, int32_t, uint8_t*,
                             size_t length, uint32_t* flags,
                             nghttp2_data_source* source, void*) {
    auto& upload = *static_cast<UploadSource*>(source->ptr);
    const size_t count = std::min(length, upload.remaining);
    *flags |= NGHTTP2_DATA_FLAG_NO_COPY;
    if (count == upload.remaining) {
      *flags |= NGHTTP2_DATA_FLAG_EOF;
      if (!upload.end_stream) {
        *flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
      }
    }
    return static_cast<ssize_t>(count);
  }

  static int on_send_data(nghttp2_session*, nghttp2_frame* frame,
                          const uint8_t* frame_header,
                          size_t length, nghttp2_data_source* source,
                          void* user_data) {
    auto& self = *static_cast<Http2Client*>(user_data);
    auto& upload = *static_cast<UploadSource*>(source->ptr);
    if (frame->data.padlen != 0 || length > upload.remaining) {
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    try {
      send_all(
          self.socket.get(),
          std::span(reinterpret_cast<const std::byte*>(frame_header),
                    kFrameHeaderSize));
      if (upload.data != nullptr) {
        send_all(self.socket.get(),
                 std::span(upload.data + upload.sent, length));
      } else if (upload.seek) {
        sendfile_exact(self.socket.get(), upload.fd, upload.offset, length);
      } else {
        splice_exact(upload.fd, self.socket.get(), length,
                     SPLICE_F_MOVE | SPLICE_F_MORE);
      }
      upload.remaining -= length;
      upload.sent += length;
      return 0;
    } catch (const std::system_error& error) {
      self.callback_errno = error.code().value();
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    } catch (...) {
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
  }

  static int on_data_chunk(nghttp2_session*, uint8_t,
                           int32_t stream_id,
                           const uint8_t* data, size_t length,
                           void* user_data) {
    auto& self = *static_cast<Http2Client*>(user_data);
    if (self.ingress.shadow_advance_active()) {
      if (self.active == nullptr || stream_id != self.active->stream_id ||
          length > self.active->max_body_bytes -
                       self.active->response.body_bytes ||
          length > self.active->max_body_bytes -
                       self.active->shadow_callback_bytes) {
        return NGHTTP2_ERR_CALLBACK_FAILURE;
      }
      self.active->shadow_callback_bytes += length;
      return 0;
    }
    if (self.active == nullptr || stream_id != self.active->stream_id ||
        self.active->destination == nullptr) {
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    try {
      if (length > self.active->max_body_bytes -
                       self.active->response.body_bytes) {
        return NGHTTP2_ERR_CALLBACK_FAILURE;
      }
      write_all(
          self.active->destination->write_fd(),
          std::span(reinterpret_cast<const std::byte*>(data), length));
    } catch (...) {
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    self.active->response.body_bytes += length;
    self.active->response.fallback_copied_bytes += length;
    return 0;
  }

  static int on_stream_close(nghttp2_session*, int32_t stream_id,
                             uint32_t error_code, void* user_data) {
    auto& self = *static_cast<Http2Client*>(user_data);
    if (self.active != nullptr && stream_id == self.active->stream_id) {
      self.active->close_error = error_code;
      self.active->closed = true;
    }
    return 0;
  }

  void flush() {
    callback_errno = 0;
    const int result = nghttp2_session_send(session.get());
    if (result != 0) {
      if (callback_errno != 0) {
        throw std::system_error(callback_errno, std::generic_category(),
                                "send(HTTP/2)");
      }
      throw_nghttp2(result, "nghttp2_session_send");
    }
  }

  void send_upload(UploadSource& source);
  Response receive_upload_response(size_t max_response_body);

  void ensure_ready();
};

Http2Client::Http2Client(UniqueFd connected_socket,
                         std::string request_authority,
                         std::string reconnect_host,
                         uint16_t reconnect_port,
                         std::shared_ptr<TlsTunnel> connected_tunnel)
    : tunnel(std::move(connected_tunnel)),
      socket(std::move(connected_socket)),
      authority(std::move(request_authority)),
      host(std::move(reconnect_host)),
      port(reconnect_port),
      tls(tunnel != nullptr) {
  Pipe probe = Pipe::create(kPreferredIoSize);
  maximum_frame_size = uint32_t(std::clamp<size_t>(
      probe.capacity(), 16U * 1024U,
      kPreferredIoSize));
  initialize_session();
}

void Http2Client::ensure_ready() {
  if (!reconnect_required &&
      nghttp2_session_check_request_allowed(session.get()) != 0) {
    return;
  }
  if (host.empty() || port == 0) {
    throw std::runtime_error("HTTP/2 connection is not reusable");
  }

  UniqueFd connected;
  std::shared_ptr<TlsTunnel> connected_tunnel;
  if (tls) {
    TlsConnection transport = connect_tls(host, port);
    if (!transport.http2) {
      throw std::runtime_error(
          "TLS reconnect changed protocol from HTTP/2");
    }
    connected = std::move(transport.socket);
    connected_tunnel = std::move(transport.tunnel);
  } else {
    connected = connect_tcp(host, port);
  }
  socket.reset();
  tunnel.reset();
  session.reset();
  option.reset();
  callbacks.reset();
  ingress.reset();
  socket             = std::move(connected);
  tunnel             = std::move(connected_tunnel);
  active             = nullptr;
  reconnect_required = false;
  initialize_session();
}

RangeResponse Http2Client::get_range(std::string_view path, uint64_t offset,
                                size_t length,
                                Pipe& destination,
                                std::span<const Header> extra_headers,
                                bool capture_headers,
                                bool measure_transport) {
  ensure_ready();
  if (active != nullptr) {
    throw std::logic_error("this synchronous connection already has a request");
  }
  if (length == 0 || length > destination.capacity()) {
    throw std::invalid_argument("range must fit in the destination pipe");
  }

  const uint64_t inclusive_end =
      offset + static_cast<uint64_t>(length) - 1;
  if (inclusive_end < offset) {
    throw std::overflow_error("range end overflow");
  }
  const Header* supplied_range = nullptr;
  for (const Header& value : extra_headers) {
    if (value.name == "range") {
      if (supplied_range != nullptr) {
        throw std::invalid_argument("duplicate GET range header");
      }
      supplied_range = &value;
    }
  }
  const ssostr<32> range = supplied_range == nullptr
                           ? range_header_value(offset, inclusive_end)
                           : ssostr<32>{};
  const std::string_view range_value = supplied_range == nullptr
                                           ? sso_view(range)
                                           : sso_view(supplied_range->value);

  std::vector<nghttp2_nv>& headers = request_headers;
  headers.clear();
  headers.reserve(6 + extra_headers.size());
  headers.push_back(make_header(":method", "GET"));
  headers.push_back(make_header(":scheme", tls ? "https" : "http"));
  headers.push_back(make_header(":authority", authority));
  headers.push_back(make_header(":path", path));
  headers.push_back(make_header("range", range_value));
  headers.push_back(make_header("accept-encoding", "identity"));
  for (const Header& value : extra_headers) {
    if (value.name == "range") {
      continue;
    }
    if (sso_view(value.name).starts_with(':') ||
        value.name == "accept-encoding") {
      throw std::invalid_argument("GET header conflicts with generated header");
    }
    headers.push_back(
        make_header(sso_view(value.name), sso_view(value.value)));
  }

  ActiveRequest request;
  request.destination       = &destination;
  request.max_body_bytes    = length;
  request.capture_headers   = capture_headers;
  request.measure_transport = measure_transport;
  if (capture_headers) {
    request.response.headers.reserve(16);
  }
  const int stream_id = nghttp2_submit_request(
      session.get(), nullptr, headers.data(), headers.size(), nullptr,
      nullptr);
  if (stream_id < 0) {
    throw_nghttp2(stream_id, "nghttp2_submit_request");
  }
  request.stream_id          = stream_id;
  request.response.stream_id = stream_id;
  request.response.requires_consume = true;
  active                     = &request;

  try {
    flush();
    while (!request.closed) {
      const size_t shadow_before = request.shadow_callback_bytes;
      const ReceiveResult received = ingress.receive_one(
          session.get(), socket.get(), destination.write_fd(),
          length - request.response.body_bytes, request.stream_id);
      if (received.spliced_payload !=
          request.shadow_callback_bytes - shadow_before) {
        throw std::runtime_error(
            "nghttp2 rejected externally spliced DATA");
      }
      if (received.header.type == NGHTTP2_DATA) {
        request.response.body_bytes += received.spliced_payload;
        request.response.externally_spliced_bytes += received.spliced_payload;
        if (measure_transport) {
          request.response.wire_last_data_ns = http2_monotonic_ns();
        }
      }
      flush();
    }
  } catch (...) {
    active = nullptr;
    reconnect_required = true;
    throw;
  }
  active = nullptr;

  if (request.close_error != NGHTTP2_NO_ERROR) {
    throw std::runtime_error("HTTP/2 stream closed with error " +
                             std::to_string(request.close_error));
  }
  return std::move(request.response);
}

RangeResponse Http2Client::put_from_fd(std::string_view path,
                                  std::span<const Header> extra_headers,
                                  int source_fd,
                                  uint64_t source_offset,
                                  size_t length) {
  ensure_ready();
  if (active != nullptr) {
    throw std::logic_error("this synchronous connection already has a request");
  }
  if (length != 0 && source_fd < 0) {
    throw std::invalid_argument("invalid PUT source fd");
  }

  const std::string content_length = std::to_string(length);
  std::vector<nghttp2_nv>& headers = request_headers;
  headers.clear();
  headers.reserve(5 + extra_headers.size());
  headers.push_back(make_header(":method", "PUT"));
  headers.push_back(make_header(":scheme", tls ? "https" : "http"));
  headers.push_back(make_header(":authority", authority));
  headers.push_back(make_header(":path", path));
  headers.push_back(make_header("content-length", content_length));
  for (const Header& value : extra_headers) {
    if (sso_view(value.name).starts_with(':') ||
        value.name == "content-length") {
      throw std::invalid_argument("PUT header conflicts with generated header");
    }
    headers.push_back(
        make_header(sso_view(value.name), sso_view(value.value)));
  }

  Pipe response_pipe = Pipe::create();
  UploadSource upload{
      .fd = source_fd,
      .offset = source_offset,
      .remaining = length,
  };
  ActiveRequest request;
  request.destination    = &response_pipe;
  request.max_body_bytes = response_pipe.capacity();
  request.upload         = &upload;
  request.response.headers.reserve(16);

  nghttp2_data_provider provider{
      .source = {.ptr = &upload},
      .read_callback = read_upload,
  };
  const int stream_id = nghttp2_submit_request(
      session.get(), nullptr, headers.data(), headers.size(),
      length == 0 ? nullptr : &provider, nullptr);
  if (stream_id < 0) {
    throw_nghttp2(stream_id, "nghttp2_submit_request(PUT)");
  }
  request.stream_id          = stream_id;
  request.response.stream_id = stream_id;
  active                     = &request;

  try {
    flush();
    while (!request.closed) {
      const size_t before        = request.response.body_bytes;
      const size_t shadow_before = request.shadow_callback_bytes;
      const ReceiveResult received = ingress.receive_one(
          session.get(), socket.get(),
          response_pipe.write_fd(),
          request.max_body_bytes - request.response.body_bytes,
          request.stream_id);
      if (received.spliced_payload !=
          request.shadow_callback_bytes - shadow_before) {
        throw std::runtime_error(
            "nghttp2 rejected externally spliced DATA");
      }
      if (received.header.type == NGHTTP2_DATA) {
        request.response.body_bytes += received.spliced_payload;
        request.response.externally_spliced_bytes += received.spliced_payload;
      }
      const size_t appended = request.response.body_bytes - before;
      if (appended != 0) {
        const size_t old_size = request.response.body.size();
        request.response.body.resize(old_size + appended);
        read_all(response_pipe.read_fd(),
                 std::span(request.response.body).subspan(old_size));
        const int consumed = nghttp2_session_consume(
            session.get(), request.stream_id, appended);
        if (consumed != 0) {
          throw_nghttp2(consumed, "nghttp2_session_consume(PUT response)");
        }
      }
      flush();
    }
  } catch (...) {
    active = nullptr;
    reconnect_required = true;
    throw;
  }
  active = nullptr;
  request.response.externally_sent_bytes = upload.sent;

  if (request.close_error != NGHTTP2_NO_ERROR) {
    throw std::runtime_error("HTTP/2 PUT stream closed with error " +
                             std::to_string(request.close_error));
  }
  if (upload.remaining != 0) {
    throw std::runtime_error("HTTP/2 PUT ended before the source was sent");
  }
  return std::move(request.response);
}

void Http2Client::begin_upload(
    std::string_view method, std::string_view path,
    std::span<const Header> extra_headers, size_t content_length) {
  ensure_ready();
  if (active != nullptr || upload_request) {
    throw std::logic_error("HTTP/2 connection already has a request");
  }
  if (method.empty()) {
    throw std::invalid_argument("streaming HTTP/2 method is empty");
  }

  std::vector<nghttp2_nv>& headers = request_headers;
  headers.clear();
  const bool fixed_length = content_length != kUnknownBodyLength;
  const std::string length = fixed_length
                                 ? std::to_string(content_length)
                                 : std::string();
  headers.reserve(5 + extra_headers.size() + unsigned(fixed_length));
  headers.push_back(make_header(":method", method));
  headers.push_back(make_header(":scheme", tls ? "https" : "http"));
  headers.push_back(make_header(":authority", authority));
  headers.push_back(make_header(":path", path));
  headers.push_back(make_header("accept-encoding", "identity"));
  if (fixed_length) {
    headers.push_back(make_header("content-length", length));
  }
  for (const Header& value : extra_headers) {
    if (sso_view(value.name).starts_with(':') ||
        value.name == "accept-encoding" ||
        value.name == "content-length" ||
        value.name == "transfer-encoding") {
      throw std::invalid_argument(
          "streaming HTTP/2 header conflicts with generated header");
    }
    headers.push_back(
        make_header(sso_view(value.name), sso_view(value.value)));
  }

  upload_response = Pipe::create();
  upload_request = std::make_unique<ActiveRequest>();
  upload_request->destination    = &upload_response;
  upload_request->max_body_bytes = upload_response.capacity();
  upload_request->response.headers.reserve(16);
  const int stream_id = nghttp2_submit_headers(
      session.get(), NGHTTP2_FLAG_NONE, -1, nullptr,
      headers.data(), headers.size(), nullptr);
  if (stream_id < 0) {
    upload_request.reset();
    upload_response = Pipe();
    throw_nghttp2(stream_id, "nghttp2_submit_headers(streaming upload)");
  }
  upload_request->stream_id          = stream_id;
  upload_request->response.stream_id = stream_id;
  upload_length                      = content_length;
  upload_sent                        = 0;
  active                             = upload_request.get();
  try {
    flush();
  } catch (...) {
    cancel_upload();
    throw;
  }
}

void Http2Client::send_upload(UploadSource& source) {
  if (!upload_request || active != upload_request.get()) {
    throw std::logic_error("HTTP/2 streaming upload is not active");
  }
  nghttp2_data_provider provider{
      .source = {.ptr = &source},
      .read_callback = read_upload,
  };
  const uint8_t flags = source.end_stream
                            ? NGHTTP2_FLAG_END_STREAM
                            : NGHTTP2_FLAG_NONE;
  const int result = nghttp2_submit_data(
      session.get(), flags, upload_request->stream_id, &provider);
  if (result != 0) {
    throw_nghttp2(result, "nghttp2_submit_data(streaming upload)");
  }
  for (;;) {
    flush();
    if (source.remaining == 0) {
      break;
    }
    ActiveRequest& request = *upload_request;
    const size_t before        = request.response.body_bytes;
    const size_t shadow_before = request.shadow_callback_bytes;
    const ReceiveResult received = ingress.receive_one(
        session.get(), socket.get(), upload_response.write_fd(),
        request.max_body_bytes - request.response.body_bytes,
        request.stream_id);
    if (received.spliced_payload !=
        request.shadow_callback_bytes - shadow_before) {
      throw std::runtime_error(
          "nghttp2 rejected externally spliced streaming response DATA");
    }
    if (received.header.type == NGHTTP2_DATA) {
      request.response.body_bytes += received.spliced_payload;
      request.response.externally_spliced_bytes += received.spliced_payload;
    }
    if (request.closed && source.remaining != 0) {
      throw std::runtime_error(
          "HTTP/2 peer ended a streaming upload before consuming its body");
    }
    const size_t appended = request.response.body_bytes - before;
    if (appended != 0) {
      const size_t old_size = request.response.body.size();
      request.response.body.resize(old_size + appended);
      read_all(upload_response.read_fd(),
               std::span(request.response.body).subspan(old_size));
    }
  }
  upload_request->response.externally_sent_bytes += source.sent;
}

void Http2Client::upload_bytes(std::span<const std::byte> bytes) {
  if (bytes.empty()) {
    return;
  }
  if (upload_length != kUnknownBodyLength &&
      bytes.size() > upload_length - upload_sent) {
    throw std::runtime_error("HTTP/2 upload exceeds Content-Length");
  }
  UploadSource source{
      .data = bytes.data(),
      .remaining = bytes.size(),
      .end_stream = false,
  };
  try {
    send_upload(source);
    upload_sent += bytes.size();
  } catch (...) {
    cancel_upload();
    throw;
  }
}

void Http2Client::upload_from_fd(int fd, uint64_t offset, size_t length,
                                 bool seek) {
  if (length == 0) {
    return;
  }
  if (fd < 0) {
    throw std::invalid_argument("invalid streaming upload fd");
  }
  if (upload_length != kUnknownBodyLength &&
      length > upload_length - upload_sent) {
    throw std::runtime_error("HTTP/2 upload exceeds Content-Length");
  }
  UploadSource source{
      .fd = fd,
      .offset = offset,
      .remaining = length,
      .seek = seek,
      .end_stream = false,
  };
  try {
    send_upload(source);
    upload_sent += length;
  } catch (...) {
    cancel_upload();
    throw;
  }
}

Response Http2Client::receive_upload_response(size_t max_response_body) {
  if (!upload_request || active != upload_request.get()) {
    throw std::logic_error("HTTP/2 streaming upload is not active");
  }
  ActiveRequest& request = *upload_request;
  request.max_body_bytes =
      std::min(max_response_body, upload_response.capacity());
  while (!request.closed) {
    const size_t before        = request.response.body_bytes;
    const size_t shadow_before = request.shadow_callback_bytes;
    const ReceiveResult received = ingress.receive_one(
        session.get(), socket.get(), upload_response.write_fd(),
        request.max_body_bytes - request.response.body_bytes,
        request.stream_id);
    if (received.spliced_payload !=
        request.shadow_callback_bytes - shadow_before) {
      throw std::runtime_error(
          "nghttp2 rejected externally spliced streaming response DATA");
    }
    if (received.header.type == NGHTTP2_DATA) {
      request.response.body_bytes += received.spliced_payload;
      request.response.externally_spliced_bytes += received.spliced_payload;
    }
    const size_t appended = request.response.body_bytes - before;
    if (appended != 0) {
      const size_t old_size = request.response.body.size();
      request.response.body.resize(old_size + appended);
      read_all(upload_response.read_fd(),
               std::span(request.response.body).subspan(old_size));
    }
    flush();
  }
  if (request.close_error != NGHTTP2_NO_ERROR) {
    throw std::runtime_error("HTTP/2 streaming upload closed with error " +
                             std::to_string(request.close_error));
  }
  if (request.response.body_bytes != 0) {
    consume(request.response);
  }
  return std::move(request.response);
}

Response Http2Client::finish_upload(size_t max_response_body) {
  if (max_response_body == 0) {
    throw std::invalid_argument("streaming response buffer is empty");
  }
  if (upload_length != kUnknownBodyLength && upload_sent != upload_length) {
    throw std::runtime_error("HTTP/2 upload ended before Content-Length");
  }
  UploadSource source{.end_stream = true};
  try {
    send_upload(source);
    Response response = receive_upload_response(max_response_body);
    active = nullptr;
    upload_request.reset();
    upload_response = Pipe();
    upload_length   = kUnknownBodyLength;
    upload_sent     = 0;
    return response;
  } catch (...) {
    cancel_upload();
    throw;
  }
}

void Http2Client::cancel_upload() noexcept {
  active = nullptr;
  upload_request.reset();
  upload_response = Pipe();
  upload_length   = kUnknownBodyLength;
  upload_sent     = 0;
  socket.reset();
  reconnect_required = true;
}

RangeResponse Http2Client::request_no_body(
    std::string_view method, std::string_view path,
    std::span<const Header> extra_headers, size_t max_response_body) {
  ensure_ready();
  if (active != nullptr) {
    throw std::logic_error("this synchronous connection already has a request");
  }
  if (method.empty() || max_response_body == 0) {
    throw std::invalid_argument("invalid bodyless HTTP/2 request");
  }

  std::vector<nghttp2_nv>& headers = request_headers;
  headers.clear();
  headers.reserve(5 + extra_headers.size());
  headers.push_back(make_header(":method", method));
  headers.push_back(make_header(":scheme", tls ? "https" : "http"));
  headers.push_back(make_header(":authority", authority));
  headers.push_back(make_header(":path", path));
  headers.push_back(make_header("accept-encoding", "identity"));
  for (const Header& value : extra_headers) {
    if (sso_view(value.name).starts_with(':') ||
        value.name == "accept-encoding") {
      throw std::invalid_argument(
          "request header conflicts with generated header");
    }
    headers.push_back(
        make_header(sso_view(value.name), sso_view(value.value)));
  }

  const bool head = method == "HEAD";
  Pipe response_pipe = head ? Pipe() : Pipe::create();
  ActiveRequest request;
  request.destination    = head ? nullptr : &response_pipe;
  request.max_body_bytes = head ? 0 : max_response_body;
  request.response.headers.reserve(16);
  const int stream_id = nghttp2_submit_request(
      session.get(), nullptr, headers.data(), headers.size(), nullptr,
      nullptr);
  if (stream_id < 0) {
    throw_nghttp2(stream_id, "nghttp2_submit_request(bodyless)");
  }
  request.stream_id          = stream_id;
  request.response.stream_id = stream_id;
  active                     = &request;

  try {
    flush();
    while (!request.closed) {
      const size_t before        = request.response.body_bytes;
      const size_t shadow_before = request.shadow_callback_bytes;
      const ReceiveResult received = ingress.receive_one(
          session.get(), socket.get(),
          head ? -1 : response_pipe.write_fd(),
          request.max_body_bytes - request.response.body_bytes,
          request.stream_id);
      if (received.spliced_payload !=
          request.shadow_callback_bytes - shadow_before) {
        throw std::runtime_error(
            "nghttp2 rejected externally spliced DATA");
      }
      if (received.header.type == NGHTTP2_DATA) {
        request.response.body_bytes += received.spliced_payload;
        request.response.externally_spliced_bytes += received.spliced_payload;
      }
      const size_t appended = request.response.body_bytes - before;
      if (appended != 0) {
        if (head) {
          throw std::runtime_error("HTTP/2 HEAD returned a response body");
        }
        const size_t old_size = request.response.body.size();
        request.response.body.resize(old_size + appended);
        read_all(response_pipe.read_fd(),
                 std::span(request.response.body).subspan(old_size));
        const int consumed = nghttp2_session_consume(
            session.get(), request.stream_id, appended);
        if (consumed != 0) {
          throw_nghttp2(consumed,
                        "nghttp2_session_consume(bodyless response)");
        }
      }
      flush();
    }
  } catch (...) {
    active = nullptr;
    reconnect_required = true;
    throw;
  }
  active = nullptr;

  if (request.close_error != NGHTTP2_NO_ERROR) {
    throw std::runtime_error("HTTP/2 stream closed with error " +
                             std::to_string(request.close_error));
  }
  return std::move(request.response);
}

bool Http2Client::probe_server() {
  if (active != nullptr) {
    throw std::logic_error("cannot probe HTTP/2 during an active request");
  }
  flush();
  set_socket_receive_timeout(socket.get(),
                                 kProtocolProbeTimeoutMs);

  constexpr std::array prefix{
      std::byte{'H'}, std::byte{'T'}, std::byte{'T'}, std::byte{'P'},
      std::byte{'/'}};
  std::array<std::byte, prefix.size()> received_prefix{};
  try {
    const ssize_t received =
        ::recv(socket.get(), received_prefix.data(),
               received_prefix.size(), MSG_WAITALL);
    if (received == 0) {
      set_socket_receive_timeout(socket.get(), kRequestIoTimeoutMs);
      return false;
    }
    if (received < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        set_socket_receive_timeout(socket.get(), kRequestIoTimeoutMs);
        return false;
      }
      throw std::system_error(errno, std::generic_category(),
                              "recv(HTTP/2 probe)");
    }
    if (received != static_cast<ssize_t>(prefix.size())) {
      set_socket_receive_timeout(socket.get(), kRequestIoTimeoutMs);
      return false;
    }
    if (std::equal(prefix.begin(), prefix.end(), received_prefix.begin())) {
      set_socket_receive_timeout(socket.get(), kRequestIoTimeoutMs);
      return false;
    }

    Pipe sink = Pipe::create();
    const ReceiveResult frame = ingress.receive_one(
        session.get(), socket.get(), sink.write_fd(),
        sink.capacity(), 0, received_prefix);
    flush();
    set_socket_receive_timeout(socket.get(), kRequestIoTimeoutMs);
    return frame.header.type == NGHTTP2_SETTINGS &&
           frame.header.stream_id == 0;
  } catch (...) {
    set_socket_receive_timeout(socket.get(), kRequestIoTimeoutMs);
    throw;
  }
}

void Http2Client::consume(const RangeResponse& response) {
  try {
    const int result = nghttp2_session_consume(
        session.get(), response.stream_id, response.body_bytes);
    if (result != 0) {
      throw_nghttp2(result, "nghttp2_session_consume");
    }
    flush();
  } catch (...) {
    reconnect_required = true;
    throw;
  }
}



// HTTP/1.1 client.
constexpr size_t kMaxResponseHeaderBytes = 64U * 1024U;
constexpr size_t kHttp1ReadSize = 1024;

[[noreturn]] void http1_throw_errno(const char* operation);

[[noreturn]] void http1_throw_errno(const char* operation) {
  throw std::system_error(errno, std::generic_category(), operation);
}

uint64_t http1_monotonic_ns() {
  timespec value{};
  if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
    http1_throw_errno("clock_gettime");
  }
  return static_cast<uint64_t>(value.tv_sec) * 1'000'000'000ULL +
         static_cast<uint64_t>(value.tv_nsec);
}

ssostr<32> ascii_lower(const ssostr<32>& value) {
  ssostr<32> result(value);
  for (char& ch : result) {
    if (ch >= 'A' && ch <= 'Z') {
      ch += 'a' - 'A';
    }
  }
  return result;
}

void ascii_lower_in_place(ssostr<32>& value) {
  for (char& ch : value) {
    if (ch >= 'A' && ch <= 'Z') {
      ch += 'a' - 'A';
    }
  }
}

bool ascii_equal(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); ++i) {
    char ch = left[i];
    if (ch >= 'A' && ch <= 'Z') {
      ch += 'a' - 'A';
    }
    if (ch != right[i]) {
      return false;
    }
  }
  return true;
}

bool contains_cr_or_lf(std::string_view value) {
  return value.find('\r') != std::string_view::npos ||
         value.find('\n') != std::string_view::npos;
}

bool valid_header_name(const ssostr<32>& name) {
  if (name.empty()) {
    return false;
  }
  return std::ranges::all_of(name, [](char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= 'a' && ch <= 'z') || ch == '!' || ch == '#' || ch == '$' ||
           ch == '%' || ch == '&' || ch == '\'' || ch == '*' || ch == '+' ||
           ch == '-' || ch == '.' || ch == '^' || ch == '_' || ch == '`' ||
           ch == '|' || ch == '~';
  });
}

bool valid_method(std::string_view method) {
  return !method.empty() && std::ranges::all_of(method, [](char ch) {
           return ch >= 'A' && ch <= 'Z';
         });
}

void build_request_head(
    std::string& request, std::string_view method, std::string_view path,
    std::string_view authority,
    std::span<const Header> generated_headers,
    std::span<const Header> headers,
    std::optional<size_t> content_length, bool chunked = false,
    bool trusted_headers = false) {
  if (!valid_method(method) || path.empty() || path.front() != '/' ||
      contains_cr_or_lf(path) || path.find(' ') != std::string_view::npos) {
    throw std::invalid_argument("invalid HTTP/1.1 request line");
  }
  if (authority.empty() || contains_cr_or_lf(authority)) {
    throw std::invalid_argument("invalid HTTP/1.1 authority");
  }

  size_t capacity = method.size() + path.size() + authority.size() + 55;
  for (const Header& header : generated_headers) {
    capacity += header.name.size() + header.value.size() + 4;
  }
  for (const Header& header : headers) {
    capacity += header.name.size() + header.value.size() + 4;
  }
  if (content_length || chunked) {
    capacity += 40;
  }
  request.clear();
  request.reserve(capacity);
  request.append(method);
  request.push_back(' ');
  request.append(path);
  constexpr std::string_view request_host = " HTTP/1.1\r\nhost: ";
  constexpr std::string_view colon_space = ": ";
  constexpr std::string_view line_end = "\r\n";
  constexpr std::string_view content_length_name = "content-length: ";
  constexpr std::string_view keep_alive =
      "connection: keep-alive\r\n\r\n";
  request.append(request_host);
  request.append(authority);
  request.append(line_end);

  const auto append_header = [&](const Header& header) {
    if (!trusted_headers &&
        (!valid_header_name(header.name) ||
         contains_cr_or_lf(sso_view(header.value)) ||
         ascii_equal(sso_view(header.name), "host") ||
         ascii_equal(sso_view(header.name), "connection") ||
         ascii_equal(sso_view(header.name), "transfer-encoding") ||
         (content_length &&
          ascii_equal(sso_view(header.name), "content-length")))) {
      throw std::invalid_argument("invalid or conflicting HTTP/1.1 header");
    }
    request.append(header.name.data(), header.name.size());
    request.append(colon_space);
    request.append(header.value.data(), header.value.size());
    request.append(line_end);
  };
  for (const Header& header : generated_headers) {
    append_header(header);
  }
  for (const Header& header : headers) {
    append_header(header);
  }

  if (content_length) {
    request.append(content_length_name);
    request.append(std::to_string(*content_length));
    request.append(line_end);
  } else if (chunked) {
    request.append("transfer-encoding: chunked\r\n");
  }
  request.append(keep_alive);
}

class ResponseParser {
 public:
  ResponseParser(Response& response, Pipe* destination,
                 size_t max_body_bytes, bool head_response,
                 bool capture_headers)
      : response_(response),
        destination_(destination),
        max_body_bytes_(max_body_bytes),
        captured_body_(destination == nullptr ? &response.body : nullptr),
        head_response_(head_response) {
    static const llhttp_settings_t capture_settings = [] {
      llhttp_settings_t settings;
      llhttp_settings_init(&settings);
      settings.on_header_field = on_header_field;
      settings.on_header_value = on_header_value;
      settings.on_header_value_complete = on_header_value_complete;
      settings.on_headers_complete = on_headers_complete;
      settings.on_body = on_body;
      settings.on_message_complete = on_message_complete;
      return settings;
    }();
    static const llhttp_settings_t discard_settings = [] {
      llhttp_settings_t settings;
      llhttp_settings_init(&settings);
      settings.on_headers_complete = on_headers_complete;
      settings.on_body = on_body;
      settings.on_message_complete = on_message_complete;
      return settings;
    }();
    if (capture_headers) {
      response_.headers.reserve(16);
    }
    llhttp_init(&parser_, HTTP_RESPONSE,
                capture_headers ? &capture_settings : &discard_settings);
    parser_.data = this;
    if (head_response) {
      parser_.method = HTTP_HEAD;
    }

  }

  ResponseParser(const ResponseParser&) = delete;
  ResponseParser& operator=(const ResponseParser&) = delete;

  size_t execute(std::span<const std::byte> bytes) {
    return execute(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  }

  void finish() {
    const llhttp_errno_t result = llhttp_finish(&parser_);
    check_result(result);
  }

  void resume() noexcept {
    llhttp_resume(&parser_);
  }

  [[nodiscard]] bool headers_complete() const noexcept {
    return headers_complete_;
  }

  [[nodiscard]] bool message_complete() const noexcept {
    return message_complete_;
  }

  [[nodiscard]] bool needs_eof() const noexcept {
    return llhttp_message_needs_eof(&parser_) != 0;
  }

  [[nodiscard]] bool should_keep_alive() const noexcept {
    return should_keep_alive_;
  }

  [[nodiscard]] std::optional<size_t> fixed_body_remaining() const {
    if ((parser_.flags & F_CONTENT_LENGTH) == 0 ||
        (parser_.flags & F_CHUNKED) != 0) {
      return std::nullopt;
    }
    if (parser_.content_length > SIZE_MAX) {
      throw std::runtime_error("HTTP/1.1 Content-Length exceeds size_t");
    }
    return static_cast<size_t>(parser_.content_length);
  }

 private:
  enum class HeaderPart { none, field, value };

  static ResponseParser& self(llhttp_t* parser) {
    return *static_cast<ResponseParser*>(parser->data);
  }

  static int on_header_field(llhttp_t* parser, const char* data,
                             size_t length) {
    auto& value = self(parser);
    if (value.header_part_ == HeaderPart::value) {
      value.commit_header();
    }
    value.current_header_name_.append(data, length);
    value.header_part_ = HeaderPart::field;
    return 0;
  }

  static int on_header_value(llhttp_t* parser, const char* data,
                             size_t length) {
    auto& value = self(parser);
    value.current_header_value_.append(data, length);
    value.header_part_ = HeaderPart::value;
    return 0;
  }

  static int on_headers_complete(llhttp_t* parser) {
    auto& value = self(parser);
    value.commit_header();
    value.response_.status = llhttp_get_status_code(parser);
    if (value.head_response_) {
      const bool http11 = parser->http_major > 0 && parser->http_minor > 0;
      value.should_keep_alive_ =
          http11 ? (parser->flags & F_CONNECTION_CLOSE) == 0
                 : (parser->flags & F_CONNECTION_KEEP_ALIVE) != 0;
    } else {
      value.should_keep_alive_ = llhttp_should_keep_alive(parser) != 0;
    }
    value.headers_complete_ = true;
    if (value.head_response_) {
      return 1;
    }
    if (value.response_.status >= 100 && value.response_.status < 200 &&
        value.response_.status != 101) {
      return 0;
    }
    llhttp_pause(parser);
    return 0;
  }

  static int on_header_value_complete(llhttp_t* parser) {
    self(parser).commit_header();
    return 0;
  }

  static int on_body(llhttp_t* parser, const char* data,
                     size_t length) {
    auto& value = self(parser);
    if (value.response_.body_bytes > value.max_body_bytes_ ||
        length > value.max_body_bytes_ - value.response_.body_bytes) {
      llhttp_set_error_reason(parser,
                              "HTTP/1.1 response exceeds body limit");
      return HPE_USER;
    }
    try {
      const std::span bytes(reinterpret_cast<const std::byte*>(data), length);
      if (value.captured_body_ != nullptr) {
        const size_t old_size = value.captured_body_->size();
        value.captured_body_->resize(old_size + length);
        memcpy(value.captured_body_->data() + old_size,
               bytes.data(), bytes.size());
      } else {
        write_all(value.destination_->write_fd(), bytes);
      }
      value.response_.fallback_copied_bytes += length;
      value.response_.body_bytes += length;
      return 0;
    } catch (...) {
      value.callback_error_ = std::current_exception();
      llhttp_set_error_reason(parser, "HTTP/1.1 body sink failed");
      return HPE_USER;
    }
  }

  static int on_message_complete(llhttp_t* parser) {
    auto& value = self(parser);
    value.message_complete_ = true;
    return value.response_.status >= 100 && value.response_.status < 200 &&
                   value.response_.status != 101
               ? HPE_PAUSED
               : 0;
  }

  void commit_header() {
    if (current_header_name_.empty()) {
      return;
    }
    ascii_lower_in_place(current_header_name_);
    response_.headers.insert_or_assign(std::move(current_header_name_),
                                       std::move(current_header_value_));
    current_header_name_.clear();
    current_header_value_.clear();
    header_part_ = HeaderPart::none;
  }

  size_t execute(const char* data, size_t length) {
    const llhttp_errno_t result = llhttp_execute(&parser_, data, length);
    if (result == HPE_PAUSED) {
      const char* const end = llhttp_get_error_pos(&parser_);
      if (end < data || end > data + length) {
        throw std::runtime_error("llhttp returned an invalid pause position");
      }
      return static_cast<size_t>(end - data);
    }
    check_result(result);
    return length;
  }

  void check_result(llhttp_errno_t result) {
    if (result == HPE_OK) {
      return;
    }
    if (callback_error_) {
      std::rethrow_exception(callback_error_);
    }
    const char* reason = llhttp_get_error_reason(&parser_);
    throw std::runtime_error(
        std::string("llhttp: ") + llhttp_errno_name(result) +
        (reason == nullptr ? std::string{} : std::string(": ") + reason));
  }

  Response& response_;
  Pipe* destination_;
  size_t max_body_bytes_;
  std::vector<std::byte>* captured_body_;
  llhttp_t parser_{};
  ssostr<32> current_header_name_;
  ssostr<32> current_header_value_;
  std::exception_ptr callback_error_;
  HeaderPart header_part_ = HeaderPart::none;
  bool headers_complete_ = false;
  bool message_complete_ = false;
  bool should_keep_alive_ = false;
  bool head_response_ = false;
};

class Http1Client final : public HttpClient {
 public:
  Http1Client(UniqueFd connected_socket, std::string request_authority,
              std::string reconnect_host = {}, uint16_t reconnect_port = 0,
              std::shared_ptr<TlsTunnel> connected_tunnel = {})
      : tunnel(std::move(connected_tunnel)),
        socket(std::move(connected_socket)),
        authority(std::move(request_authority)),
        host(std::move(reconnect_host)),
        port(reconnect_port),
        tls(tunnel != nullptr) {}

  Response get_range(std::string_view path, uint64_t offset,
                     size_t length, Pipe& destination,
                     std::span<const Header> headers,
                     bool capture_headers,
                     bool measure_transport) override;

  Response put_from_fd(std::string_view path,
                       std::span<const Header> headers, int source_fd,
                       uint64_t source_offset, size_t length) override;

  void begin_upload(std::string_view method, std::string_view path,
                    std::span<const Header> headers,
                    size_t content_length) override;

  void upload_bytes(std::span<const std::byte> bytes) override;

  void upload_from_fd(int fd, uint64_t offset, size_t length,
                      bool seek) override;

  Response finish_upload(size_t max_response_body) override;

  void cancel_upload() noexcept override;

  Response request_no_body(std::string_view method,
                           std::string_view path,
                           std::span<const Header> headers,
                           size_t max_response_body) override;

  void consume(const Response&) override {}

 private:
  std::shared_ptr<TlsTunnel> tunnel;
  UniqueFd socket;
  std::string authority;
  std::string host;
  std::string request_head;
  std::string upload_method;
  uint16_t port = 0;
  bool request_active = false;
  bool upload_chunked = false;
  size_t upload_sent = 0;
  size_t upload_length = kUnknownBodyLength;
  bool tls = false;

  void ensure_connected() {
    if (socket) {
      return;
    }
    if (host.empty() || port == 0) {
      throw std::runtime_error("HTTP/1.1 connection is not reusable");
    }
    if (tls) {
      TlsConnection transport = connect_tls(host, port);
      if (transport.http2) {
        throw std::runtime_error(
            "TLS reconnect changed protocol from HTTP/1.1");
      }
      tunnel = std::move(transport.tunnel);
      socket = std::move(transport.socket);
    } else {
      socket = connect_tcp(host, port);
    }
  }

  Response receive_response(std::string_view method, Pipe* destination,
                            size_t max_body_bytes, uint64_t wire_start_ns,
                            bool capture_headers,
                            bool measure_transport) {
    std::array<std::byte, kHttp1ReadSize> buf;
    size_t begin = 0;
    size_t end   = 0;

    for (;;) {
      Response response;
      response.wire_start_ns = wire_start_ns;
      ResponseParser parser(
          response, destination, max_body_bytes, method == "HEAD",
          capture_headers);

      size_t header_bytes = 0;
      while (!parser.headers_complete()) {
        if (begin == end) {
          if (header_bytes == kMaxResponseHeaderBytes) {
            throw std::runtime_error(
                "HTTP/1.1 response headers exceed 64 KiB");
          }
          const size_t room = kMaxResponseHeaderBytes - header_bytes;
          ssize_t n;
          do {
            n = ::recv(socket.get(), buf.data(),
                       std::min(buf.size(), room), 0);
          } while (n < 0 && errno == EINTR);
          if (n == 0) {
            throw std::system_error(
                std::make_error_code(std::errc::connection_reset),
                "HTTP/1.1 peer closed before response headers");
          }
          if (n < 0) {
            http1_throw_errno("recv(HTTP/1.1 header)");
          }
          begin = 0;
          end   = static_cast<size_t>(n);
        }

        const size_t before = response.body_bytes;
        const size_t n = parser.execute(
            std::span(buf).subspan(begin, end - begin));
        begin += n;
        header_bytes += n;
        if (begin == end) {
          begin = 0;
          end   = 0;
        }
        if (measure_transport && response.body_bytes != before) {
          response.wire_last_data_ns = http1_monotonic_ns();
        }
        if (n == 0 && !parser.headers_complete()) {
          throw std::runtime_error("llhttp made no response progress");
        }
      }

      if (response.status >= 100 && response.status < 200 &&
          response.status != 101) {
        if (!parser.message_complete()) {
          throw std::runtime_error("incomplete informational response");
        }
        continue;
      }

      bool response_complete = parser.message_complete();
      if (!response_complete) {
        const std::optional<size_t> fixed_remaining =
            parser.fixed_body_remaining();
        if (fixed_remaining) {
          size_t remaining = *fixed_remaining;
          const size_t buffered = end - begin;
          if (buffered > remaining ||
              remaining > max_body_bytes - response.body_bytes) {
            throw std::runtime_error(
                "HTTP/1.1 response exceeds requested body size");
          }
          if (buffered != 0) {
            const std::span bytes = std::span(buf).subspan(begin, buffered);
            if (destination == nullptr) {
              response.body.insert(response.body.end(),
                                   bytes.begin(), bytes.end());
            } else {
              write_all(destination->write_fd(), bytes);
            }
            response.body_bytes += buffered;
            response.fallback_copied_bytes += buffered;
            remaining -= buffered;
            begin = 0;
            end   = 0;
          }
          if (remaining != 0) {
            if (destination == nullptr) {
              const size_t old_size = response.body.size();
              response.body.resize(old_size + remaining);
              size_t received = 0;
              while (received != remaining) {
                ssize_t n;
                do {
                  n = ::recv(socket.get(),
                             response.body.data() + old_size + received,
                             remaining - received, 0);
                } while (n < 0 && errno == EINTR);
                if (n == 0) {
                  throw std::system_error(
                      std::make_error_code(std::errc::connection_reset),
                      "HTTP/1.1 peer closed during response body");
                }
                if (n < 0) {
                  http1_throw_errno("recv(HTTP/1.1 fixed body)");
                }
                received += size_t(n);
              }
              response.body_bytes += remaining;
              response.fallback_copied_bytes += remaining;
            } else {
              response.externally_spliced_bytes = splice_exact(
                  socket.get(), destination->write_fd(), remaining,
                  SPLICE_F_MOVE | SPLICE_F_MORE,
                  &response.transport_splice_calls);
              response.body_bytes += remaining;
            }
          }
          if (measure_transport && response.body_bytes != 0) {
            response.wire_last_data_ns = http1_monotonic_ns();
          }
          response_complete = true;
        } else {
          parser.resume();
          while (!parser.message_complete()) {
            if (begin == end) {
              ssize_t n;
              do {
                n = ::recv(socket.get(), buf.data(), buf.size(), 0);
              } while (n < 0 && errno == EINTR);
              if (n < 0) {
                http1_throw_errno("recv(HTTP/1.1 body)");
              }
              if (n == 0) {
                parser.finish();
                socket.reset();
                break;
              }
              begin = 0;
              end   = static_cast<size_t>(n);
            }

            const size_t before = response.body_bytes;
            const size_t n = parser.execute(
                std::span(buf).subspan(begin, end - begin));
            begin += n;
            if (begin == end) {
              begin = 0;
              end   = 0;
            }
            if (measure_transport && response.body_bytes != before) {
              response.wire_last_data_ns = http1_monotonic_ns();
            }
          }
          response_complete = parser.message_complete();
        }
      }

      if (!response_complete) {
        throw std::runtime_error("incomplete HTTP/1.1 response");
      }
      if (!parser.should_keep_alive()) {
        socket.reset();
      }
      return response;
    }
  }

  Response perform(std::string_view method, std::string_view path,
                         std::span<const Header> headers,
                         std::optional<size_t> content_length,
                         int source_fd, uint64_t source_offset,
                         Pipe* response_pipe,
                         size_t max_response_body,
                         bool capture_headers,
                         bool measure_transport,
                         std::span<const Header> generated_headers = {},
                         bool trusted_headers = false) {
    if (request_active) {
      throw std::logic_error("HTTP/1.1 connection already has a request");
    }
    request_active = true;
    try {
      ensure_connected();
      build_request_head(request_head, method, path, authority,
                         generated_headers, headers, content_length, false,
                         trusted_headers);
      const uint64_t wire_start_ns =
          measure_transport ? http1_monotonic_ns() : 0;
      send_all(
          socket.get(),
          std::span(reinterpret_cast<const std::byte*>(request_head.data()),
                    request_head.size()));

      size_t sent = 0;
      if (content_length && *content_length != 0) {
        uint64_t offset = source_offset;
        sent = sendfile_exact(socket.get(), source_fd, offset,
                                  *content_length);
      }
      Response response = receive_response(
          method, response_pipe, max_response_body, wire_start_ns,
          capture_headers, measure_transport);
      response.externally_sent_bytes = sent;
      request_active = false;
      return response;
    } catch (...) {
      request_active = false;
      socket.reset();
      throw;
    }
  }
};

Response Http1Client::get_range(
    std::string_view path, uint64_t offset, size_t length,
    Pipe& destination, std::span<const Header> extra_headers,
    bool capture_headers, bool measure_transport) {
  if (length == 0 || length > destination.capacity()) {
    throw std::invalid_argument("range must fit in the destination pipe");
  }
  const uint64_t inclusive_end =
      offset + static_cast<uint64_t>(length) - 1;
  if (inclusive_end < offset) {
    throw std::overflow_error("range end overflow");
  }

  const Header* supplied_range = nullptr;
  for (const Header& header : extra_headers) {
    if (ascii_equal(sso_view(header.name), "range")) {
      if (supplied_range != nullptr) {
        throw std::invalid_argument("duplicate GET range header");
      }
      supplied_range = &header;
    } else if (ascii_equal(sso_view(header.name), "accept-encoding")) {
      throw std::invalid_argument("GET header conflicts with generated header");
    }
  }

  const ssostr<32> range = supplied_range == nullptr
                           ? range_header_value(offset, inclusive_end)
                           : ssostr<32>{};
  const std::array generated_range_headers{
      Header{"range", range},
      Header{"accept-encoding", "identity"},
  };
  const std::array generated_accept_header{
      Header{"accept-encoding", "identity"},
  };
  const std::span<const Header> generated_headers =
      supplied_range == nullptr
          ? std::span<const Header>(generated_range_headers)
          : std::span<const Header>(generated_accept_header);

  return perform("GET", path, extra_headers, std::nullopt, -1, 0,
                 &destination, length, capture_headers,
                 measure_transport, generated_headers, true);
}

Response Http1Client::put_from_fd(
    std::string_view path, std::span<const Header> extra_headers,
    int source_fd, uint64_t source_offset, size_t length) {
  if (length != 0 && source_fd < 0) {
    throw std::invalid_argument("invalid PUT source fd");
  }
  for (const Header& header : extra_headers) {
    if (ascii_lower(header.name) == "content-length") {
      throw std::invalid_argument("PUT header conflicts with generated header");
    }
  }

  Response response = perform(
      "PUT", path, extra_headers, length, source_fd, source_offset,
      nullptr, kPreferredIoSize, true, false);
  return response;
}

void Http1Client::begin_upload(
    std::string_view method, std::string_view path,
    std::span<const Header> extra_headers, size_t content_length) {
  if (request_active) {
    throw std::logic_error("HTTP/1.1 connection already has a request");
  }
  request_active = true;
  try {
    ensure_connected();
    const bool fixed_length = content_length != kUnknownBodyLength;
    const std::optional<size_t> length = fixed_length
                                             ? std::optional(content_length)
                                             : std::nullopt;
    build_request_head(request_head, method, path, authority, {},
                       extra_headers, length, !fixed_length);
    send_all(
        socket.get(),
        std::span(reinterpret_cast<const std::byte*>(request_head.data()),
                  request_head.size()));
    upload_method.assign(method);
    upload_chunked = !fixed_length;
    upload_sent    = 0;
    upload_length  = content_length;
  } catch (...) {
    cancel_upload();
    throw;
  }
}

void send_chunk_head(int socket, size_t length) {
  std::array<char, sizeof(size_t) * 2 + 2> head;
  const auto result = std::to_chars(
      head.data(), head.data() + head.size() - 2, length, 16);
  if (result.ec != std::errc{}) {
    throw std::runtime_error("unable to format HTTP chunk length");
  }
  char* p = result.ptr;
  *p++ = '\r';
  *p++ = '\n';
  send_all(socket,
           std::span(reinterpret_cast<const std::byte*>(head.data()),
                     size_t(p - head.data())));
}

void Http1Client::upload_bytes(std::span<const std::byte> bytes) {
  if (bytes.empty()) {
    return;
  }
  if (!request_active || upload_method.empty()) {
    throw std::logic_error("HTTP/1.1 streaming upload is not active");
  }
  try {
    if (!upload_chunked && bytes.size() > upload_length - upload_sent) {
      throw std::runtime_error("HTTP/1.1 upload exceeds Content-Length");
    }
    if (upload_chunked) {
      send_chunk_head(socket.get(), bytes.size());
    }
    send_all(socket.get(), bytes);
    if (upload_chunked) {
      constexpr std::array end{std::byte{'\r'}, std::byte{'\n'}};
      send_all(socket.get(), end);
    }
    upload_sent += bytes.size();
  } catch (...) {
    cancel_upload();
    throw;
  }
}

void Http1Client::upload_from_fd(int fd, uint64_t offset, size_t length,
                                 bool seek) {
  if (length == 0) {
    return;
  }
  if (fd < 0) {
    throw std::invalid_argument("invalid streaming upload fd");
  }
  if (!request_active || upload_method.empty()) {
    throw std::logic_error("HTTP/1.1 streaming upload is not active");
  }
  try {
    if (!upload_chunked && length > upload_length - upload_sent) {
      throw std::runtime_error("HTTP/1.1 upload exceeds Content-Length");
    }
    if (upload_chunked) {
      send_chunk_head(socket.get(), length);
    }
    if (seek) {
      sendfile_exact(socket.get(), fd, offset, length);
    } else {
      splice_exact(fd, socket.get(), length,
                   SPLICE_F_MOVE | SPLICE_F_MORE);
    }
    if (upload_chunked) {
      constexpr std::array end{std::byte{'\r'}, std::byte{'\n'}};
      send_all(socket.get(), end);
    }
    upload_sent += length;
  } catch (...) {
    cancel_upload();
    throw;
  }
}

Response Http1Client::finish_upload(size_t max_response_body) {
  if (!request_active || upload_method.empty()) {
    throw std::logic_error("HTTP/1.1 streaming upload is not active");
  }
  if (max_response_body == 0) {
    throw std::invalid_argument("streaming response buffer is empty");
  }
  try {
    if (!upload_chunked && upload_sent != upload_length) {
      throw std::runtime_error("HTTP/1.1 upload ended before Content-Length");
    }
    if (upload_chunked) {
      constexpr std::array end{
          std::byte{'0'}, std::byte{'\r'}, std::byte{'\n'},
          std::byte{'\r'}, std::byte{'\n'}};
      send_all(socket.get(), end);
    }
    Response response = receive_response(
        upload_method, nullptr, max_response_body, 0, true, false);
    response.externally_sent_bytes = upload_sent;
    request_active = false;
    upload_chunked = false;
    upload_method.clear();
    upload_sent   = 0;
    upload_length = kUnknownBodyLength;
    return response;
  } catch (...) {
    cancel_upload();
    throw;
  }
}

void Http1Client::cancel_upload() noexcept {
  request_active = false;
  upload_chunked = false;
  upload_method.clear();
  upload_sent   = 0;
  upload_length = kUnknownBodyLength;
  socket.reset();
}

Response Http1Client::request_no_body(
    std::string_view method, std::string_view path,
    std::span<const Header> extra_headers,
    size_t max_response_body) {
  if (method.empty() || max_response_body == 0) {
    throw std::invalid_argument("invalid bodyless HTTP/1.1 request");
  }
  const bool needs_content_length =
      method == "PUT" || method == "POST" || method == "PATCH";
  Response response = perform(
      method, path, extra_headers,
      needs_content_length ? std::optional<size_t>(0) : std::nullopt,
      -1, 0, nullptr, max_response_body, true, false);
  return response;
}

// HTTP/2-preferred protocol selection.
std::unique_ptr<HttpClient> HttpClient::connect(
    std::string_view host, uint16_t port, std::string authority, bool tls) {
  if (authority.empty()) {
    authority = std::string(host) + ':' + std::to_string(port);
  }

  if (tls) {
    TlsConnection transport = connect_tls(host, port);
    if (transport.http2) {
      return std::make_unique<Http2Client>(
          std::move(transport.socket), authority, std::string(host), port,
          std::move(transport.tunnel));
    }
    return std::make_unique<Http1Client>(
        std::move(transport.socket), std::move(authority), std::string(host),
        port, std::move(transport.tunnel));
  }

  auto h2 = std::make_unique<Http2Client>(
      connect_tcp(host, port), authority, std::string(host), port);
  try {
    if (h2->probe_server()) {
      return h2;
    }
  } catch (const std::system_error&) {
    // A HTTP/1.1-only peer may reset the connection as soon as it sees the
    // HTTP/2 preface. Reconnect below and use HTTP/1.1.
  }
  return std::make_unique<Http1Client>(
      connect_tcp(host, port), std::move(authority), std::string(host), port);
}
