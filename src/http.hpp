#pragma once

#include "io.hpp"

#include <nghttp2/nghttp2.h>
#include <terark/gold_hash_map.hpp>
#include <terark/sso.hpp>

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

// Shared HTTP values.
template<size_t SSOCap>
using ssostr = terark::minimal_sso<SSOCap>;

template<size_t SSOCap>
inline std::string_view sso_view(const ssostr<SSOCap>& s) noexcept {
  return std::string_view(s.data(), s.size());
}

struct Response {
  int stream_id = -1;
  int status = 0;
  size_t body_bytes = 0;
  size_t externally_spliced_bytes = 0;
  size_t fallback_copied_bytes = 0;
  size_t externally_sent_bytes = 0;
  size_t transport_splice_calls = 0;
  uint64_t wire_start_ns = 0;
  uint64_t wire_last_data_ns = 0;
  bool requires_consume = false;
  bool low_water_used = false;
  terark::gold_hash_map<ssostr<32>, ssostr<32>> headers;
  std::vector<std::byte> body;
};

struct Header {
  ssostr<32> name;
  ssostr<32> value;
};

// HTTP/2 zero-copy ingress and client.
struct ReceiveResult {
  nghttp2_frame_hd header{};
  size_t spliced_payload = 0;
  size_t copied_payload = 0;
};

inline constexpr size_t kDefaultShadowSpanSize = 256U * 1024U;
inline constexpr size_t kUnknownBodyLength = SIZE_MAX;

bool http1_low_water_preflight(std::string& error) noexcept;

class ExternalDataIngress {
 public:
  explicit ExternalDataIngress(
      size_t shadow_span_bytes = kDefaultShadowSpanSize);
  ~ExternalDataIngress();

  ExternalDataIngress(const ExternalDataIngress&) = delete;
  ExternalDataIngress& operator=(const ExternalDataIngress&) = delete;
  ExternalDataIngress(ExternalDataIngress&&) = delete;
  ExternalDataIngress& operator=(ExternalDataIngress&&) = delete;

  ReceiveResult receive_one(nghttp2_session* session, int socket_fd,
                            int data_pipe_write_fd,
                            size_t max_payload_bytes,
                            int32_t expected_stream_id,
                            std::span<const std::byte> frame_prefix = {});

  bool capture_frame_header(const nghttp2_frame_hd& header) noexcept;
  void reset() noexcept;

  [[nodiscard]] bool shadow_advance_active() const noexcept {
    return shadow_advance_active_;
  }

 private:
  void advance_shadow_payload(nghttp2_session* session,
                              size_t payload_bytes);

  const uint8_t* shadow_span_ = nullptr;
  size_t shadow_span_bytes_ = 0;
  std::vector<std::byte> payload_;
  bool shadow_advance_active_ = false;
  nghttp2_frame_hd header_{};
  bool header_ready_ = false;
};

using RangeResponse = Response;

class HttpClient {
 public:
  static std::unique_ptr<HttpClient> connect(
      std::string_view host, uint16_t port,
      std::string authority = {}, bool tls = false,
      int io_timeout_ms = kRequestIoTimeoutMs,
      int connect_timeout_ms = kConnectTimeoutMs,
      int probe_timeout_ms = kProtocolProbeTimeoutMs,
      bool http1_low_water_available = true);

  virtual ~HttpClient() = default;

  HttpClient(const HttpClient&) = delete;
  HttpClient& operator=(const HttpClient&) = delete;

  virtual Response get_range(
      std::string_view path, uint64_t offset, size_t length,
      Pipe& destination, std::span<const Header> headers = {},
      bool capture_headers = true,
      bool measure_transport = false) = 0;

  virtual Response put_from_fd(
      std::string_view path, std::span<const Header> headers,
      int source_fd, uint64_t source_offset, size_t length) = 0;

  virtual void begin_upload(
      std::string_view method, std::string_view path,
      std::span<const Header> headers = {},
      size_t content_length = kUnknownBodyLength) = 0;

  virtual void upload_bytes(std::span<const std::byte> bytes) = 0;

  virtual void upload_from_fd(int fd, uint64_t offset, size_t length,
                              bool seek) = 0;

  virtual Response finish_upload(
      size_t max_response_body = kPreferredIoSize) = 0;

  virtual void cancel_upload() noexcept = 0;

  virtual Response request_no_body(
      std::string_view method, std::string_view path,
      std::span<const Header> headers = {},
      size_t max_response_body = kPreferredIoSize) = 0;

  virtual void consume(const Response& response) = 0;

 protected:
  HttpClient() = default;
  HttpClient(HttpClient&&) noexcept = default;
  HttpClient& operator=(HttpClient&&) noexcept = default;
};
