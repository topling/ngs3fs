#include "io.hpp"
#include "s3.hpp"

#include <nghttp2/nghttp2.h>

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <errno.h>
#include <charconv>
#include <chrono>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <exception>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>


[[noreturn]] void fail_errno(const char* operation) {
  throw std::system_error(errno, std::generic_category(), operation);
}

void require(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error(std::string(message));
  }
}

template <class Operation>
void retry_after_fuse_release(const char* name, Operation&& operation) {
  constexpr size_t kMaxAttempts = 200;
  for (size_t attempt = 0; attempt < kMaxAttempts; ++attempt) {
    if (operation() == 0) {
      return;
    }
    const int error = errno;
    if (error != EBUSY || attempt + 1 == kMaxAttempts) {
      errno = error;
      fail_errno(name);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
}

void test_write_all(int fd, std::span<const std::byte> bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t count =
        ::write(fd, bytes.data() + offset, bytes.size() - offset);
    if (count > 0) {
      offset += static_cast<size_t>(count);
    } else if (count < 0 && errno == EINTR) {
      continue;
    } else {
      fail_errno("write");
    }
  }
}

void pread_all(int fd, std::span<std::byte> bytes, uint64_t file_offset) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t count = ::pread(
        fd, bytes.data() + offset, bytes.size() - offset,
        static_cast<off_t>(file_offset + offset));
    if (count > 0) {
      offset += static_cast<size_t>(count);
    } else if (count < 0 && errno == EINTR) {
      continue;
    } else if (count == 0) {
      throw std::runtime_error("pread reached unexpected EOF");
    } else {
      fail_errno("pread");
    }
  }
}

struct SessionDeleter {
  void operator()(nghttp2_session* value) const {
    nghttp2_session_del(value);
  }
};

struct CallbacksDeleter {
  void operator()(nghttp2_session_callbacks* value) const {
    nghttp2_session_callbacks_del(value);
  }
};

nghttp2_nv header(std::string_view name, std::string_view value) {
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

struct RequestRange {
  uint64_t first = 0;
  uint64_t last = 0;
  bool valid = false;
};

struct Request {
  bool active_get = false;
  std::string method;
  std::string path;
  std::string rename_source;
  std::string rename_source_if_match;
  std::string copy_source;
  std::string copy_source_if_match;
  std::string if_match;
  std::string content_length;
  std::string checksum_algorithm;
  std::string checksum_type;
  std::string checksum_value;
  std::string checksum_mode;
  std::string object_attributes;
  std::string max_parts;
  std::string part_number_marker;
  std::string multipart_object_size;
  std::string write_id;
  std::string expected_bucket_owner;
  std::string request_payer;
  std::string authorization;
  RequestRange range;
  std::vector<std::byte> body;
};

struct ResponseSource {
  const std::vector<std::byte>* object = nullptr;
  std::shared_ptr<struct SpecialObject> keepalive;
  std::vector<std::byte> body;
  size_t cursor = 0;
  size_t end = 0;
  size_t begin = 0;
  uint64_t ttfb_ms = 100;
  uint64_t bytes_per_second = 0;
  std::chrono::steady_clock::time_point payload_started;
  std::string content_length;
  std::string content_range;
  bool delay = false;
  bool delayed = false;
};

struct SpecialObject {
  std::vector<std::byte> bytes;
  std::vector<std::byte> corrupted_bytes;
  std::string etag;
  std::string version_id;
  std::string last_modified;
  std::string last_modified_iso;
  int corrupt_gets_remaining = 0;
  int get_requests           = 0;
  int attributes_requests    = 0;
  size_t checksum_part_size  = 0;
  bool attributes_unsupported = false;
  bool invalid_content_range = false;
  size_t pause_after = 0;
  std::atomic<bool> tail_paused{false};
  std::atomic<bool> resume_tail{false};
  std::atomic<size_t> send_limit{SIZE_MAX};
  size_t limited_range_end = SIZE_MAX;
  std::vector<RequestRange> get_ranges;
  std::vector<std::string> get_paths;
};

struct SharedServerState {
  std::mutex mutex;
  uint64_t get_ttfb_ms = 100;
  uint64_t get_bytes_per_second = 0;
  ChecksumAlgorithm checksum = CHECKSUM_XXHASH128;
  std::vector<std::byte> object;
  std::string object_key = "mmap.bin";
  std::string etag = "\"before-put\"";
  std::string version_id = "version-1";
  std::string last_modified = "Sun, 06 Nov 1994 08:49:37 GMT";
  std::string last_modified_iso = "1994-11-06T08:49:37.000Z";
  std::string write_id;
  int rename_attempts = 0;
  int rename_probe_attempts = 0;
  std::string hidden_key;
  int active_gets = 0;
  int maximum_active_gets = 0;
  int get_requests = 0;
  std::vector<std::string> get_paths;
  int head_requests = 0;
  int list_requests = 0;
  int put_requests = 0;
  int create_multipart_requests = 0;
  int upload_part_requests = 0;
  int complete_multipart_requests = 0;
  int checksum_mode_requests = 0;
  int object_attributes_requests = 0;
  int request_timeout_errors = 0;
  int complete_embedded_errors = 0;
  int copy_embedded_errors = 0;
  std::map<unsigned, std::vector<std::byte>> uploaded_parts;
  std::map<unsigned, unsigned> upload_attempts;
  std::map<std::string, std::shared_ptr<SpecialObject>> special_objects;
  std::string overwrite_hidden_key;
  size_t overwrite_destination_size = 0;
  size_t overwrite_destination_max_read_end = 0;
  int overwrite_destination_gets = 0;
  bool overwrite_rename_committed = false;
  bool copy_completed = false;
  bool delete_completed = false;
  bool active_writer_delete_completed = false;
  bool hidden_rename_completed = false;
  bool hidden_delete_completed = false;
  bool deep_present = true;
  bool drop_complete_response_once = false;
  bool complete_response_dropped = false;
  bool inject_request_timeout = true;
  bool inject_complete_slowdown = true;
  bool inject_copy_slowdown = true;
  std::atomic<bool> stop = false;
  std::exception_ptr failure;
};

std::string request_object_key(std::string_view path) {
  constexpr std::string_view prefix = "/bucket/";
  require(path.starts_with(prefix), "request used an unexpected bucket");
  path.remove_prefix(prefix.size());
  const size_t query = path.find('?');
  if (query != std::string_view::npos) {
    path = path.substr(0, query);
  }
  return std::string(path);
}

struct ServerState {
  SharedServerState* shared = nullptr;
  std::map<int32_t, Request> requests;
  std::map<int32_t, std::unique_ptr<ResponseSource>> responses;
  bool close_without_response = false;

  ~ServerState() {
    // nghttp2_session_del does not close streams through on_stream_close.
    // A cancelled speculative GET can terminate its entire connection.
    std::lock_guard guard(shared->mutex);
    for (const auto& [id, request] : requests) {
      (void)id;
      if (request.active_get) --shared->active_gets;
    }
  }
};

bool parse_range(std::string_view value, RequestRange& range) {
  constexpr std::string_view prefix = "bytes=";
  if (!value.starts_with(prefix)) {
    return false;
  }
  value.remove_prefix(prefix.size());
  const size_t dash = value.find('-');
  if (dash == std::string_view::npos) {
    return false;
  }
  const auto first = std::from_chars(value.data(), value.data() + dash,
                                     range.first);
  const auto last = std::from_chars(value.data() + dash + 1,
                                    value.data() + value.size(), range.last);
  range.valid = first.ec == std::errc{} &&
                first.ptr == value.data() + dash &&
                last.ec == std::errc{} &&
                last.ptr == value.data() + value.size() &&
                range.first <= range.last;
  return range.valid;
}

int on_begin_headers(nghttp2_session*, const nghttp2_frame* frame,
                     void* user_data) {
  if (frame->hd.type == NGHTTP2_HEADERS &&
      frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id] = {};
  }
  return 0;
}

int on_header(nghttp2_session*, const nghttp2_frame* frame,
              const uint8_t* name, size_t name_length,
              const uint8_t* value, size_t value_length,
              uint8_t, void* user_data) {
  if (frame->hd.type != NGHTTP2_HEADERS ||
      frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
    return 0;
  }
  const std::string_view header_name(
      reinterpret_cast<const char*>(name), name_length);
  if (header_name == "range") {
    auto& state = *static_cast<ServerState*>(user_data);
    auto& range = state.requests[frame->hd.stream_id].range;
    parse_range(std::string_view(reinterpret_cast<const char*>(value),
                                 value_length),
                range);
  } else if (header_name == ":method") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].method.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == ":path") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].path.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-rename-source") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].rename_source.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-rename-source-if-match") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].rename_source_if_match.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-copy-source") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].copy_source.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-copy-source-if-match") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].copy_source_if_match.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "if-match") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].if_match.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "content-length") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].content_length.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-checksum-algorithm") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].checksum_algorithm.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-checksum-type") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].checksum_type.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-checksum-mode") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].checksum_mode.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-object-attributes") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].object_attributes.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-max-parts") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].max_parts.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-part-number-marker") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].part_number_marker.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-meta-ngs3fs-write-id") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].write_id.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-expected-bucket-owner") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].expected_bucket_owner.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-request-payer" ||
             header_name == "x-oss-request-payer") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].request_payer.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "authorization") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].authorization.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name.starts_with("x-amz-checksum-")) {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].checksum_value.assign(
        reinterpret_cast<const char*>(value), value_length);
  } else if (header_name == "x-amz-mp-object-size") {
    auto& state = *static_cast<ServerState*>(user_data);
    state.requests[frame->hd.stream_id].multipart_object_size.assign(
        reinterpret_cast<const char*>(value), value_length);
  }
  return 0;
}

int on_data_chunk(nghttp2_session*, uint8_t, int32_t stream_id,
                  const uint8_t* data, size_t length,
                  void* user_data) {
  auto& request =
      static_cast<ServerState*>(user_data)->requests[stream_id];
  const size_t old_size = request.body.size();
  request.body.resize(old_size + length);
  memcpy(request.body.data() + old_size, data, length);
  return 0;
}

ssize_t read_body(nghttp2_session*, int32_t, uint8_t* buffer,
                  size_t length, uint32_t* flags,
                  nghttp2_data_source* source, void*) {
  auto& response = *static_cast<ResponseSource*>(source->ptr);
  if (response.delay && !response.delayed) {
    response.delayed = true;
    // One first-body-byte delay per GET, never one delay per DATA frame or block.
    std::this_thread::sleep_for(std::chrono::milliseconds(response.ttfb_ms));
    response.payload_started = std::chrono::steady_clock::now();
  }
  if (response.keepalive && response.keepalive->pause_after != 0 &&
      response.cursor >= response.keepalive->pause_after) {
    response.keepalive->tail_paused.store(true, std::memory_order_release);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!response.keepalive->resume_tail.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  size_t count =
      std::min(length, response.end - response.cursor);
  if (response.keepalive && response.cursor < response.keepalive->pause_after) {
    count = std::min(count, response.keepalive->pause_after - response.cursor);
  }
  if (response.keepalive && response.end <= response.keepalive->limited_range_end) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    size_t limit = response.keepalive->send_limit.load(std::memory_order_acquire);
    while (response.cursor >= limit && std::chrono::steady_clock::now() < deadline) {
      response.keepalive->tail_paused.store(true, std::memory_order_release);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      limit = response.keepalive->send_limit.load(std::memory_order_acquire);
    }
    if (response.cursor < limit) count = std::min(count, limit - response.cursor);
  }
  if (count != 0) {
    if (response.delay && response.bytes_per_second != 0) {
      const double seconds = double(response.cursor + count - response.begin) /
                             double(response.bytes_per_second);
      std::this_thread::sleep_until(response.payload_started +
          std::chrono::duration_cast<std::chrono::steady_clock::duration>(
              std::chrono::duration<double>(seconds)));
    }
    memcpy(buffer, response.object->data() + response.cursor, count);
  }
  response.cursor += count;
  if (response.cursor == response.end) {
    *flags |= NGHTTP2_DATA_FLAG_EOF;
  }
  return static_cast<ssize_t>(count);
}

int submit_text_response(nghttp2_session* session, ServerState& connection,
                         int32_t stream_id, std::string_view status,
                         std::string_view text,
                         std::string_view etag = {},
                         std::string_view version = {},
                         std::string_view checksum_algorithm = {},
                         std::string_view checksum_type = {},
                         std::string_view checksum_name = {},
                         std::string_view checksum_value = {}) {
  auto response = std::make_unique<ResponseSource>();
  response->body.resize(text.size());
  if (!text.empty()) {
    memcpy(response->body.data(), text.data(), text.size());
  }
  response->object = &response->body;
  response->end = response->body.size();
  response->content_length = std::to_string(response->body.size());
  ResponseSource* source = response.get();
  connection.responses[stream_id] = std::move(response);
  std::vector<nghttp2_nv> headers;
  headers.reserve(8);
  headers.push_back(header(":status", status));
  headers.push_back(header("content-length", source->content_length));
  headers.push_back(header("content-type", "application/xml"));
  if (!etag.empty()) {
    headers.push_back(header("etag", etag));
  }
  if (!version.empty()) {
    headers.push_back(header("x-amz-version-id", version));
  }
  if (!checksum_algorithm.empty()) {
    headers.push_back(header("x-amz-checksum-algorithm",
                             checksum_algorithm));
  }
  if (!checksum_type.empty()) {
    headers.push_back(header("x-amz-checksum-type", checksum_type));
  }
  if (!checksum_name.empty()) {
    headers.push_back(header(checksum_name, checksum_value));
  }
  nghttp2_data_provider provider{
      .source = {.ptr = source},
      .read_callback = read_body,
  };
  return nghttp2_submit_response(session, stream_id, headers.data(),
                                 headers.size(), &provider);
}

unsigned request_part_number(std::string_view path) {
  constexpr std::string_view name = "partNumber=";
  const size_t begin = path.find(name);
  require(begin != std::string_view::npos,
          "UploadPart omitted partNumber");
  const char* first = path.data() + begin + name.size();
  const char* last = path.data() + path.size();
  const char* ampersand = std::find(first, last, '&');
  unsigned number = 0;
  const auto parsed = std::from_chars(first, ampersand, number);
  require(parsed.ec == std::errc{} && parsed.ptr == ampersand && number != 0,
          "UploadPart used an invalid partNumber");
  return number;
}

std::string encoded_checksum(ChecksumAlgorithm algorithm,
                             std::span<const std::byte> body) {
  DataChecksum checksum(algorithm);
  checksum.update(body);
  const ChecksumValue value = checksum.finish();
  return std::string(value.base64.data(), value.base64.size());
}

std::string response_checksum(ChecksumAlgorithm algorithm,
                              std::span<const std::byte> body) {
  DataChecksum checksum(algorithm);
  checksum.update(body);
  const ChecksumValue value = checksum.finish();
  if (algorithm == CHECKSUM_CRC64XZ) {
    return std::to_string(value.integer);
  }
  return std::string(value.base64.data(), value.base64.size());
}

std::string_view version_header(ChecksumAlgorithm algorithm) {
  return algorithm == CHECKSUM_CRC64XZ
             ? "x-oss-version-id"
             : "x-amz-version-id";
}

int on_frame_recv(nghttp2_session* session, const nghttp2_frame* frame,
                  void* user_data) {
  if (frame->hd.type != NGHTTP2_HEADERS &&
      frame->hd.type != NGHTTP2_DATA) {
    return 0;
  }
  if ((frame->hd.flags & NGHTTP2_FLAG_END_STREAM) == 0) {
    return 0;
  }

  auto& connection = *static_cast<ServerState*>(user_data);
  Request& request = connection.requests[frame->hd.stream_id];
  SharedServerState& state = *connection.shared;
  std::lock_guard state_guard(state.mutex);
  require(request.expected_bucket_owner == "111122223333",
          "request omitted x-amz-expected-bucket-owner");
  require(request.request_payer == "requester",
          "request omitted provider requester-pays header");
  const std::string_view payer =
      state.checksum == CHECKSUM_CRC64XZ
          ? "x-oss-request-payer"
          : "x-amz-request-payer";
  require(request.authorization.find("x-amz-expected-bucket-owner") !=
              std::string::npos &&
              request.authorization.find(payer) != std::string::npos,
          "mount-wide S3 headers were not included in SigV4 SignedHeaders");
  if (request.method == "POST" && frame->hd.type == NGHTTP2_HEADERS &&
      request.path.ends_with("?uploads=")) {
    require(request.checksum_algorithm == checksum_s3_name(state.checksum),
            "CreateMultipartUpload used the wrong checksum algorithm");
    require(request.checksum_type == checksum_multipart_type(state.checksum),
            "CreateMultipartUpload used the wrong checksum type");
    state.write_id = request.write_id;
    ++state.create_multipart_requests;
    const int submitted = submit_text_response(
        session, connection, frame->hd.stream_id, "200",
        "<s3:InitiateMultipartUploadResult xmlns:s3=\"urn:s3\">"
        "<s3:UploadId>upload&#x2D;1</s3:UploadId>"
        "</s3:InitiateMultipartUploadResult>", {}, {},
        checksum_s3_name(state.checksum),
        checksum_multipart_type(state.checksum));
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "PUT" && frame->hd.type == NGHTTP2_DATA &&
      request.path.find("partNumber=") != std::string::npos) {
    require(request.path.find("uploadId=upload-1") != std::string::npos,
            "UploadPart used the wrong upload ID");
    require(request.content_length == std::to_string(request.body.size()),
            "UploadPart omitted an exact Content-Length");
    const std::string expected_request_checksum =
        checksum_is_s3(state.checksum)
            ? encoded_checksum(state.checksum, request.body)
            : std::string{};
    require(request.checksum_value == expected_request_checksum,
            "UploadPart sent an invalid checksum");
    const unsigned number = request_part_number(request.path);
    const unsigned attempt = ++state.upload_attempts[number];
    if (number == 1 && attempt == 1) {
      const std::array retry_headers{
          header(":status", "500"),
          header("content-length", "0"),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, retry_headers.data(),
          retry_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    state.uploaded_parts[number] = std::move(request.body);
    ++state.upload_part_requests;
    const std::string etag = "\"part-" + std::to_string(number) + "\"";
    const std::string checksum = response_checksum(
        state.checksum, state.uploaded_parts[number]);
    const std::array response_headers{
        header(":status", "200"),
        header("content-length", "0"),
        header("etag", etag),
        header(checksum_header_name(state.checksum),
               checksum),
    };
    const int submitted = nghttp2_submit_response(
        session, frame->hd.stream_id, response_headers.data(),
        response_headers.size(), nullptr);
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "POST" && frame->hd.type == NGHTTP2_DATA &&
      request.path.find("uploadId=upload-1") != std::string::npos) {
    require(request.content_length == std::to_string(request.body.size()),
            "CompleteMultipartUpload omitted an exact Content-Length");
    if (state.inject_complete_slowdown) {
      state.inject_complete_slowdown = false;
      ++state.complete_embedded_errors;
      const int submitted = submit_text_response(
          session, connection, frame->hd.stream_id, "200",
          "<s3:Error xmlns:s3=\"urn:s3\">"
          "<s3:Code>SlowDown</s3:Code>"
          "<s3:Message>retry completion</s3:Message>"
          "<s3:RequestId>complete-retry</s3:RequestId></s3:Error>");
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (state.complete_response_dropped && state.uploaded_parts.empty()) {
      const std::array response_headers{
          header(":status", "404"),
          header("content-length", "0"),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, response_headers.data(),
          response_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    require(!state.uploaded_parts.empty(),
            "CompleteMultipartUpload had no uploaded parts");
    const std::string_view xml(
        reinterpret_cast<const char*>(request.body.data()),
        request.body.size());
    const std::string checksum_tag =
        "<" + std::string(checksum_xml_name(state.checksum)) + ">";
    if (checksum_multipart_type(state.checksum) == "COMPOSITE") {
      require(xml.find(checksum_tag) != std::string_view::npos,
              "CompleteMultipartUpload omitted part checksums");
    } else {
      require(xml.find(checksum_tag) == std::string_view::npos,
              "FULL_OBJECT completion included part checksums");
    }
    state.object.clear();
    for (auto& [number, part] : state.uploaded_parts) {
      require(number != 0, "invalid stored multipart part number");
      state.object.insert(state.object.end(), part.begin(), part.end());
    }
    const std::string complete_checksum =
        response_checksum(state.checksum, state.object);
    const bool full_checksum =
        checksum_multipart_type(state.checksum) == "FULL_OBJECT";
    if (full_checksum) {
      require(request.checksum_value == complete_checksum,
              "CompleteMultipartUpload sent an invalid full checksum");
      require(request.multipart_object_size ==
                  std::to_string(state.object.size()),
              "CompleteMultipartUpload omitted x-amz-mp-object-size");
    }
    state.uploaded_parts.clear();
    state.etag = "\"after-put\"";
    state.version_id = "version-2";
    state.last_modified = "Tue, 08 Nov 1994 08:49:37 GMT";
    state.last_modified_iso = "1994-11-08T08:49:37.000Z";
    ++state.complete_multipart_requests;
    ++state.put_requests;
    if (state.drop_complete_response_once) {
      state.drop_complete_response_once = false;
      state.complete_response_dropped   = true;
      connection.close_without_response = true;
      return 0;
    }
    std::string result =
        "<s3:CompleteMultipartUploadResult xmlns:s3=\"urn:s3\">"
        "<s3:ETag>&quot;after-put&quot;</s3:ETag>";
    if (full_checksum) {
      result += "<s3:";
      result += checksum_xml_name(state.checksum);
      result += ">";
      result += complete_checksum;
      result += "</s3:";
      result += checksum_xml_name(state.checksum);
      result += ">";
    }
    result += "</s3:CompleteMultipartUploadResult>";
    const int submitted = submit_text_response(
        session, connection, frame->hd.stream_id, "200",
        result, state.etag, state.version_id, {}, {},
        state.checksum == CHECKSUM_CRC64XZ
            ? checksum_header_name(state.checksum)
            : std::string_view{},
        state.checksum == CHECKSUM_CRC64XZ
            ? std::string_view(complete_checksum)
            : std::string_view{});
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "PUT" && frame->hd.type == NGHTTP2_HEADERS &&
      request.path.ends_with("?renameObject")) {
    require(!request.rename_source.empty(),
            "RenameObject omitted x-amz-rename-source");
    if (request.path.find(".ngs3fs-rename-probe-") != std::string::npos) {
      require(request.rename_source.ends_with(".missing"),
              "RenameObject probe source should not exist");
      ++state.rename_probe_attempts;
      const int submitted = submit_text_response(
          session, connection, frame->hd.stream_id, "404",
          "<s3:Error xmlns:s3=\"urn:s3\">"
          "<s3:Code>NoSuchKey</s3:Code></s3:Error>");
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    const size_t query = request.path.find('?');
    require(query != std::string::npos,
            "RenameObject request omitted its query");
    const std::string_view destination(request.path.data(), query);
    const std::string source_key = request_object_key(request.rename_source);
    const std::string destination_key = request_object_key(destination);
    const auto source_object = state.special_objects.find(source_key);
    const std::string_view expected_etag = source_object ==
            state.special_objects.end()
        ? std::string_view(state.etag)
        : std::string_view(source_object->second->etag);
    require(request.rename_source_if_match == expected_etag,
            "RenameObject omitted the pinned source ETag");
    if (source_key == "overwrite-dest.bin" &&
        destination_key.starts_with(".~ngs3fs~.pending-delete/")) {
      require(source_object != state.special_objects.end(),
              "native overwrite hide source was not present");
      state.overwrite_hidden_key = destination_key;
      state.special_objects.emplace(
          destination_key, std::move(source_object->second));
      state.special_objects.erase(source_key);
      state.overwrite_destination_size =
          state.special_objects.at(destination_key)->bytes.size();
      const std::array response_headers{
          header(":status", "200"),
          header("content-length", "0"),
      };
      state.hidden_rename_completed = true;
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, response_headers.data(),
          response_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (source_key == "overwrite-source.bin" &&
        destination_key == "overwrite-dest.bin") {
      require(source_object != state.special_objects.end(),
              "native overwrite source was not present");
      state.special_objects[destination_key] =
          std::move(source_object->second);
      state.special_objects.erase(source_key);
      state.overwrite_rename_committed = true;
      const std::array response_headers{
          header(":status", "200"),
          header("content-length", "0"),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, response_headers.data(),
          response_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (source_key == "checksum-rename.bin" &&
        destination_key == "checksum-renamed.bin") {
      require(source_object != state.special_objects.end(),
              "checksum rename source was not present");
      state.special_objects.emplace(
          destination_key, std::move(source_object->second));
      state.special_objects.erase(source_key);
      const std::array response_headers{
          header(":status", "200"),
          header("content-length", "0"),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, response_headers.data(),
          response_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (request.rename_source == "/bucket/unlink-open.bin") {
      require(destination.starts_with(
                  "/bucket/.~ngs3fs~.pending-delete/"),
              "native unlink did not move the reader to the private key");
      state.hidden_key.assign(destination.substr(std::string_view(
          "/bucket/").size()));
      state.hidden_rename_completed = true;
      state.object_key = state.hidden_key;
      const std::array response_headers{
          header(":status", "200"),
          header("content-length", "0"),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, response_headers.data(),
          response_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (request.rename_source == "/bucket/mmap.bin") {
      require(destination == "/bucket/renamed.bin",
              "RenameObject used the wrong destination for mmap.bin");
      ++state.rename_attempts;
    } else if (request.rename_source == "/bucket/renamed.bin") {
      require(destination == "/bucket/copied.bin",
              "RenameObject used the wrong destination for renamed.bin");
      ++state.rename_attempts;
      const std::array unsupported_headers{
          header(":status", "501"),
          header("content-length", "0"),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, unsupported_headers.data(),
          unsupported_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    } else {
      throw std::runtime_error("RenameObject used an unexpected source");
    }
    require(destination == "/bucket/renamed.bin",
            "RenameObject source was encoded twice or changed");
    state.object_key = "renamed.bin";
    const std::array response_headers{
        header(":status", "200"),
        header("content-length", "0"),
    };
    const int submitted = nghttp2_submit_response(
        session, frame->hd.stream_id, response_headers.data(),
        response_headers.size(), nullptr);
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "PUT" && frame->hd.type == NGHTTP2_HEADERS &&
      !request.copy_source.empty()) {
    require(request.path == "/bucket/copied.bin",
            "CopyObject used the wrong destination");
    require(request.copy_source ==
                "/bucket/renamed.bin?versionId=version-2",
            "CopyObject source omitted the pinned version or was encoded "
            "twice");
    require(request.copy_source_if_match == state.etag,
            "CopyObject omitted the pinned source ETag");
    if (state.inject_copy_slowdown) {
      state.inject_copy_slowdown = false;
      ++state.copy_embedded_errors;
      const int submitted = submit_text_response(
          session, connection, frame->hd.stream_id, "200",
          "<s3:Error xmlns:s3=\"urn:s3\">"
          "<s3:Code>SlowDown</s3:Code>"
          "<s3:Message>retry copy</s3:Message></s3:Error>");
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    state.copy_completed = true;
    state.object_key = "copied.bin";
    const int submitted = submit_text_response(
        session, connection, frame->hd.stream_id, "200",
        "<s3:CopyObjectResult xmlns:s3=\"urn:s3\">"
        "<s3:ETag>&quot;after-put&quot;</s3:ETag>"
        "<s3:LastModified>1994-11-08T08:49:37.000Z</s3:LastModified>"
        "</s3:CopyObjectResult>");
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "DELETE" && frame->hd.type == NGHTTP2_HEADERS) {
    if (request.path.find("?uploadId=") != std::string::npos) {
      require(request.path == "/bucket/mmap.bin?uploadId=upload-1",
              "AbortMultipartUpload used an unexpected object or upload ID");
      fprintf(stderr, "mock S3: AbortMultipartUpload path=%s retained_parts=%zu\n",
              request.path.c_str(), state.uploaded_parts.size());
      // Abort discards only the in-progress upload, never the published object.
      // Keep attempt counters intact so a failed upload remains diagnosable.
      state.uploaded_parts.clear();
      const std::array response_headers{
          header(":status", "204"),
          header("content-length", "0"),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, response_headers.data(),
          response_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (request.path == "/bucket/active-unlink-writer.bin") {
      state.active_writer_delete_completed = true;
      const std::array response_headers{
          header(":status", "204"),
          header("content-length", "0"),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, response_headers.data(),
          response_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (!state.overwrite_hidden_key.empty() &&
        request.path == "/bucket/" + state.overwrite_hidden_key) {
      state.special_objects.erase(state.overwrite_hidden_key);
      state.hidden_delete_completed = true;
      const std::array response_headers{
          header(":status", "204"),
          header("content-length", "0"),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, response_headers.data(),
          response_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (state.hidden_rename_completed &&
        request.path == "/bucket/" + state.hidden_key) {
      state.hidden_delete_completed = true;
      state.object.clear();
      const std::array response_headers{
          header(":status", "204"),
          header("content-length", "0"),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, response_headers.data(),
          response_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    require(state.copy_completed,
            "DeleteObject ran before CopyObject completed");
    require(request.path == "/bucket/renamed.bin",
            "DeleteObject used a stale source version");
    require(request.if_match == state.etag,
            "DeleteObject omitted the pinned source ETag");
    state.delete_completed = true;
    const std::array response_headers{
        header(":status", "204"),
        header("content-length", "0"),
    };
    const int submitted = nghttp2_submit_response(
        session, frame->hd.stream_id, response_headers.data(),
        response_headers.size(), nullptr);
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "HEAD" && frame->hd.type == NGHTTP2_HEADERS) {
    ++state.head_requests;
    const std::string key = request_object_key(request.path);
    const auto special = state.special_objects.find(key);
    if (special != state.special_objects.end()) {
      const SpecialObject& object = *special->second;
      const std::string content_length = std::to_string(object.bytes.size());
      const std::array response_headers{
          header(":status", "200"),
          header("content-length", content_length),
          header("etag", object.etag),
          header(version_header(state.checksum), object.version_id),
          header("last-modified", object.last_modified),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, response_headers.data(),
          response_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (key == "overwrite-dest.bin" || key == "overwrite-source.bin" ||
        (!state.overwrite_hidden_key.empty() &&
         key == state.overwrite_hidden_key)) {
      const std::array response_headers{
          header(":status", "404"),
          header("content-length", "0"),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, response_headers.data(),
          response_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    const std::string content_length = std::to_string(state.object.size());
    std::vector<nghttp2_nv> response_headers{
        header(":status", "200"),
        header("content-length", content_length),
        header("etag", state.etag),
        header(version_header(state.checksum), state.version_id),
        header("last-modified", state.last_modified),
    };
    if (!state.write_id.empty()) {
      response_headers.push_back(header(
          state.checksum == CHECKSUM_CRC64XZ
              ? "x-oss-meta-ngs3fs-write-id"
              : "x-amz-meta-ngs3fs-write-id",
          state.write_id));
    }
    const int submitted = nghttp2_submit_response(
        session, frame->hd.stream_id, response_headers.data(),
        response_headers.size(), nullptr);
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "GET" && frame->hd.type == NGHTTP2_HEADERS &&
      request.path.find("list-type=2") != std::string::npos) {
    ++state.list_requests;
    const bool direct =
        request.path.find("delimiter=%2F") != std::string::npos;
    const bool deep =
        request.path.find("prefix=deep%2F") != std::string::npos;
    const bool second_root_page = request.path.find(
        "continuation-token=root-page-2") != std::string::npos;
    require(direct, "directory listing omitted delimiter=/");
    require(request.path.find("max-keys=1000") != std::string::npos,
            "directory listing did not request max-keys=1000");
    std::string xml =
        "<s3:ListBucketResult xmlns:s3=\"urn:s3\">";
    if (state.list_requests == 1) {
      xml += "<!--";
      xml.append(5U * 1024U * 1024U, 'x');
      xml += "-->";
    }
    if (!deep && !second_root_page) {
      xml += "<s3:IsTruncated>1</s3:IsTruncated>"
             "<s3:NextContinuationToken>root-page&#x2D;2"
             "</s3:NextContinuationToken>";
      if (!state.object_key.starts_with(".~ngs3fs~.pending-delete/")) {
        xml += "<s3:Contents><s3:Key>";
        xml += state.object_key;
        xml += "</s3:Key><s3:ETag>";
        xml += state.etag;
        xml += "</s3:ETag><s3:LastModified>";
        xml += state.last_modified_iso;
        xml += "</s3:LastModified><s3:Size>";
        xml += std::to_string(state.object.size());
        xml += "</s3:Size></s3:Contents>";
      }
      for (const auto& [key, object] : state.special_objects) {
        if (key.starts_with(".~ngs3fs~.pending-delete/")) {
          continue;
        }
        xml += "<s3:Contents><s3:Key>";
        xml += key;
        xml += "</s3:Key><s3:ETag>";
        xml += object->etag;
        xml += "</s3:ETag><s3:LastModified>";
        xml += object->last_modified_iso;
        xml += "</s3:LastModified><s3:Size>";
        xml += std::to_string(object->bytes.size());
        xml += "</s3:Size></s3:Contents>";
      }
      if (state.deep_present) {
        xml += "<s3:Contents><s3:Key>deep</s3:Key>"
               "<s3:ETag>\"file\"</s3:ETag>"
               "<s3:LastModified>1994-11-06T08:49:37.000Z"
               "</s3:LastModified>"
               "<s3:Size>0</s3:Size></s3:Contents>";
      }
    } else {
      xml += "<s3:IsTruncated>0</s3:IsTruncated>";
    }
    if (state.deep_present) {
      if (second_root_page) {
        xml += "<s3:CommonPrefixes><s3:Prefix>deep&#47;"
               "</s3:Prefix></s3:CommonPrefixes>";
      } else if (deep) {
        xml += "<s3:Contents><s3:Key>deep&#x2F;child.bin</s3:Key>"
               "<s3:ETag>\"deep\"</s3:ETag>"
               "<s3:LastModified>1994-11-06T08:49:37.000Z"
               "</s3:LastModified>"
               "<s3:Size>3</s3:Size></s3:Contents>";
      }
    }
    xml += "</s3:ListBucketResult>";
    const int submitted = submit_text_response(
        session, connection, frame->hd.stream_id, "200", xml);
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "GET" && frame->hd.type == NGHTTP2_HEADERS &&
      request.path.find("?attributes") != std::string::npos) {
    require(request.object_attributes == "ObjectParts,Checksum",
            "GetObjectAttributes omitted requested attributes");
    require(request.max_parts == "1000",
            "GetObjectAttributes omitted max-parts=1000");
    const std::string key = request_object_key(request.path);
    const auto special = state.special_objects.find(key);
    const std::vector<std::byte>* object = &state.object;
    SpecialObject* special_object = nullptr;
    if (special != state.special_objects.end()) {
      special_object = special->second.get();
      object = &special->second->bytes;
    }
    ++state.object_attributes_requests;
    if (special_object != nullptr) {
      ++special_object->attributes_requests;
    }
    if (special_object != nullptr &&
        special_object->attributes_unsupported) {
      return submit_text_response(session, connection, frame->hd.stream_id,
                                  "403", {});
    }
    if (object->empty()) {
      return submit_text_response(session, connection, frame->hd.stream_id,
                                  "501", {});
    }
    size_t first = 0;
    size_t last  = object->size();
    unsigned part_number = 1;
    bool truncated = false;
    if (special_object != nullptr &&
        special_object->checksum_part_size != 0) {
      if (request.part_number_marker.empty()) {
        last = special_object->checksum_part_size;
        truncated = last < object->size();
      } else {
        require(request.part_number_marker == "1",
                "GetObjectAttributes used the wrong part marker");
        first = special_object->checksum_part_size;
        part_number = 2;
      }
    } else {
      require(request.part_number_marker.empty(),
              "single-page GetObjectAttributes used a part marker");
    }
    std::string xml =
        "<s3:GetObjectAttributesOutput xmlns:s3=\"urn:s3\">"
        "<s3:ObjectParts><s3:IsTruncated>";
    xml += truncated ? "true" : "false";
    xml += "</s3:IsTruncated>";
    if (truncated) {
      xml += "<s3:NextPartNumberMarker>1</s3:NextPartNumberMarker>";
    }
    xml += "<s3:Part><s3:PartNumber>";
    xml += std::to_string(part_number);
    xml += "</s3:PartNumber><s3:Size>";
    xml += std::to_string(last - first);
    xml += "</s3:Size><s3:";
    xml += checksum_xml_name(state.checksum);
    xml += '>';
    xml += encoded_checksum(
        state.checksum,
        std::span(*object).subspan(first, last - first));
    xml += "</s3:";
    xml += checksum_xml_name(state.checksum);
    xml += "></s3:Part></s3:ObjectParts>"
           "</s3:GetObjectAttributesOutput>";
    const int submitted = submit_text_response(
        session, connection, frame->hd.stream_id, "200", xml);
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "PUT" && frame->hd.type == NGHTTP2_DATA) {
    require(request.content_length == std::to_string(request.body.size()),
            "PutObject omitted an exact Content-Length");
    const std::string expected_request_checksum =
        checksum_is_s3(state.checksum)
            ? encoded_checksum(state.checksum, request.body)
            : std::string{};
    require(request.checksum_value == expected_request_checksum,
            "PutObject sent an invalid checksum");
    const std::string key = request_object_key(request.path);
    if (key == "overwrite-dest.bin" || key == "overwrite-source.bin") {
      auto object = std::make_shared<SpecialObject>();
      object->bytes = std::move(request.body);
      object->etag = key == "overwrite-dest.bin"
          ? "\"overwrite-destination\""
          : "\"overwrite-source\"";
      object->version_id = key == "overwrite-dest.bin"
          ? "overwrite-destination-v1"
          : "overwrite-source-v1";
      object->last_modified = "Wed, 09 Nov 1994 08:49:37 GMT";
      object->last_modified_iso = "1994-11-09T08:49:37.000Z";
      state.special_objects[key] = object;
      ++state.put_requests;
      const std::string checksum = response_checksum(
          state.checksum, object->bytes);
      const std::array response_headers{
          header(":status", "200"),
          header("content-length", "0"),
          header("etag", object->etag),
          header(version_header(state.checksum), object->version_id),
          header("last-modified", object->last_modified),
          header(checksum_header_name(state.checksum), checksum),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, response_headers.data(),
          response_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    state.object = std::move(request.body);
    constexpr std::string_view bucket_prefix = "/bucket/";
    require(request.path.starts_with(bucket_prefix),
            "PutObject used an unexpected request path");
    state.object_key.assign(request.path.substr(bucket_prefix.size()));
    state.write_id = request.write_id;
    state.etag = "\"after-put\"";
    state.version_id = "version-2";
    state.last_modified = "Mon, 07 Nov 1994 08:49:37 GMT";
    state.last_modified_iso = "1994-11-07T08:49:37.000Z";
    ++state.put_requests;
    const std::string checksum = response_checksum(
        state.checksum, state.object);
    const std::array response_headers{
        header(":status", "200"),
        header("content-length", "0"),
        header("etag", state.etag),
        header(version_header(state.checksum), state.version_id),
        header("last-modified", state.last_modified),
        header(checksum_header_name(state.checksum),
               checksum),
    };
    const int submitted = nghttp2_submit_response(
        session, frame->hd.stream_id, response_headers.data(),
        response_headers.size(), nullptr);
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "PUT" && frame->hd.type == NGHTTP2_HEADERS) {
    const std::string expected_request_checksum =
        checksum_is_s3(state.checksum)
            ? encoded_checksum(state.checksum, {})
            : std::string{};
    require(request.checksum_value == expected_request_checksum,
            "empty PutObject sent an invalid checksum");
    state.object.clear();
    constexpr std::string_view bucket_prefix = "/bucket/";
    require(request.path.starts_with(bucket_prefix),
            "empty PutObject used an unexpected request path");
    state.object_key.assign(request.path.substr(bucket_prefix.size()));
    state.etag = "\"after-put\"";
    state.version_id = "version-2";
    state.last_modified = "Mon, 07 Nov 1994 08:49:37 GMT";
    state.last_modified_iso = "1994-11-07T08:49:37.000Z";
    ++state.put_requests;
    const std::string checksum = response_checksum(state.checksum, {});
    const std::array response_headers{
        header(":status", "200"),
        header("content-length", "0"),
        header("etag", state.etag),
        header(version_header(state.checksum), state.version_id),
        header("last-modified", state.last_modified),
        header(checksum_header_name(state.checksum),
               checksum),
    };
    const int submitted = nghttp2_submit_response(
        session, frame->hd.stream_id, response_headers.data(),
        response_headers.size(), nullptr);
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method != "GET" || frame->hd.type != NGHTTP2_HEADERS ||
      frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }

  const std::string key = request_object_key(request.path);
  if (key == "mmap.bin" && state.inject_request_timeout) {
    state.inject_request_timeout = false;
    ++state.request_timeout_errors;
    const int submitted = submit_text_response(
        session, connection, frame->hd.stream_id, "400",
        "<s3:Error xmlns:s3=\"urn:s3\">"
        "<s3:Code>RequestTimeout</s3:Code>"
        "<s3:Message>request timed out</s3:Message>"
        "<s3:RequestId>retry-request</s3:RequestId></s3:Error>");
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  const auto special = state.special_objects.find(key);
  if (special == state.special_objects.end() &&
      (key == "overwrite-dest.bin" || key == "overwrite-source.bin" ||
       (!state.overwrite_hidden_key.empty() &&
        key == state.overwrite_hidden_key))) {
    return submit_text_response(session, connection, frame->hd.stream_id,
                                "404", {});
  }
  const bool is_special = special != state.special_objects.end();
  std::shared_ptr<SpecialObject> response_object;
  if (is_special) {
    response_object = special->second;
  } else {
    response_object                = std::make_shared<SpecialObject>();
    response_object->bytes         = state.object;
    response_object->etag          = state.etag;
    response_object->version_id    = state.version_id;
    response_object->last_modified = state.last_modified;
    response_object->last_modified_iso = state.last_modified_iso;
  }
  const std::vector<std::byte>* object = &response_object->bytes;
  const RequestRange range = request.range;
  if (is_special) {
    ++response_object->get_requests;
    response_object->get_ranges.push_back(range);
    response_object->get_paths.push_back(request.path);
    if (response_object->corrupt_gets_remaining != 0) {
      --response_object->corrupt_gets_remaining;
      object = &response_object->corrupted_bytes;
    }
  }
  const std::string_view object_etag   = response_object->etag;
  const std::string_view object_version = response_object->version_id;
  if (!range.valid || range.last >= object->size()) {
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  auto response = std::make_unique<ResponseSource>();
  response->keepalive = std::move(response_object);
  response->object = object;
  response->delay = true;
  response->cursor = static_cast<size_t>(range.first);
  response->end = static_cast<size_t>(range.last + 1);
  response->begin = response->cursor;
  response->ttfb_ms = state.get_ttfb_ms;
  response->bytes_per_second = state.get_bytes_per_second;
  response->content_length =
      std::to_string(response->end - response->cursor);
  response->content_range =
      "bytes " + std::to_string(range.first) + '-' +
      std::to_string(range.last) + '/' +
      std::to_string(object->size());
  if (is_special && response->keepalive->invalid_content_range) {
    response->content_range =
        "bytes " + std::to_string(range.first + 1) + '-' +
        std::to_string(range.last + 1) + '/' +
        std::to_string(object->size());
  }
  ResponseSource* source = response.get();
  connection.responses[frame->hd.stream_id] = std::move(response);
  request.active_get = true;
  ++state.active_gets;
  ++state.get_requests;
  state.get_paths.push_back(request.path + " [" + std::to_string(range.first) + "," +
                            std::to_string(range.last) + "]");
  if (key == "overwrite-dest.bin" || key == state.overwrite_hidden_key) {
    if (is_special) {
      ++state.overwrite_destination_gets;
      state.overwrite_destination_max_read_end = std::max(
          state.overwrite_destination_max_read_end,
          static_cast<size_t>(range.last + 1));
    }
  }
  state.maximum_active_gets =
      std::max(state.maximum_active_gets, state.active_gets);

  const bool complete_object = range.first == 0 &&
      range.last + 1 == object->size();
  std::string checksum;
  std::vector<nghttp2_nv> response_headers{
      header(":status", "206"),
      header("content-length", source->content_length),
      header("content-range", source->content_range),
      header("etag", object_etag),
      header(version_header(state.checksum), object_version),
  };
  if (!request.checksum_mode.empty()) {
    require(request.checksum_mode == "ENABLED" && complete_object,
            "checksum mode was used for a partial object range");
    ++state.checksum_mode_requests;
    // Corruption injection changes the transport bytes, not the object's
    // authoritative checksum. Otherwise a corrupt GET incorrectly verifies.
    checksum = encoded_checksum(state.checksum, source->keepalive->bytes);
    response_headers.push_back(
        header(checksum_header_name(state.checksum), checksum));
    response_headers.push_back(
        header("x-amz-checksum-type", "FULL_OBJECT"));
  } else if (state.checksum == CHECKSUM_CRC64XZ) {
    checksum = response_checksum(state.checksum, source->keepalive->bytes);
    response_headers.push_back(
        header(checksum_header_name(state.checksum), checksum));
  }
  nghttp2_data_provider provider{
      .source = {.ptr = source},
      .read_callback = read_body,
  };
  const int submitted = nghttp2_submit_response(
      session, frame->hd.stream_id, response_headers.data(),
      response_headers.size(), &provider);
  return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
}

int on_stream_close(nghttp2_session*, int32_t stream_id,
                    uint32_t, void* user_data) {
  auto& connection = *static_cast<ServerState*>(user_data);
  const auto request = connection.requests.find(stream_id);
  if (request != connection.requests.end() && request->second.active_get) {
    SharedServerState& state = *connection.shared;
    std::lock_guard state_guard(state.mutex);
    --state.active_gets;
  }
  connection.requests.erase(stream_id);
  connection.responses.erase(stream_id);
  return 0;
}

void flush_server(nghttp2_session* session, int socket_fd) {
  for (;;) {
    const uint8_t* bytes = nullptr;
    const ssize_t length = nghttp2_session_mem_send(session, &bytes);
    require(length >= 0, "nghttp2_session_mem_send failed");
    if (length == 0) {
      return;
    }
    size_t offset = 0;
    while (offset < static_cast<size_t>(length)) {
      const ssize_t sent = ::send(
          socket_fd, bytes + offset,
          static_cast<size_t>(length) - offset, MSG_NOSIGNAL);
      if (sent > 0) {
        offset += static_cast<size_t>(sent);
      } else if (sent < 0 && errno == EINTR) {
        continue;
      } else {
        fail_errno("server send");
      }
    }
  }
}

struct Listener {
  UniqueFd socket;
  uint16_t port = 0;
};

Listener make_listener() {
  UniqueFd socket(
      ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
  if (!socket) {
    fail_errno("socket");
  }
  int enabled = 1;
  if (::setsockopt(socket.get(), SOL_SOCKET, SO_REUSEADDR, &enabled,
                   sizeof(enabled)) != 0) {
    fail_errno("setsockopt");
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (::bind(socket.get(), reinterpret_cast<sockaddr*>(&address),
             sizeof(address)) != 0 ||
      ::listen(socket.get(), 64) != 0) {
    fail_errno("bind/listen");
  }
  socklen_t address_size = sizeof(address);
  if (::getsockname(socket.get(), reinterpret_cast<sockaddr*>(&address),
                    &address_size) != 0) {
    fail_errno("getsockname");
  }
  return Listener{.socket = std::move(socket),
                  .port = ntohs(address.sin_port)};
}

void record_server_failure(SharedServerState& shared,
                           std::exception_ptr failure) noexcept {
  std::lock_guard state_guard(shared.mutex);
  if (!shared.failure) {
    shared.failure = std::move(failure);
  }
}

void rethrow_server_failure(SharedServerState& shared) {
  std::exception_ptr failure;
  {
    std::lock_guard state_guard(shared.mutex);
    failure = shared.failure;
  }
  if (failure) {
    std::rethrow_exception(failure);
  }
}

void serve_connection(UniqueFd socket, SharedServerState& shared) noexcept {
  try {
    nghttp2_session_callbacks* raw_callbacks = nullptr;
    require(nghttp2_session_callbacks_new(&raw_callbacks) == 0,
            "nghttp2_session_callbacks_new failed");
    std::unique_ptr<nghttp2_session_callbacks, CallbacksDeleter> callbacks(
        raw_callbacks);
    nghttp2_session_callbacks_set_on_begin_headers_callback(
        callbacks.get(), on_begin_headers);
    nghttp2_session_callbacks_set_on_header_callback(callbacks.get(),
                                                      on_header);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
        callbacks.get(), on_data_chunk);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks.get(),
                                                          on_frame_recv);
    nghttp2_session_callbacks_set_on_stream_close_callback(
        callbacks.get(), on_stream_close);

    ServerState state{.shared = &shared};
    nghttp2_session* raw_session = nullptr;
    require(nghttp2_session_server_new(&raw_session, callbacks.get(),
                                       &state) == 0,
            "nghttp2_session_server_new failed");
    std::unique_ptr<nghttp2_session, SessionDeleter> session(raw_session);
    require(nghttp2_submit_settings(session.get(), NGHTTP2_FLAG_NONE,
                                    nullptr, 0) == 0,
            "nghttp2_submit_settings failed");
    flush_server(session.get(), socket.get());

    std::array<uint8_t, 64U * 1024U> input{};
    for (;;) {
      const ssize_t count = ::read(socket.get(), input.data(), input.size());
      if (count == 0) {
        return;
      }
      if (count < 0 && errno == EINTR) {
        continue;
      }
      if (count < 0 && errno == ECONNRESET) {
        return;
      }
      if (count < 0) {
        fail_errno("server read");
      }
      const ssize_t consumed = nghttp2_session_mem_recv(
          session.get(), input.data(), static_cast<size_t>(count));
      require(consumed == count, "nghttp2 server did not consume input");
      if (state.close_without_response) {
        return;
      }
      flush_server(session.get(), socket.get());
    }
  } catch (const std::system_error& error) {
    if (error.code().value() == ECONNRESET ||
        error.code().value() == EPIPE) {
      return;
    }
    std::exception_ptr failure = std::current_exception();
    fprintf(stderr, "mock S3 connection failed: %s\n", error.what());
    record_server_failure(shared, std::move(failure));
  } catch (...) {
    std::exception_ptr failure = std::current_exception();
    try {
      std::rethrow_exception(failure);
    } catch (const std::exception& error) {
      fprintf(stderr, "mock S3 connection failed: %s\n", error.what());
    } catch (...) {
      fputs("mock S3 connection failed with a non-standard exception\n",
            stderr);
    }
    record_server_failure(shared, std::move(failure));
  }
}

void run_server(std::stop_token stop, int listener,
                SharedServerState& shared) noexcept {
  std::vector<std::jthread> connections;
  try {
    while (!stop.stop_requested() && !shared.stop.load()) {
      UniqueFd socket(::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC));
      if (!socket) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          continue;
        }
        if (stop.stop_requested() || shared.stop.load()) {
          break;
        }
        fail_errno("accept4");
      }
      connections.emplace_back(serve_connection, std::move(socket),
                               std::ref(shared));
    }
  } catch (...) {
    record_server_failure(shared, std::current_exception());
  }
}

class MountedProcess {
 public:
  MountedProcess(std::string mountpoint, pid_t process)
      : mountpoint_(std::move(mountpoint)), process_(process) {}

  MountedProcess(const MountedProcess&) = delete;
  MountedProcess& operator=(const MountedProcess&) = delete;

  ~MountedProcess() { stop(); }

  void crash() noexcept {
    if (process_ <= 0) {
      return;
    }
    ::kill(process_, SIGKILL);
    int status = 0;
    while (::waitpid(process_, &status, 0) < 0 && errno == EINTR) {
    }
    process_ = -1;
    unmount(true);
  }

  void restart(pid_t process) noexcept { process_ = process; }
  bool stopped_cleanly() const noexcept { return clean_exit_; }

  void stop() noexcept {
    if (process_ <= 0) {
      return;
    }
    unmount(false);

    for (int attempt = 0; attempt < 200; ++attempt) {
      int status = 0;
      const pid_t result = ::waitpid(process_, &status, WNOHANG);
      if (result == process_) {
        clean_exit_ = WIFEXITED(status) && WEXITSTATUS(status) == 0;
        process_ = -1;
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ::kill(process_, SIGTERM);
    int status = 0;
    while (::waitpid(process_, &status, 0) < 0 && errno == EINTR) {
    }
    process_ = -1;
  }

 private:
  void unmount(bool lazy) noexcept {
    const pid_t unmount_process = ::fork();
    if (unmount_process == 0) {
      ::execlp("fusermount3", "fusermount3", lazy ? "-uz" : "-u",
               mountpoint_.c_str(), static_cast<char*>(nullptr));
      _exit(127);
    }
    if (unmount_process > 0) {
      int unmount_status = 0;
      while (::waitpid(unmount_process, &unmount_status, 0) < 0 &&
             errno == EINTR) {
      }
    }
  }
  std::string mountpoint_;
  pid_t process_ = -1;
  bool clean_exit_ = false;
};

std::string make_mountpoint() {
  std::array<char, 64> path{};
  const char pattern[] = "/tmp/ngs3fs-fuse-test-XXXXXX";
  std::copy(std::begin(pattern), std::end(pattern), path.begin());
  if (::mkdtemp(path.data()) == nullptr) {
    fail_errno("mkdtemp");
  }
  return path.data();
}

pid_t start_daemon(std::string_view executable, std::string_view mountpoint,
                   uint16_t port, std::string_view checksum,
                   std::string_view cache_dir,
                   std::string_view engine = "auto",
                   std::string_view reactors = "1",
                   bool verify_reads = true, bool whole_retry = false,
                   bool low_budget = false, const char* max_window = nullptr,
                   const char* memory_budget = nullptr,
                   const char* cache_block = "2MiB") {
  // The receive-pool implementation deliberately omits uncached checksum
  // verification. Cached and legacy tests still request it explicitly.
  if (engine == "uring" && cache_dir.empty()) verify_reads = false;
  const char* low_limit = engine == "uring" ? "4MiB" : "2MiB";
  const char* mount_limit = memory_budget ? memory_budget : (low_budget ? low_limit : "0");
  const std::string port_text = std::to_string(port);
  const std::string uid_text  = std::to_string(::getuid());
  const std::string gid_text  = std::to_string(::getgid());
  const pid_t process = ::fork();
  if (process < 0) {
    fail_errno("fork");
  }
  if (process == 0) {
    ::setenv("AWS_ACCESS_KEY_ID", "integration-access-key", 1);
    ::setenv("AWS_SECRET_ACCESS_KEY", "integration-secret-key", 1);
    ::unsetenv("AWS_SESSION_TOKEN");
    if (max_window) {
      ::setenv("UNSTABLE_NGS3FS_MAX_PREFETCH_WINDOW_SIZE", max_window, 1);
    } else {
      ::unsetenv("UNSTABLE_NGS3FS_MAX_PREFETCH_WINDOW_SIZE");
    }
    if (!verify_reads && !cache_dir.empty()) {
      ::execl(executable.data(), "ngs3fs", "-e", "127.0.0.1", "-p",
              port_text.c_str(), "-a", "mock-s3", "-b", "bucket", "-u",
              uid_text.c_str(), "-g", gid_text.c_str(), "-m", "0640", "-D",
              "0750", "-I", "1", "--checksum", checksum.data(),
              "--expected-bucket-owner", "111122223333", "--requester-pays",
              "--stats-interval", "86400", "-L", cache_dir.data(),
              "--cache-reserve", "0", "--cache-block-size", cache_block,
              "--io-engine", engine.data(), "--reactors", reactors.data(),
              "-f", mountpoint.data(), static_cast<char*>(nullptr));
    } else if (!verify_reads) {
      ::execl(executable.data(), "ngs3fs", "-e", "127.0.0.1", "-p",
              port_text.c_str(), "-a", "mock-s3", "-b", "bucket", "-u",
              uid_text.c_str(), "-g", gid_text.c_str(), "-m", "0640", "-D",
              "0750", "-I", "1", "--checksum", checksum.data(),
              "--expected-bucket-owner", "111122223333", "--requester-pays",
              "--stats-interval", "86400", "--io-engine", engine.data(),
              "--max-prefetch-memory", mount_limit,
              "--max-file-prefetch-memory", mount_limit,
              "--reactors", reactors.data(), "-f", mountpoint.data(),
              static_cast<char*>(nullptr));
    } else if (cache_dir.empty()) {
      ::execl(executable.data(), "ngs3fs", "-e", "127.0.0.1", "-p",
              port_text.c_str(), "-a", "mock-s3", "-b", "bucket", "-u",
              uid_text.c_str(), "-g", gid_text.c_str(), "-m", "0640", "-D",
              "0750", "-I", "1",
              "--checksum", checksum.data(), "--verify-read-checksum",
              "--max-file-prefetch-memory", low_budget ? low_limit : (whole_retry ? "8MiB" : "0"),
              "--max-prefetch-memory", low_budget ? low_limit : "0",
              "--expected-bucket-owner", "111122223333", "--requester-pays",
              "--stats-interval", "86400", "--io-engine", engine.data(),
              "--reactors", reactors.data(), "-f", mountpoint.data(),
              static_cast<char*>(nullptr));
    } else {
      ::execl(executable.data(), "ngs3fs", "-e", "127.0.0.1", "-p",
              port_text.c_str(), "-a", "mock-s3", "-b", "bucket", "-u",
              uid_text.c_str(), "-g", gid_text.c_str(), "-m", "0640", "-D",
              "0750", "-I", "1",
              "--checksum", checksum.data(), "--verify-read-checksum",
              "--expected-bucket-owner", "111122223333", "--requester-pays",
              "--stats-interval", "86400", "-L", cache_dir.data(),
              "--cache-reserve", "0", "--io-engine", engine.data(),
              "--reactors", reactors.data(), "-f", mountpoint.data(),
              static_cast<char*>(nullptr));
    }
    _exit(127);
  }
  return process;
}

pid_t start_passthrough_daemon(
    std::string_view executable, std::string_view mountpoint, uint16_t port,
    std::string_view checksum, std::string_view cache_dir,
    std::string_view trace_executable, std::string_view trace_output,
    bool unlimited) {
  const std::string port_text = std::to_string(port);
  const std::string uid_text  = std::to_string(::getuid());
  const std::string gid_text  = std::to_string(::getgid());
  std::vector<std::string> daemon_arguments{
      std::string(executable), "-e", "127.0.0.1", "-p", port_text,
      "-a", "mock-s3", "-b", "bucket", "-u", uid_text, "-g", gid_text,
      "-m", "0640", "-D", "0750", "-I", "1", "--checksum",
      std::string(checksum), "--expected-bucket-owner", "111122223333",
      "--requester-pays", "--stats-interval", "86400", "-L",
      std::string(cache_dir), "--cache-block-size", "2MiB"};
  if (unlimited) {
    daemon_arguments.emplace_back("--cache-unlimited");
  } else {
    daemon_arguments.emplace_back("--cache-size");
    daemon_arguments.emplace_back("64MiB");
    daemon_arguments.emplace_back("--cache-reserve");
    daemon_arguments.emplace_back("0");
  }
  daemon_arguments.insert(daemon_arguments.end(), {
      "--io-engine", "auto", "--reactors", "1", "-f",
      std::string(mountpoint)});

  const pid_t process = ::fork();
  if (process < 0) fail_errno("fork passthrough daemon");
  if (process == 0) {
    ::setenv("AWS_ACCESS_KEY_ID", "integration-access-key", 1);
    ::setenv("AWS_SECRET_ACCESS_KEY", "integration-secret-key", 1);
    ::unsetenv("AWS_SESSION_TOKEN");
    ::unsetenv("UNSTABLE_NGS3FS_MAX_PREFETCH_WINDOW_SIZE");

    std::vector<std::string> traced_arguments;
    std::vector<std::string>* selected = &daemon_arguments;
    if (!trace_executable.empty()) {
      traced_arguments = {
          std::string(trace_executable), "-f", "-qq", "-e", "trace=ioctl",
          "-o", std::string(trace_output)};
      traced_arguments.insert(traced_arguments.end(),
                              daemon_arguments.begin(),
                              daemon_arguments.end());
      selected = &traced_arguments;
    }
    std::vector<char*> arguments;
    arguments.reserve(selected->size() + 1);
    for (std::string& argument : *selected) {
      arguments.push_back(argument.data());
    }
    arguments.push_back(nullptr);
    ::execv(arguments.front(), arguments.data());
    _exit(127);
  }
  return process;
}

struct TraceIoctlStats {
  size_t calls = 0;
  size_t successes = 0;
  size_t unavailable = 0;
};

TraceIoctlStats trace_ioctl_stats(std::string_view path,
                                  std::string_view needle,
                                  bool positive_result_succeeds) {
  const int fd = ::open(path.data(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    if (errno == ENOENT) return {};
    fail_errno("open passthrough trace");
  }
  std::string contents;
  std::array<char, 4096> buffer{};
  for (;;) {
    const ssize_t count = ::read(fd, buffer.data(), buffer.size());
    if (count > 0) {
      contents.append(buffer.data(), size_t(count));
    } else if (count < 0 && errno == EINTR) {
      continue;
    } else if (count == 0) {
      break;
    } else {
      const int error = errno;
      ::close(fd);
      errno = error;
      fail_errno("read passthrough trace");
    }
  }
  ::close(fd);
  const std::string_view generic_needle =
      needle == "FUSE_DEV_IOC_BACKING_OPEN"
          ? "_IOC(_IOC_WRITE, 0xe5, 0x1, 0x10)"
          : "_IOC(_IOC_WRITE, 0xe5, 0x2, 0x4)";
  TraceIoctlStats stats;
  for (size_t line_begin = 0; line_begin < contents.size();) {
    const size_t line_end = contents.find('\n', line_begin);
    const size_t symbolic = contents.find(needle, line_begin);
    const size_t generic = contents.find(generic_needle, line_begin);
    const bool matching_line = symbolic < line_end || generic < line_end;
    const size_t result = contents.find(" = ", line_begin);
    if (matching_line && result < line_end) {
      ++stats.calls;
      const size_t value = result + 3;
      int64_t result_value = 0;
      const auto parsed = std::from_chars(
          contents.data() + value,
          contents.data() + (line_end == std::string::npos
                                 ? contents.size() : line_end),
          result_value);
      if (parsed.ec == std::errc{} &&
          (positive_result_succeeds ? result_value > 0
                                    : result_value == 0)) {
        ++stats.successes;
      }
      const size_t result_end = line_end == std::string::npos
                                    ? contents.size() : line_end;
      const std::string_view result_text(contents.data() + value,
                                         result_end - value);
      if (result_text.starts_with("-1 EPERM ") ||
          result_text.starts_with("-1 ENOSYS ") ||
          result_text.starts_with("-1 EOPNOTSUPP ")) {
        ++stats.unavailable;
      }
    }
    if (line_end == std::string::npos) break;
    line_begin = line_end + 1;
  }
  return stats;
}

void wait_until_mounted(std::string_view file_path, pid_t process) {
  for (int attempt = 0; attempt < 500; ++attempt) {
    struct stat status{};
    if (::stat(file_path.data(), &status) == 0) {
      return;
    }
    int child_status = 0;
    if (::waitpid(process, &child_status, WNOHANG) == process) {
      throw std::runtime_error("ngs3fs exited before mount became ready");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  throw std::runtime_error("timed out waiting for FUSE mount");
}

int main(int argc, char** argv) {
  if (argc < 2 || argc > 7) {
    std::cerr << "usage: fuse_mmap_integration_test NGS3FS "
                 "[CHECKSUM [plain|cache|prefetch|prefetch-verified "
                 "|passthrough|passthrough-budget "
                 "[auto|legacy|uring [REACTORS [STRACE]]]]]\n";
    return 2;
  }
  if (::access("/dev/fuse", R_OK | W_OK) != 0) {
    std::cout << "SKIP: /dev/fuse is unavailable\n";
    return 0;
  }

  std::string mountpoint;
  std::string cache_dir;
  std::string trace_dir;
  try {
    ChecksumAlgorithm checksum = CHECKSUM_XXHASH128;
    if (argc >= 3 &&
        (!parse_checksum_algorithm(argv[2], checksum) ||
         (checksum != CHECKSUM_XXHASH128 &&
          checksum != CHECKSUM_CRC64NVME &&
          checksum != CHECKSUM_CRC64XZ))) {
      throw std::invalid_argument(
          "integration checksum must be xxhash128, crc64nvme, or crc64xz");
    }
    const std::string_view engine = argc >= 5 ? argv[4] : "auto";
    if (engine != "auto" && engine != "legacy" && engine != "uring") {
      throw std::invalid_argument("unknown integration-test io engine");
    }
    const std::string_view reactors = argc >= 6 ? argv[5] : "1";
    if (reactors != "1" && reactors != "2") {
      throw std::invalid_argument("integration-test reactors must be 1 or 2");
    }
    const bool cache_blocks_small = argc >= 4 && std::string_view(argv[3]) == "cache-blocks-96k";
    const bool cache_blocks = cache_blocks_small ||
        (argc >= 4 && std::string_view(argv[3]) == "cache-blocks");
    const bool passthrough_mode = argc >= 4 &&
        (std::string_view(argv[3]) == "passthrough" ||
         std::string_view(argv[3]) == "passthrough-budget");
    const bool passthrough_unlimited = passthrough_mode &&
        std::string_view(argv[3]) == "passthrough";
    const bool trace_passthrough = passthrough_unlimited && ::geteuid() == 0;
    const bool verified_clean = argc >= 4 && std::string_view(argv[3]) == "prefetch-verified-clean";
    const bool shutdown_prefetch = argc >= 4 && std::string_view(argv[3]) == "prefetch-shutdown";
    const bool partial_prefetch = argc >= 4 && std::string_view(argv[3]) == "prefetch-unverifiable";
    const bool pressure_prefetch = argc >= 4 && std::string_view(argv[3]) == "prefetch-pressure";
    const bool pool_limited = argc >= 4 && std::string_view(argv[3]) == "prefetch-pool-limited";
    const bool pool_pressure = argc >= 4 && std::string_view(argv[3]) == "prefetch-pool-pressure";
    const bool legacy_window_test = engine == "legacy" && argc >= 4 &&
        std::string_view(argv[3]) == "prefetch";
    const bool pool_prefetch = engine == "uring" && argc >= 4 &&
        (pool_limited || std::string_view(argv[3]) == "prefetch-pool" ||
         std::string_view(argv[3]) == "prefetch" || shutdown_prefetch);
    const bool verified_prefetch = verified_clean || (argc >= 4 &&
        std::string_view(argv[3]) == "prefetch-verified");
    const bool budget_prefetch = argc >= 4 && std::string_view(argv[3]) == "prefetch-budget";
    const bool prefetch_mode = verified_prefetch || budget_prefetch || shutdown_prefetch || partial_prefetch || pressure_prefetch || pool_prefetch || pool_pressure || (argc >= 4 &&
        std::string_view(argv[3]) == "prefetch");
    require(!pool_pressure || engine == "uring", "pool pressure requires io_uring");
    require(engine != "uring" || !(verified_prefetch || partial_prefetch || pressure_prefetch),
            "uring checksum/STORE/connection-pressure experiments were replaced by prefetch-pool and prefetch-budget");
    std::vector<std::byte> expected(512U * 1024U + 37U);
    for (size_t i = 0; i < expected.size(); ++i) {
      expected[i] = static_cast<std::byte>((i * 29U + 7U) & 0xffU);
    }
    std::vector<std::byte> small_expected = expected;
    small_expected.resize(64U * 1024U + 37U);
    for (size_t i = 0; i < small_expected.size(); ++i) {
      small_expected[i] ^= static_cast<std::byte>(0x5aU);
    }
    std::vector<std::byte> final_expected(
        8U * 1024U * 1024U + 256U * 1024U + 37U);
    for (size_t i = 0; i < final_expected.size(); ++i) {
      final_expected[i] = static_cast<std::byte>((i * 17U + 11U) & 0xffU);
    }
    std::vector<std::byte> overwrite_old(4U * 1024U * 1024U);
    std::vector<std::byte> overwrite_source(4U * 1024U * 1024U);
    for (size_t i = 0; i < overwrite_old.size(); ++i) {
      overwrite_old[i] = static_cast<std::byte>((i * 31U + 19U) & 0xffU);
      overwrite_source[i] = static_cast<std::byte>((i * 47U + 23U) & 0xffU);
    }

    Listener listener = make_listener();
    SharedServerState shared;
    const auto environment_number = [](const char* name, uint64_t fallback) {
      const char* text = ::getenv(name);
      if (!text) return fallback;
      char* end = nullptr;
      errno = 0;
      const auto value = ::strtoull(text, &end, 10);
      require(*text && *text != '-' && errno == 0 && end && !*end,
              "invalid integration-test network timing environment value");
      return uint64_t(value);
    };
    shared.get_ttfb_ms = environment_number("NGS3FS_TEST_TTFB_MS", 100);
    shared.get_bytes_per_second = environment_number(
        "NGS3FS_TEST_PAYLOAD_BYTES_PER_SECOND", 0);
    require(shared.get_ttfb_ms <= 1000,
            "integration-test TTFB must not exceed its one-second fixture limit");
    if (::getenv("NGS3FS_TEST_TTFB_MS") || ::getenv("NGS3FS_TEST_PAYLOAD_BYTES_PER_SECOND")) {
      fprintf(stderr, "GET fixture: once-per-GET first-body delay=%llu ms, payload=%llu bytes/s (0=unlimited)\n",
              (unsigned long long)shared.get_ttfb_ms,
              (unsigned long long)shared.get_bytes_per_second);
    }
    shared.checksum = checksum;
    shared.object   = expected;
    auto add_overwrite_object = [&](std::string key,
                                    std::vector<std::byte> bytes,
                                    std::string etag) {
      auto object = std::make_shared<SpecialObject>();
      object->bytes = std::move(bytes);
      object->etag = std::move(etag);
      object->version_id = key + "-v1";
      object->last_modified = "Wed, 09 Nov 1994 08:49:37 GMT";
      object->last_modified_iso = "1994-11-09T08:49:37.000Z";
      shared.special_objects.emplace(std::move(key), std::move(object));
    };
    add_overwrite_object("overwrite-dest.bin", overwrite_old,
                         "\"overwrite-destination\"");
    add_overwrite_object("overwrite-source.bin", overwrite_source,
                         "\"overwrite-source\"");
    std::vector<std::byte> read_ahead(4U * 1024U * 1024U + 37U);
    for (size_t i = 0; i < read_ahead.size(); ++i) {
      read_ahead[i] = static_cast<std::byte>((i * 53U + 31U) & 0xffU);
    }
    add_overwrite_object("read-ahead.bin", read_ahead,
                         "\"read-ahead\"");
    add_overwrite_object("read-ahead-sequential.bin", read_ahead,
                         "\"read-ahead-sequential\"");
    add_overwrite_object("read-ahead-random.bin", read_ahead,
                         "\"read-ahead-random\"");
    add_overwrite_object("read-ahead-store.bin", read_ahead,
                         "\"read-ahead-store\"");
    if (cache_blocks) {
      add_overwrite_object("cache-blocks.bin", read_ahead, "\"cache-blocks\"");
      std::vector<std::byte> bytes(40 * 1024 * 1024 + 37);
      for (size_t i = 0; i != bytes.size(); ++i) {
        bytes[i] = std::byte((i * 53 + (i >> 17) * 11 + 31) & 0xff);
      }
      add_overwrite_object("cache-sequential.bin", std::move(bytes), "\"cache-sequential\"");
    }
    if (pool_pressure) {
      auto bytes = read_ahead;
      bytes.resize(8U * 1024U * 1024U + 37);
      for (size_t i = read_ahead.size(); i < bytes.size(); ++i) {
        bytes[i] = std::byte((i * 53 + 31) & 0xff);
      }
      add_overwrite_object("budget-inflight.bin", std::move(bytes),
                           "\"budget-inflight\"");
      add_overwrite_object("budget-demand.bin", read_ahead,
                           "\"budget-demand\"");
      add_overwrite_object("budget-pinned.bin", read_ahead,
                           "\"budget-pinned\"");
    }
    if (pool_prefetch) {
      add_overwrite_object("pool-random.bin", std::vector<std::byte>(8U * 1024U * 1024U + 37),
                           "\"pool-random\"");
      add_overwrite_object("pool-sequential.bin", std::vector<std::byte>(64U * 1024U * 1024U + 37),
                           "\"pool-sequential\"");
      add_overwrite_object("pool-tail.bin", read_ahead, "\"pool-tail\"");
      add_overwrite_object("pool-order.bin", read_ahead, "\"pool-order\"");
      shared.special_objects.at("pool-order.bin")->limited_range_end = 2U * 1024U * 1024U;
      for (const char* name : {"pool-random.bin", "pool-sequential.bin", "pool-order.bin"}) {
        auto& bytes = shared.special_objects.at(name)->bytes;
        for (size_t i = 0; i != bytes.size(); ++i) {
          bytes[i] = std::byte((i * 53 + (i >> 17) * 11 + 31) & 0xff);
        }
      }
    }
    if (pressure_prefetch) {
      for (unsigned i = 0; i != 8; ++i) {
        const std::string name = "pressure-" + std::to_string(i) + ".bin";
        add_overwrite_object(name, read_ahead, '"' + name + '"');
        shared.special_objects.at(name)->pause_after = 512U * 1024U;
      }
    }
    if (verified_prefetch) {
      auto& object = *shared.special_objects.at("read-ahead-store.bin");
      object.bytes.resize(engine == "legacy" ? 32U * 1024U + 37U : 768U * 1024U + 37U);
      object.corrupted_bytes = object.bytes;
      object.corrupted_bytes[engine == "legacy" ? 12345 : 384U * 1024U + 17U] ^= std::byte{0x80};
      object.corrupt_gets_remaining = verified_clean ? 0 : 1;
    }
    std::vector<std::byte> checksum_test(256U * 1024U + 37U);
    for (size_t i = 0; i < checksum_test.size(); ++i) {
      checksum_test[i] = static_cast<std::byte>((i * 13U + 41U) & 0xffU);
    }
    auto add_checksum_object = [&](std::string key, int corrupt_gets,
                                   size_t part_size = 0,
                                   bool unsupported = false) {
      auto object = std::make_shared<SpecialObject>();
      object->bytes           = checksum_test;
      object->corrupted_bytes = checksum_test;
      object->corrupted_bytes[12345] ^= std::byte{0x80};
      object->etag              = '"' + key + '"';
      object->version_id        = key + "-v1";
      object->last_modified     = "Thu, 10 Nov 1994 08:49:37 GMT";
      object->last_modified_iso = "1994-11-10T08:49:37.000Z";
      object->corrupt_gets_remaining = corrupt_gets;
      object->checksum_part_size      = part_size;
      object->attributes_unsupported = unsupported;
      shared.special_objects.emplace(std::move(key), std::move(object));
    };
    add_checksum_object("checksum-retry.bin", 1, 128U * 1024U);
    add_checksum_object("checksum-release.bin", 1, 128U * 1024U);
    add_checksum_object("checksum-rename.bin", 1, 4U * 1024U * 1024U);
    {
      auto& object = *shared.special_objects.at("checksum-rename.bin");
      object.bytes.resize(4U * 1024U * 1024U);
      object.corrupted_bytes = object.bytes;
      object.corrupted_bytes[12345] ^= std::byte{0x80};
    }
    add_checksum_object("checksum-fail.bin", 2);
    add_checksum_object("checksum-unsupported.bin", 0, 0, true);
    add_checksum_object("budget-retry.bin", 1);
    shared.special_objects.at("budget-retry.bin")->bytes.resize(32U * 1024U + 37U);
    shared.special_objects.at("budget-retry.bin")->corrupted_bytes.resize(32U * 1024U + 37U);
    add_checksum_object("invalid-content-range.bin", 0);
    shared.special_objects.at(
        "invalid-content-range.bin")->invalid_content_range = true;
    std::jthread server(run_server, listener.socket.get(), std::ref(shared));

    mountpoint = make_mountpoint();
    if (argc >= 4) {
      if (std::string_view(argv[3]) != "plain" &&
          std::string_view(argv[3]) != "cache" && !prefetch_mode &&
          !cache_blocks && !passthrough_mode) {
        throw std::invalid_argument("unknown integration-test mode");
      }
      if (std::string_view(argv[3]) == "cache" || cache_blocks ||
          passthrough_mode) {
        cache_dir = make_mountpoint();
      }
    }
    const std::string checksum_option(checksum_option_name(checksum));
    // Legacy policy checks retain four 1 MiB random windows. The default
    // file-size cap also reserves demand space and intentionally shrinks the
    // fourth window; 8 MiB covers this 4 MiB working set plus that reserve.
    // Dedicated memory-pressure modes continue to use their smaller caps.
    const char* memory_budget = pool_pressure ? "8MiB" :
        pool_prefetch ? "128MiB" : legacy_window_test ? "8MiB" : nullptr;
    pid_t process;
    std::string trace_path;
    if (passthrough_mode) {
      if (trace_passthrough && argc != 7) {
        throw std::invalid_argument(
            "root passthrough integration mode requires strace");
      }
      if (trace_passthrough) {
        trace_dir = make_mountpoint();
        trace_path = trace_dir + "/ioctl.trace";
      }
      process = start_passthrough_daemon(
          argv[1], mountpoint, listener.port, checksum_option, cache_dir,
          trace_passthrough ? argv[6] : "", trace_path,
          passthrough_unlimited);
    } else {
      process = start_daemon(
          argv[1], mountpoint, listener.port, checksum_option, cache_dir,
          engine, reactors,
          !cache_blocks && (!prefetch_mode || verified_prefetch ||
                            budget_prefetch || partial_prefetch),
          verified_prefetch, budget_prefetch,
          pool_limited ? "16777216" : nullptr, memory_budget,
          cache_blocks_small ? "96KiB" : "2MiB");
    }
    MountedProcess mounted(mountpoint, process);
    const std::string file_path = mountpoint + "/mmap.bin";
    wait_until_mounted(file_path, process);

    if (passthrough_mode) {
      UniqueFd initial(::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
      require(bool(initial), "open initial ordinary cached reader");
      struct stat initial_status{};
      require(::fstat(initial.get(), &initial_status) == 0,
              "stat initial ordinary cached reader");
      std::vector<std::byte> initial_bytes(expected.size());
      pread_all(initial.get(), initial_bytes, 0);
      require(initial_bytes == expected,
              "initial full-cache download returned wrong bytes");
      initial.reset();

      UniqueFd first;
      TraceIoctlStats open_stats;
      if (trace_passthrough) {
        // A READ reply can complete just before the daemon processes RELEASE.
        // Such an intervening OPEN is correctly kept ordinary, so close it and
        // give the next OPEN a chance to perform the promotion.
        for (unsigned attempt = 0; attempt != 200; ++attempt) {
          first.reset(::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
          require(bool(first), "open passthrough candidate");
          open_stats = trace_ioctl_stats(
              trace_path, "FUSE_DEV_IOC_BACKING_OPEN", true);
          if (open_stats.calls != 0) break;
          first.reset();
          std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        require(open_stats.calls != 0,
                "no backing registration was attempted after normal RELEASE");
      } else {
        first.reset(::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
        require(bool(first), "open budget-limited cached reader");
      }
      UniqueFd second(::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
      require(bool(second), "open simultaneous passthrough candidate");
      struct stat first_status{};
      struct stat second_status{};
      require(::fstat(first.get(), &first_status) == 0 &&
                  ::fstat(second.get(), &second_status) == 0,
              "stat passthrough candidates");
      require(first_status.st_ino == initial_status.st_ino &&
                  second_status.st_ino == initial_status.st_ino,
              "passthrough promotion changed the FUSE inode number");

      std::vector<std::byte> first_bytes(expected.size());
      std::vector<std::byte> second_bytes(expected.size());
      pread_all(first.get(), first_bytes, 0);
      pread_all(second.get(), second_bytes, 0);
      require(first_bytes == expected && second_bytes == expected,
              "simultaneous readonly candidates returned wrong bytes");

      void* old_mapping = ::mmap(nullptr, expected.size(), PROT_READ,
                                 MAP_SHARED, first.get(), 0);
      if (old_mapping == MAP_FAILED) fail_errno("mmap passthrough candidate");
      require(memcmp(old_mapping, expected.data(), expected.size()) == 0,
              "passthrough candidate mmap returned wrong bytes");
      first.reset();
      require(memcmp(old_mapping, expected.data(), expected.size()) == 0,
              "mmap stopped working after its readonly fd closed");

      if (trace_passthrough) {
        open_stats = trace_ioctl_stats(
            trace_path, "FUSE_DEV_IOC_BACKING_OPEN", true);
      }
      const bool native_passthrough = open_stats.successes != 0;
      const bool passthrough_unavailable = trace_passthrough &&
          !native_passthrough && open_stats.calls != 0 &&
          open_stats.unavailable == open_stats.calls;
      if (trace_passthrough && native_passthrough) {
        require(open_stats.calls == 1 && open_stats.successes == 1,
                "simultaneous readonly opens did not reuse one backing ID");
      } else if (trace_passthrough) {
        require(passthrough_unavailable,
                "backing registration failed without an explicit "
                "EPERM/ENOSYS/EOPNOTSUPP limitation");
      }

      second.reset();
      if (native_passthrough) {
        for (unsigned attempt = 0; attempt != 200 &&
             trace_ioctl_stats(trace_path,
                               "FUSE_DEV_IOC_BACKING_CLOSE", false)
                 .successes == 0;
             ++attempt) {
          std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        const TraceIoctlStats close_stats = trace_ioctl_stats(
            trace_path, "FUSE_DEV_IOC_BACKING_CLOSE", false);
        require(close_stats.calls == 1 && close_stats.successes == 1,
                "last readonly close did not close the shared backing ID");
      } else {
        require(::munmap(old_mapping, expected.size()) == 0,
                "unmap ordinary fallback mapping");
        old_mapping = MAP_FAILED;
      }

      std::vector<std::byte> replacement(expected.size());
      for (size_t i = 0; i < replacement.size(); ++i) {
        replacement[i] = std::byte((i * 61U + 37U) & 0xffU);
      }
      UniqueFd writer;
      retry_after_fuse_release("open replacement after passthrough", [&] {
        writer.reset(::open(file_path.c_str(),
                            O_WRONLY | O_TRUNC | O_CLOEXEC));
        return writer ? 0 : -1;
      });
      test_write_all(writer.get(), replacement);
      writer.reset();

      if (native_passthrough) {
        require(memcmp(old_mapping, expected.data(), expected.size()) == 0,
                "old passthrough mmap changed after generation replacement");
      }
      UniqueFd replacement_reader;
      retry_after_fuse_release("open replacement generation", [&] {
        replacement_reader.reset(
            ::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
        return replacement_reader ? 0 : -1;
      });
      struct stat replacement_status{};
      require(::fstat(replacement_reader.get(), &replacement_status) == 0 &&
                  replacement_status.st_ino == initial_status.st_ino,
              "replacement generation changed the FUSE inode number");
      std::vector<std::byte> replacement_bytes(replacement.size());
      pread_all(replacement_reader.get(), replacement_bytes, 0);
      require(replacement_bytes == replacement,
              "new readonly open did not see the replacement generation");
      if (old_mapping != MAP_FAILED) {
        require(::munmap(old_mapping, expected.size()) == 0,
                "unmap old passthrough generation");
      }
      replacement_reader.reset();

      mounted.stop();
      require(mounted.stopped_cleanly(),
              "passthrough wiring mount did not exit cleanly");
      server.request_stop();
      server.join();
      rethrow_server_failure(shared);
      require(::rmdir(mountpoint.c_str()) == 0,
              "remove passthrough wiring mountpoint");
      std::filesystem::remove_all(cache_dir);
      cache_dir.clear();
      mountpoint.clear();

      if (passthrough_unavailable) {
        fprintf(stdout,
                "SKIP: kernel/libfuse explicitly denied native passthrough; "
                "ordinary unlimited-cache fallback passed; trace retained "
                "at %s\n", trace_path.c_str());
        return 77;
      } else if (trace_passthrough) {
        fprintf(stdout,
                "FUSE native passthrough OPEN wiring, shared backing, mmap "
                "lifetime and immutable replacement passed\n");
      } else if (passthrough_unlimited) {
        fprintf(stdout,
                "FUSE unlimited-cache ordinary fallback semantics passed; "
                "native passthrough evidence requires root\n");
      } else {
        fprintf(stdout,
                "FUSE budget-limited cache correctly retained ordinary "
                "cached I/O\n");
      }
      if (!trace_dir.empty()) {
        std::filesystem::remove_all(trace_dir);
        trace_dir.clear();
      }
      return 0;
    }

    if (cache_blocks) {
      const size_t block = cache_blocks_small ? 96 * 1024 : 2 * 1024 * 1024;
      const size_t page = size_t(::sysconf(_SC_PAGESIZE));
      const auto wait = [&](const auto& condition, const char* message) {
        for (unsigned i = 0; i != 3000 && !condition(); ++i) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        require(condition(), message);
      };
      auto object = shared.special_objects.at("cache-blocks.bin");
      UniqueFd fd(::open((mountpoint + "/cache-blocks.bin").c_str(), O_RDONLY | O_CLOEXEC));
      UniqueFd sibling(::open((mountpoint + "/cache-blocks.bin").c_str(), O_RDONLY | O_CLOEXEC));
      require(bool(fd) && bool(sibling), "open cached block readers");
      require(::posix_fadvise(fd.get(), 0, 0, POSIX_FADV_RANDOM) == 0 &&
                  ::posix_fadvise(sibling.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
              "disable kernel readahead for cached block checks");
      object->send_limit.store(block - page, std::memory_order_release);
      std::array<std::atomic<bool>, 2> done{};
      std::array<std::exception_ptr, 2> errors;
      std::array<std::vector<std::byte>, 2> bytes{std::vector<std::byte>(page), std::vector<std::byte>(page)};
      std::array<std::jthread, 2> readers;
      struct Resume {
        SpecialObject& object;
        ~Resume() { object.send_limit.store(SIZE_MAX, std::memory_order_release); }
      } resume{*object};
      const auto launch = [&](unsigned i, int file, size_t offset) {
        readers[i] = std::jthread([&, i, file, offset] {
          try { pread_all(file, bytes[i], offset); }
          catch (...) { errors[i] = std::current_exception(); }
          done[i].store(true, std::memory_order_release);
        });
      };
      launch(0, fd.get(), page);
      wait([&] { return object->tail_paused.load(std::memory_order_acquire); },
           "cached GET did not stop before its last block page");
      launch(1, sibling.get(), block / 2);
      std::this_thread::sleep_for(std::chrono::milliseconds(30));
      require(!done[0].load(std::memory_order_acquire) && !done[1].load(std::memory_order_acquire),
              "cached READ replied before its complete block was stored");
      {
        std::lock_guard guard(shared.mutex);
        require(object->get_ranges.size() == 1 && object->get_ranges[0].first == 0 &&
                    object->get_ranges[0].last == block - 1,
                "cached pending overlap duplicated GET or random GET was not block aligned");
      }
      object->send_limit.store(SIZE_MAX, std::memory_order_release);
      for (unsigned i = 0; i != readers.size(); ++i) {
        readers[i].join();
        if (errors[i]) std::rethrow_exception(errors[i]);
        const size_t offset = i ? block / 2 : page;
        require(std::equal(bytes[i].begin(), bytes[i].end(), object->bytes.begin() + offset),
                "cached block reader received wrong bytes");
      }
      std::vector<std::byte> cross(2 * page);
      pread_all(fd.get(), cross, block - page);
      require(std::equal(cross.begin(), cross.end(), object->bytes.begin() + block - page),
              "cached READ crossing complete and missing blocks differs");
      require(::posix_fadvise(fd.get(), 0, off_t(2 * block), POSIX_FADV_DONTNEED) == 0,
              "drop ordinary FUSE pages for cached-hit check");
      pread_all(fd.get(), cross, block - page);
      {
        std::lock_guard guard(shared.mutex);
        require(object->get_ranges.size() == 2 && object->get_ranges.back().first == block &&
                    object->get_ranges.back().last == 2 * block - 1,
                "cached cross-block read refetched complete data");
      }
      std::vector<std::byte> tail(37);
      pread_all(fd.get(), tail, object->bytes.size() - tail.size());
      require(std::equal(tail.begin(), tail.end(), object->bytes.end() - tail.size()),
              "cached EOF bytes differ");
      {
        std::lock_guard guard(shared.mutex);
        const auto& range = object->get_ranges.back();
        require(range.first == (object->bytes.size() - 1) / block * block &&
                    range.last == object->bytes.size() - 1,
                "cached EOF GET was not aligned and clipped");
        require(shared.checksum_mode_requests == 0 && shared.object_attributes_requests == 0,
                "unchecked cached reader requested checksum metadata");
      }
      fd.reset();
      sibling.reset();

      int heads_before_reopen;
      size_t gets_before_reopen;
      {
        std::lock_guard guard(shared.mutex);
        heads_before_reopen = shared.head_requests;
        gets_before_reopen = object->get_ranges.size();
      }
      for (unsigned i = 0; i != 8; ++i) {
        UniqueFd reopened(::open((mountpoint + "/cache-blocks.bin").c_str(),
                                  O_RDONLY | O_CLOEXEC));
        require(bool(reopened), "reopen retained cached metadata");
        require(::posix_fadvise(reopened.get(), 0, 0, POSIX_FADV_RANDOM) == 0 &&
                    ::posix_fadvise(reopened.get(), 0, off_t(2 * block),
                                    POSIX_FADV_DONTNEED) == 0,
                "drop FUSE pages for retained metadata reopen");
        pread_all(reopened.get(), cross, block - page);
        require(std::equal(cross.begin(), cross.end(),
                           object->bytes.begin() + block - page),
                "retained cached metadata reopened with different bytes");
      }
      {
        std::lock_guard guard(shared.mutex);
        require(shared.head_requests == heads_before_reopen + 8,
                "cached metadata reuse skipped per-open HEAD validation");
        require(object->get_ranges.size() == gets_before_reopen,
                "cached metadata reopen unnecessarily downloaded clean data");
      }

      auto sequence = shared.special_objects.at("cache-sequential.bin");
      UniqueFd seq(::open((mountpoint + "/cache-sequential.bin").c_str(), O_RDONLY | O_CLOEXEC));
      require(bool(seq), "open cached sequential reader");
      require(::posix_fadvise(seq.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
              "disable kernel readahead for cached window checks");
      sequence->send_limit.store(block, std::memory_order_release);
      std::atomic<bool> first_done{false};
      std::exception_ptr first_error;
      std::vector<std::byte> first(page);
      std::jthread first_reader([&] {
        try { pread_all(seq.get(), first, 0); }
        catch (...) { first_error = std::current_exception(); }
        first_done.store(true, std::memory_order_release);
      });
      Resume resume_sequence{*sequence};
      wait([&] { return sequence->tail_paused.load(std::memory_order_acquire); },
           "cached sequential GET did not stop after first block");
      wait([&] { return first_done.load(std::memory_order_acquire); },
           "cached READ waited for the speculative GET tail after its block completed");
      first_reader.join();
      if (first_error) std::rethrow_exception(first_error);
      require(std::equal(first.begin(), first.end(), sequence->bytes.begin()),
              "cached sequential first block differs");
      const size_t initial = (8 * 1024 * 1024 + block - 1) / block * block;
      {
        std::lock_guard guard(shared.mutex);
        require(sequence->get_ranges.size() == 1 && sequence->get_ranges[0].first == 0 &&
                    sequence->get_ranges[0].last == initial - 1,
                "cached sequential initial GET is not eight MiB rounded to blocks");
      }
      // A non-adjacent hit in the pending window must neither create a GET
      // nor reset sequential growth. Completing that block releases its READ
      // while the remaining GET is still deliberately paused.
      first_done.store(false, std::memory_order_release);
      first_reader = std::jthread([&] {
        try { pread_all(seq.get(), first, block + page); }
        catch (...) { first_error = std::current_exception(); }
        first_done.store(true, std::memory_order_release);
      });
      sequence->send_limit.store(2 * block, std::memory_order_release);
      wait([&] { return first_done.load(std::memory_order_acquire); },
           "cached pending-window READ did not resume at its block boundary");
      first_reader.join();
      if (first_error) std::rethrow_exception(first_error);
      require(std::equal(first.begin(), first.end(), sequence->bytes.begin() + block + page),
              "cached pending-window READ differs");
      sequence->send_limit.store(SIZE_MAX, std::memory_order_release);
      size_t begin = 0;
      size_t window = initial;
      for (unsigned n = 0; n != 3; ++n) {
        const size_t end = std::min(sequence->bytes.size(), begin + window);
        pread_all(seq.get(), first, end - page);
        require(std::equal(first.begin(), first.end(), sequence->bytes.begin() + end - page),
                "cached sequential window tail differs");
        {
          std::lock_guard guard(shared.mutex);
          require(sequence->get_ranges.size() == n + 1 &&
                      sequence->get_ranges.back().first == begin &&
                      sequence->get_ranges.back().last == end - 1,
                  "cached sequential GET did not double once per consumed window");
        }
        if (end == sequence->bytes.size()) break;
        begin = end;
        window *= 2;
        pread_all(seq.get(), first, begin);
      }
      seq.reset();
      mounted.stop();
      require(mounted.stopped_cleanly(), "cached block mount did not exit cleanly");
      shared.stop.store(true);
      server.request_stop();
      server.join();
      require(::rmdir(mountpoint.c_str()) == 0, "remove cached block mountpoint");
      std::filesystem::remove_all(cache_dir);
      fprintf(stderr, "cached %zu-byte blocks: whole-block gate, pending dedup, hits, crossing, EOF and window growth passed\n", block);
      return 0;
    }

    if (pressure_prefetch) {
      std::array<UniqueFd, 8> files;
      std::array<std::shared_ptr<SpecialObject>, 8> objects;
      struct ResumePressure {
        decltype(objects)& values;
        ~ResumePressure() {
          for (const auto& value : values) {
            if (value) value->resume_tail.store(true, std::memory_order_release);
          }
        }
      } resume{objects};
      // Open every handle before filling the seven bulk slots, and suppress
      // the kernel's independent readahead so each demand is deterministic.
      for (unsigned i = 0; i != files.size(); ++i) {
        const std::string name = "pressure-" + std::to_string(i) + ".bin";
        {
          std::lock_guard guard(shared.mutex);
          objects[i] = shared.special_objects.at(name);
        }
        files[i].reset(::open((mountpoint + '/' + name).c_str(), O_RDONLY | O_CLOEXEC));
        if (!files[i]) fail_errno("open prefetch pressure object");
        require(::posix_fadvise(files[i].get(), 0, 0, POSIX_FADV_RANDOM) == 0,
                "disable kernel readahead for prefetch pressure");
      }
      std::array<std::byte, 4096> bytes{};
      for (unsigned i = 0; i != 7; ++i) {
        pread_all(files[i].get(), bytes, 0);
        require(std::equal(bytes.begin(), bytes.end(), read_ahead.begin()),
                "prefetch pressure demand bytes differ");
        for (unsigned attempt = 0; attempt != 2000 &&
             !objects[i]->tail_paused.load(std::memory_order_acquire); ++attempt) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        require(objects[i]->tail_paused.load(std::memory_order_acquire),
                "prefetch pressure bulk GET did not reach its pause point");
        {
          std::lock_guard guard(shared.mutex);
          const auto& ranges = objects[i]->get_ranges;
          require(ranges.size() == 1 && ranges[0].first == 0 &&
                      ranges[0].last == 1024U * 1024U - 1,
                  "prefetch pressure bulk GET was not 1 MiB");
        }
      }
      pread_all(files.back().get(), bytes, 0);
      require(std::equal(bytes.begin(), bytes.end(), read_ahead.begin()),
              "prefetch pressure final demand bytes differ");
      {
        std::lock_guard guard(shared.mutex);
        const auto& ranges = objects.back()->get_ranges;
        require(ranges.size() == 1 && ranges[0].first == 0 &&
                    ranges[0].last == 256U * 1024U - 1,
                "busy prefetch pressure did not use a 256 KiB expansion");
      }
      for (auto& object : objects) object->resume_tail.store(true, std::memory_order_release);
      for (auto& fd : files) fd.reset();
      mounted.stop();
      shared.stop.store(true);
      server.request_stop();
      server.join();
      require(::rmdir(mountpoint.c_str()) == 0, "remove prefetch pressure mountpoint");
      fprintf(stderr, "seven paused prefetch windows force a 256 KiB expansion: passed\n");
      return 0;
    }

    if (pool_pressure) {
      constexpr size_t block = 2U * 1024U * 1024U;
      const size_t page = size_t(::sysconf(_SC_PAGESIZE));
      const size_t off = block - page;
      std::vector<std::byte> bytes(2 * page);
      const auto wait = [](auto&& ready, const char* message) {
        for (unsigned n = 0; n != 3000; ++n) {
          if (ready()) return;
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        throw std::runtime_error(message);
      };
      const auto idle = [&] {
        wait([&] {
          std::lock_guard guard(shared.mutex);
          return shared.active_gets == 0;
        }, "partial-GET pressure did not drain");
      };
      // Pressure can reclaim a completed, unpinned block even though its
      // GET is still receiving the next block. Waiting for the full GET
      // here would prevent the unrelated demand from making progress.
      auto inflight = shared.special_objects.at("budget-inflight.bin");
      auto pinned = shared.special_objects.at("budget-pinned.bin");
      inflight->send_limit.store(block, std::memory_order_release);
      pinned->send_limit.store(block, std::memory_order_release);
      UniqueFd a(::open((mountpoint + "/budget-inflight.bin").c_str(), O_RDONLY | O_CLOEXEC));
      UniqueFd b(::open((mountpoint + "/budget-demand.bin").c_str(), O_RDONLY | O_CLOEXEC));
      UniqueFd c(::open((mountpoint + "/budget-pinned.bin").c_str(), O_RDONLY | O_CLOEXEC));
      require(bool(a) && bool(b) && bool(c), "open partial-GET budget pressure readers");
      require(::posix_fadvise(a.get(), 0, 0, POSIX_FADV_RANDOM) == 0 &&
                  ::posix_fadvise(b.get(), 0, 0, POSIX_FADV_RANDOM) == 0 &&
                  ::posix_fadvise(c.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
              "disable kernel prefetch for partial-GET pressure");
      std::array<std::byte, 128U * 1024U> first{}, other{};
      std::vector<std::byte> crossing(2 * page);
      std::array<std::atomic<bool>, 3> completed{};
      std::array<std::exception_ptr, 3> failures{};
      std::array<std::jthread, 3> pending;
      struct ResumeBudget {
        SpecialObject& object;
        ~ResumeBudget() { object.send_limit.store(SIZE_MAX, std::memory_order_release); }
      } resume{*inflight};
      ResumeBudget resume_pinned{*pinned};
      pending[0] = std::jthread([&] {
        try { pread_all(a.get(), first, 0); }
        catch (...) { failures[0] = std::current_exception(); }
        completed[0].store(true, std::memory_order_release);
      });
      wait([&] { return completed[0].load(std::memory_order_acquire) &&
                        inflight->tail_paused.load(std::memory_order_acquire); },
           "budget-shrunk sequential GET did not publish its complete first block");
      pending[0].join();
      if (failures[0]) std::rethrow_exception(failures[0]);
      require(std::equal(first.begin(), first.end(), inflight->bytes.begin()),
              "partial-GET pressure first READ differs");
      {
        std::lock_guard guard(shared.mutex);
        const auto& ranges = inflight->get_ranges;
        require(ranges.size() == 1 && ranges[0].first == 0 &&
                    ranges[0].last == 2 * block - 1 && shared.active_gets > 0,
                "sequential GET did not leave 4 MiB reserved for demand within the 8 MiB cap");
      }
      pending[2] = std::jthread([&] {
        try {
          require(::readahead(c.get(), off_t(off), crossing.size()) == 0,
                  "submit batched cross-block readahead under pressure");
          pread_all(c.get(), crossing, off);
        } catch (...) { failures[2] = std::current_exception(); }
        completed[2].store(true, std::memory_order_release);
      });
      wait([&] { return pinned->tail_paused.load(std::memory_order_acquire); },
           "cross-block pressure request did not pin both blocks before its pause");
      require(!completed[2].load(std::memory_order_acquire),
              "cross-block pressure request exposed its incomplete second block");
      {
        std::lock_guard guard(shared.mutex);
        const auto& ranges = pinned->get_ranges;
        require(ranges.size() == 1 && ranges[0].first == 0 &&
                    ranges[0].last == 2 * block - 1,
                "explicit cross-block readahead did not reserve both demand blocks together");
      }
      pending[1] = std::jthread([&] {
        try { pread_all(b.get(), other, 4096); }
        catch (...) { failures[1] = std::current_exception(); }
        completed[1].store(true, std::memory_order_release);
      });
      wait([&] { return completed[1].load(std::memory_order_acquire); },
           "unrelated demand could not evict a complete block from a pending GET");
      pending[1].join();
      if (failures[1]) std::rethrow_exception(failures[1]);
      require(std::equal(other.begin(), other.end(), read_ahead.begin() + 4096),
              "partial-GET pressure unrelated READ differs");
      require(!completed[2].load(std::memory_order_acquire),
              "pressure exposed pinned bytes before their receive completed");
      inflight->send_limit.store(SIZE_MAX, std::memory_order_release);
      pinned->send_limit.store(SIZE_MAX, std::memory_order_release);
      wait([&] { return completed[2].load(std::memory_order_acquire); },
           "pinned crossing demand did not resume after pressure");
      pending[2].join();
      if (failures[2]) std::rethrow_exception(failures[2]);
      require(std::equal(crossing.begin(), crossing.end(), read_ahead.begin() + off),
              "pinned crossing pressure READ differs");
      idle();
      require(::readahead(a.get(), off_t(off), bytes.size()) == 0,
              "submit batched READ across evicted and retained blocks");
      pread_all(a.get(), bytes, off);
      require(std::equal(bytes.begin(), bytes.end(), inflight->bytes.begin() + off),
              "cross-GET READ did not recover an evicted block beside retained coverage");
      idle();
      {
        std::lock_guard guard(shared.mutex);
        for (const char* name : {"budget-inflight.bin", "budget-pinned.bin", "budget-demand.bin"}) {
          fprintf(stderr, "partial-GET pressure %s ranges:", name);
          for (const auto& range : shared.special_objects.at(name)->get_ranges) {
            fprintf(stderr, " [%llu,%llu]", (unsigned long long)range.first,
                    (unsigned long long)range.last);
          }
          fprintf(stderr, "\n");
        }
        const auto& ranges = inflight->get_ranges;
        require(ranges.size() == 2 && ranges.back().first == 0 &&
                    ranges.back().last == block - 1,
                "pressure-evicted block was not refetched without duplicating the retained block");
      }
      a.reset();
      b.reset();
      c.reset();
      mounted.stop();
      require(mounted.stopped_cleanly(), "partial-GET pressure shutdown was not clean");
      shared.stop.store(true);
      server.request_stop();
      server.join();
      require(::rmdir(mountpoint.c_str()) == 0, "remove partial-GET pressure mountpoint");
      fprintf(stderr, "8 MiB stress-only budget: reclaim complete block from pending GET, pinned crossing demand, aligned refetch: passed\n");
      return 0;
    }

    if (budget_prefetch) {
      constexpr std::array<const char*, 3> names{
          "read-ahead.bin", "read-ahead-sequential.bin", "read-ahead-random.bin"};
      std::array<std::exception_ptr, 8> errors;
      std::atomic<bool> start{false};
      std::vector<std::jthread> readers;
      for (unsigned n = 0; n < errors.size(); ++n) {
        readers.emplace_back([&, n] {
          try {
            UniqueFd fd(::open((mountpoint + "/" + names[n % names.size()]).c_str(), O_RDONLY | O_CLOEXEC));
            if (!fd) fail_errno("open budget concurrent reader");
            require(::posix_fadvise(fd.get(), 0, 0, POSIX_FADV_RANDOM) == 0, "budget random advice");
            while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
            std::array<std::byte, 16 * 1024> bytes;
            for (unsigned i = 0; i < 64; ++i) {
              const size_t offset = ((i * 71 + n * 23) % 240) * 16U * 1024U;
              pread_all(fd.get(), bytes, offset);
              require(std::equal(bytes.begin(), bytes.end(), read_ahead.begin() + offset),
                      "low-budget concurrent read differs");
            }
          } catch (...) { errors[n] = std::current_exception(); }
        });
      }
      start.store(true, std::memory_order_release);
      for (auto& reader : readers) reader.join();
      for (const auto& error : errors) if (error) std::rethrow_exception(error);
      if (engine == "uring") {
        constexpr size_t block = 2U * 1024U * 1024U;
        const auto wait = [](auto&& ready, const char* message) {
          for (unsigned n = 0; n != 3000; ++n) {
            if (ready()) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          }
          throw std::runtime_error(message);
        };
        const auto idle = [&] {
          wait([&] {
            std::lock_guard guard(shared.mutex);
            return shared.active_gets == 0;
          }, "low-budget GET did not complete");
        };
        // The minimum 4 MiB cap must admit the whole crossing READ: pinning
        // its retained first block cannot starve the missing second block.
        UniqueFd fd(::open((mountpoint + "/read-ahead-store.bin").c_str(), O_RDONLY | O_CLOEXEC));
        require(bool(fd), "open tiny-budget crossing object");
        require(::posix_fadvise(fd.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
                "tiny-budget crossing random advice");
        std::array<std::byte, 128U * 1024U> bytes{};
        pread_all(fd.get(), bytes, 4096);
        idle();
        const size_t off = block - bytes.size() / 2;
        require(::readahead(fd.get(), off_t(off), bytes.size()) == 0,
                "submit batched minimum-budget cross-block readahead");
        pread_all(fd.get(), bytes, off);
        require(std::equal(bytes.begin(), bytes.end(), read_ahead.begin() + off),
                "tiny-budget crossing READ did not preserve retained coverage");
        idle();
        {
          std::lock_guard guard(shared.mutex);
          const auto& ranges = shared.special_objects.at("read-ahead-store.bin")->get_ranges;
          require(ranges.size() == 2 && ranges[0].first == 0 &&
                      ranges[0].last == block - 1 && ranges[1].first == block &&
                      ranges[1].last == 2 * block - 1,
                  "tiny-budget crossing READ did not fetch exactly its missing aligned block");
        }
        fd.reset();
        // All requests straddle the allocation boundary. The 4 MiB mount
        // budget fits only one such demand; concurrent files must queue and
        // progress without retaining a partial set of pins forever.
        readers.clear();
        errors.fill(nullptr);
        start.store(false, std::memory_order_release);
        for (unsigned n = 0; n < errors.size(); ++n) {
          readers.emplace_back([&, n] {
            try {
              UniqueFd cross_fd(::open((mountpoint + "/" + names[n % names.size()]).c_str(),
                                       O_RDONLY | O_CLOEXEC));
              require(bool(cross_fd), "open concurrent cross-block budget reader");
              require(::posix_fadvise(cross_fd.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
                      "cross-block budget random advice");
              while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
              std::array<std::byte, 128U * 1024U> data{};
              for (unsigned i = 0; i != 12; ++i) {
                require(::posix_fadvise(cross_fd.get(), off_t(off), off_t(data.size()),
                                        POSIX_FADV_DONTNEED) == 0,
                        "drop cross-block pages for budget pressure");
                require(::readahead(cross_fd.get(), off_t(off), data.size()) == 0,
                        "submit concurrent batched cross-block readahead");
                pread_all(cross_fd.get(), data, off);
                require(std::equal(data.begin(), data.end(), read_ahead.begin() + off),
                        "concurrent cross-block budget READ differs");
              }
            } catch (...) { errors[n] = std::current_exception(); }
          });
        }
        start.store(true, std::memory_order_release);
        for (auto& reader : readers) reader.join();
        for (const auto& error : errors) if (error) std::rethrow_exception(error);
      }
      if (engine != "uring") {
        UniqueFd retry(::open((mountpoint + "/budget-retry.bin").c_str(), O_RDONLY | O_CLOEXEC));
        if (!retry) fail_errno("open demand-only checksum retry");
        std::vector<std::byte> bytes(32U * 1024U + 37U);
        pread_all(retry.get(), bytes, 0);
        bool corrected = false;
        for (unsigned i = 0; i < 2000; ++i) {
          int gets;
          { std::lock_guard guard(shared.mutex); gets = shared.special_objects.at("budget-retry.bin")->get_requests; }
          pread_all(retry.get(), bytes, 0);
          if (gets == 2 && std::equal(bytes.begin(), bytes.end(), checksum_test.begin())) {
            corrected = true;
            break;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        require(corrected, "demand-only retry did not progress within the memory limit");
      }
      mounted.stop();
      shared.stop.store(true);
      server.request_stop();
      server.join();
      if (::rmdir(mountpoint.c_str()) != 0) fail_errno("rmdir budget mountpoint");
      fprintf(stderr, "%s mount/file limits: 8 readers, 3 files, 512 random reads%s passed\n",
              engine == "uring" ? "4 MiB" : "2 MiB",
              engine == "uring" ? " and 96 cross-block reads" :
                                  " and demand-only checksum retry");
      return 0;
    }

    if (pool_prefetch) {
      constexpr size_t block = 2U * 1024U * 1024U;
      const size_t offset = shutdown_prefetch ? 0 : 4096;
      auto object = shared.special_objects.at("pool-random.bin");
      object->send_limit.store(shutdown_prefetch ? block : 512U * 1024U,
                               std::memory_order_release);
      const std::string path = mountpoint + "/pool-random.bin";
      UniqueFd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
      if (!fd) fail_errno("open pooled prefetch object");
      UniqueFd sibling(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
      require(bool(sibling), "open cross-handle pooled reader");
      require(::posix_fadvise(fd.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
              "disable kernel read-ahead for pooled prefetch");
      require(::posix_fadvise(sibling.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
              "disable cross-handle kernel read-ahead for pooled prefetch");
      const auto wait = [](auto&& ready, const char* message) {
        for (unsigned n = 0; n != 3000; ++n) {
          if (ready()) return;
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        throw std::runtime_error(message);
      };
      const auto idle = [&] {
        wait([&] {
          std::lock_guard guard(shared.mutex);
          return shared.active_gets == 0;
        }, "pooled GET did not complete");
      };
      const size_t page = size_t(::sysconf(_SC_PAGESIZE));
      const size_t probe = offset + 384U * 1024U;
      void* mapping = ::mmap(nullptr, page, PROT_READ, MAP_SHARED, fd.get(), off_t(probe));
      require(mapping != MAP_FAILED, "map unfaulted pooled prefetch probe");
      struct UnmapPool {
        void* mapping;
        size_t length;
        ~UnmapPool() { if (mapping != MAP_FAILED) ::munmap(mapping, length); }
      } unmap{mapping, page};
      unsigned char resident = 0;
      require(::mincore(mapping, page, &resident) == 0 && !(resident & 1),
              "pooled prefetch probe unexpectedly resident before READ");
      std::array<std::byte, 128U * 1024U> first{};
      std::array<std::byte, 128U * 1024U> second{};
      std::array<std::atomic<bool>, 2> done{};
      std::array<std::exception_ptr, 2> errors{};
      std::vector<std::jthread> readers;
      struct ResumePool {
        SpecialObject& object;
        ~ResumePool() { object.send_limit.store(SIZE_MAX, std::memory_order_release); }
      } resume{*object};
      readers.emplace_back([&] {
        try {
          pread_all(fd.get(), first, offset);
        } catch (...) {
          errors[0] = std::current_exception();
        }
        done[0].store(true, std::memory_order_release);
      });
      wait([&] { return object->tail_paused.load(std::memory_order_acquire); },
           "pooled GET did not reach its controlled pause");
      {
        std::lock_guard guard(shared.mutex);
        const auto& ranges = object->get_ranges;
        require(ranges.size() == 1 && ranges[0].first == 0 &&
                    ranges[0].last == (shutdown_prefetch ? 4 * block : block) - 1,
                "pooled GET did not use aligned 2 MiB random or 8 MiB sequential coverage");
        require(shared.active_gets > 0,
                "pooled GET completed before the paused-tail observation");
        require(shared.checksum_mode_requests == 0,
                "new uncached read path unexpectedly requested checksum verification");
      }
      if (shutdown_prefetch) {
        wait([&] { return done[0].load(std::memory_order_acquire); },
             "READ waited beyond its complete block for the paused sequential GET tail");
      } else {
        // The first READ's entire 128 KiB is already on the wire, but its
        // containing 2 MiB block is incomplete and must not be published yet.
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        require(!done[0].load(std::memory_order_acquire),
                "pooled READ replied before its entire 2 MiB block arrived");
        readers.emplace_back([&] {
          try {
            pread_all(sibling.get(), second, offset + 768U * 1024U);
          } catch (...) {
            errors[1] = std::current_exception();
          }
          done[1].store(true, std::memory_order_release);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        require(!done[1].load(std::memory_order_acquire),
                "pooled READ exposed bytes beyond completed network input");
        {
          std::lock_guard guard(shared.mutex);
          require(object->get_requests == 1,
                  "cross-handle pending READ issued a duplicate GET");
        }
        object->send_limit.store(SIZE_MAX, std::memory_order_release);
        wait([&] { return done[0].load(std::memory_order_acquire) &&
                          done[1].load(std::memory_order_acquire); },
             "whole-block readers did not complete after the existing receive resumed");
        readers[1].join();
        if (errors[1]) std::rethrow_exception(errors[1]);
        require(std::equal(second.begin(), second.end(),
                           object->bytes.begin() + offset + 768U * 1024U),
                "pooled READ after the block became complete differs");
      }
      readers[0].join();
      if (errors[0]) std::rethrow_exception(errors[0]);
      require(std::equal(first.begin(), first.end(), object->bytes.begin() + offset),
              "pooled prefetch initial READ differs");
      require(::mincore(mapping, page, &resident) == 0 && !(resident & 1),
              "uncached pooled prefetch proactively STOREd an unrelated page");
      if (!shutdown_prefetch) {
        idle();
        require(::mincore(mapping, page, &resident) == 0 && !(resident & 1),
                "completed uncached pooled prefetch proactively STOREd unrelated data");
        {
          std::lock_guard guard(shared.mutex);
          require(object->get_requests == 1,
                  "overlapping pending READ issued a duplicate GET");
        }
        // The next random miss fetches the aligned block. A later READ spans
        // both GETs and must use their existing coverage, not redownload it.
        std::array<std::byte, 64U * 1024U> cross{};
        pread_all(fd.get(), cross, offset + block);
        idle();
        // Both sides are covered by different GETs. Make the kernel ask for
        // the whole crossing interval rather than reuse its earlier pages.
        const size_t edge = block - cross.size() / 2;
        require(::posix_fadvise(fd.get(), off_t(edge), off_t(cross.size()),
                                POSIX_FADV_DONTNEED) == 0,
                "drop kernel pages across existing pooled GETs");
        require(::readahead(fd.get(), off_t(edge), cross.size()) == 0,
                "submit batched READ across existing pooled GETs");
        pread_all(fd.get(), cross, edge);
        require(std::equal(cross.begin(), cross.end(), object->bytes.begin() + edge),
                "READ crossing two existing GETs differs");
        {
          std::lock_guard guard(shared.mutex);
          require(object->get_requests == 2,
                  "READ crossing two existing GETs downloaded overlapping bytes");
        }
        // Replying every byte does not discard a block. While the memory
        // budget fits, a page-cache miss must reuse the retained whole block.
        for (size_t off = 0; off < block; off += cross.size()) {
          pread_all(fd.get(), cross, off);
        }
        require(::posix_fadvise(fd.get(), off_t(edge), off_t(cross.size()),
                                POSIX_FADV_DONTNEED) == 0,
                "drop kernel pages across fully served and retained coverage");
        require(::readahead(fd.get(), off_t(edge), cross.size()) == 0,
                "submit batched READ across retained pooled blocks");
        pread_all(fd.get(), cross, edge);
        require(std::equal(cross.begin(), cross.end(), object->bytes.begin() + edge),
                "READ crossing fully served and retained coverage differs");
        idle();
        {
          std::lock_guard guard(shared.mutex);
          require(object->get_requests == 2,
                  "fully served blocks were discarded without memory pressure");
        }
        {
          auto ordered = shared.special_objects.at("pool-order.bin");
          ordered->send_limit.store(offset + 512U * 1024U, std::memory_order_release);
          UniqueFd order_fd(::open((mountpoint + "/pool-order.bin").c_str(), O_RDONLY | O_CLOEXEC));
          require(bool(order_fd), "open reverse-completion pooled object");
          require(::posix_fadvise(order_fd.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
                  "disable kernel prefetch for reverse GET completion");
          std::atomic<bool> crossed{false};
          std::atomic<bool> initial_done{false};
          std::array<std::byte, 64U * 1024U> initial{};
          std::exception_ptr initial_error;
          std::exception_ptr crossing_error;
          std::jthread initial_read;
          std::jthread crossing;
          ResumePool resume_order{*ordered};
          initial_read = std::jthread([&] {
            try { pread_all(order_fd.get(), initial, offset); }
            catch (...) { initial_error = std::current_exception(); }
            initial_done.store(true, std::memory_order_release);
          });
          wait([&] { return ordered->tail_paused.load(std::memory_order_acquire); },
               "first GET did not pause before reverse completion test");
          require(!initial_done.load(std::memory_order_acquire),
                  "incomplete first block was published before reverse completion test");
          pread_all(order_fd.get(), cross, offset + block);
          wait([&] {
            std::lock_guard guard(shared.mutex);
            return ordered->get_requests == 2 && shared.active_gets == 1;
          }, "later GET did not finish before the earlier paused GET");
          require(::posix_fadvise(order_fd.get(), off_t(edge), off_t(cross.size()),
                                  POSIX_FADV_DONTNEED) == 0,
                  "drop kernel pages before reverse-completion crossing READ");
          crossing = std::jthread([&] {
            try {
              require(::readahead(order_fd.get(), off_t(edge), cross.size()) == 0,
                      "submit batched READ across complete and pending GETs");
              pread_all(order_fd.get(), cross, edge);
            }
            catch (...) { crossing_error = std::current_exception(); }
            crossed.store(true, std::memory_order_release);
          });
          std::this_thread::sleep_for(std::chrono::milliseconds(20));
          require(!crossed.load(std::memory_order_acquire),
                  "completed later GET exposed the earlier GET's missing prefix");
          ordered->send_limit.store(SIZE_MAX, std::memory_order_release);
          wait([&] { return crossed.load(std::memory_order_acquire); },
               "cross-GET READ did not wake after its own missing prefix arrived");
          crossing.join();
          initial_read.join();
          if (initial_error) std::rethrow_exception(initial_error);
          if (crossing_error) std::rethrow_exception(crossing_error);
          require(std::equal(initial.begin(), initial.end(), ordered->bytes.begin() + offset),
                  "reverse-completion first block READ differs");
          require(std::equal(cross.begin(), cross.end(), ordered->bytes.begin() + edge),
                  "reverse-completion cross-GET READ differs");
          idle();
          std::lock_guard guard(shared.mutex);
          require(ordered->get_requests == 2,
                  "reverse-completion cross-GET READ issued a duplicate GET");
        }
        auto sequential = shared.special_objects.at("pool-sequential.bin");
        UniqueFd seq(::open((mountpoint + "/pool-sequential.bin").c_str(), O_RDONLY | O_CLOEXEC));
        require(bool(seq), "open pooled sequential object");
        require(::posix_fadvise(seq.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
                "disable kernel prefetch for pooled GET-window growth");
        for (size_t off = 0; off <= 24U * 1024U * 1024U; off += cross.size()) {
          pread_all(seq.get(), cross, off);
          require(std::equal(cross.begin(), cross.end(), sequential->bytes.begin() + off),
                  "pooled sequential bytes differ");
        }
        idle();
        {
          std::lock_guard guard(shared.mutex);
          const auto& ranges = sequential->get_ranges;
          require(ranges.size() == 3,
                  "sequential pooled reads did not use exactly three growing windows");
          require(ranges[0].first == 0 && ranges[0].last == 8U * 1024U * 1024U - 1,
                  "sequential pooled GET did not start at 8 MiB");
          require(ranges[1].first == 8U * 1024U * 1024U &&
                      ranges[1].last == 24U * 1024U * 1024U - 1,
                  "sequential pooled GET did not double to 16 MiB");
          const size_t third = (pool_limited ? 16U : 32U) * 1024U * 1024U;
          require(ranges[2].first == 24U * 1024U * 1024U &&
                      ranges[2].last == 24U * 1024U * 1024U + third - 1,
                  "sequential pooled GET ignored its configured maximum window");
        }
        seq.reset();
        auto tail_object = shared.special_objects.at("pool-tail.bin");
        UniqueFd tail(::open((mountpoint + "/pool-tail.bin").c_str(), O_RDONLY | O_CLOEXEC));
        require(bool(tail), "open pooled short-tail object");
        require(::posix_fadvise(tail.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
                "disable kernel prefetch for aligned EOF range test");
        const size_t tail_off = tail_object->bytes.size() / block * block;
        std::vector<std::byte> tail_bytes(tail_object->bytes.size() - tail_off);
        pread_all(tail.get(), tail_bytes, tail_off);
        require(std::equal(tail_bytes.begin(), tail_bytes.end(), tail_object->bytes.begin() + tail_off),
                "pooled EOF tail bytes differ");
        idle();
        {
          std::lock_guard guard(shared.mutex);
          const auto& ranges = tail_object->get_ranges;
          require(ranges.size() == 1 && ranges[0].first == tail_off &&
                      ranges[0].last == tail_object->bytes.size() - 1,
                  "short EOF block was not published using exactly the aligned tail range");
        }
        // The already retained short EOF block also participates in an iov
        // READ with the previous full block; it must not be fetched again.
        const size_t tail_edge = tail_off - page;
        tail_bytes.resize(page + tail_object->bytes.size() - tail_off);
        require(::posix_fadvise(tail.get(), off_t(tail_off), off_t(page),
                                POSIX_FADV_DONTNEED) == 0,
                "drop kernel EOF page before cross-block tail READ");
        require(::readahead(tail.get(), off_t(tail_edge), tail_bytes.size()) == 0,
                "submit batched READ across full and short EOF blocks");
        pread_all(tail.get(), tail_bytes, tail_edge);
        require(std::equal(tail_bytes.begin(), tail_bytes.end(),
                           tail_object->bytes.begin() + tail_edge),
                "READ crossing a full block and a short EOF block differs");
        idle();
        {
          std::lock_guard guard(shared.mutex);
          const auto& ranges = tail_object->get_ranges;
          require(ranges.size() == 2 && ranges.back().first == tail_off - block &&
                      ranges.back().last == tail_off - 1,
                  "cross-block EOF READ redownloaded its retained tail");
        }
      }
      require(::munmap(mapping, page) == 0, "unmap pooled probe");
      unmap.mapping = MAP_FAILED;
      fd.reset();
      sibling.reset();
      std::jthread resume_download([object] {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        object->send_limit.store(SIZE_MAX, std::memory_order_release);
      });
      mounted.stop();
      require(mounted.stopped_cleanly(), "pooled prefetch shutdown did not exit cleanly");
      shared.stop.store(true);
      server.request_stop();
      server.join();
      require(::rmdir(mountpoint.c_str()) == 0,
              "remove pooled prefetch mountpoint");
      fprintf(stderr, "%s: passed\n", shutdown_prefetch ?
              "pooled prefetch shutdown with active receive" :
              "pooled whole-block gate, pending dedup, no STORE, retained blocks, cross-block reply, window growth and aligned EOF");
      return 0;
    }

    if (prefetch_mode) {
      if (verified_prefetch && engine == "legacy") {
        auto object = shared.special_objects.at("read-ahead-store.bin");
        UniqueFd fd(::open((mountpoint + "/read-ahead-store.bin").c_str(), O_RDONLY | O_CLOEXEC));
        require(bool(fd), "open legacy whole-object checksum reader");
        std::vector<std::byte> bytes(object->bytes.size());
        pread_all(fd.get(), bytes, 0);
        bool verified = false;
        for (unsigned n = 0; n != 3000; ++n) {
          pread_all(fd.get(), bytes, 0);
          int gets;
          { std::lock_guard guard(shared.mutex); gets = object->get_requests; }
          if (bytes == object->bytes && gets == (verified_clean ? 1 : 2)) {
            verified = true;
            break;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        require(verified, "legacy whole-object checksum verification/retry did not complete");
        {
          std::lock_guard guard(shared.mutex);
          require(shared.checksum_mode_requests == (verified_clean ? 1 : 2),
                  "legacy verification did not request a checksum for every full GET");
        }
        fd.reset();
        mounted.stop();
        shared.stop.store(true);
        server.request_stop();
        server.join();
        require(::rmdir(mountpoint.c_str()) == 0, "remove legacy checksum mountpoint");
        fprintf(stderr, "legacy whole-object checksum %s: passed\n", verified_clean ? "verification" : "retry");
        return 0;
      }
      const auto wait_for_gets = [&] {
        for (unsigned attempt = 0; attempt != 2000; ++attempt) {
          bool sent;
          {
            std::lock_guard state_guard(shared.mutex);
            sent = shared.active_gets == 0;
          }
          // A sent response is not a completed client window: asynchronous
          // checksum and client-side receive completion can still be pending.
          // The next disjoint READ
          // legitimately uses demand-only I/O until that window completes.
          // The exact-window assertions below require a quiescent client too.
          if (sent) {
            return;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        {
          std::lock_guard state_guard(shared.mutex);
          fprintf(stderr, "read-ahead GET timeout: active=%d ranges=",
                  shared.active_gets);
          const auto& ranges = shared.special_objects.at(
              "read-ahead.bin")->get_ranges;
          for (const RequestRange& range : ranges) {
            fprintf(stderr, " [%llu,%llu]",
                    static_cast<unsigned long long>(range.first),
                    static_cast<unsigned long long>(range.last));
          }
          fprintf(stderr, "\n");
        }
        throw std::runtime_error("timed out waiting for read-ahead GET");
      };
      // Legacy staging must satisfy actual requests without proactively
      // publishing unrelated prefetch pages into the FUSE page cache.
      // Uring reaches the receive-pool contract branch above instead.
      if (engine == "legacy" && !verified_prefetch) {
        std::shared_ptr<SpecialObject> object;
        {
          std::lock_guard guard(shared.mutex);
          object = shared.special_objects.at("read-ahead-store.bin");
          object->pause_after = 512U * 1024U;
        }
        std::vector<std::jthread> readers;
        struct ResumeTail {
          SpecialObject& object;
          ~ResumeTail() { object.resume_tail.store(true, std::memory_order_release); }
        } resume{*object};
        const std::string path = mountpoint + "/read-ahead-store.bin";
        UniqueFd first(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
        UniqueFd second(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
        require(first && second, "open legacy retained-prefetch object");
        require(::posix_fadvise(first.get(), 0, 0, POSIX_FADV_RANDOM) == 0 &&
                    ::posix_fadvise(second.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
                "disable kernel read-ahead for no-STORE evidence");
        const size_t page = size_t(::sysconf(_SC_PAGESIZE));
        constexpr size_t probe = 384U * 1024U;
        void* mapping = ::mmap(nullptr, page, PROT_READ, MAP_SHARED, first.get(), off_t(probe));
        require(mapping != MAP_FAILED, "map unfaulted no-STORE probe page");
        struct Unmap {
          void* mapping;
          size_t length;
          ~Unmap() { if (mapping != MAP_FAILED) ::munmap(mapping, length); }
        } unmap{mapping, page};
        unsigned char resident = 0;
        require(::mincore(mapping, page, &resident) == 0 && !(resident & 1),
                "no-STORE probe page was already resident before any READ");
        std::array<std::byte, 64U * 1024U> first_bytes{};
        pread_all(first.get(), first_bytes, 0);
        require(std::equal(first_bytes.begin(), first_bytes.end(), read_ahead.begin()),
                "legacy retained-prefetch initial READ differs");
        bool paused = false;
        for (unsigned i = 0; i < 2000; ++i) {
          require(::mincore(mapping, page, &resident) == 0 && !(resident & 1),
                  "legacy in-flight prefetch proactively STOREd an unrelated page");
          if (object->tail_paused.load(std::memory_order_acquire)) {
            paused = true;
            break;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        require(paused && !object->resume_tail.load(std::memory_order_acquire),
                "prefetch did not reach the paused-tail observation point");
        for (unsigned i = 0; i < 20; ++i) {
          require(::mincore(mapping, page, &resident) == 0 && !(resident & 1),
                  "legacy paused prefetch proactively STOREd an unrelated page");
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        {
          std::lock_guard guard(shared.mutex);
          require(shared.active_gets > 0, "paused GET already completed before no-STORE evidence");
        }
        if (shutdown_prefetch) {
          // Exit with a live download and a replied demand prefix, not after the
          // staging has already retired. Let the server resume independently.
          require(::munmap(mapping, page) == 0, "unmap shutdown probe");
          unmap.mapping = MAP_FAILED;
          first.reset();
          second.reset();
          std::jthread resume_download([object] {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            object->resume_tail.store(true, std::memory_order_release);
          });
          mounted.stop();
          require(mounted.stopped_cleanly(), "prefetch shutdown did not exit cleanly");
          shared.stop.store(true);
          server.request_stop();
          server.join();
          require(::rmdir(mountpoint.c_str()) == 0, "remove prefetch shutdown mountpoint");
          fprintf(stderr, "shutdown with active prefetch and demand-only replies: passed\n");
          return 0;
        }
        if (partial_prefetch) {
          object->resume_tail.store(true, std::memory_order_release);
          wait_for_gets();
          require(::mincore(mapping, page, &resident) == 0 && !(resident & 1),
                  "unverifiable partial range was STOREd after download completion");
          {
            std::lock_guard guard(shared.mutex);
            require(object->get_requests == 1, "unexpected GET before demand reuse");
          }
          pread_all(second.get(), first_bytes, probe);
          require(std::equal(first_bytes.begin(), first_bytes.end(), read_ahead.begin() + probe),
                  "unverifiable range demand reuse differs");
          {
            std::lock_guard guard(shared.mutex);
            require(object->get_requests == 1, "unverifiable retained range was fetched again");
          }
          require(::munmap(mapping, page) == 0, "unmap unverifiable probe");
          unmap.mapping = MAP_FAILED;
          first.reset();
          second.reset();
          mounted.stop();
          shared.stop.store(true);
          server.request_stop();
          server.join();
          require(::rmdir(mountpoint.c_str()) == 0, "remove unverifiable mountpoint");
          fprintf(stderr, "unverifiable partial range: no STORE and actual READ reuse passed\n");
          return 0;
        }
        std::array<std::exception_ptr, 8> errors{};
        std::atomic<unsigned> entered{0};
        struct FinishReaders {
          SpecialObject& object;
          std::vector<std::jthread>& readers;
          ~FinishReaders() {
            object.resume_tail.store(true, std::memory_order_release);
            for (auto& reader : readers) if (reader.joinable()) reader.join();
          }
        } finish_readers{*object, readers};
        for (size_t n = 0; n < errors.size(); ++n) {
          readers.emplace_back([&, n] {
            entered.fetch_add(1, std::memory_order_release);
            try {
              // Cross the received/pending boundary and alternate handles.
              const size_t offset = 448U * 1024U + n * page;
              std::vector<std::byte> bytes(256U * 1024U);
              pread_all(n % 2 ? first.get() : second.get(), bytes, offset);
              require(std::equal(bytes.begin(), bytes.end(), read_ahead.begin() + offset),
                      "overlapping retained-prefetch READ bytes differ");
            } catch (...) { errors[n] = std::current_exception(); }
          });
        }
        while (entered.load(std::memory_order_acquire) != errors.size()) {
          std::this_thread::yield();
        }
        // Leave concurrent READs pending while another satisfied READ completes.
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        pread_all(first.get(), first_bytes, 0);
        object->resume_tail.store(true, std::memory_order_release);
        for (auto& thread : readers) thread.join();
        for (auto& error : errors) if (error) std::rethrow_exception(error);
        if (verified_prefetch) {
          bool corrected = false;
          for (unsigned i = 0; i < 2000; ++i) {
            pread_all(second.get(), first_bytes, probe);
            if (std::equal(first_bytes.begin(), first_bytes.end(), read_ahead.begin() + probe)) {
              corrected = true;
              break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          }
          if (!corrected) {
            std::lock_guard guard(shared.mutex);
            const auto& retried = *shared.special_objects.at(
                "read-ahead-store.bin");
            fprintf(stderr,
                    "checksum-retry diagnostic: gets=%d ranges=%zu "
                    "active=%d checksum-mode=%d\n",
                    retried.get_requests, retried.get_ranges.size(),
                    shared.active_gets, shared.checksum_mode_requests);
          }
          require(corrected, "checksum retry did not replace corrupt early READ data");
        }
        wait_for_gets();
        for (unsigned i = 0; i < 20; ++i) {
          require(::mincore(mapping, page, &resident) == 0 && !(resident & 1),
                  "completed legacy prefetch proactively STOREd an unrelated page");
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        pread_all(second.get(), first_bytes, probe);
        require(std::equal(first_bytes.begin(), first_bytes.end(), read_ahead.begin() + probe),
                "retained prefetch bytes differ after completion");
        {
          std::lock_guard guard(shared.mutex);
          const int expected_gets = verified_prefetch && !verified_clean ? 2 : 1;
          if (object->get_requests != expected_gets) {
            fprintf(stderr,
                    "prefetch-share diagnostic: expected=%d actual=%d ranges=",
                    expected_gets, object->get_requests);
            for (const RequestRange& range : object->get_ranges) {
              fprintf(stderr, " [%llu,%llu]",
                      static_cast<unsigned long long>(range.first),
                      static_cast<unsigned long long>(range.last));
            }
            fprintf(stderr, "\n");
          }
          require(object->get_requests == expected_gets,
                  "overlapping cross-handle READ did not share its in-flight prefetch");
        }
        fprintf(stderr, "%s and overlapping cross-handle READ: passed\n",
                verified_clean ? "checksum-verified retained prefetch" :
                verified_prefetch ? "checksum-retry retained prefetch" :
                "legacy uncached prefetch without any proactive STORE");
      }
      const std::string path = mountpoint + "/read-ahead.bin";
      if (verified_prefetch) {
        mounted.stop();
        shared.stop.store(true);
        server.request_stop();
        server.join();
        if (::rmdir(mountpoint.c_str()) != 0) fail_errno("rmdir verified prefetch mountpoint");
        return 0;
      }
      UniqueFd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
      if (!fd) {
        fail_errno("open read-ahead object");
      }
      std::array<std::byte, 64U * 1024U> bytes{};
      pread_all(fd.get(), bytes, 0);
      require(std::equal(bytes.begin(), bytes.end(), read_ahead.begin()),
              "initial read-ahead bytes differ");
      constexpr uint64_t initial_window_tail =
          1024U * 1024U - bytes.size();
      pread_all(fd.get(), bytes, initial_window_tail);
      require(std::equal(bytes.begin(), bytes.end(),
                         read_ahead.begin() + initial_window_tail),
              "out-of-order read-ahead hit differs");
      wait_for_gets();
      pread_all(fd.get(), bytes, 2U * 1024U * 1024U);
      require(std::equal(bytes.begin(), bytes.end(),
                         read_ahead.begin() + 2U * 1024U * 1024U),
              "read-ahead miss differs");
      uint64_t continued_offset;
      {
        std::lock_guard state_guard(shared.mutex);
        const auto& ranges = shared.special_objects.at(
            "read-ahead.bin")->get_ranges;
        require(ranges.size() == 2,
                "read-ahead miss did not use one independent GET");
        require(ranges[1].first == 2U * 1024U * 1024U &&
                    ranges[1].last == 3U * 1024U * 1024U - 1,
                "random jump did not restart a 1 MiB window");
        continued_offset = ranges[1].last + 1;
      }
      wait_for_gets();
      pread_all(fd.get(), bytes, continued_offset);
      require(std::equal(bytes.begin(), bytes.end(),
                         read_ahead.begin() + continued_offset),
              "continued read-ahead bytes differ");
      pread_all(fd.get(), bytes, 256U * 1024U);
      require(std::equal(bytes.begin(), bytes.end(),
                         read_ahead.begin() + 256U * 1024U),
              "retained read-ahead bytes differ");
      fd.reset();
      const std::string sequential_path =
          mountpoint + "/read-ahead-sequential.bin";
      UniqueFd sequential_fd(
          ::open(sequential_path.c_str(), O_RDONLY | O_CLOEXEC));
      if (!sequential_fd) {
        fail_errno("open sequential read-ahead object");
      }
      // Test client-window growth using ordered demand, without the kernel's
      // independent asynchronous readahead issuing a later offset first.
      require(::posix_fadvise(sequential_fd.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
              "disable kernel readahead for ordered client-window test");
      std::vector<std::byte> sequential_bytes(1280U * 1024U);
      for (size_t offset = 0; offset != sequential_bytes.size(); offset += 4096) {
        pread_all(sequential_fd.get(), std::span(sequential_bytes).subspan(offset, 4096), offset);
      }
      require(std::equal(sequential_bytes.begin(), sequential_bytes.end(),
                         read_ahead.begin()),
              "sequential read-ahead bytes differ");
      sequential_fd.reset();
      {
        std::lock_guard state_guard(shared.mutex);
        const auto& ranges = shared.special_objects.at(
            "read-ahead.bin")->get_ranges;
        require(ranges.size() == 3,
                "read-ahead did not preserve and restart its windows");
        require(ranges[0].first == 0 &&
                    ranges[0].last == 1024U * 1024U - 1,
                "initial read-ahead range is not 1 MiB");
        require(ranges[1].first == 2U * 1024U * 1024U,
                "out-of-range read was not fetched independently");
        require(ranges[2].first == continued_offset &&
                    ranges[2].last == read_ahead.size() - 1,
                "continued run did not grow and clip its window");
        const auto& sequential_ranges = shared.special_objects.at(
            "read-ahead-sequential.bin")->get_ranges;
        if (sequential_ranges.size() != 2) {
          for (const auto& range : sequential_ranges) {
            fprintf(stderr, "sequential GET: first=%llu last=%llu\n",
                    (unsigned long long)range.first, (unsigned long long)range.last);
          }
        }
        require(sequential_ranges.size() == 2,
                "sequential read-ahead did not cross exactly two windows");
        require(sequential_ranges[0].first == 0 &&
                    sequential_ranges[0].last == 1024U * 1024U - 1,
                "sequential read-ahead initial window is not 1 MiB");
        require(sequential_ranges[1].first == 1024U * 1024U &&
                    sequential_ranges[1].last == 3U * 1024U * 1024U - 1,
                "sequential read-ahead second window is not 2 MiB");
      }
      const std::string random_path =
          mountpoint + "/read-ahead-random.bin";
      UniqueFd random_fd(::open(random_path.c_str(), O_RDONLY | O_CLOEXEC));
      if (!random_fd) {
        fail_errno("open random-extent read-ahead object");
      }
      constexpr std::array<uint64_t, 4> random_offsets{
          0,
          2U * 1024U * 1024U,
          1U * 1024U * 1024U,
          3U * 1024U * 1024U,
      };
      for (const uint64_t random_offset : random_offsets) {
        pread_all(random_fd.get(), bytes, random_offset);
        require(std::equal(bytes.begin(), bytes.end(),
                           read_ahead.begin() + random_offset),
                "random-extent read-ahead bytes differ");
        const uint64_t window_tail =
            random_offset + 1024U * 1024U - bytes.size();
        pread_all(random_fd.get(), bytes, window_tail);
        require(std::equal(bytes.begin(), bytes.end(),
                           read_ahead.begin() + window_tail),
                "random-extent tail bytes differ");
        wait_for_gets();
      }
      random_fd.reset();
      {
        std::lock_guard state_guard(shared.mutex);
        const auto& ranges = shared.special_objects.at(
            "read-ahead-random.bin")->get_ranges;
        if (ranges.size() != random_offsets.size()) {
          for (const auto& range : ranges) {
            fprintf(stderr, "random-extent GET: first=%llu last=%llu\n",
                    (unsigned long long)range.first, (unsigned long long)range.last);
          }
        }
        require(ranges.size() == random_offsets.size(),
                "random extents did not each use one GET");
        for (size_t i = 0; i != ranges.size(); ++i) {
          require(ranges[i].first == random_offsets[i] &&
                      ranges[i].last == random_offsets[i] +
                          1024U * 1024U - 1,
                  "random extent did not restart a 1 MiB window");
        }
      }
      mounted.stop();
      shared.stop.store(true);
      server.request_stop();
      server.join();
      if (::rmdir(mountpoint.c_str()) != 0) {
        fail_errno("rmdir read-ahead mountpoint");
      }
      std::cout << "FUSE uncached read-ahead integration passed\n";
      return 0;
    }

    {
      struct statvfs status{};
      if (::statvfs(mountpoint.c_str(), &status) != 0) {
        fail_errno("statvfs mountpoint");
      }
      require(status.f_bsize == 256U * 1024U,
              "fixed statfs optimal I/O size was not applied");
      require(status.f_frsize == 4096,
              "statfs fragment size must remain 4 KiB");
    }

    UniqueFd file(::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!file) {
      fail_errno("open mounted object");
    }
    struct stat status{};
    if (::fstat(file.get(), &status) != 0) {
      fail_errno("fstat mounted object");
    }
    require(S_ISREG(status.st_mode), "non-empty object is not a regular file");
    require(status.st_uid == ::getuid() && status.st_gid == ::getgid(),
            "mount UID/GID were not applied");
    require((status.st_mode & 07777) == 0640,
            "mount file mode was not applied");
    require(static_cast<uint64_t>(status.st_size) == expected.size(),
            "mounted object size mismatch");

    DIR* directory = ::opendir(mountpoint.c_str());
    if (directory == nullptr) {
      fail_errno("opendir mountpoint");
    }
    bool found_regular_entry = false;
    bool found_directory_entry = false;
    bool leaked_deep_child = false;
    while (dirent* entry = ::readdir(directory)) {
      if (std::string_view(entry->d_name) == "mmap.bin") {
        found_regular_entry = entry->d_type == DT_REG;
      } else if (std::string_view(entry->d_name) == "deep") {
        found_directory_entry = entry->d_type == DT_DIR;
      } else if (std::string_view(entry->d_name) == "child.bin") {
        leaked_deep_child = true;
      }
    }
    ::closedir(directory);
    require(found_regular_entry,
            "readdir did not return the configured object as DT_REG");
    require(found_directory_entry && !leaked_deep_child,
            "readdir did not return only direct children");
    size_t lists_before_deep_lookup;
    {
      std::lock_guard state_guard(shared.mutex);
      lists_before_deep_lookup = shared.list_requests;
    }
    struct stat deep_status{};
    require(::stat((mountpoint + "/deep").c_str(), &deep_status) == 0 &&
                S_ISDIR(deep_status.st_mode),
            "deep directory was not cached from the root listing");
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.list_requests == lists_before_deep_lookup,
              "deep cached lookup issued another ListObjectsV2 request");
    }
    struct stat child_status{};
    require(::stat((mountpoint + "/deep/child.bin").c_str(),
                   &child_status) == 0 && S_ISREG(child_status.st_mode),
            "child lookup did not refresh the deep directory");
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.list_requests == lists_before_deep_lookup + 1,
              "child lookup did not issue one direct-child ListObjectsV2");
    }
    const std::string nonempty_directory_rename =
        mountpoint + "/deep-renamed";
    errno = 0;
    require(::rename((mountpoint + "/deep").c_str(),
                     nonempty_directory_rename.c_str()) != 0 &&
                errno == EXDEV,
            "renaming a non-empty directory must fail with EXDEV");
    {
      std::lock_guard state_guard(shared.mutex);
      shared.deep_present = false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    directory = ::opendir(mountpoint.c_str());
    if (directory == nullptr) {
      fail_errno("opendir mountpoint after external deletion");
    }
    found_directory_entry = false;
    while (dirent* entry = ::readdir(directory)) {
      found_directory_entry = found_directory_entry ||
          std::string_view(entry->d_name) == "deep";
    }
    ::closedir(directory);
    require(!found_directory_entry,
            "readdir retained a child absent from the refreshed listing");

    UniqueFd second_reader(::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!second_reader) {
      fail_errno("open second reader");
    }
    errno = 0;
    UniqueFd writer_during_readers(
        ::open(file_path.c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC));
    require(!writer_during_readers && errno == EBUSY,
            "writable open must conflict with existing readers");

    constexpr size_t concurrent_read_size = 4096;
    constexpr uint64_t second_read_offset = 256U * 1024U;
    std::array<std::byte, concurrent_read_size> first_read{};
    std::array<std::byte, concurrent_read_size> second_read{};
    std::exception_ptr first_read_failure;
    std::exception_ptr second_read_failure;
    std::jthread first_reader([&] {
      try {
        pread_all(file.get(), first_read, 0);
      } catch (...) {
        first_read_failure = std::current_exception();
      }
    });
    std::jthread other_reader([&] {
      try {
        pread_all(second_reader.get(), second_read, second_read_offset);
      } catch (...) {
        second_read_failure = std::current_exception();
      }
    });
    first_reader.join();
    other_reader.join();
    if (first_read_failure) {
      std::rethrow_exception(first_read_failure);
    }
    if (second_read_failure) {
      std::rethrow_exception(second_read_failure);
    }
    require(std::equal(first_read.begin(), first_read.end(), expected.begin()),
            "first concurrent read returned wrong bytes");
    require(std::equal(second_read.begin(), second_read.end(),
                       expected.begin() + second_read_offset),
            "second concurrent read returned wrong bytes");
    if (cache_dir.empty()) {
      UniqueFd parallel_a(::open(
          (mountpoint + "/read-ahead.bin").c_str(),
          O_RDONLY | O_CLOEXEC));
      UniqueFd parallel_b(::open(
          (mountpoint + "/read-ahead-random.bin").c_str(),
          O_RDONLY | O_CLOEXEC));
      if (!parallel_a || !parallel_b) {
        fail_errno("open independent concurrent readers");
      }
      std::array<std::byte, concurrent_read_size> parallel_a_bytes{};
      std::array<std::byte, concurrent_read_size> parallel_b_bytes{};
      std::exception_ptr parallel_a_failure;
      std::exception_ptr parallel_b_failure;
      std::barrier launch(3);
      std::jthread parallel_a_reader([&] {
        launch.arrive_and_wait();
        try {
          pread_all(parallel_a.get(), parallel_a_bytes, 0);
        } catch (...) {
          parallel_a_failure = std::current_exception();
        }
      });
      std::jthread parallel_b_reader([&] {
        launch.arrive_and_wait();
        try {
          pread_all(parallel_b.get(), parallel_b_bytes, 0);
        } catch (...) {
          parallel_b_failure = std::current_exception();
        }
      });
      launch.arrive_and_wait();
      parallel_a_reader.join();
      parallel_b_reader.join();
      if (parallel_a_failure) {
        std::rethrow_exception(parallel_a_failure);
      }
      if (parallel_b_failure) {
        std::rethrow_exception(parallel_b_failure);
      }
      require(std::equal(parallel_a_bytes.begin(), parallel_a_bytes.end(),
                         read_ahead.begin()),
              "first independent concurrent read returned wrong bytes");
      require(std::equal(parallel_b_bytes.begin(), parallel_b_bytes.end(),
                         read_ahead.begin()),
              "second independent concurrent read returned wrong bytes");
    }
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.request_timeout_errors == 1 &&
                  !shared.inject_request_timeout,
              "400 RequestTimeout was not retried exactly once");
    }
    second_reader.reset();

    void* mapping = ::mmap(nullptr, expected.size(), PROT_READ, MAP_PRIVATE,
                           file.get(), 0);
    if (mapping == MAP_FAILED) {
      fail_errno("mmap mounted object");
    }
    const auto mapped = std::span(
        static_cast<const std::byte*>(mapping), expected.size());
    const bool equal = std::equal(mapped.begin(), mapped.end(),
                                  expected.begin(), expected.end());
    ::munmap(mapping, expected.size());
    require(equal, "mmap bytes differ from HTTP/2 object");
    file.reset();

    if (!cache_dir.empty()) {
      const std::string retry_path =
          mountpoint + "/checksum-retry.bin";
      UniqueFd retry_first(
          ::open(retry_path.c_str(), O_RDONLY | O_CLOEXEC));
      UniqueFd retry_second(
          ::open(retry_path.c_str(), O_RDONLY | O_CLOEXEC));
      if (!retry_first || !retry_second) {
        fail_errno("open checksum retry object");
      }
      std::array<std::byte, 4096> retry_a{};
      std::array<std::byte, 4096> retry_b{};
      std::exception_ptr retry_a_failure;
      std::exception_ptr retry_b_failure;
      std::jthread retry_a_thread([&] {
        try {
          pread_all(retry_first.get(), retry_a, 0);
        } catch (...) {
          retry_a_failure = std::current_exception();
        }
      });
      bool first_fetch_started = false;
      for (unsigned attempt = 0; attempt != 200; ++attempt) {
        {
          std::lock_guard state_guard(shared.mutex);
          first_fetch_started =
              shared.special_objects.at("checksum-retry.bin")
                      ->get_requests != 0;
        }
        if (first_fetch_started) {
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      require(first_fetch_started,
              "first checksum fetch did not reach the server");
      std::jthread retry_b_thread([&] {
        try {
          pread_all(retry_second.get(), retry_b, 128U * 1024U);
        } catch (...) {
          retry_b_failure = std::current_exception();
        }
      });
      retry_a_thread.join();
      retry_b_thread.join();
      if (retry_a_failure) {
        std::rethrow_exception(retry_a_failure);
      }
      if (retry_b_failure) {
        std::rethrow_exception(retry_b_failure);
      }
      bool checksum_retry_started = false;
      for (unsigned attempt = 0; attempt != 2000; ++attempt) {
        {
          std::lock_guard state_guard(shared.mutex);
          checksum_retry_started =
              shared.special_objects.at("checksum-retry.bin")
                      ->get_requests >= 2;
        }
        if (checksum_retry_started) {
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      require(checksum_retry_started,
              "background checksum retry did not reach the server");
      std::array<std::byte, 4096> retry_verified{};
      constexpr uint64_t corrupt_page_offset = 12U * 1024U;
      const auto corrected_part_visible = [&](int fd,
                                               std::span<std::byte> output) {
        for (unsigned attempt = 0; attempt != 400; ++attempt) {
          ssize_t result;
          do {
            result = ::pread(fd, output.data(), output.size(),
                             off_t(corrupt_page_offset));
          } while (result < 0 && errno == EINTR);
          if (result < 0) {
            fail_errno("read checksum-corrected page");
          }
          require(result == ssize_t(output.size()),
                  "short read while waiting for checksum correction");
          if (std::equal(output.begin(), output.end(),
                         checksum_test.begin() + corrupt_page_offset)) {
            return true;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return false;
      };
      require(corrected_part_visible(retry_first.get(), retry_verified),
              "checksum retry did not replace the corrupt cached part");
      {
        std::lock_guard state_guard(shared.mutex);
        const SpecialObject& retried =
            *shared.special_objects.at("checksum-retry.bin");
        require(retried.get_requests == 2,
                "concurrent readers did not share exactly one checksum retry: " +
                    std::to_string(retried.get_requests));
        require(retried.attributes_requests == 2,
                "GetObjectAttributes pagination was not completed once");
        require(retried.get_ranges.size() == 2 &&
                    retried.get_ranges[1].first == 0 &&
                    retried.get_ranges[1].last == 128U * 1024U - 1,
                "checksum retry did not GET the exact multipart part");
      }

      const std::string release_path =
          mountpoint + "/checksum-release.bin";
      UniqueFd release_file(
          ::open(release_path.c_str(), O_RDONLY | O_CLOEXEC));
      if (!release_file) {
        fail_errno("open checksum release object");
      }
      std::array<std::byte, 4096> release_first{};
      pread_all(release_file.get(), release_first, 0);
      require(std::equal(release_first.begin(), release_first.end(),
                         checksum_test.begin()),
              "checksum release initial read returned wrong bytes");
      release_file.reset();
      bool released_retry_started = false;
      for (unsigned attempt = 0; attempt != 2000; ++attempt) {
        {
          std::lock_guard state_guard(shared.mutex);
          released_retry_started =
              shared.special_objects.at("checksum-release.bin")
                      ->get_requests >= 2;
        }
        if (released_retry_started) {
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      require(released_retry_started,
              "release cancelled the background checksum retry");
      UniqueFd released_reopen(
          ::open(release_path.c_str(), O_RDONLY | O_CLOEXEC));
      if (!released_reopen) {
        fail_errno("reopen checksum release object");
      }
      std::array<std::byte, 4096> released_verified{};
      require(corrected_part_visible(
                  released_reopen.get(), released_verified),
              "background checksum retry did not survive release");
      {
        std::lock_guard state_guard(shared.mutex);
        const SpecialObject& released =
            *shared.special_objects.at("checksum-release.bin");
        require(released.get_requests == 2,
                "released checksum read did not perform exactly one retry");
      }

      const std::string checksum_rename_source =
          mountpoint + "/checksum-rename.bin";
      const std::string checksum_rename_destination =
          mountpoint + "/checksum-renamed.bin";
      UniqueFd checksum_rename_file(
          ::open(checksum_rename_source.c_str(), O_RDONLY | O_CLOEXEC));
      if (!checksum_rename_file) {
        fail_errno("open checksum rename object");
      }
      std::array<std::byte, 4096> checksum_rename_first{};
      // Start at a nonzero offset: a random GET fills one 2 MiB block, half
      // this 4 MiB checksum part. The other block is fetched after rename;
      // no timing barrier holds a GET identity lock against rename.
      require(posix_fadvise(checksum_rename_file.get(), 0, 0, POSIX_FADV_RANDOM) == 0,
              "disable kernel prefetch for checksum rename test");
      pread_all(checksum_rename_file.get(), checksum_rename_first, 4096);
      require(::rename(checksum_rename_source.c_str(),
                       checksum_rename_destination.c_str()) == 0,
              "rename before checksum unit completion failed");
      std::array<std::byte, 4096> checksum_rename_second{};
      pread_all(checksum_rename_file.get(), checksum_rename_second, 2U * 1024U * 1024U);
      require(std::equal(checksum_rename_first.begin(),
                         checksum_rename_first.end(), checksum_test.begin() + 4096),
              "renamed checksum object's initial read returned wrong bytes");
      bool renamed_checksum_retry_started = false;
      for (unsigned attempt = 0; attempt != 2000; ++attempt) {
        {
          std::lock_guard state_guard(shared.mutex);
          renamed_checksum_retry_started =
              shared.special_objects.at("checksum-renamed.bin")
                      ->get_requests >= 3;
        }
        if (renamed_checksum_retry_started) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      require(renamed_checksum_retry_started,
              "checksum retry did not follow the renamed identity");
      std::array<std::byte, 4096> renamed_checksum_verified{};
      require(corrected_part_visible(
                  checksum_rename_file.get(), renamed_checksum_verified),
              "renamed checksum retry did not replace corrupt cached bytes");
      {
        std::lock_guard state_guard(shared.mutex);
        const SpecialObject& renamed =
            *shared.special_objects.at("checksum-renamed.bin");
        require(renamed.get_requests == 3 && renamed.get_paths.size() == 3,
                "renamed checksum object did not perform one exact retry");
        require(renamed.get_paths[0].starts_with(
                    "/bucket/checksum-rename.bin") &&
                    renamed.get_paths[1].starts_with(
                    "/bucket/checksum-renamed.bin") &&
                    renamed.get_paths[2].starts_with(
                    "/bucket/checksum-renamed.bin"),
                "checksum retry was signed for the pre-rename path");
      }

      const std::string unsupported_path =
          mountpoint + "/checksum-unsupported.bin";
      UniqueFd unsupported(
          ::open(unsupported_path.c_str(), O_RDONLY | O_CLOEXEC));
      if (!unsupported) {
        fail_errno("open checksum-unsupported object");
      }
      std::vector<std::byte> unsupported_bytes(checksum_test.size());
      pread_all(unsupported.get(), unsupported_bytes, 0);
      require(unsupported_bytes == checksum_test,
              "unsupported ObjectParts checksum made cached read fail");
      {
        std::lock_guard state_guard(shared.mutex);
        const SpecialObject& object =
            *shared.special_objects.at("checksum-unsupported.bin");
        require(object.attributes_requests == 1 &&
                    object.get_requests == 1,
                "unsupported checksum manifest was retried or blocked read");
      }

      const std::string fail_path = mountpoint + "/checksum-fail.bin";
      UniqueFd fail_file(::open(fail_path.c_str(), O_RDONLY | O_CLOEXEC));
      if (!fail_file) {
        fail_errno("open permanently corrupt checksum object");
      }
      std::array<std::byte, 4096> failed_bytes{};
      ssize_t first_failed = ::pread(fail_file.get(), failed_bytes.data(),
                                     failed_bytes.size(), 0);
      if (first_failed < 0 && errno != EIO) {
        fail_errno("first read of permanently corrupt checksum object");
      }
      bool invalidated = first_failed < 0 && errno == EIO;
      for (unsigned attempt = 0; !invalidated && attempt != 200; ++attempt) {
        errno = 0;
        const ssize_t result = ::pread(
            fail_file.get(), failed_bytes.data(), 1, 0);
        if (result < 0 && errno == EIO) {
          invalidated = true;
          break;
        }
        require(result == 1,
                "unexpected result while waiting for checksum invalidation");
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
      require(invalidated,
              "checksum failure did not invalidate an already returned page");
      errno = 0;
      require(::pread(fail_file.get(), failed_bytes.data(), 1,
                      256U * 1024U) < 0 && errno == EIO,
              "a permanently bad cached part did not fail a later read");
      errno = 0;
      require(::pread(fail_file.get(), failed_bytes.data(), 1,
                      256U * 1024U) < 0 && errno == EIO,
              "a permanently bad cached part was retried more than once");
      {
        std::lock_guard state_guard(shared.mutex);
        require(shared.special_objects.at("checksum-fail.bin")
                        ->get_requests == 2,
                "permanent checksum mismatch did not stop after one retry");
        require(shared.object_attributes_requests >= 3,
                "cached checksum reads did not load ObjectParts manifests");
      }
    }

    errno = 0;
    UniqueFd read_write(::open(file_path.c_str(), O_RDWR | O_CLOEXEC));
    require(!read_write && errno == EOPNOTSUPP,
            "O_RDWR must be rejected to prohibit writable mmap");
    errno = 0;
    UniqueFd untruncated_writer(
        ::open(file_path.c_str(), O_WRONLY | O_CLOEXEC));
    require(!untruncated_writer && errno == EOPNOTSUPP,
            "a writer without O_TRUNC must be rejected");

    int gets_before_write;
    {
      std::lock_guard state_guard(shared.mutex);
      gets_before_write = shared.get_requests;
    }
    UniqueFd writer(
        ::open(file_path.c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC));
    if (!writer) {
      fail_errno("open mounted object for write");
    }
    UniqueFd writer_alias(::dup(writer.get()));
    if (!writer_alias) {
      fail_errno("dup mounted writer");
    }
    errno = 0;
    UniqueFd competing_writer(
        ::open(file_path.c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC));
    require(!competing_writer && errno == EBUSY,
            "second writable open must fail with EBUSY");
    errno = 0;
    UniqueFd reader_during_write(
        ::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
    require(!reader_during_write && errno == EBUSY,
            "readable open must conflict with an existing writer");
    errno = 0;
    void* writable_mapping = ::mmap(nullptr, 4096, PROT_WRITE, MAP_SHARED,
                                    writer.get(), 0);
    require(writable_mapping == MAP_FAILED,
            "writable mmap unexpectedly succeeded");
    std::byte rejected_byte{};
    errno = 0;
    require(::pwrite(writer.get(), &rejected_byte, 1, 1) < 0 &&
                errno == ESPIPE,
            "positional write must fail on a sequential writer");
    struct stat writing_status{};
    require(::stat(file_path.c_str(), &writing_status) == 0 && writing_status.st_size == 0,
            "path stat restored pre-truncate size during a local write");
    const size_t first_write = std::min<size_t>(37, small_expected.size());
    test_write_all(writer.get(), std::span(small_expected).first(first_write));
    require(::stat(file_path.c_str(), &writing_status) == 0 &&
                writing_status.st_size == off_t(first_write),
            "path stat did not publish the accepted partial write size");
    if (::fsync(writer.get()) != 0) {
      fail_errno("fsync partial mounted page");
    }
    size_t write_offset = first_write;
    while (write_offset < small_expected.size()) {
      const size_t length = std::min<size_t>(
          64U * 1024U, small_expected.size() - write_offset);
      test_write_all(writer.get(),
                std::span(small_expected).subspan(write_offset, length));
      write_offset += length;
    }
    if (::fsync(writer.get()) != 0) {
      fail_errno("fsync mounted object");
    }
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.get_requests == gets_before_write,
              "write open or cached sequential writes downloaded the object");
      require(shared.put_requests == 0,
              "fsync unexpectedly published the object");
    }
    const int alias_fd = writer_alias.release();
    int heads_before_small_commit;
    {
      std::lock_guard state_guard(shared.mutex);
      heads_before_small_commit = shared.head_requests;
    }
    if (::close(alias_fd) != 0) {
      fail_errno("close duplicated mounted writer");
    }
    int gets_after_small_write;
    int checksum_mode_after_small_write;
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.create_multipart_requests == 0 &&
                  shared.upload_part_requests == 0 &&
                  shared.complete_multipart_requests == 0,
              "small write unexpectedly used multipart upload");
      require(shared.put_requests == 1,
              "flush did not publish one small PutObject");
      require(shared.head_requests == heads_before_small_commit,
              "PutObject Last-Modified response triggered a redundant HEAD");
      gets_after_small_write = shared.get_requests;
      checksum_mode_after_small_write = shared.checksum_mode_requests;
    }
    UniqueFd visible_after_flush(
        ::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!visible_after_flush) {
      fail_errno("close-to-open while duplicated writer remains");
    }
    if (!cache_dir.empty()) {
#if !defined(__SANITIZE_THREAD__) && !defined(__SANITIZE_ADDRESS__)
      // mincore observes an evictable kernel cache, not a program invariant.
      // Sanitizer instrumentation can delay this check or add enough memory
      // pressure for ordinary reclaim to make it nondeterministic;
      // byte validation below remains active in every build. First/last
      // partial pages need not be populated now that write-side STORE is gone.
      const long page_size = ::sysconf(_SC_PAGESIZE);
      require(page_size > 0, "sysconf(_SC_PAGESIZE) failed");
      const size_t mapped_size =
          (small_expected.size() + size_t(page_size) - 1) /
          size_t(page_size) * size_t(page_size);
      void* resident_mapping = ::mmap(
          nullptr, small_expected.size(), PROT_READ, MAP_PRIVATE,
          visible_after_flush.get(), 0);
      if (resident_mapping == MAP_FAILED) {
        fail_errno("mmap locally written object for mincore");
      }
      std::vector<unsigned char> residency(
          mapped_size / size_t(page_size));
      if (::mincore(resident_mapping, mapped_size, residency.data()) != 0) {
        const int error = errno;
        ::munmap(resident_mapping, small_expected.size());
        errno = error;
        fail_errno("mincore locally written object");
      }
      if (residency.size() > 2) {
        require(std::all_of(
                    residency.begin() + 1, residency.end() - 1,
                    [](unsigned char page) { return (page & 1U) != 0; }),
                "fully written cached pages were not retained in page cache");
      }
      ::munmap(resident_mapping, small_expected.size());
#endif
    }
    const long native_page_size = ::sysconf(_SC_PAGESIZE);
    require(native_page_size > 0, "invalid page size for written-page retention check");
    const size_t write_page_size = size_t(native_page_size);
    std::vector<std::byte> retained_write_page(write_page_size);
    if (2 * write_page_size <= small_expected.size()) {
      pread_all(visible_after_flush.get(), retained_write_page, write_page_size);
      require(std::equal(retained_write_page.begin(), retained_write_page.end(),
                         small_expected.begin() + write_page_size),
              "fully written page differs after close-to-open");
      std::lock_guard state_guard(shared.mutex);
      require(shared.get_requests == gets_after_small_write,
              "read-open discarded a fully written page already in page cache");
    }
    std::vector<std::byte> visible_bytes(small_expected.size());
    pread_all(visible_after_flush.get(), visible_bytes, 0);
    require(visible_bytes == small_expected,
            "flush did not expose the completed object");
    visible_after_flush.reset();
    {
      std::lock_guard state_guard(shared.mutex);
      // No proactive STORE fills partially written boundary pages. An uncached
      // READ may fetch them from S3; a local-cache READ can use its committed
      // data file. Contents and flush visibility are required for both paths.
      if (!cache_dir.empty()) {
        require(shared.get_requests == gets_after_small_write,
                "local cache refetched the object it just committed");
        require(shared.checksum_mode_requests == checksum_mode_after_small_write,
                "local-cache read unexpectedly requested checksum mode");
      }
    }
    if (::fsync(writer.get()) != 0) {
      fail_errno("fsync sealed mounted object");
    }
    const int writer_fd = writer.release();
    if (::close(writer_fd) != 0) {
      fail_errno("repeat close of mounted writer");
    }
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.put_requests == 1 &&
                  shared.upload_part_requests == 0 &&
                  shared.complete_multipart_requests == 0,
              "repeat flush or release uploaded data again");
      require(shared.object == small_expected,
              "small PutObject published the wrong bytes");
      shared.put_requests                = 0;
      shared.create_multipart_requests   = 0;
      shared.upload_part_requests        = 0;
      shared.complete_multipart_requests = 0;
      shared.upload_attempts.clear();
    }

    std::vector<std::byte> external_expected = small_expected;
    external_expected.front() ^= std::byte{0xff};
    int gets_before_external_open;
    {
      std::lock_guard state_guard(shared.mutex);
      shared.object = external_expected;
      shared.etag = "\"external-put\"";
      shared.version_id = "version-3";
      // Keep the server timestamp and object size unchanged. Generation
      // validation must use ETag/Version rather than second-resolution mtime.
      shared.last_modified = "Mon, 07 Nov 1994 08:49:37 GMT";
      shared.last_modified_iso = "1994-11-07T08:49:37.000Z";
      gets_before_external_open = shared.get_requests;
    }
    UniqueFd external_reader(
        ::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!external_reader) {
      fail_errno("open externally overwritten object");
    }
    std::vector<std::byte> external_bytes(external_expected.size());
    pread_all(external_reader.get(), external_bytes, 0);
    require(external_bytes == external_expected,
            "same-mtime generation change reused stale page cache");
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.get_requests > gets_before_external_open,
              "external generation change did not fetch new object data");
    }

    std::vector<std::byte> next_generation = external_expected;
    next_generation.back() ^= std::byte{0x5a};
    {
      std::lock_guard state_guard(shared.mutex);
      shared.object = next_generation;
      shared.etag = "\"external-put-again\"";
      shared.version_id = "version-4";
    }
    errno = 0;
    UniqueFd conflicting_reader(
        ::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
    require(!conflicting_reader && errno == EBUSY,
            "new generation opened while an old reader was still active");
    std::byte stale_byte{};
    errno = 0;
    require(::pread(external_reader.get(), &stale_byte, 1, 0) < 0 &&
                errno == ESTALE,
            "old generation reader did not become ESTALE");
    external_reader.reset();
    UniqueFd next_reader;
    retry_after_fuse_release("open next object generation", [&] {
      next_reader.reset(::open(
          file_path.c_str(), O_RDONLY | O_CLOEXEC));
      return next_reader ? 0 : -1;
    });
    std::vector<std::byte> next_bytes(next_generation.size());
    pread_all(next_reader.get(), next_bytes, 0);
    require(next_bytes == next_generation,
            "next generation reader returned stale data");
    next_reader.reset();

    {
      std::lock_guard state_guard(shared.mutex);
      shared.drop_complete_response_once = true;
    }
    UniqueFd multipart_writer(
        ::open(file_path.c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC));
    if (!multipart_writer) {
      rethrow_server_failure(shared);
      fail_errno("open mounted object for multipart write");
    }
    constexpr size_t write_chunk = 256U * 1024U;
    write_offset = 0;
    while (write_offset < final_expected.size()) {
      const size_t length = std::min(
          write_chunk, final_expected.size() - write_offset);
      test_write_all(multipart_writer.get(),
                std::span(final_expected).subspan(write_offset, length));
      write_offset += length;
    }
    int heads_before_multipart_commit;
    {
      std::lock_guard state_guard(shared.mutex);
      heads_before_multipart_commit = shared.head_requests;
    }
    multipart_writer.reset();
    int gets_after_multipart_write;
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.create_multipart_requests == 1,
              "large write did not create one multipart upload");
      require(shared.upload_part_requests == 2,
              "large write completed " +
                  std::to_string(shared.upload_part_requests) +
                  " parts instead of one full part and one tail part");
      require(shared.upload_attempts[1] == 2 &&
                  shared.upload_attempts[2] == 1,
              "multipart UploadPart retry counts are wrong");
      require(shared.complete_multipart_requests == 1 &&
                  shared.put_requests == 1,
              "large write did not complete one multipart upload");
      require(shared.complete_embedded_errors == 1 &&
                  !shared.inject_complete_slowdown,
              "HTTP 200 embedded CompleteMultipartUpload error was not "
              "retried exactly once");
      require(shared.complete_response_dropped,
              "multipart outcome-unknown fault was not injected");
      require(shared.head_requests == heads_before_multipart_commit + 1,
              "completion without Last-Modified did not issue one HEAD");
      require(shared.object == final_expected,
              "multipart upload published the wrong bytes");
      gets_after_multipart_write = shared.get_requests;
    }

    UniqueFd reopened(::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!reopened) {
      fail_errno("reopen mounted object");
    }
    if (::fstat(reopened.get(), &status) != 0) {
      fail_errno("fstat reopened object");
    }
    require(static_cast<uint64_t>(status.st_size) ==
                final_expected.size(),
            "close-to-open did not publish the new object size");
    pread_all(reopened.get(), retained_write_page, write_page_size);
    require(std::equal(retained_write_page.begin(), retained_write_page.end(),
                       final_expected.begin() + write_page_size),
            "fully written multipart page differs after close-to-open");
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.get_requests == gets_after_multipart_write,
              "multipart read-open discarded a fully written page already in page cache");
    }
    void* updated_mapping =
        ::mmap(nullptr, final_expected.size(), PROT_READ, MAP_PRIVATE,
               reopened.get(), 0);
    if (updated_mapping == MAP_FAILED) {
      fail_errno("mmap updated object");
    }
    const auto updated = std::span(
        static_cast<const std::byte*>(updated_mapping), final_expected.size());
    const bool updated_equal =
        std::equal(updated.begin(), updated.end(), final_expected.begin(),
                   final_expected.end());
    ::munmap(updated_mapping, final_expected.size());
    require(updated_equal, "reopened mmap does not contain published bytes");
    if (!cache_dir.empty()) {
      std::lock_guard state_guard(shared.mutex);
      require(shared.get_requests == gets_after_multipart_write,
              "local cache refetched the multipart object it just committed");
    }
    const std::string renamed_path = mountpoint + "/renamed.bin";
    if (cache_dir.empty()) {
      errno = 0;
      require(::rename(file_path.c_str(), renamed_path.c_str()) != 0 &&
                  errno == EBUSY,
              "uncached rename with an open handle must fail with EBUSY");
    } else {
      require(::rename(file_path.c_str(), renamed_path.c_str()) == 0,
              "cached rename did not preserve an open reader");
      std::array<std::byte, 4096> after_rename{};
      pread_all(reopened.get(), after_rename, 0);
      require(std::equal(after_rename.begin(), after_rename.end(),
                         final_expected.begin()),
              "open reader changed generation after cached rename");
    }
    reopened.reset();
    if (cache_dir.empty()) {
      retry_after_fuse_release("rename mounted object", [&] {
        return ::rename(file_path.c_str(), renamed_path.c_str());
      });
    }
    struct stat old_status{};
    errno = 0;
    require(::stat(file_path.c_str(), &old_status) != 0 && errno == ENOENT,
            "old name remained visible after RenameObject");
    UniqueFd renamed(::open(renamed_path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!renamed) {
      fail_errno("open renamed object");
    }
    renamed.reset();

    const std::string copied_path = mountpoint + "/copied.bin";
    retry_after_fuse_release(
        "rename mounted object with CopyObject fallback", [&] {
          return ::rename(renamed_path.c_str(), copied_path.c_str());
        });
    errno = 0;
    require(::stat(renamed_path.c_str(), &old_status) != 0 &&
                errno == ENOENT,
            "CopyObject fallback left the old name visible");
    UniqueFd copied(::open(copied_path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!copied) {
      fail_errno("open CopyObject-renamed object");
    }
    copied.reset();

    std::vector<std::byte> recovery_expected;
    if (!cache_dir.empty()) {
      recovery_expected.resize(192U * 1024U + 19U);
      for (size_t i = 0; i < recovery_expected.size(); ++i) {
        recovery_expected[i] = std::byte((i * 43U + 3U) & 255U);
      }
      UniqueFd recovery_writer(
          ::open(copied_path.c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC));
      if (!recovery_writer) {
        fail_errno("open cached crash-recovery writer");
      }
      test_write_all(recovery_writer.get(), recovery_expected);
      mounted.crash();
      recovery_writer.reset();

      const pid_t restarted = start_daemon(
          argv[1], mountpoint, listener.port, checksum_option, cache_dir,
          engine, reactors);
      mounted.restart(restarted);
      wait_until_mounted(copied_path, restarted);
      UniqueFd recovered;
      for (unsigned attempt = 0; attempt < 500; ++attempt) {
        recovered.reset(::open(
            copied_path.c_str(), O_RDONLY | O_CLOEXEC));
        if (recovered) {
          break;
        }
        if (errno != EBUSY && errno != EAGAIN && errno != ESTALE) {
          fail_errno("open recovered cached object");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      require(bool(recovered), "cached recovery data was not readable");
      std::vector<std::byte> recovered_bytes(recovery_expected.size());
      pread_all(recovered.get(), recovered_bytes, 0);
      require(recovered_bytes == recovery_expected,
              "cached write recovery published the wrong bytes");
      recovered.reset();
      bool published = false;
      for (unsigned attempt = 0; attempt < 500; ++attempt) {
        {
          std::lock_guard state_guard(shared.mutex);
          published = shared.object == recovery_expected;
        }
        if (published) {
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      {
        std::lock_guard state_guard(shared.mutex);
        require(published && shared.object == recovery_expected,
                "recovered server object differs from local cache (size=" +
                    std::to_string(shared.object.size()) + ")");
      }
    }

    if (!cache_dir.empty()) {
      const std::string new_append_path = mountpoint + "/new-append.bin";
      const std::array new_append_bytes{
          std::byte{'n'}, std::byte{'e'}, std::byte{'w'},
      };
      UniqueFd new_append(::open(
          new_append_path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC,
          0640));
      if (!new_append) {
        fail_errno("open new file with O_APPEND");
      }
      test_write_all(new_append.get(), new_append_bytes);
      new_append.reset();
      {
        std::lock_guard state_guard(shared.mutex);
        require(shared.object == std::vector<std::byte>(
                                new_append_bytes.begin(), new_append_bytes.end()),
                "new O_APPEND file published the wrong bytes");
      }

      errno = 0;
      UniqueFd nonempty_append(::open(
          new_append_path.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC));
      require(!nonempty_append && errno == EOPNOTSUPP,
              "O_APPEND on an existing non-empty file must fail");

      const std::array truncate_append_bytes{
          std::byte{'t'}, std::byte{'r'}, std::byte{'u'}, std::byte{'n'},
      };
      UniqueFd truncate_append(::open(
          new_append_path.c_str(),
          O_WRONLY | O_TRUNC | O_APPEND | O_CLOEXEC));
      if (!truncate_append) {
        fail_errno("open non-empty file with O_TRUNC|O_APPEND");
      }
      test_write_all(truncate_append.get(), truncate_append_bytes);
      truncate_append.reset();
      {
        std::lock_guard state_guard(shared.mutex);
        require(shared.object == std::vector<std::byte>(
                                truncate_append_bytes.begin(),
                                truncate_append_bytes.end()),
                "O_TRUNC|O_APPEND published the wrong bytes");
        // This compact mock stores only one ordinary object. Restore the main
        // object after finishing the independent open-flag scenario.
        shared.object     = recovery_expected;
        shared.object_key = "copied.bin";
      }

      const std::string empty_append_path =
          mountpoint + "/empty-append.bin";
      UniqueFd empty_creator(::open(
          empty_append_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
          0640));
      if (!empty_creator) {
        fail_errno("create empty file for O_APPEND");
      }
      empty_creator.reset();
      UniqueFd empty_append(::open(
          empty_append_path.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC));
      if (!empty_append) {
        fail_errno("open existing empty file with O_APPEND");
      }
      const std::array empty_append_bytes{std::byte{'e'}};
      test_write_all(empty_append.get(), empty_append_bytes);
      empty_append.reset();
      {
        std::lock_guard state_guard(shared.mutex);
        shared.object     = recovery_expected;
        shared.object_key = "copied.bin";
      }
    }

    size_t puts_before_truncate;
    {
      std::lock_guard state_guard(shared.mutex);
      puts_before_truncate = shared.put_requests;
    }
    retry_after_fuse_release("truncate mounted object", [&] {
      return ::truncate(copied_path.c_str(), 0);
    });
    struct stat truncated_status{};
    if (::stat(copied_path.c_str(), &truncated_status) != 0) {
      fail_errno("stat truncated mounted object");
    }
    require(truncated_status.st_size == 0,
            "standalone truncate did not update the inode size");
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.put_requests == puts_before_truncate + 1 &&
                  shared.object.empty(),
              "standalone truncate did not publish one empty PutObject");
    }

    if (!cache_dir.empty()) {
      const std::string active_unlink_path =
          mountpoint + "/active-unlink-writer.bin";
      std::array<std::byte, 4096> active_unlink_bytes{};
      for (size_t i = 0; i < active_unlink_bytes.size(); ++i) {
        active_unlink_bytes[i] =
            std::byte((i * 31U + 13U) & 255U);
      }
      UniqueFd active_unlink_writer(::open(
          active_unlink_path.c_str(),
          O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0640));
      if (!active_unlink_writer) {
        fail_errno("create active-unlink writer");
      }
      test_write_all(active_unlink_writer.get(), active_unlink_bytes);
      require(::unlink(active_unlink_path.c_str()) == 0,
              "cached unlink with an active writer failed");

      const auto write_error = [](int error) {
        return error == ESTALE || error == EIO;
      };
      std::byte rejected_byte{};
      errno = 0;
      require(::write(active_unlink_writer.get(), &rejected_byte, 1) < 0 &&
                  write_error(errno),
              "write after unlink did not fail with ESTALE/EIO");
      errno = 0;
      require(::fsync(active_unlink_writer.get()) != 0 &&
                  write_error(errno),
              "fsync after unlink did not fail with ESTALE/EIO");

      const int active_unlink_fd = active_unlink_writer.release();
      errno = 0;
      const int close_result = ::close(active_unlink_fd);
      const int close_error  = errno;
      require(close_result != 0 && write_error(close_error),
              "flush on close after unlink did not fail with ESTALE/EIO");

      // close(2) may return before the asynchronous FUSE_RELEASE is retired.
      // A same-name O_EXCL create is a cheap observable release barrier: the
      // old writer still registered in the mount makes it return EBUSY.
      UniqueFd release_probe;
      for (unsigned attempt = 0; attempt < 200; ++attempt) {
        errno = 0;
        release_probe.reset(::open(
            active_unlink_path.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0640));
        if (release_probe) {
          break;
        }
        require(errno == EBUSY || errno == EAGAIN || errno == ESTALE,
                "unexpected error while waiting for active writer release");
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
      require(bool(release_probe),
              "timed out waiting for active writer FUSE_RELEASE");
      require(::unlink(active_unlink_path.c_str()) == 0,
              "unlink of release barrier writer failed");
      release_probe.reset();

      int puts_before_active_restart;
      int multipart_before_active_restart;
      {
        std::lock_guard state_guard(shared.mutex);
        require(shared.active_writer_delete_completed,
                "active-writer unlink did not issue DeleteObject");
        puts_before_active_restart = shared.put_requests;
        multipart_before_active_restart =
            shared.create_multipart_requests +
            shared.upload_part_requests +
            shared.complete_multipart_requests;
      }
      mounted.crash();
      const pid_t restarted = start_daemon(
          argv[1], mountpoint, listener.port, checksum_option, cache_dir,
          engine, reactors);
      mounted.restart(restarted);
      wait_until_mounted(copied_path, restarted);
      {
        std::lock_guard state_guard(shared.mutex);
        require(shared.put_requests == puts_before_active_restart &&
                    shared.create_multipart_requests +
                            shared.upload_part_requests +
                            shared.complete_multipart_requests ==
                        multipart_before_active_restart,
                "unlinked active writer left recoverable cached data");
      }

      const std::string unlink_path = mountpoint + "/unlink-open.bin";
      UniqueFd unlink_writer(::open(
          unlink_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
          0640));
      if (!unlink_writer) {
        fail_errno("create object for native unlink");
      }
      test_write_all(unlink_writer.get(), expected);
      unlink_writer.reset();

      UniqueFd unlink_reader(
          ::open(unlink_path.c_str(), O_RDONLY | O_CLOEXEC));
      if (!unlink_reader) {
        fail_errno("open native-unlink reader");
      }
      std::array<std::byte, 4096> cached_prefix{};
      pread_all(unlink_reader.get(), cached_prefix, 0);
      require(std::equal(cached_prefix.begin(), cached_prefix.end(),
                         expected.begin()),
              "native-unlink reader returned wrong cached bytes");

      require(::unlink(unlink_path.c_str()) == 0,
              "cached unlink with an open reader failed");
      struct stat unlinked_status{};
      errno = 0;
      require(::stat(unlink_path.c_str(), &unlinked_status) != 0 &&
                  errno == ENOENT,
              "native unlink left the visible name in the mount");

      directory = ::opendir(mountpoint.c_str());
      if (directory == nullptr) {
        fail_errno("opendir mountpoint after native unlink");
      }
      bool leaked_unlinked_file = false;
      bool leaked_pending_directory = false;
      while (dirent* entry = ::readdir(directory)) {
        leaked_unlinked_file = leaked_unlinked_file ||
            std::string_view(entry->d_name) == "unlink-open.bin";
        leaked_pending_directory = leaked_pending_directory ||
            std::string_view(entry->d_name) == ".~ngs3fs~.pending-delete";
      }
      ::closedir(directory);
      require(!leaked_unlinked_file && !leaked_pending_directory,
              "native unlink exposed its hidden object or directory");

      std::array<std::byte, 4096> hidden_read{};
      pread_all(unlink_reader.get(), hidden_read, 256U * 1024U);
      require(std::equal(hidden_read.begin(), hidden_read.end(),
                         expected.begin() + 256U * 1024U),
              "open reader could not read after native unlink");
      unlink_reader.reset();
      for (unsigned attempt = 0; attempt < 500; ++attempt) {
        {
          std::lock_guard state_guard(shared.mutex);
          if (shared.hidden_delete_completed) {
            break;
          }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      {
        std::lock_guard state_guard(shared.mutex);
        require(shared.hidden_rename_completed,
                "native unlink did not issue RenameObject");
        require(shared.hidden_delete_completed,
                "native unlink did not delete the hidden object after close");
      }
    }

    if (!cache_dir.empty()) {
      {
        std::lock_guard state_guard(shared.mutex);
        shared.hidden_rename_completed = false;
        shared.hidden_delete_completed = false;
        shared.overwrite_hidden_key.clear();
        shared.overwrite_rename_committed = false;
        shared.overwrite_destination_gets = 0;
        shared.overwrite_destination_max_read_end = 0;
      }
      const std::string overwrite_source_path =
          mountpoint + "/overwrite-source.bin";
      const std::string overwrite_destination_path =
          mountpoint + "/overwrite-dest.bin";
      UniqueFd old_destination(::open(
          overwrite_destination_path.c_str(), O_RDONLY | O_CLOEXEC));
      if (!old_destination) {
        fail_errno("open old overwrite destination");
      }
      std::array<std::byte, 4096> old_prefix{};
      constexpr uint64_t old_prefix_offset = 4096;
      pread_all(old_destination.get(), old_prefix, old_prefix_offset);
      require(std::equal(old_prefix.begin(), old_prefix.end(),
                         overwrite_old.begin() + old_prefix_offset),
              "old overwrite destination returned wrong cached prefix");
      {
        std::lock_guard state_guard(shared.mutex);
        require(shared.overwrite_destination_gets != 0 &&
                    shared.overwrite_destination_max_read_end <
                        overwrite_old.size(),
                "old overwrite destination was not only partially cached");
      }

      require(::rename(overwrite_source_path.c_str(),
                       overwrite_destination_path.c_str()) == 0,
              "native rename over an open cached destination failed");
      struct stat overwrite_status{};
      if (::stat(overwrite_destination_path.c_str(), &overwrite_status) != 0) {
        fail_errno("stat overwritten destination");
      }
      require(static_cast<size_t>(overwrite_status.st_size) ==
                  overwrite_source.size(),
              "overwritten destination has the wrong size");

      UniqueFd new_destination(::open(
          overwrite_destination_path.c_str(), O_RDONLY | O_CLOEXEC));
      if (!new_destination) {
        fail_errno("open new overwrite destination");
      }
      std::array<std::byte, 4096> new_prefix{};
      pread_all(new_destination.get(), new_prefix, 0);
      require(std::equal(new_prefix.begin(), new_prefix.end(),
                         overwrite_source.begin()),
              "new overwrite destination did not read source content");

      constexpr uint64_t old_tail_offset = 3U * 1024U * 1024U;
      std::array<std::byte, 4096> old_tail{};
      pread_all(old_destination.get(), old_tail, old_tail_offset);
      require(std::equal(old_tail.begin(), old_tail.end(),
                         overwrite_old.begin() + old_tail_offset),
              "open old reader did not follow the hidden object");
      old_destination.reset();
      for (unsigned attempt = 0; attempt < 500; ++attempt) {
        bool deleted;
        {
          std::lock_guard state_guard(shared.mutex);
          deleted = shared.hidden_delete_completed;
        }
        if (deleted) {
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      {
        std::lock_guard state_guard(shared.mutex);
        require(shared.hidden_rename_completed &&
                    shared.overwrite_rename_committed &&
                    shared.hidden_delete_completed,
                "native overwrite did not hide, replace, and delete reader "
                "generation");
        require(shared.special_objects.find("overwrite-source.bin") ==
                    shared.special_objects.end() &&
                    shared.special_objects.find("overwrite-dest.bin") !=
                    shared.special_objects.end() &&
                    shared.special_objects.find(shared.overwrite_hidden_key) ==
                    shared.special_objects.end(),
                "native overwrite left the wrong remote object set");
      }
      new_destination.reset();
    }

    const std::string invalid_range_path =
        mountpoint + "/invalid-content-range.bin";
    UniqueFd invalid_range(
        ::open(invalid_range_path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!invalid_range) {
      fail_errno("open invalid Content-Range object");
    }
    std::byte invalid_range_byte{};
    errno = 0;
    require(::pread(invalid_range.get(), &invalid_range_byte, 1, 0) == -1 &&
                errno == EIO,
            "inconsistent S3 Content-Range did not fail with EIO");
    invalid_range.reset();

    mounted.stop();
    shared.stop.store(true);
    server.request_stop();
    server.join();
    {
      std::lock_guard state_guard(shared.mutex);
      if (shared.failure) {
        std::rethrow_exception(shared.failure);
      }
      require(shared.object.empty(),
              "server object does not contain the truncated result");
      const size_t expected_probes = cache_dir.empty() ? 1 : 2;
      require(shared.rename_probe_attempts == expected_probes &&
                  shared.rename_attempts == 2 && shared.copy_completed &&
                  shared.delete_completed &&
                  shared.copy_embedded_errors == 1 &&
                  !shared.inject_copy_slowdown,
              "rename protocol coverage was incomplete");
      if (!cache_dir.empty()) {
        require(shared.hidden_rename_completed &&
                    shared.hidden_delete_completed,
                "native unlink protocol coverage was incomplete");
      }
      if (cache_dir.empty()) {
        require(shared.maximum_active_gets >= 2,
                "read requests did not overlap across FUSE workers");
      }
    }
    if (::rmdir(mountpoint.c_str()) != 0) {
      fail_errno("rmdir mountpoint");
    }
    if (!cache_dir.empty()) {
      std::filesystem::remove_all(cache_dir);
    }
    if (!trace_dir.empty()) {
      std::filesystem::remove_all(trace_dir);
    }
    std::cout << "FUSE mmap integration passed: " << expected.size()
              << " bytes\n";
    return 0;
  } catch (const std::exception& error) {
    if (!mountpoint.empty()) {
      ::rmdir(mountpoint.c_str());
    }
    if (!cache_dir.empty()) {
      std::filesystem::remove_all(cache_dir);
    }
    if (!trace_dir.empty()) {
      fprintf(stderr, "passthrough trace retained at %s/ioctl.trace\n",
              trace_dir.c_str());
    }
    std::cerr << "fuse_mmap_integration_test: " << error.what() << '\n';
    return 1;
  }
}
