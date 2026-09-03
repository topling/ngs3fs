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
#include <errno.h>
#include <charconv>
#include <chrono>
#include <stddef.h>
#include <stdint.h>
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

void write_all(int fd, std::span<const std::byte> bytes) {
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
  RequestRange range;
  std::vector<std::byte> body;
};

struct ResponseSource {
  const std::vector<std::byte>* object = nullptr;
  std::shared_ptr<struct SpecialObject> keepalive;
  std::vector<std::byte> body;
  size_t cursor = 0;
  size_t end = 0;
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
  std::vector<RequestRange> get_ranges;
};

struct SharedServerState {
  std::mutex mutex;
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
  int head_requests = 0;
  int list_requests = 0;
  int put_requests = 0;
  int create_multipart_requests = 0;
  int upload_part_requests = 0;
  int complete_multipart_requests = 0;
  int checksum_mode_requests = 0;
  int object_attributes_requests = 0;
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
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  const size_t count =
      std::min(length, response.end - response.cursor);
  memcpy(buffer, response.object->data() + response.cursor, count);
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
  memcpy(response->body.data(), text.data(), text.size());
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
    state.copy_completed = true;
    state.object_key = "copied.bin";
    const std::array response_headers{
        header(":status", "200"),
        header("content-length", "0"),
    };
    const int submitted = nghttp2_submit_response(
        session, frame->hd.stream_id, response_headers.data(),
        response_headers.size(), nullptr);
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "DELETE" && frame->hd.type == NGHTTP2_HEADERS) {
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
          header("x-amz-version-id", object.version_id),
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
        header("x-amz-version-id", state.version_id),
        header("last-modified", state.last_modified),
    };
    if (!state.write_id.empty()) {
      response_headers.push_back(header(
          "x-amz-meta-ngs3fs-write-id", state.write_id));
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
    if (!deep && !second_root_page &&
        !state.object_key.starts_with(".~ngs3fs~.pending-delete/")) {
      xml += "<s3:IsTruncated>1</s3:IsTruncated>"
             "<s3:NextContinuationToken>root-page&#x2D;2"
             "</s3:NextContinuationToken>";
      xml += "<s3:Contents><s3:Key>";
      xml += state.object_key;
      xml += "</s3:Key><s3:ETag>";
      xml += state.etag;
      xml += "</s3:ETag><s3:LastModified>";
      xml += state.last_modified_iso;
      xml += "</s3:LastModified><s3:Size>";
      xml += std::to_string(state.object.size());
      xml += "</s3:Size></s3:Contents>";
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
          header("x-amz-version-id", object->version_id),
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
        header("x-amz-version-id", state.version_id),
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
        header("x-amz-version-id", state.version_id),
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
  response->content_length =
      std::to_string(response->end - response->cursor);
  response->content_range =
      "bytes " + std::to_string(range.first) + '-' +
      std::to_string(range.last) + '/' +
      std::to_string(object->size());
  ResponseSource* source = response.get();
  connection.responses[frame->hd.stream_id] = std::move(response);
  ++state.active_gets;
  ++state.get_requests;
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
      header("x-amz-version-id", object_version),
  };
  if (!request.checksum_mode.empty()) {
    require(request.checksum_mode == "ENABLED" && complete_object,
            "checksum mode was used for a partial object range");
    ++state.checksum_mode_requests;
    checksum = encoded_checksum(state.checksum, *object);
    response_headers.push_back(
        header(checksum_header_name(state.checksum), checksum));
    response_headers.push_back(
        header("x-amz-checksum-type", "FULL_OBJECT"));
  } else if (state.checksum == CHECKSUM_CRC64XZ) {
    checksum = response_checksum(state.checksum, *object);
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
  if (request != connection.requests.end() && request->second.method == "GET") {
    SharedServerState& state = *connection.shared;
    std::lock_guard state_guard(state.mutex);
    if (state.active_gets != 0) {
      --state.active_gets;
    }
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
    write_all(socket_fd,
              std::span(reinterpret_cast<const std::byte*>(bytes),
                        static_cast<size_t>(length)));
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
  } catch (...) {
    record_server_failure(shared, std::current_exception());
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

  void stop() noexcept {
    if (process_ <= 0) {
      return;
    }
    unmount(false);

    for (int attempt = 0; attempt < 200; ++attempt) {
      int status = 0;
      const pid_t result = ::waitpid(process_, &status, WNOHANG);
      if (result == process_) {
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
                   std::string_view cache_dir) {
  const std::string port_text = std::to_string(port);
  const std::string uid_text  = std::to_string(::getuid());
  const std::string gid_text  = std::to_string(::getgid());
  const pid_t process = ::fork();
  if (process < 0) {
    fail_errno("fork");
  }
  if (process == 0) {
    if (cache_dir.empty()) {
      ::execl(executable.data(), "ngs3fs", "-e", "127.0.0.1", "-p",
              port_text.c_str(), "-a", "mock-s3", "-b", "bucket", "-u",
              uid_text.c_str(), "-g", gid_text.c_str(), "-m", "0640", "-D",
              "0750", "--io-size", "384KiB", "-R", "256KiB", "-I", "1",
              "--checksum", checksum.data(), "--verify-read-checksum",
              "--stats-interval", "86400", "-f", mountpoint.data(),
              static_cast<char*>(nullptr));
    } else {
      ::execl(executable.data(), "ngs3fs", "-e", "127.0.0.1", "-p",
              port_text.c_str(), "-a", "mock-s3", "-b", "bucket", "-u",
              uid_text.c_str(), "-g", gid_text.c_str(), "-m", "0640", "-D",
              "0750", "--io-size", "384KiB", "-R", "256KiB", "-I", "1",
              "--checksum", checksum.data(), "--verify-read-checksum",
              "--stats-interval", "86400", "--max-cache-fetch-size", "1MiB",
              "-L", cache_dir.data(),
              "--cache-reserve", "0", "-f", mountpoint.data(),
              static_cast<char*>(nullptr));
    }
    _exit(127);
  }
  return process;
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
  if (argc < 2 || argc > 4) {
    std::cerr << "usage: fuse_mmap_integration_test NGS3FS "
                 "[CHECKSUM [cache]]\n";
    return 2;
  }
  if (::access("/dev/fuse", R_OK | W_OK) != 0) {
    std::cout << "SKIP: /dev/fuse is unavailable\n";
    return 0;
  }

  std::string mountpoint;
  std::string cache_dir;
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
    std::vector<std::byte> overwrite_old(2U * 1024U * 1024U);
    std::vector<std::byte> overwrite_source(2U * 1024U * 1024U);
    for (size_t i = 0; i < overwrite_old.size(); ++i) {
      overwrite_old[i] = static_cast<std::byte>((i * 31U + 19U) & 0xffU);
      overwrite_source[i] = static_cast<std::byte>((i * 47U + 23U) & 0xffU);
    }

    Listener listener = make_listener();
    SharedServerState shared;
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
    add_checksum_object("checksum-fail.bin", 2);
    add_checksum_object("checksum-unsupported.bin", 0, 0, true);
    std::jthread server(run_server, listener.socket.get(), std::ref(shared));

    mountpoint = make_mountpoint();
    if (argc == 4) {
      if (std::string_view(argv[3]) != "cache") {
        throw std::invalid_argument("unknown integration-test mode");
      }
      cache_dir = make_mountpoint();
    }
    const std::string checksum_option(checksum_option_name(checksum));
    const pid_t process = start_daemon(
        argv[1], mountpoint, listener.port, checksum_option, cache_dir);
    MountedProcess mounted(mountpoint, process);
    const std::string file_path = mountpoint + "/mmap.bin";
    wait_until_mounted(file_path, process);

    {
      struct statvfs status{};
      if (::statvfs(mountpoint.c_str(), &status) != 0) {
        fail_errno("statvfs mountpoint");
      }
      require(status.f_bsize == 384U * 1024U,
              "configured statfs optimal I/O size was not applied");
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
      std::array<std::byte, 4096> retry_verified{};
      pread_all(retry_first.get(), retry_verified, 64U * 1024U);
      require(std::equal(retry_verified.begin(), retry_verified.end(),
                         checksum_test.begin() + 64U * 1024U),
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
    const size_t first_write = std::min<size_t>(37, small_expected.size());
    write_all(writer.get(), std::span(small_expected).first(first_write));
    if (::fsync(writer.get()) != 0) {
      fail_errno("fsync partial mounted page");
    }
    size_t write_offset = first_write;
    while (write_offset < small_expected.size()) {
      const size_t length = std::min<size_t>(
          64U * 1024U, small_expected.size() - write_offset);
      write_all(writer.get(),
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
#if !defined(__SANITIZE_THREAD__)
      // mincore observes an evictable kernel cache, not a program invariant.
      // TSan's instrumentation can delay this check enough for ordinary
      // reclaim to make it nondeterministic; byte/no-GET validation below
      // remains active in every build.
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
      require(std::all_of(
                  residency.begin(), residency.end(),
                  [](unsigned char page) { return (page & 1U) != 0; }),
              "partial cached writes were not retained in page cache");
      ::munmap(resident_mapping, small_expected.size());
#endif
    }
    std::vector<std::byte> visible_bytes(small_expected.size());
    pread_all(visible_after_flush.get(), visible_bytes, 0);
    require(visible_bytes == small_expected,
            "flush did not expose the completed object");
    visible_after_flush.reset();
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.get_requests == gets_after_small_write,
              "read-open discarded page cache left by the local writer");
      require(shared.checksum_mode_requests ==
                  checksum_mode_after_small_write,
              "cached read unexpectedly requested checksum mode");
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
      write_all(multipart_writer.get(),
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
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.get_requests == gets_after_multipart_write,
              "multipart read-open discarded locally written page cache");
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
      write_all(recovery_writer.get(), recovery_expected);
      mounted.crash();
      recovery_writer.reset();

      const pid_t restarted = start_daemon(
          argv[1], mountpoint, listener.port, checksum_option, cache_dir);
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
      write_all(new_append.get(), new_append_bytes);
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
      write_all(truncate_append.get(), truncate_append_bytes);
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
      write_all(empty_append.get(), empty_append_bytes);
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
      write_all(active_unlink_writer.get(), active_unlink_bytes);
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
          argv[1], mountpoint, listener.port, checksum_option, cache_dir);
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
      write_all(unlink_writer.get(), expected);
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
      pread_all(old_destination.get(), old_prefix, 0);
      require(std::equal(old_prefix.begin(), old_prefix.end(),
                         overwrite_old.begin()),
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

      constexpr uint64_t old_tail_offset = 1536U * 1024U;
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
                  shared.delete_completed,
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
    std::cerr << "fuse_mmap_integration_test: " << error.what() << '\n';
    return 1;
  }
}
