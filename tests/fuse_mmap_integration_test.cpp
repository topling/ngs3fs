#include "io.hpp"

#include <nghttp2/nghttp2.h>

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
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
  RequestRange range;
  std::vector<std::byte> body;
};

struct ResponseSource {
  const std::vector<std::byte>* object = nullptr;
  std::vector<std::byte> body;
  size_t cursor = 0;
  size_t end = 0;
  std::string content_length;
  bool delay = false;
  bool delayed = false;
};

struct SharedServerState {
  std::mutex mutex;
  std::vector<std::byte> object;
  std::string object_key = "mmap.bin";
  std::string etag = "\"before-put\"";
  std::string version_id = "version-1";
  int rename_attempts = 0;
  int rename_probe_attempts = 0;
  int active_gets = 0;
  int maximum_active_gets = 0;
  int get_requests = 0;
  int list_requests = 0;
  int put_requests = 0;
  int create_multipart_requests = 0;
  int upload_part_requests = 0;
  int complete_multipart_requests = 0;
  std::map<unsigned, std::vector<std::byte>> uploaded_parts;
  std::map<unsigned, unsigned> upload_attempts;
  bool copy_completed = false;
  bool delete_completed = false;
  bool deep_present = true;
  std::atomic<bool> stop = false;
  std::exception_ptr failure;
};

struct ServerState {
  SharedServerState* shared = nullptr;
  std::map<int32_t, Request> requests;
  std::map<int32_t, std::unique_ptr<ResponseSource>> responses;
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
                         std::string_view version = {}) {
  auto response = std::make_unique<ResponseSource>();
  response->body.resize(text.size());
  memcpy(response->body.data(), text.data(), text.size());
  response->object = &response->body;
  response->end = response->body.size();
  response->content_length = std::to_string(response->body.size());
  ResponseSource* source = response.get();
  connection.responses[stream_id] = std::move(response);
  std::vector<nghttp2_nv> headers;
  headers.reserve(5);
  headers.push_back(header(":status", status));
  headers.push_back(header("content-length", source->content_length));
  headers.push_back(header("content-type", "application/xml"));
  if (!etag.empty()) {
    headers.push_back(header("etag", etag));
  }
  if (!version.empty()) {
    headers.push_back(header("x-amz-version-id", version));
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
    ++state.create_multipart_requests;
    const int submitted = submit_text_response(
        session, connection, frame->hd.stream_id, "200",
        "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId>"
        "</InitiateMultipartUploadResult>");
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "PUT" && frame->hd.type == NGHTTP2_DATA &&
      request.path.find("partNumber=") != std::string::npos) {
    require(request.path.find("uploadId=upload-1") != std::string::npos,
            "UploadPart used the wrong upload ID");
    require(request.content_length == std::to_string(request.body.size()),
            "UploadPart omitted an exact Content-Length");
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
    const std::array response_headers{
        header(":status", "200"),
        header("content-length", "0"),
        header("etag", etag),
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
    require(!state.uploaded_parts.empty(),
            "CompleteMultipartUpload had no uploaded parts");
    state.object.clear();
    for (auto& [number, part] : state.uploaded_parts) {
      require(number != 0, "invalid stored multipart part number");
      state.object.insert(state.object.end(), part.begin(), part.end());
    }
    state.uploaded_parts.clear();
    state.etag = "\"after-put\"";
    state.version_id = "version-2";
    ++state.complete_multipart_requests;
    ++state.put_requests;
    const int submitted = submit_text_response(
        session, connection, frame->hd.stream_id, "200",
        "<CompleteMultipartUploadResult><ETag>\"after-put\"</ETag>"
        "</CompleteMultipartUploadResult>",
        state.etag, state.version_id);
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
          "<Error><Code>NoSuchKey</Code></Error>");
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    const std::string expected_source =
        state.rename_attempts == 0 ? "/bucket/mmap.bin"
                                   : "/bucket/renamed.bin";
    require(request.rename_source == expected_source,
            "RenameObject source was encoded twice or changed");
    require(request.rename_source_if_match == state.etag,
            "RenameObject omitted the pinned source ETag");
    ++state.rename_attempts;
    if (state.rename_attempts == 2) {
      const std::array unsupported_headers{
          header(":status", "501"),
          header("content-length", "0"),
      };
      const int submitted = nghttp2_submit_response(
          session, frame->hd.stream_id, unsupported_headers.data(),
          unsupported_headers.size(), nullptr);
      return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
    }
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
    const std::string content_length = std::to_string(state.object.size());
    const std::array response_headers{
        header(":status", "200"),
        header("content-length", content_length),
        header("etag", state.etag),
        header("x-amz-version-id", state.version_id),
    };
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
    std::string xml = "<ListBucketResult>";
    if (state.list_requests == 1) {
      xml += "<!--";
      xml.append(5U * 1024U * 1024U, 'x');
      xml += "-->";
    }
    if (!deep && !second_root_page) {
      xml += "<IsTruncated>true</IsTruncated>"
             "<NextContinuationToken>root-page-2</NextContinuationToken>";
      xml += "<Contents><Key>";
      xml += state.object_key;
      xml += "</Key><ETag>";
      xml += state.etag;
      xml += "</ETag><Size>";
      xml += std::to_string(state.object.size());
      xml += "</Size></Contents>";
      if (state.deep_present) {
        xml += "<Contents><Key>deep</Key><ETag>\"file\"</ETag>"
               "<Size>0</Size></Contents>";
      }
    } else {
      xml += "<IsTruncated>false</IsTruncated>";
    }
    if (state.deep_present) {
      if (second_root_page) {
        xml += "<CommonPrefixes><Prefix>deep/</Prefix>"
               "</CommonPrefixes>";
      } else if (deep) {
        xml += "<Contents><Key>deep/child.bin</Key>"
               "<ETag>\"deep\"</ETag><Size>3</Size></Contents>";
      }
    }
    xml += "</ListBucketResult>";
    const int submitted = submit_text_response(
        session, connection, frame->hd.stream_id, "200", xml);
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "PUT" && frame->hd.type == NGHTTP2_DATA) {
    require(request.content_length == std::to_string(request.body.size()),
            "PutObject omitted an exact Content-Length");
    state.object = std::move(request.body);
    state.etag = "\"after-put\"";
    state.version_id = "version-2";
    ++state.put_requests;
    const std::array response_headers{
        header(":status", "200"),
        header("content-length", "0"),
        header("etag", state.etag),
        header("x-amz-version-id", state.version_id),
    };
    const int submitted = nghttp2_submit_response(
        session, frame->hd.stream_id, response_headers.data(),
        response_headers.size(), nullptr);
    return submitted == 0 ? 0 : NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (request.method == "PUT" && frame->hd.type == NGHTTP2_HEADERS) {
    state.object.clear();
    state.etag = "\"after-put\"";
    state.version_id = "version-2";
    ++state.put_requests;
    const std::array response_headers{
        header(":status", "200"),
        header("content-length", "0"),
        header("etag", state.etag),
        header("x-amz-version-id", state.version_id),
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

  const RequestRange range = request.range;
  if (!range.valid || range.last >= state.object.size()) {
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }

  auto response = std::make_unique<ResponseSource>();
  response->object = &state.object;
  response->delay = true;
  response->cursor = static_cast<size_t>(range.first);
  response->end = static_cast<size_t>(range.last + 1);
  response->content_length =
      std::to_string(response->end - response->cursor);
  ResponseSource* source = response.get();
  connection.responses[frame->hd.stream_id] = std::move(response);
  ++state.active_gets;
  ++state.get_requests;
  state.maximum_active_gets =
      std::max(state.maximum_active_gets, state.active_gets);

  const std::array response_headers{
      header(":status", "206"),
      header("content-length", source->content_length),
      header("etag", state.etag),
      header("x-amz-version-id", state.version_id),
  };
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
      if (count < 0) {
        fail_errno("server read");
      }
      const ssize_t consumed = nghttp2_session_mem_recv(
          session.get(), input.data(), static_cast<size_t>(count));
      require(consumed == count, "nghttp2 server did not consume input");
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

  void stop() noexcept {
    if (process_ <= 0) {
      return;
    }
    const pid_t unmount = ::fork();
    if (unmount == 0) {
      ::execlp("fusermount3", "fusermount3", "-u", mountpoint_.c_str(),
               static_cast<char*>(nullptr));
      _exit(127);
    }
    if (unmount > 0) {
      int unmount_status = 0;
      while (::waitpid(unmount, &unmount_status, 0) < 0 && errno == EINTR) {
      }
    }

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
                   uint16_t port) {
  const std::string port_text = std::to_string(port);
  const std::string uid_text  = std::to_string(::getuid());
  const std::string gid_text  = std::to_string(::getgid());
  const pid_t process = ::fork();
  if (process < 0) {
    fail_errno("fork");
  }
  if (process == 0) {
    ::execl(executable.data(), "ngs3fs", "-e", "127.0.0.1", "-p",
            port_text.c_str(), "-a", "mock-s3", "-b", "bucket", "-u",
            uid_text.c_str(), "-g", gid_text.c_str(), "-m", "0640", "-D",
            "0750", "-R", "256KiB", "-I", "1", "-f", mountpoint.data(),
            static_cast<char*>(nullptr));
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
  if (argc != 2) {
    std::cerr << "missing ngs3fs executable path\n";
    return 2;
  }
  if (::access("/dev/fuse", R_OK | W_OK) != 0) {
    std::cout << "SKIP: /dev/fuse is unavailable\n";
    return 0;
  }

  std::string mountpoint;
  try {
    std::vector<std::byte> expected(512U * 1024U + 37U);
    for (size_t i = 0; i < expected.size(); ++i) {
      expected[i] = static_cast<std::byte>((i * 29U + 7U) & 0xffU);
    }
    std::vector<std::byte> small_expected = expected;
    small_expected.resize(expected.size() - 8192U - 37U);
    for (size_t i = 0; i < small_expected.size(); ++i) {
      small_expected[i] ^= static_cast<std::byte>(0x5aU);
    }
    std::vector<std::byte> final_expected(
        8U * 1024U * 1024U + 256U * 1024U + 37U);
    for (size_t i = 0; i < final_expected.size(); ++i) {
      final_expected[i] = static_cast<std::byte>((i * 17U + 11U) & 0xffU);
    }

    Listener listener = make_listener();
    SharedServerState shared;
    shared.object = expected;
    std::jthread server(run_server, listener.socket.get(), std::ref(shared));

    mountpoint = make_mountpoint();
    const pid_t process = start_daemon(argv[1], mountpoint, listener.port);
    MountedProcess mounted(mountpoint, process);
    const std::string file_path = mountpoint + "/mmap.bin";
    wait_until_mounted(file_path, process);

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
    if (::close(alias_fd) != 0) {
      fail_errno("close duplicated mounted writer");
    }
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.create_multipart_requests == 0 &&
                  shared.upload_part_requests == 0 &&
                  shared.complete_multipart_requests == 0,
              "small write unexpectedly used multipart upload");
      require(shared.put_requests == 1,
              "flush did not publish one small PutObject");
    }
    UniqueFd visible_after_flush(
        ::open(file_path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!visible_after_flush) {
      fail_errno("close-to-open while duplicated writer remains");
    }
    std::array<std::byte, 1> visible_byte{};
    pread_all(visible_after_flush.get(), visible_byte, 0);
    require(visible_byte[0] == small_expected[0],
            "flush did not expose the completed object");
    visible_after_flush.reset();
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

    UniqueFd multipart_writer(
        ::open(file_path.c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC));
    if (!multipart_writer) {
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
    multipart_writer.reset();
    {
      std::lock_guard state_guard(shared.mutex);
      require(shared.create_multipart_requests == 1,
              "large write did not create one multipart upload");
      require(shared.upload_part_requests == 2,
              "large write did not upload one full part and one tail part");
      require(shared.upload_attempts[1] == 2 &&
                  shared.upload_attempts[2] == 1,
              "multipart UploadPart retry counts are wrong");
      require(shared.complete_multipart_requests == 1 &&
                  shared.put_requests == 1,
              "large write did not complete one multipart upload");
      require(shared.object == final_expected,
              "multipart upload published the wrong bytes");
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
    const std::string renamed_path = mountpoint + "/renamed.bin";
    errno = 0;
    require(::rename(file_path.c_str(), renamed_path.c_str()) != 0 &&
                errno == EBUSY,
            "rename with an open handle must fail with EBUSY");
    reopened.reset();
    if (::rename(file_path.c_str(), renamed_path.c_str()) != 0) {
      fail_errno("rename mounted object");
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
    if (::rename(renamed_path.c_str(), copied_path.c_str()) != 0) {
      fail_errno("rename mounted object with CopyObject fallback");
    }
    errno = 0;
    require(::stat(renamed_path.c_str(), &old_status) != 0 &&
                errno == ENOENT,
            "CopyObject fallback left the old name visible");
    UniqueFd copied(::open(copied_path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!copied) {
      fail_errno("open CopyObject-renamed object");
    }
    copied.reset();

    size_t puts_before_truncate;
    {
      std::lock_guard state_guard(shared.mutex);
      puts_before_truncate = shared.put_requests;
    }
    if (::truncate(copied_path.c_str(), 0) != 0) {
      fail_errno("truncate mounted object");
    }
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
      require(shared.rename_probe_attempts == 1 &&
                  shared.rename_attempts == 2 && shared.copy_completed &&
                  shared.delete_completed,
              "rename protocol coverage was incomplete");
      require(shared.maximum_active_gets >= 2,
              "read requests did not overlap across FUSE workers");
    }
    if (::rmdir(mountpoint.c_str()) != 0) {
      fail_errno("rmdir mountpoint");
    }
    std::cout << "FUSE mmap integration passed: " << expected.size()
              << " bytes\n";
    return 0;
  } catch (const std::exception& error) {
    if (!mountpoint.empty()) {
      ::rmdir(mountpoint.c_str());
    }
    std::cerr << "fuse_mmap_integration_test: " << error.what() << '\n';
    return 1;
  }
}
