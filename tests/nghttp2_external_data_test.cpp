#include "http.hpp"
#include "io.hpp"

#include <nghttp2/nghttp2.h>

#include <sys/socket.h>

#include <algorithm>
#include <array>
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

struct SessionDeleter {
  void operator()(nghttp2_session* session) const {
    nghttp2_session_del(session);
  }
};

struct CallbacksDeleter {
  void operator()(nghttp2_session_callbacks* callbacks) const {
    nghttp2_session_callbacks_del(callbacks);
  }
};

struct OptionDeleter {
  void operator()(nghttp2_option* option) const {
    nghttp2_option_del(option);
  }
};

template <typename T, typename Deleter>
using Handle = std::unique_ptr<T, Deleter>;

struct ClientCapture {
  ExternalDataIngress ingress{7};
  int data_frames = 0;
  int data_chunk_callbacks = 0;
  int stream_closes = 0;
  size_t external_bytes = 0;
  size_t copied_data_bytes = 0;
  size_t shadow_callback_bytes = 0;
  size_t ordinary_callback_bytes = 0;
};

struct BodySource {
  std::string_view body;
  size_t offset = 0;
};

int on_begin_frame(nghttp2_session*, const nghttp2_frame_hd* h,
                   void* user_data) {
  auto* c = static_cast<ClientCapture*>(user_data);
  return c->ingress.capture_frame_header(*h)
             ? 0
             : NGHTTP2_ERR_CALLBACK_FAILURE;
}

int on_frame_recv(nghttp2_session*, const nghttp2_frame* frame,
                  void* user_data) {
  auto* capture = static_cast<ClientCapture*>(user_data);
  if (frame->hd.type == NGHTTP2_DATA) {
    ++capture->data_frames;
  }
  return 0;
}

int on_data_chunk(nghttp2_session*, uint8_t, int32_t,
                  const uint8_t*, size_t length, void* user_data) {
  auto* capture = static_cast<ClientCapture*>(user_data);
  ++capture->data_chunk_callbacks;
  if (capture->ingress.shadow_advance_active()) {
    capture->shadow_callback_bytes += length;
  } else {
    capture->ordinary_callback_bytes += length;
  }
  return 0;
}

int on_stream_close(nghttp2_session*, int32_t, uint32_t,
                    void* user_data) {
  auto* capture = static_cast<ClientCapture*>(user_data);
  ++capture->stream_closes;
  return 0;
}

ssize_t read_body(nghttp2_session*, int32_t, uint8_t* buffer,
                  size_t length, uint32_t* data_flags,
                  nghttp2_data_source* source, void*) {
  auto* body = static_cast<BodySource*>(source->ptr);
  const size_t remaining = body->body.size() - body->offset;
  const size_t count = std::min(length, remaining);
  memcpy(buffer, body->body.data() + body->offset, count);
  body->offset += count;
  if (body->offset == body->body.size()) {
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
  }
  return static_cast<ssize_t>(count);
}

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

void pump_normal(nghttp2_session* source, nghttp2_session* destination) {
  for (;;) {
    const uint8_t* bytes = nullptr;
    const ssize_t length = nghttp2_session_mem_send(source, &bytes);
    assert(length >= 0);
    if (length == 0) {
      return;
    }
    const ssize_t consumed = nghttp2_session_mem_recv(
        destination, bytes, static_cast<size_t>(length));
    assert(consumed == length);
  }
}

void pump_external_data(nghttp2_session* source,
                        nghttp2_session* destination,
                        ClientCapture& capture,
                        int32_t expected_stream_id) {
  std::array<int, 2> raw_sockets{-1, -1};
  assert(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0,
                      raw_sockets.data()) == 0);
  UniqueFd sender(raw_sockets[0]);
  UniqueFd receiver(raw_sockets[1]);
  Pipe data_pipe = Pipe::create();

  for (;;) {
    const uint8_t* bytes = nullptr;
    const ssize_t signed_length = nghttp2_session_mem_send(source, &bytes);
    assert(signed_length >= 0);
    if (signed_length == 0) {
      return;
    }

    const size_t length = static_cast<size_t>(signed_length);
    write_all(
        sender.get(),
        std::span(reinterpret_cast<const std::byte*>(bytes), length));

    const auto received = capture.ingress.receive_one(
        destination, receiver.get(), data_pipe.write_fd(),
        data_pipe.capacity(), expected_stream_id);
    capture.external_bytes += received.spliced_payload;
    if (received.header.type == NGHTTP2_DATA) {
      capture.copied_data_bytes += received.copied_payload;
    }

    if (received.spliced_payload != 0) {
      std::vector<std::byte> actual(received.spliced_payload);
      read_all(data_pipe.read_fd(), actual);
      const size_t header_size = length - received.header.length;
      const auto expected = std::span(
          reinterpret_cast<const std::byte*>(bytes + header_size),
          received.spliced_payload);
      assert(std::ranges::equal(actual, expected));
    }
  }
}

int main() {
  nghttp2_session_callbacks* raw_callbacks = nullptr;
  assert(nghttp2_session_callbacks_new(&raw_callbacks) == 0);
  Handle<nghttp2_session_callbacks, CallbacksDeleter> callbacks(raw_callbacks);
  nghttp2_session_callbacks_set_on_begin_frame_callback(callbacks.get(),
                                                         on_begin_frame);
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks.get(),
                                                        on_frame_recv);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks.get(),
                                                             on_data_chunk);
  nghttp2_session_callbacks_set_on_stream_close_callback(callbacks.get(),
                                                          on_stream_close);

  nghttp2_option* raw_option = nullptr;
  assert(nghttp2_option_new(&raw_option) == 0);
  Handle<nghttp2_option, OptionDeleter> option(raw_option);
  nghttp2_option_set_no_auto_window_update(option.get(), 1);

  ClientCapture capture;
  nghttp2_session* raw_client = nullptr;
  assert(nghttp2_session_client_new2(&raw_client, callbacks.get(), &capture,
                                     option.get()) == 0);
  Handle<nghttp2_session, SessionDeleter> client(raw_client);

  nghttp2_session_callbacks* raw_server_callbacks = nullptr;
  assert(nghttp2_session_callbacks_new(&raw_server_callbacks) == 0);
  Handle<nghttp2_session_callbacks, CallbacksDeleter> server_callbacks(
      raw_server_callbacks);

  nghttp2_session* raw_server = nullptr;
  assert(nghttp2_session_server_new(&raw_server, server_callbacks.get(),
                                    nullptr) == 0);
  Handle<nghttp2_session, SessionDeleter> server(raw_server);

  assert(nghttp2_submit_settings(client.get(), NGHTTP2_FLAG_NONE, nullptr, 0) ==
         0);
  assert(nghttp2_submit_settings(server.get(), NGHTTP2_FLAG_NONE, nullptr, 0) ==
         0);

  std::array request_headers{
      header(":method", "GET"),
      header(":scheme", "http"),
      header(":authority", "localhost"),
      header(":path", "/bucket/key"),
  };
  const int stream_id = nghttp2_submit_request(
      client.get(), nullptr, request_headers.data(), request_headers.size(),
      nullptr, nullptr);
  assert(stream_id == 1);

  pump_normal(client.get(), server.get());

  constexpr std::string_view payload =
      "external DATA never enters the receiving userspace buffer";
  BodySource body{.body = payload};
  nghttp2_data_provider provider{
      .source = {.ptr = &body},
      .read_callback = read_body,
  };
  const std::string content_length = std::to_string(payload.size());
  std::array response_headers{
      header(":status", "200"),
      header("content-length", content_length),
  };
  assert(nghttp2_submit_response(
             server.get(), stream_id, response_headers.data(),
             response_headers.size(), &provider) == 0);

  pump_external_data(server.get(), client.get(), capture, stream_id);

  assert(capture.external_bytes == payload.size());
  assert(capture.data_frames == 1);
  assert(capture.data_chunk_callbacks > 1);
  assert(capture.shadow_callback_bytes == payload.size());
  assert(capture.ordinary_callback_bytes == 0);
  assert(capture.stream_closes == 1);
  assert(nghttp2_session_consume(client.get(), stream_id, payload.size()) == 0);

  std::array second_request_headers{
      header(":method", "GET"),
      header(":scheme", "http"),
      header(":authority", "localhost"),
      header(":path", "/bucket/copied-key"),
  };
  const int second_stream_id = nghttp2_submit_request(
      client.get(), nullptr, second_request_headers.data(),
      second_request_headers.size(), nullptr, nullptr);
  assert(second_stream_id == 3);
  pump_normal(client.get(), server.get());

  constexpr std::string_view ordinary_payload = "ordinary callback payload";
  BodySource second_body{.body = ordinary_payload};
  nghttp2_data_provider second_provider{
      .source = {.ptr = &second_body},
      .read_callback = read_body,
  };
  const std::string second_content_length =
      std::to_string(ordinary_payload.size());
  std::array second_response_headers{
      header(":status", "200"),
      header("content-length", second_content_length),
  };
  assert(nghttp2_submit_response(
             server.get(), second_stream_id, second_response_headers.data(),
             second_response_headers.size(), &second_provider) == 0);
  // Deliberately provide the previous stream as the expected destination.
  // DATA for another stream must take the validated copied path and must
  // never be spliced into that destination pipe.
  pump_external_data(server.get(), client.get(), capture, stream_id);

  assert(capture.ordinary_callback_bytes == ordinary_payload.size());
  assert(capture.copied_data_bytes == ordinary_payload.size());
  assert(capture.external_bytes == payload.size());
  assert(capture.data_frames == 2);
  assert(capture.stream_closes == 2);

  std::cout << "stock nghttp2 shadow DATA state-machine test passed\n";
  return 0;
}
