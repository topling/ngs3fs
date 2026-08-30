#include "http.hpp"

#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstdlib>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

void require(bool value, const char* message) {
  if (!value) {
    throw std::runtime_error(message);
  }
}

int select_http1(SSL*, const unsigned char** output,
                 unsigned char* output_length, const unsigned char*,
                 unsigned, void*) {
  static constexpr unsigned char protocol[] = "http/1.1";
  *output = protocol;
  *output_length = sizeof(protocol) - 1;
  return SSL_TLSEXT_ERR_OK;
}

int main() {
  char directory[] = "/tmp/ngs3fs-tls-XXXXXX";
  require(::mkdtemp(directory) != nullptr, "mkdtemp failed");
  const std::string certificate_path =
      std::string(directory) + "/certificate.pem";

  EVP_PKEY_CTX* key_context = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
  require(key_context != nullptr, "EVP_PKEY_CTX_new_id failed");
  require(EVP_PKEY_keygen_init(key_context) == 1,
          "EVP_PKEY_keygen_init failed");
  require(EVP_PKEY_CTX_set_rsa_keygen_bits(key_context, 2048) == 1,
          "EVP_PKEY_CTX_set_rsa_keygen_bits failed");
  EVP_PKEY* raw_key = nullptr;
  require(EVP_PKEY_keygen(key_context, &raw_key) == 1,
          "EVP_PKEY_keygen failed");
  EVP_PKEY_CTX_free(key_context);
  std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>
      key(raw_key, EVP_PKEY_free);

  std::unique_ptr<X509, decltype(&X509_free)> certificate(
      X509_new(), X509_free);
  require(certificate != nullptr, "X509_new failed");
  X509_set_version(certificate.get(), 2);
  ASN1_INTEGER_set(X509_get_serialNumber(certificate.get()), 1);
  X509_gmtime_adj(X509_getm_notBefore(certificate.get()), -60);
  X509_gmtime_adj(X509_getm_notAfter(certificate.get()), 3600);
  require(X509_set_pubkey(certificate.get(), key.get()) == 1,
          "X509_set_pubkey failed");
  X509_NAME* subject = X509_get_subject_name(certificate.get());
  require(X509_NAME_add_entry_by_txt(
              subject, "CN", MBSTRING_ASC,
              reinterpret_cast<const unsigned char*>("127.0.0.1"),
              -1, -1, 0) == 1,
          "X509 subject failed");
  require(X509_set_issuer_name(certificate.get(), subject) == 1,
          "X509 issuer failed");
  X509_EXTENSION* alternative_name = X509V3_EXT_conf_nid(
      nullptr, nullptr, NID_subject_alt_name,
      const_cast<char*>("IP:127.0.0.1"));
  require(alternative_name != nullptr, "X509 SAN failed");
  X509_add_ext(certificate.get(), alternative_name, -1);
  X509_EXTENSION_free(alternative_name);
  X509_EXTENSION* constraints = X509V3_EXT_conf_nid(
      nullptr, nullptr, NID_basic_constraints,
      const_cast<char*>("critical,CA:TRUE"));
  require(constraints != nullptr, "X509 basic constraints failed");
  X509_add_ext(certificate.get(), constraints, -1);
  X509_EXTENSION_free(constraints);
  require(X509_sign(certificate.get(), key.get(), EVP_sha256()) != 0,
          "X509_sign failed");

  FILE* output = fopen(certificate_path.c_str(), "w");
  require(output != nullptr, "open certificate output failed");
  require(PEM_write_X509(output, certificate.get()) == 1,
          "PEM_write_X509 failed");
  fclose(output);
  require(::setenv("SSL_CERT_FILE", certificate_path.c_str(), 1) == 0,
          "setenv SSL_CERT_FILE failed");

  std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> server_context(
      SSL_CTX_new(TLS_server_method()), SSL_CTX_free);
  require(server_context != nullptr, "server SSL_CTX_new failed");
  require(SSL_CTX_use_certificate(
              server_context.get(), certificate.get()) == 1,
          "SSL_CTX_use_certificate failed");
  require(SSL_CTX_use_PrivateKey(server_context.get(), key.get()) == 1,
          "SSL_CTX_use_PrivateKey failed");
  SSL_CTX_set_alpn_select_cb(server_context.get(), select_http1, nullptr);

  const int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  require(listener >= 0, "socket failed");
  sockaddr_in address{
      .sin_family = AF_INET,
      .sin_port = 0,
      .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)},
  };
  require(::bind(listener, reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) == 0,
          "bind failed");
  require(::listen(listener, 1) == 0, "listen failed");
  socklen_t address_length = sizeof(address);
  require(::getsockname(listener, reinterpret_cast<sockaddr*>(&address),
                        &address_length) == 0,
          "getsockname failed");

  std::exception_ptr server_error;
  std::thread server([&] {
    try {
      const int accepted = ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
      require(accepted >= 0, "accept failed");
      std::unique_ptr<SSL, decltype(&SSL_free)> ssl(
          SSL_new(server_context.get()), SSL_free);
      require(ssl != nullptr && SSL_set_fd(ssl.get(), accepted) == 1,
              "server SSL setup failed");
      require(SSL_accept(ssl.get()) == 1, "SSL_accept failed");
      std::array<char, 4096> request{};
      const int bytes = SSL_read(ssl.get(), request.data(), request.size());
      require(bytes > 0, "server SSL_read failed");
      static constexpr std::string_view response =
          "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n"
          "Connection: close\r\n\r\nok";
      require(SSL_write(ssl.get(), response.data(), int(response.size())) ==
                  int(response.size()),
              "server SSL_write failed");
      SSL_shutdown(ssl.get());
      ::close(accepted);
    } catch (...) {
      server_error = std::current_exception();
    }
  });

  std::unique_ptr<HttpClient> client = HttpClient::connect(
      "127.0.0.1", ntohs(address.sin_port), "127.0.0.1", true);
  const Response response = client->request_no_body("GET", "/");
  require(response.status == 200, "TLS response status mismatch");
  require(response.body.size() == 2 &&
              std::to_integer<char>(response.body[0]) == 'o' &&
              std::to_integer<char>(response.body[1]) == 'k',
          "TLS response body mismatch");
  client.reset();
  server.join();
  if (server_error) {
    std::rethrow_exception(server_error);
  }

  server_error = nullptr;
  std::thread stalled_server([&] {
    try {
      UniqueFd accepted(
          ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC));
      require(accepted.get() >= 0, "stalled accept failed");
      std::unique_ptr<SSL, decltype(&SSL_free)> ssl(
          SSL_new(server_context.get()), SSL_free);
      require(ssl != nullptr && SSL_set_fd(ssl.get(), accepted.get()) == 1,
              "stalled server SSL setup failed");
      require(SSL_accept(ssl.get()) == 1, "stalled SSL_accept failed");
      std::array<char, 4096> request{};
      require(SSL_read(ssl.get(), request.data(), request.size()) > 0,
              "stalled server SSL_read failed");
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } catch (...) {
      server_error = std::current_exception();
    }
  });

  std::unique_ptr<HttpClient> stalled_client = HttpClient::connect(
      "127.0.0.1", ntohs(address.sin_port), "127.0.0.1", true, 100);
  const auto timeout_start = std::chrono::steady_clock::now();
  bool timed_out = false;
  try {
    stalled_client->request_no_body("GET", "/stalled");
  } catch (const std::exception&) {
    timed_out = true;
  }
  const auto timeout_elapsed = std::chrono::steady_clock::now() -
                               timeout_start;
  require(timed_out, "stalled TLS request did not time out");
  require(timeout_elapsed < std::chrono::milliseconds(800),
          "stalled TLS timeout waited for peer close");
  stalled_server.join();
  if (server_error) {
    std::rethrow_exception(server_error);
  }

  server_error = nullptr;
  std::thread recovery_server([&] {
    try {
      UniqueFd accepted(
          ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC));
      require(accepted.get() >= 0, "recovery accept failed");
      std::unique_ptr<SSL, decltype(&SSL_free)> ssl(
          SSL_new(server_context.get()), SSL_free);
      require(ssl != nullptr && SSL_set_fd(ssl.get(), accepted.get()) == 1,
              "recovery server SSL setup failed");
      require(SSL_accept(ssl.get()) == 1, "recovery SSL_accept failed");
      std::array<char, 4096> request{};
      require(SSL_read(ssl.get(), request.data(), request.size()) > 0,
              "recovery server SSL_read failed");
      static constexpr std::string_view first_response =
          "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
      require(SSL_write(ssl.get(), first_response.data(),
                        int(first_response.size())) ==
                  int(first_response.size()),
              "recovery server first SSL_write failed");
      require(SSL_read(ssl.get(), request.data(), request.size()) > 0,
              "idle reuse server SSL_read failed");
      static constexpr std::string_view second_response =
          "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n"
          "Connection: close\r\n\r\nok";
      require(SSL_write(ssl.get(), second_response.data(),
                        int(second_response.size())) ==
                  int(second_response.size()),
              "recovery server second SSL_write failed");
      SSL_shutdown(ssl.get());
    } catch (...) {
      server_error = std::current_exception();
    }
  });

  const Response recovered =
      stalled_client->request_no_body("GET", "/recovered");
  require(recovered.status == 200, "TLS recovery status mismatch");
  require(recovered.body.size() == 2 &&
              std::to_integer<char>(recovered.body[0]) == 'o' &&
              std::to_integer<char>(recovered.body[1]) == 'k',
          "TLS recovery body mismatch");
  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  const Response idle_reused =
      stalled_client->request_no_body("GET", "/idle-reused");
  require(idle_reused.status == 200, "idle TLS reuse status mismatch");
  require(idle_reused.body.size() == 2 &&
              std::to_integer<char>(idle_reused.body[0]) == 'o' &&
              std::to_integer<char>(idle_reused.body[1]) == 'k',
          "idle TLS reuse body mismatch");
  stalled_client.reset();
  recovery_server.join();
  if (server_error) {
    std::rethrow_exception(server_error);
  }

  ::close(listener);
  ::unlink(certificate_path.c_str());
  ::rmdir(directory);
}
