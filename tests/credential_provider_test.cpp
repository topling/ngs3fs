#include "credentials.hpp"
#include "io.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <errno.h>
#include <span>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

struct TemporaryCredentials {
  TemporaryCredentials() {
    char pattern[] = "/tmp/ngs3fs-credentials.XXXXXX";
    char* created = ::mkdtemp(pattern);
    assert(created != nullptr);
    directory = created;
    credentials = directory + "/credentials";
    config = directory + "/config";
    token = directory + "/token";
  }

  ~TemporaryCredentials() {
    ::unlink(credentials.c_str());
    ::unlink(config.c_str());
    ::unlink(token.c_str());
    ::rmdir(directory.c_str());
  }

  void write_file(const std::string& path, std::string_view text) {
    UniqueFd file(::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
                         0600));
    assert(file);
    write_all(file.get(), std::span(
        reinterpret_cast<const std::byte*>(text.data()), text.size()));
  }

  std::string directory;
  std::string credentials;
  std::string config;
  std::string token;
};

struct CredentialExchange {
  std::string request_line;
  std::string required_header;
  std::string body;
};

UniqueFd make_listener(uint16_t& port) {
  UniqueFd listener(
      ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP));
  assert(listener);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  assert(::bind(listener.get(), reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) == 0);
  assert(::listen(listener.get(), 8) == 0);
  socklen_t length = sizeof(address);
  assert(::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address),
                       &length) == 0);
  port = ntohs(address.sin_port);
  return listener;
}

UniqueFd accept_credential_connection(int listener) {
  for (;;) {
    const int socket = ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
    if (socket >= 0) {
      return UniqueFd(socket);
    }
    assert(errno == EINTR);
  }
}

void send_credential_text(int socket, std::string_view text) {
  write_all(socket, std::span(
      reinterpret_cast<const std::byte*>(text.data()), text.size()));
}

std::string read_credential_request(int socket) {
  std::string request;
  while (!request.ends_with("\r\n\r\n")) {
    char ch = 0;
    const ssize_t bytes = ::read(socket, &ch, 1);
    assert(bytes == 1);
    request.push_back(ch);
    assert(request.size() <= 512U * 1024U);
  }
  return request;
}

void reject_h2_probe(int listener) {
  UniqueFd probe = accept_credential_connection(listener);
  std::array<std::byte, 24> magic{};
  read_all(probe.get(), magic);
  constexpr std::string_view expected =
      "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
  assert(memcmp(magic.data(), expected.data(), expected.size()) == 0);
  send_credential_text(
      probe.get(),
      "HTTP/1.1 505 HTTP Version Not Supported\r\n"
      "content-length: 0\r\nconnection: close\r\n\r\n");
  assert(::shutdown(probe.get(), SHUT_WR) == 0);
  std::array<std::byte, 4096> discarded{};
  for (;;) {
    const ssize_t bytes = ::read(
        probe.get(), discarded.data(), discarded.size());
    if (bytes > 0 || (bytes < 0 && errno == EINTR)) {
      continue;
    }
    assert(bytes == 0 || errno == ECONNRESET);
    return;
  }
}

void serve_credentials(int listener,
                       std::vector<CredentialExchange> exchanges) {
  for (const CredentialExchange& exchange : exchanges) {
    reject_h2_probe(listener);
    UniqueFd socket = accept_credential_connection(listener);
    const std::string request = read_credential_request(socket.get());
    assert(request.starts_with(exchange.request_line));
    if (!exchange.required_header.empty()) {
      assert(request.find(exchange.required_header) != std::string::npos);
    }
    send_credential_text(
        socket.get(),
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n"
        "connection: close\r\ncontent-length: " +
            std::to_string(exchange.body.size()) + "\r\n\r\n" +
            exchange.body);
  }
}

void clear_credential_environment() {
  constexpr const char* names[] = {
      "AWS_ACCESS_KEY_ID",
      "AWS_SECRET_ACCESS_KEY",
      "AWS_SESSION_TOKEN",
      "AWS_WEB_IDENTITY_TOKEN_FILE",
      "AWS_ROLE_ARN",
      "AWS_ROLE_SESSION_NAME",
      "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
      "AWS_CONTAINER_CREDENTIALS_FULL_URI",
      "AWS_CONTAINER_AUTHORIZATION_TOKEN",
      "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
      "AWS_ENDPOINT_URL_STS",
      "AWS_EC2_METADATA_DISABLED",
      "AWS_EC2_METADATA_SERVICE_ENDPOINT",
  };
  for (const char* name : names) {
    assert(::unsetenv(name) == 0);
  }
}

CredentialProviderOptions options() {
  return CredentialProviderOptions{
      .region = "us-east-1",
      .connect_timeout_ms = 100,
      .request_timeout_ms = 1000,
      .metadata_timeout_ms = 100,
      .shared_reload_ms = 10,
  };
}

void configure_files(const TemporaryCredentials& files) {
  assert(::setenv("AWS_SHARED_CREDENTIALS_FILE",
                  files.credentials.c_str(), 1) == 0);
  assert(::setenv("AWS_CONFIG_FILE", files.config.c_str(), 1) == 0);
  assert(::setenv("AWS_PROFILE", "default", 1) == 0);
  assert(::setenv("AWS_EC2_METADATA_DISABLED", "true", 1) == 0);
}

void test_environment() {
  TemporaryCredentials files;
  configure_files(files);
  assert(::setenv("AWS_ACCESS_KEY_ID", "environment-key", 1) == 0);
  assert(::setenv("AWS_SECRET_ACCESS_KEY", "environment-secret", 1) == 0);
  assert(::setenv("AWS_SESSION_TOKEN", "environment-token", 1) == 0);
  CredentialProvider provider(options());
  const Credentials& credentials = provider.get();
  assert(credentials.access_key_id == "environment-key");
  assert(credentials.secret_access_key == "environment-secret");
  assert(credentials.session_token == "environment-token");
  assert(std::string_view(provider.source_name()) == "environment");
  assert(!provider.refreshable());
  clear_credential_environment();
}

void test_partial_environment() {
  TemporaryCredentials files;
  configure_files(files);
  assert(::setenv("AWS_ACCESS_KEY_ID", "incomplete", 1) == 0);
  bool threw = false;
  try {
    CredentialProvider provider(options());
  } catch (const std::exception&) {
    threw = true;
  }
  assert(threw);
  clear_credential_environment();
}

void test_shared_file() {
  TemporaryCredentials files;
  configure_files(files);
  files.write_file(files.credentials,
                   "[default]\n"
                   "aws_access_key_id = file-key\n"
                   "aws_secret_access_key = file-secret\n"
                   "aws_session_token = file-token\n");
  files.write_file(files.config,
                   "[default]\n"
                   "region = us-east-1\n"
                   "aws_access_key_id = config-key\n"
                   "aws_secret_access_key = config-secret\n");
  CredentialProvider provider(options());
  const Credentials& credentials = provider.get();
  assert(credentials.access_key_id == "file-key");
  assert(credentials.secret_access_key == "file-secret");
  assert(credentials.session_token == "file-token");
  assert(std::string_view(provider.source_name()) == "shared-file");
  assert(provider.refreshable());
  files.write_file(files.credentials,
                   "[default]\n"
                   "aws_access_key_id = rotated-key\n"
                   "aws_secret_access_key = rotated-secret\n");
  for (unsigned attempt = 0; attempt != 100; ++attempt) {
    if (provider.get().access_key_id == "rotated-key") {
      break;
    }
    ::usleep(10U * 1000U);
  }
  assert(provider.get().access_key_id == "rotated-key");
}

void test_process(const char* helper) {
  TemporaryCredentials files;
  configure_files(files);
  files.write_file(files.credentials, "");
  files.write_file(files.config,
                   std::string("[default]\ncredential_process = ") +
                       helper + "\n");
  CredentialProvider provider(options());
  const Credentials& credentials = provider.get();
  assert(credentials.access_key_id == "process-key");
  assert(credentials.secret_access_key == "process-secret");
  assert(credentials.session_token == "process-token");
  assert(std::string_view(provider.source_name()) == "credential-process");
  assert(provider.refreshable());
}

void test_anonymous() {
  TemporaryCredentials files;
  configure_files(files);
  CredentialProvider provider(options());
  assert(provider.get().access_key_id.empty());
  assert(std::string_view(provider.source_name()) == "anonymous");
}

void test_web_identity() {
  TemporaryCredentials files;
  configure_files(files);
  files.write_file(files.token, "identity-token\n");
  uint16_t port = 0;
  UniqueFd listener = make_listener(port);
  std::vector<CredentialExchange> exchanges{
      CredentialExchange{
          .request_line =
              "GET /?Action=AssumeRoleWithWebIdentity&Version=2011-06-15",
          .body =
              "<AssumeRoleWithWebIdentityResponse>"
              "<AssumeRoleWithWebIdentityResult><Credentials>"
              "<AccessKeyId>web-key</AccessKeyId>"
              "<SecretAccessKey>web-secret</SecretAccessKey>"
              "<SessionToken>web-token</SessionToken>"
              "<Expiration>2099-01-02T03:04:05Z</Expiration>"
              "</Credentials></AssumeRoleWithWebIdentityResult>"
              "</AssumeRoleWithWebIdentityResponse>",
      },
  };
  std::jthread server(serve_credentials, listener.get(), std::move(exchanges));
  const std::string endpoint =
      "http://127.0.0.1:" + std::to_string(port);
  assert(::setenv("AWS_WEB_IDENTITY_TOKEN_FILE", files.token.c_str(), 1) == 0);
  assert(::setenv("AWS_ROLE_ARN", "arn:aws:iam::1:role/test", 1) == 0);
  assert(::setenv("AWS_ROLE_SESSION_NAME", "ngs3fs-test", 1) == 0);
  assert(::setenv("AWS_ENDPOINT_URL_STS", endpoint.c_str(), 1) == 0);
  CredentialProvider provider(options());
  assert(provider.get().access_key_id == "web-key");
  assert(std::string_view(provider.source_name()) == "web-identity");
  clear_credential_environment();
}

void test_container() {
  TemporaryCredentials files;
  configure_files(files);
  files.write_file(files.token, "file-token-must-not-win");
  uint16_t port = 0;
  UniqueFd listener = make_listener(port);
  std::vector<CredentialExchange> exchanges{
      CredentialExchange{
          .request_line = "GET /credentials HTTP/1.1\r\n",
          .required_header = "authorization: bearer-test\r\n",
          .body =
              "{\"AccessKeyId\":\"container-key\","
              "\"SecretAccessKey\":\"container-secret\","
              "\"Token\":\"container-token\","
              "\"Expiration\":\"2099-01-02T03:04:05Z\"}",
      },
  };
  std::jthread server(serve_credentials, listener.get(), std::move(exchanges));
  const std::string endpoint =
      "http://127.0.0.1:" + std::to_string(port) + "/credentials";
  assert(::setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", endpoint.c_str(),
                  1) == 0);
  assert(::setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN", "bearer-test",
                  1) == 0);
  assert(::setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
                  files.token.c_str(), 1) == 0);
  CredentialProvider provider(options());
  assert(provider.get().access_key_id == "container-key");
  assert(std::string_view(provider.source_name()) == "container");
  clear_credential_environment();
}

void test_imds() {
  TemporaryCredentials files;
  configure_files(files);
  uint16_t port = 0;
  UniqueFd listener = make_listener(port);
  std::vector<CredentialExchange> exchanges{
      CredentialExchange{
          .request_line = "PUT /latest/api/token HTTP/1.1\r\n",
          .required_header =
              "x-aws-ec2-metadata-token-ttl-seconds: 21600\r\n",
          .body = "imds-token",
      },
      CredentialExchange{
          .request_line =
              "GET /latest/meta-data/iam/security-credentials/ HTTP/1.1\r\n",
          .required_header =
              "x-aws-ec2-metadata-token: imds-token\r\n",
          .body = "ngs3fs-role",
      },
      CredentialExchange{
          .request_line =
              "GET /latest/meta-data/iam/security-credentials/ngs3fs-role "
              "HTTP/1.1\r\n",
          .required_header =
              "x-aws-ec2-metadata-token: imds-token\r\n",
          .body =
              "{\"AccessKeyId\":\"imds-key\","
              "\"SecretAccessKey\":\"imds-secret\","
              "\"Token\":\"imds-session\","
              "\"Expiration\":\"2099-01-02T03:04:05Z\"}",
      },
  };
  std::jthread server(serve_credentials, listener.get(), std::move(exchanges));
  const std::string endpoint =
      "http://127.0.0.1:" + std::to_string(port);
  assert(::unsetenv("AWS_EC2_METADATA_DISABLED") == 0);
  assert(::setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", endpoint.c_str(),
                  1) == 0);
  CredentialProvider provider(options());
  assert(provider.get().access_key_id == "imds-key");
  assert(std::string_view(provider.source_name()) == "imds-v2");
  clear_credential_environment();
}

int main(int argc, char** argv) {
  assert(argc == 2);
  clear_credential_environment();
  test_environment();
  test_partial_environment();
  test_shared_file();
  test_process(argv[1]);
  test_anonymous();
  test_web_identity();
  test_container();
  test_imds();
  return 0;
}
