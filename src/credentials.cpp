#include "credentials.hpp"

#include "http.hpp"

#include <yyjson.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <wordexp.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <ctype.h>
#include <errno.h>
#include <fstream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <stdio.h>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

extern char** environ;

constexpr size_t kMaximumCredentialResponse = 1024U * 1024U;
constexpr uint64_t kCredentialRetryNs = 30ULL * 1000ULL * 1000ULL * 1000ULL;

struct CredentialEndpoint {
  std::string host;
  std::string authority;
  std::string path;
  uint16_t port = 0;
  bool tls      = false;
};

static uint64_t credential_monotonic_ns() {
  timespec t{};
  if (::clock_gettime(CLOCK_MONOTONIC, &t) != 0) {
    throw std::system_error(errno, std::generic_category(), "clock_gettime");
  }
  return uint64_t(t.tv_sec) * 1'000'000'000ULL + uint64_t(t.tv_nsec);
}

static int64_t credential_epoch_seconds() {
  timespec t{};
  if (::clock_gettime(CLOCK_REALTIME, &t) != 0) {
    throw std::system_error(errno, std::generic_category(), "clock_gettime");
  }
  return t.tv_sec;
}

static std::string environment_value(const char* name) {
  const char* value = ::getenv(name);
  return value == nullptr ? std::string{} : std::string(value);
}

static bool environment_true(const char* name) noexcept {
  const char* value = ::getenv(name);
  if (value == nullptr) {
    return false;
  }
  std::string_view s(value);
  return ascii_equal_ignore_case(s, "true") || s == "1";
}

static std::string_view credential_trim(std::string_view s) noexcept {
  while (!s.empty() && isspace(u_char(s.front()))) {
    s.remove_prefix(1);
  }
  while (!s.empty() && isspace(u_char(s.back()))) {
    s.remove_suffix(1);
  }
  return s;
}

static std::string read_credential_file(const std::string& path,
                                        size_t limit) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::system_error(errno == 0 ? ENOENT : errno,
                            std::generic_category(), path);
  }
  std::string text;
  text.resize(limit + 1);
  input.read(text.data(), std::streamsize(text.size()));
  const size_t bytes = size_t(input.gcount());
  if (bytes > limit || (!input.eof() && input.fail())) {
    throw std::runtime_error("credential file is too large: " + path);
  }
  text.resize(bytes);
  return text;
}

static void validate_credentials(const Credentials& credentials,
                                 const char* source) {
  if (credentials.access_key_id.empty() ||
      credentials.secret_access_key.empty()) {
    throw std::runtime_error(std::string(source) +
                             " returned incomplete credentials");
  }
  if (credentials.access_key_id.find_first_of("\r\n") != std::string::npos ||
      credentials.session_token.find_first_of("\r\n") != std::string::npos) {
    throw std::runtime_error(std::string(source) +
                             " returned unsafe credential header text");
  }
}

static bool leap_year(int64_t year) noexcept {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

static int64_t days_from_civil(int64_t year, unsigned month,
                               unsigned day) noexcept {
  year -= month <= 2;
  const int64_t era = (year >= 0 ? year : year - 399) / 400;
  const unsigned yoe = unsigned(year - era * 400);
  const unsigned shifted_month = month > 2 ? month - 3 : month + 9;
  const unsigned doy = (153 * shifted_month + 2) / 5 + day - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + int64_t(doe) - 719468;
}

static int64_t parse_expiration(std::string_view s) {
  if (s.size() < 20 || s[4] != '-' || s[7] != '-' ||
      (s[10] != 'T' && s[10] != 't') || s[13] != ':' || s[16] != ':') {
    throw std::runtime_error("credential expiration is not ISO-8601 UTC");
  }
  const auto number = [&](size_t begin, size_t length) {
    unsigned value = 0;
    const char* first = s.data() + begin;
    const auto result = std::from_chars(first, first + length, value);
    if (result.ec != std::errc{} || result.ptr != first + length) {
      throw std::runtime_error("credential expiration contains invalid digits");
    }
    return value;
  };
  const unsigned year   = number(0, 4);
  const unsigned month  = number(5, 2);
  const unsigned day    = number(8, 2);
  const unsigned hour   = number(11, 2);
  const unsigned minute = number(14, 2);
  const unsigned second = number(17, 2);
  constexpr unsigned days_per_month[] = {
      0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (year < 1970 || month == 0 || month > 12 || day == 0 ||
      day > days_per_month[month] +
                unsigned(month == 2 && leap_year(year)) ||
      hour > 23 || minute > 59 || second > 60) {
    throw std::runtime_error("credential expiration is out of range");
  }
  size_t end = 19;
  if (end < s.size() && s[end] == '.') {
    ++end;
    const size_t fractional = end;
    while (end < s.size() && isdigit(u_char(s[end]))) {
      ++end;
    }
    if (end == fractional) {
      throw std::runtime_error("credential expiration has an empty fraction");
    }
  }
  if (end + 1 != s.size() || (s[end] != 'Z' && s[end] != 'z')) {
    throw std::runtime_error("credential expiration is not UTC");
  }
  return days_from_civil(year, month, day) * 86400 +
         int64_t(hour) * 3600 + int64_t(minute) * 60 + second;
}

static std::string json_string(yyjson_val* root, const char* name,
                               bool required = true) {
  yyjson_val* value = yyjson_obj_get(root, name);
  if (value == nullptr) {
    if (!required) {
      return {};
    }
    throw std::runtime_error(std::string("credential JSON omitted ") + name);
  }
  if (!yyjson_is_str(value)) {
    throw std::runtime_error(std::string("credential JSON field is not text: ") +
                             name);
  }
  return std::string(yyjson_get_str(value), yyjson_get_len(value));
}

static CredentialProvider::Snapshot parse_json_credentials(
    std::string_view text, const char* source, bool process_format) {
  yyjson_read_err error{};
  std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)> document(
      yyjson_read_opts(const_cast<char*>(text.data()), text.size(),
                       YYJSON_READ_NOFLAG, nullptr, &error),
      yyjson_doc_free);
  if (!document) {
    throw std::runtime_error(
        std::string(source) + " returned invalid JSON at byte " +
        std::to_string(error.pos) + ": " +
        (error.msg == nullptr ? "unknown error" : error.msg));
  }
  yyjson_val* root = yyjson_doc_get_root(document.get());
  if (!yyjson_is_obj(root)) {
    throw std::runtime_error(std::string(source) +
                             " credential JSON is not an object");
  }
  if (process_format) {
    yyjson_val* version = yyjson_obj_get(root, "Version");
    if (!yyjson_is_int(version) || yyjson_get_int(version) != 1) {
      throw std::runtime_error(
          "credential_process JSON Version must be 1");
    }
  }
  CredentialProvider::Snapshot result;
  result.credentials.access_key_id = json_string(root, "AccessKeyId");
  result.credentials.secret_access_key =
      json_string(root, "SecretAccessKey");
  result.credentials.session_token = json_string(
      root, process_format ? "SessionToken" : "Token", false);
  const std::string expiration = json_string(root, "Expiration", false);
  if (!expiration.empty()) {
    result.expiration = parse_expiration(expiration);
  }
  validate_credentials(result.credentials, source);
  return result;
}

static CredentialEndpoint parse_credential_endpoint(std::string_view url) {
  CredentialEndpoint endpoint;
  if (url.starts_with("https://")) {
    endpoint.tls = true;
    endpoint.port = 443;
    url.remove_prefix(8);
  } else if (url.starts_with("http://")) {
    endpoint.port = 80;
    url.remove_prefix(7);
  } else {
    throw std::runtime_error("credential endpoint must use http or https");
  }
  const size_t slash = url.find('/');
  std::string_view authority = url.substr(0, slash);
  endpoint.path = slash == std::string_view::npos
                      ? "/"
                      : std::string(url.substr(slash));
  if (authority.empty() ||
      authority.find_first_of("@?#") != std::string_view::npos ||
      endpoint.path.find('#') != std::string::npos) {
    throw std::runtime_error("invalid credential endpoint URL");
  }
  endpoint.authority.assign(authority);
  std::string_view host = authority;
  std::string_view port;
  if (host.front() == '[') {
    const size_t close = host.find(']');
    if (close == std::string_view::npos) {
      throw std::runtime_error("invalid IPv6 credential endpoint");
    }
    endpoint.host.assign(host.substr(1, close - 1));
    if (close + 1 < host.size()) {
      if (host[close + 1] != ':') {
        throw std::runtime_error("invalid IPv6 credential endpoint port");
      }
      port = host.substr(close + 2);
    }
  } else {
    const size_t colon = host.find_last_of(':');
    if (colon != std::string_view::npos) {
      port = host.substr(colon + 1);
      host = host.substr(0, colon);
    }
    endpoint.host.assign(host);
  }
  if (endpoint.host.empty()) {
    throw std::runtime_error("credential endpoint omitted its host");
  }
  if (!port.empty()) {
    unsigned value = 0;
    const auto parsed = std::from_chars(
        port.data(), port.data() + port.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != port.data() + port.size() ||
        value == 0 || value > UINT16_MAX) {
      throw std::runtime_error("invalid credential endpoint port");
    }
    endpoint.port = uint16_t(value);
  }
  return endpoint;
}

static bool safe_plaintext_container_host(std::string_view host) noexcept {
  if (host == "localhost" || host == "fd00:ec2::23" ||
      host == "fd00:ec2::254") {
    return true;
  }
  in_addr ipv4{};
  if (::inet_pton(AF_INET, std::string(host).c_str(), &ipv4) == 1) {
    const uint32_t address = ntohl(ipv4.s_addr);
    return (address & 0xff000000U) == 0x7f000000U ||
           (address & 0xffff0000U) == 0xa9fe0000U;
  }
  in6_addr ipv6{};
  return ::inet_pton(AF_INET6, std::string(host).c_str(), &ipv6) == 1 &&
         IN6_IS_ADDR_LOOPBACK(&ipv6);
}

static Response credential_request(
    const CredentialProviderOptions& options,
    const CredentialEndpoint& endpoint, std::string_view method,
    std::span<const Header> headers = {}, bool metadata = true) {
  const int io_timeout = metadata
                             ? std::min(options.request_timeout_ms,
                                        options.metadata_timeout_ms)
                             : options.request_timeout_ms;
  const int connect_timeout = metadata
                                  ? std::min(options.connect_timeout_ms,
                                             options.metadata_timeout_ms)
                                  : options.connect_timeout_ms;
  const int probe_timeout = metadata
                                ? std::min(options.protocol_probe_timeout_ms,
                                           options.metadata_timeout_ms)
                                : options.protocol_probe_timeout_ms;
  std::unique_ptr<HttpClient> client = HttpClient::connect(
      endpoint.host, endpoint.port, endpoint.authority, endpoint.tls,
      io_timeout, connect_timeout, probe_timeout);
  return client->request_no_body(
      method, endpoint.path, headers, kMaximumCredentialResponse);
}

static std::string response_text(const Response& response) {
  if (response.body.empty()) {
    return {};
  }
  return std::string(reinterpret_cast<const char*>(response.body.data()),
                     response.body.size());
}

static std::string run_credential_process(const std::string& command,
                                          int timeout_ms) {
  wordexp_t words{};
  const int expanded = ::wordexp(command.c_str(), &words, WRDE_NOCMD);
  if (expanded != 0 || words.we_wordc == 0) {
    if (expanded == 0) {
      ::wordfree(&words);
    }
    throw std::runtime_error("unable to parse credential_process command");
  }

  int raw_pipe[2] = {-1, -1};
  if (::pipe2(raw_pipe, O_CLOEXEC) != 0) {
    ::wordfree(&words);
    throw std::system_error(errno, std::generic_category(), "pipe2");
  }
  UniqueFd output(raw_pipe[0]);
  UniqueFd writer(raw_pipe[1]);
  posix_spawn_file_actions_t actions;
  int result = ::posix_spawn_file_actions_init(&actions);
  if (result != 0) {
    ::wordfree(&words);
    throw std::system_error(result, std::generic_category(),
                            "posix_spawn_file_actions_init");
  }
  result = ::posix_spawn_file_actions_adddup2(
      &actions, writer.get(), STDOUT_FILENO);
  if (result == 0) {
    result = ::posix_spawn_file_actions_addclose(&actions, output.get());
  }
  posix_spawnattr_t attributes;
  bool attributes_ready = false;
  if (result == 0) {
    result = ::posix_spawnattr_init(&attributes);
    attributes_ready = result == 0;
  }
  if (result == 0) {
    result = ::posix_spawnattr_setflags(
        &attributes, POSIX_SPAWN_SETPGROUP);
  }
  if (result == 0) {
    result = ::posix_spawnattr_setpgroup(&attributes, 0);
  }
  pid_t pid = -1;
  if (result == 0) {
    result = ::posix_spawnp(&pid, words.we_wordv[0], &actions, &attributes,
                            words.we_wordv, environ);
  }
  if (attributes_ready) {
    ::posix_spawnattr_destroy(&attributes);
  }
  ::posix_spawn_file_actions_destroy(&actions);
  ::wordfree(&words);
  if (result != 0) {
    throw std::system_error(result, std::generic_category(),
                            "posix_spawnp(credential_process)");
  }
  writer.reset();
  const int flags = ::fcntl(output.get(), F_GETFL, 0);
  if (flags < 0 || ::fcntl(output.get(), F_SETFL, flags | O_NONBLOCK) != 0) {
    const int error = errno;
    ::kill(-pid, SIGKILL);
    ::waitpid(pid, nullptr, 0);
    throw std::system_error(error, std::generic_category(), "fcntl");
  }

  std::string text;
  const uint64_t deadline = credential_monotonic_ns() +
      uint64_t(timeout_ms) * 1000ULL * 1000ULL;
  bool eof = false;
  bool exited = false;
  int status = 0;
  while (!eof || !exited) {
    char buffer[8192];
    for (;;) {
      const ssize_t bytes = ::read(output.get(), buffer, sizeof(buffer));
      if (bytes > 0) {
        if (text.size() + size_t(bytes) > kMaximumCredentialResponse) {
          ::kill(-pid, SIGKILL);
          ::waitpid(pid, nullptr, 0);
          throw std::runtime_error("credential_process output is too large");
        }
        text.append(buffer, size_t(bytes));
        continue;
      }
      if (bytes == 0) {
        eof = true;
      } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        const int error = errno;
        ::kill(-pid, SIGKILL);
        ::waitpid(pid, nullptr, 0);
        throw std::system_error(error, std::generic_category(),
                                "read(credential_process)");
      }
      break;
    }
    if (!exited) {
      const pid_t waited = ::waitpid(pid, &status, WNOHANG);
      if (waited == pid) {
        exited = true;
      } else if (waited < 0 && errno != EINTR) {
        const int error = errno;
        ::kill(-pid, SIGKILL);
        ::waitpid(pid, nullptr, 0);
        throw std::system_error(error, std::generic_category(), "waitpid");
      }
    }
    if (eof && exited) {
      break;
    }
    const uint64_t now = credential_monotonic_ns();
    if (now >= deadline) {
      ::kill(-pid, SIGKILL);
      ::waitpid(pid, nullptr, 0);
      throw std::runtime_error("credential_process timed out");
    }
    pollfd descriptor{
        .fd = output.get(),
        .events = POLLIN | POLLHUP,
        .revents = 0,
    };
    const int remaining = int(std::min<uint64_t>(
        (deadline - now + 999'999ULL) / 1'000'000ULL, 100));
    int polled;
    do {
      polled = ::poll(&descriptor, 1, remaining);
    } while (polled < 0 && errno == EINTR);
    if (polled < 0) {
      const int error = errno;
      ::kill(-pid, SIGKILL);
      ::waitpid(pid, nullptr, 0);
      throw std::system_error(error, std::generic_category(),
                              "poll(credential_process)");
    }
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    throw std::runtime_error("credential_process exited with status " +
                             std::to_string(WIFEXITED(status)
                                                ? WEXITSTATUS(status)
                                                : 128 + WTERMSIG(status)));
  }
  return text;
}

static void parse_profile_file(const std::string& path,
                               std::string_view profile,
                               bool config_file,
                               CredentialProvider::Profile& result) {
  std::ifstream input(path);
  if (!input) {
    return;
  }
  bool selected = false;
  std::string line;
  while (std::getline(input, line)) {
    std::string_view text = credential_trim(line);
    if (text.empty() || text.front() == '#' || text.front() == ';') {
      continue;
    }
    if (text.front() == '[' && text.back() == ']') {
      std::string_view section = credential_trim(
          text.substr(1, text.size() - 2));
      if (config_file && section.starts_with("profile ")) {
        section.remove_prefix(8);
        section = credential_trim(section);
      }
      selected = section == profile;
      continue;
    }
    if (!selected) {
      continue;
    }
    const size_t equal = text.find('=');
    if (equal == std::string_view::npos) {
      continue;
    }
    const std::string_view name = credential_trim(text.substr(0, equal));
    const std::string_view value = credential_trim(text.substr(equal + 1));
    if (name == "aws_access_key_id") {
      result.credentials.access_key_id.assign(value);
    } else if (name == "aws_secret_access_key") {
      result.credentials.secret_access_key.assign(value);
    } else if (name == "aws_session_token" || name == "aws_security_token") {
      result.credentials.session_token.assign(value);
    } else if (name == "credential_process") {
      result.process.assign(value);
    } else if (name == "web_identity_token_file") {
      result.token_file.assign(value);
    } else if (name == "role_arn") {
      result.role_arn.assign(value);
    } else if (name == "role_session_name") {
      result.role_session_name.assign(value);
    } else if (name == "credential_source") {
      result.credential_source.assign(value);
    } else if (name == "source_profile") {
      result.source_profile.assign(value);
    } else if (name == "sso_session" || name == "sso_start_url") {
      result.sso_session.assign(value);
    }
  }
}

CredentialProvider::CredentialProvider(CredentialProviderOptions options)
    : options_(std::move(options)) {
  discover();
  fprintf(stderr, "credentials: source=%s refreshable=%s\n",
          source_name(), refreshable() ? "yes" : "no");
  if (refreshable()) {
    refresher_ = std::jthread([this](std::stop_token stop) {
      refresh_loop(stop);
    });
  }
}

CredentialProvider::~CredentialProvider() {
  {
    // Serialize stop with the wait predicate so notification cannot land in
    // the small interval between the predicate check and the futex wait.
    std::lock_guard guard(mutex_);
    refresher_.request_stop();
  }
  condition_.notify_all();
  if (refresher_.joinable()) {
    refresher_.join();
  }
}

const char* CredentialProvider::source_name() const noexcept {
  switch (source_) {
    case SOURCE_ANONYMOUS:
      return "anonymous";
    case SOURCE_ENVIRONMENT:
      return "environment";
    case SOURCE_SHARED_FILE:
      return "shared-file";
    case SOURCE_PROCESS:
      return "credential-process";
    case SOURCE_WEB_IDENTITY:
      return "web-identity";
    case SOURCE_CONTAINER:
      return "container";
    case SOURCE_IMDS:
      return "imds-v2";
  }
  return "unknown";
}

bool CredentialProvider::refreshable() const noexcept {
  return source_ != SOURCE_ANONYMOUS && source_ != SOURCE_ENVIRONMENT;
}

CredentialProvider::Profile CredentialProvider::load_profile() const {
  Profile result;
  parse_profile_file(config_file_, profile_, true, result);
  // The shared credentials file has precedence over duplicate values in the
  // config file, matching the AWS SDK provider chain.
  parse_profile_file(shared_credentials_file_, profile_, false, result);
  return result;
}

void CredentialProvider::discover() {
  const std::string home = environment_value("HOME");
  profile_ = environment_value("AWS_PROFILE");
  if (profile_.empty()) {
    profile_ = environment_value("AWS_DEFAULT_PROFILE");
  }
  if (profile_.empty()) {
    profile_ = "default";
  }
  shared_credentials_file_ = environment_value("AWS_SHARED_CREDENTIALS_FILE");
  if (shared_credentials_file_.empty() && !home.empty()) {
    shared_credentials_file_ = home + "/.aws/credentials";
  }
  config_file_ = environment_value("AWS_CONFIG_FILE");
  if (config_file_.empty() && !home.empty()) {
    config_file_ = home + "/.aws/config";
  }

  Snapshot initial;
  initial.credentials.access_key_id = environment_value("AWS_ACCESS_KEY_ID");
  initial.credentials.secret_access_key =
      environment_value("AWS_SECRET_ACCESS_KEY");
  initial.credentials.session_token = environment_value("AWS_SESSION_TOKEN");
  if (!initial.credentials.access_key_id.empty() ||
      !initial.credentials.secret_access_key.empty()) {
    validate_credentials(initial.credentials, "environment");
    source_ = SOURCE_ENVIRONMENT;
    install(std::move(initial), credential_monotonic_ns());
    return;
  }

  token_file_ = environment_value("AWS_WEB_IDENTITY_TOKEN_FILE");
  role_arn_ = environment_value("AWS_ROLE_ARN");
  role_session_name_ = environment_value("AWS_ROLE_SESSION_NAME");
  if (!token_file_.empty() || !role_arn_.empty()) {
    if (token_file_.empty() || role_arn_.empty()) {
      throw std::runtime_error(
          "AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN must be set together");
    }
    source_ = SOURCE_WEB_IDENTITY;
    install(load_web_identity(), credential_monotonic_ns());
    return;
  }

  const Profile profile = load_profile();
  if (!profile.credentials.access_key_id.empty() ||
      !profile.credentials.secret_access_key.empty()) {
    initial.credentials = profile.credentials;
    validate_credentials(initial.credentials, "shared credentials file");
    source_ = SOURCE_SHARED_FILE;
    install(std::move(initial), credential_monotonic_ns());
    return;
  }
  if (!profile.process.empty()) {
    process_ = profile.process;
    source_ = SOURCE_PROCESS;
    install(load_process(), credential_monotonic_ns());
    return;
  }
  if (!profile.token_file.empty()) {
    if (profile.role_arn.empty()) {
      throw std::runtime_error(
          "profile web_identity_token_file and role_arn must be set together");
    }
    token_file_ = profile.token_file;
    role_arn_ = profile.role_arn;
    role_session_name_ = profile.role_session_name;
    source_ = SOURCE_WEB_IDENTITY;
    install(load_web_identity(), credential_monotonic_ns());
    return;
  }
  if (!profile.role_arn.empty() || !profile.source_profile.empty() ||
      !profile.sso_session.empty()) {
    throw std::runtime_error(
        "selected AWS profile requires AssumeRole/SSO; configure "
        "credential_process to export refreshable credentials");
  }

  const std::string relative =
      environment_value("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
  const std::string full =
      environment_value("AWS_CONTAINER_CREDENTIALS_FULL_URI");
  if (!relative.empty() || !full.empty() ||
      ascii_equal_ignore_case(profile.credential_source,
                              "EcsContainer")) {
    container_uri_ = !relative.empty()
                         ? "http://169.254.170.2" + relative
                         : full;
    if (container_uri_.empty()) {
      throw std::runtime_error(
          "EcsContainer credential source omitted its endpoint environment");
    }
    container_token_ = environment_value("AWS_CONTAINER_AUTHORIZATION_TOKEN");
    const std::string token_path =
        environment_value("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE");
    if (container_token_.empty() && !token_path.empty()) {
      container_token_ = std::string(credential_trim(
          read_credential_file(token_path, 64U * 1024U)));
    }
    if (container_token_.find_first_of("\r\n") != std::string::npos) {
      throw std::runtime_error("container authorization token contains CR/LF");
    }
    source_ = SOURCE_CONTAINER;
    install(load_container(), credential_monotonic_ns());
    return;
  }

  if (!environment_true("AWS_EC2_METADATA_DISABLED")) {
    imds_endpoint_ = environment_value("AWS_EC2_METADATA_SERVICE_ENDPOINT");
    if (imds_endpoint_.empty()) {
      imds_endpoint_ = "http://169.254.169.254";
    }
    source_ = SOURCE_IMDS;
    try {
      install(load_imds(), credential_monotonic_ns());
      return;
    } catch (const std::exception& error) {
      fprintf(stderr, "warning: IMDS credential probe failed: %s\n",
              error.what());
    }
  }

  source_ = SOURCE_ANONYMOUS;
  install({}, credential_monotonic_ns());
}

CredentialProvider::Snapshot CredentialProvider::load_shared_file() const {
  Snapshot result;
  result.credentials = load_profile().credentials;
  validate_credentials(result.credentials, "shared credentials file");
  return result;
}

CredentialProvider::Snapshot CredentialProvider::load_process() const {
  return parse_json_credentials(
      run_credential_process(process_, options_.request_timeout_ms),
      "credential_process", true);
}

CredentialProvider::Snapshot CredentialProvider::load_web_identity() const {
  const std::string token_text = read_credential_file(token_file_, 256U * 1024U);
  const std::string token(credential_trim(token_text));
  if (token.empty()) {
    throw std::runtime_error("web identity token file is empty");
  }
  std::string session = role_session_name_;
  if (session.empty()) {
    session = "ngs3fs-" + std::to_string(::getpid());
  }
  std::string host;
  if (role_arn_.starts_with("arn:aws-cn:")) {
    host = "sts." + options_.region + ".amazonaws.com.cn";
  } else {
    host = "sts." + options_.region + ".amazonaws.com";
  }
  std::string url = environment_value("AWS_ENDPOINT_URL_STS");
  if (url.empty()) {
    url = "https://" + host;
  }
  CredentialEndpoint endpoint = parse_credential_endpoint(url);
  endpoint.path = "/?Action=AssumeRoleWithWebIdentity&Version=2011-06-15";
  endpoint.path += "&RoleArn=" + uri_encode(role_arn_, false);
  endpoint.path += "&RoleSessionName=" + uri_encode(session, false);
  endpoint.path += "&WebIdentityToken=" + uri_encode(token, false);
  const Response response = credential_request(
      options_, endpoint, "GET", {}, false);
  if (response.status != 200) {
    throw std::runtime_error(
        "AssumeRoleWithWebIdentity failed with HTTP status " +
        std::to_string(response.status));
  }
  S3Xml xml(response_text(response), "AssumeRoleWithWebIdentity");
  const tinyxml2::XMLElement& root =
      xml.root("AssumeRoleWithWebIdentityResponse");
  const tinyxml2::XMLElement& result =
      xml.required_child(root, "AssumeRoleWithWebIdentityResult");
  const tinyxml2::XMLElement& values =
      xml.required_child(result, "Credentials");
  Snapshot credentials;
  credentials.credentials.access_key_id =
      xml.required_text(values, "AccessKeyId");
  credentials.credentials.secret_access_key =
      xml.required_text(values, "SecretAccessKey");
  credentials.credentials.session_token =
      xml.required_text(values, "SessionToken");
  credentials.expiration = parse_expiration(
      xml.required_text(values, "Expiration"));
  validate_credentials(credentials.credentials, "AssumeRoleWithWebIdentity");
  return credentials;
}

CredentialProvider::Snapshot CredentialProvider::load_container() const {
  CredentialEndpoint endpoint = parse_credential_endpoint(container_uri_);
  if (!endpoint.tls && !safe_plaintext_container_host(endpoint.host)) {
    throw std::runtime_error(
        "plaintext container credential endpoint is not loopback/link-local");
  }
  std::vector<Header> headers;
  if (!container_token_.empty()) {
    headers.push_back(Header{"authorization", container_token_});
  }
  const Response response = credential_request(
      options_, endpoint, "GET", headers);
  if (response.status != 200) {
    throw std::runtime_error("container credential endpoint returned HTTP " +
                             std::to_string(response.status));
  }
  return parse_json_credentials(
      response_text(response), "container credential endpoint", false);
}

CredentialProvider::Snapshot CredentialProvider::load_imds() const {
  CredentialEndpoint endpoint = parse_credential_endpoint(imds_endpoint_);
  if (endpoint.tls || !safe_plaintext_container_host(endpoint.host)) {
    throw std::runtime_error("IMDS endpoint must be a local plaintext address");
  }
  const std::string base_path = endpoint.path == "/" ? std::string{} :
      endpoint.path;
  endpoint.path = base_path + "/latest/api/token";
  const std::vector token_headers{
      Header{"x-aws-ec2-metadata-token-ttl-seconds", "21600"},
  };
  Response response = credential_request(
      options_, endpoint, "PUT", token_headers);
  if (response.status != 200) {
    throw std::runtime_error("IMDSv2 token request returned HTTP " +
                             std::to_string(response.status));
  }
  const std::string token(credential_trim(response_text(response)));
  if (token.empty() || token.find_first_of("\r\n") != std::string::npos) {
    throw std::runtime_error("IMDSv2 returned an invalid token");
  }
  const std::vector headers{
      Header{"x-aws-ec2-metadata-token", token},
  };
  endpoint.path = base_path +
      "/latest/meta-data/iam/security-credentials/";
  response = credential_request(options_, endpoint, "GET", headers);
  if (response.status != 200) {
    throw std::runtime_error("IMDS role request returned HTTP " +
                             std::to_string(response.status));
  }
  const std::string role(credential_trim(response_text(response)));
  if (role.empty() || role.find_first_of("/\r\n") != std::string::npos) {
    throw std::runtime_error("IMDS returned an invalid IAM role name");
  }
  endpoint.path += uri_encode(role, false);
  response = credential_request(options_, endpoint, "GET", headers);
  if (response.status != 200) {
    throw std::runtime_error("IMDS credential request returned HTTP " +
                             std::to_string(response.status));
  }
  return parse_json_credentials(response_text(response), "IMDS", false);
}

CredentialProvider::Snapshot CredentialProvider::load_source() {
  switch (source_) {
    case SOURCE_SHARED_FILE:
      return load_shared_file();
    case SOURCE_PROCESS:
      return load_process();
    case SOURCE_WEB_IDENTITY:
      return load_web_identity();
    case SOURCE_CONTAINER:
      return load_container();
    case SOURCE_IMDS:
      return load_imds();
    case SOURCE_ANONYMOUS:
    case SOURCE_ENVIRONMENT:
      break;
  }
  throw std::logic_error("static credential source cannot refresh");
}

void CredentialProvider::install(Snapshot value, uint64_t now_ns) {
  const int64_t now = credential_epoch_seconds();
  if (value.expiration != 0 && value.expiration <= now) {
    throw std::runtime_error(std::string(source_name()) +
                             " returned expired credentials");
  }
  auto snapshot = std::make_unique<Snapshot>(std::move(value));
  Snapshot* raw = snapshot.get();
  snapshots_.push_back(std::move(snapshot));
  current_.store(raw, std::memory_order_release);

  schedule_refresh(*raw, now_ns);
}

void CredentialProvider::schedule_refresh(const Snapshot& value,
                                          uint64_t now_ns) {
  uint64_t delay = UINT64_MAX;
  if (source_ == SOURCE_SHARED_FILE) {
    delay = uint64_t(options_.shared_reload_ms) * 1'000'000ULL;
  } else if (source_ == SOURCE_PROCESS || source_ == SOURCE_WEB_IDENTITY ||
             source_ == SOURCE_CONTAINER || source_ == SOURCE_IMDS) {
    const int64_t now = credential_epoch_seconds();
    const int64_t refresh = value.expiration == 0
                                ? 300
                                : std::max<int64_t>(
                                      1, value.expiration - now - 300);
    delay = uint64_t(refresh) * 1000ULL * 1000ULL * 1000ULL;
  }
  next_refresh_ns_.store(
      delay == UINT64_MAX || delay > UINT64_MAX - now_ns
          ? UINT64_MAX
          : now_ns + delay,
      std::memory_order_release);
}

void CredentialProvider::refresh_locked(uint64_t now_ns) {
  try {
    Snapshot value = load_source();
    if (value.expiration != 0 &&
        value.expiration <= credential_epoch_seconds()) {
      throw std::runtime_error(std::string(source_name()) +
                               " returned expired credentials");
    }
    Snapshot* current = current_.load(std::memory_order_acquire);
    if (current != nullptr && current->expiration == value.expiration &&
        current->credentials.access_key_id ==
            value.credentials.access_key_id &&
        current->credentials.secret_access_key ==
            value.credentials.secret_access_key &&
        current->credentials.session_token == value.credentials.session_token) {
      schedule_refresh(*current, now_ns);
    } else {
      install(std::move(value), now_ns);
    }
  } catch (const std::exception& error) {
    Snapshot* current = current_.load(std::memory_order_acquire);
    const int64_t now = credential_epoch_seconds();
    if (current != nullptr &&
        (current->expiration == 0 || current->expiration > now)) {
      fprintf(stderr,
              "warning: credential refresh failed; retaining current "
              "%s credentials: %s\n",
              source_name(), error.what());
      uint64_t retry = kCredentialRetryNs;
      if (current->expiration != 0) {
        retry = std::min<uint64_t>(
            retry, uint64_t(current->expiration - now) * 1'000'000'000ULL);
      }
      next_refresh_ns_.store(
          retry > UINT64_MAX - now_ns ? UINT64_MAX : now_ns + retry,
          std::memory_order_release);
      return;
    }
    throw;
  }
}

void CredentialProvider::refresh_loop(std::stop_token stop) noexcept {
  std::unique_lock guard(mutex_);
  while (!stop.stop_requested()) {
    const uint64_t due = next_refresh_ns_.load(std::memory_order_acquire);
    uint64_t now;
    try {
      now = credential_monotonic_ns();
    } catch (const std::exception& error) {
      refresh_error_ = std::current_exception();
      current_.store(nullptr, std::memory_order_release);
      fprintf(stderr, "warning: credential refresh clock failed: %s\n",
              error.what());
      return;
    }
    if (due == UINT64_MAX || now < due) {
      const auto wait = due == UINT64_MAX
                            ? std::chrono::hours(24)
                            : std::chrono::nanoseconds(due - now);
      condition_.wait_for(guard, wait, [&] {
        return stop.stop_requested() ||
               next_refresh_ns_.load(std::memory_order_acquire) != due;
      });
      continue;
    }
    try {
      refresh_locked(now);
      refresh_error_ = nullptr;
    } catch (...) {
      refresh_error_ = std::current_exception();
      current_.store(nullptr, std::memory_order_release);
      try {
        std::rethrow_exception(refresh_error_);
      } catch (const std::exception& error) {
        fprintf(stderr,
                "warning: %s credentials are unusable; retrying refresh: %s\n",
                source_name(), error.what());
      } catch (...) {
        fprintf(stderr,
                "warning: %s credentials are unusable; retrying refresh\n",
                source_name());
      }
      next_refresh_ns_.store(
          now > UINT64_MAX - kCredentialRetryNs
              ? UINT64_MAX
              : now + kCredentialRetryNs,
          std::memory_order_release);
    }
  }
}

const Credentials& CredentialProvider::get() {
  Snapshot* snapshot = current_.load(std::memory_order_acquire);
  if (snapshot != nullptr) {
    return snapshot->credentials;
  }
  std::lock_guard guard(mutex_);
  snapshot = current_.load(std::memory_order_relaxed);
  if (snapshot != nullptr) {
    return snapshot->credentials;
  }
  if (refresh_error_ != nullptr) {
    std::rethrow_exception(refresh_error_);
  }
  throw std::logic_error("credential provider has no snapshot");
}
