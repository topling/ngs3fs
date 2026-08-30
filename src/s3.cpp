#include "s3.hpp"

#define OPENSSL_SUPPRESS_DEPRECATED
#include <openssl/sha.h>

#include <ctype.h>
#include <sys/types.h>
#include <time.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <utility>

S3Xml::S3Xml(std::string_view xml, std::string_view operation)
    : operation_(operation) {
  if (xml.find('\0') != std::string_view::npos) {
    fail("returned XML containing a NUL byte");
  }
  const tinyxml2::XMLError error = document_.Parse(xml.data(), xml.size());
  if (error != tinyxml2::XML_SUCCESS) {
    std::string message = "returned invalid XML";
    if (const char* detail = document_.ErrorStr();
        detail != nullptr && *detail != '\0') {
      message += ": ";
      message += detail;
    }
    fail(message);
  }
  if (document_.RootElement() == nullptr) {
    fail("returned XML without a root element");
  }
}

bool S3Xml::named(const tinyxml2::XMLElement& element,
                  std::string_view name) noexcept {
  const char* raw = element.Name();
  if (raw == nullptr) {
    return false;
  }
  std::string_view local(raw);
  const size_t colon = local.find_last_of(':');
  if (colon != std::string_view::npos) {
    local.remove_prefix(colon + 1);
  }
  return local == name;
}

const tinyxml2::XMLElement& S3Xml::root(
    std::string_view expected) const {
  const tinyxml2::XMLElement& element = *document_.RootElement();
  if (!expected.empty() && !named(element, expected)) {
    std::string message = "returned unexpected XML root <";
    message += element.Name();
    message += ">; expected <";
    message.append(expected);
    message += '>';
    fail(message);
  }
  return element;
}

const tinyxml2::XMLElement& S3Xml::result_root(
    std::string_view expected) const {
  if (!root_is("Error")) {
    return root(expected);
  }
  const std::string code = optional_text(root(), "Code");
  fail(code.empty() ? "returned an S3 error"
                    : "returned S3 error " + code);
}

bool S3Xml::root_is(std::string_view name) const noexcept {
  return named(*document_.RootElement(), name);
}

const tinyxml2::XMLElement* S3Xml::first_child(
    const tinyxml2::XMLElement& parent,
    std::string_view name) const noexcept {
  for (const tinyxml2::XMLElement* child = parent.FirstChildElement();
       child != nullptr; child = child->NextSiblingElement()) {
    if (named(*child, name)) {
      return child;
    }
  }
  return nullptr;
}

const tinyxml2::XMLElement* S3Xml::next_sibling(
    const tinyxml2::XMLElement& element,
    std::string_view name) const noexcept {
  for (const tinyxml2::XMLElement* next = element.NextSiblingElement();
       next != nullptr; next = next->NextSiblingElement()) {
    if (named(*next, name)) {
      return next;
    }
  }
  return nullptr;
}

const tinyxml2::XMLElement* S3Xml::unique_child(
    const tinyxml2::XMLElement& parent, std::string_view name,
    bool required) const {
  const tinyxml2::XMLElement* child = first_child(parent, name);
  if (child == nullptr) {
    if (required) {
      std::string message = "response omitted <";
      message.append(name);
      message += '>';
      fail(message);
    }
    return nullptr;
  }
  if (next_sibling(*child, name) != nullptr) {
    std::string message = "response repeated <";
    message.append(name);
    message += '>';
    fail(message);
  }
  return child;
}

const tinyxml2::XMLElement& S3Xml::required_child(
    const tinyxml2::XMLElement& parent, std::string_view name) const {
  return *unique_child(parent, name, true);
}

std::string S3Xml::element_text(
    const tinyxml2::XMLElement& element) const {
  std::string text;
  for (const tinyxml2::XMLNode* node = element.FirstChild();
       node != nullptr; node = node->NextSibling()) {
    if (node->ToElement() != nullptr) {
      std::string message = "returned nested XML in <";
      message += element.Name();
      message += '>';
      fail(message);
    }
    if (const tinyxml2::XMLText* value = node->ToText()) {
      text += value->Value();
    }
  }
  return text;
}

std::string S3Xml::required_text(
    const tinyxml2::XMLElement& parent, std::string_view name) const {
  return element_text(*unique_child(parent, name, true));
}

std::string S3Xml::optional_text(
    const tinyxml2::XMLElement& parent, std::string_view name) const {
  const tinyxml2::XMLElement* child = unique_child(parent, name, false);
  return child == nullptr ? std::string{} : element_text(*child);
}

bool S3Xml::required_bool(const tinyxml2::XMLElement& parent,
                          std::string_view name) const {
  const std::string text = required_text(parent, name);
  std::string_view value(text);
  while (!value.empty() && isspace(u_char(value.front()))) {
    value.remove_prefix(1);
  }
  while (!value.empty() && isspace(u_char(value.back()))) {
    value.remove_suffix(1);
  }
  if (value == "true" || value == "1") {
    return true;
  }
  if (value == "false" || value == "0") {
    return false;
  }
  std::string message = "returned invalid boolean in <";
  message.append(name);
  message += '>';
  fail(message);
}

[[noreturn]] void S3Xml::fail(std::string_view message) const {
  std::string error(operation_.data(), operation_.size());
  error += ' ';
  error.append(message);
  throw std::runtime_error(error);
}

Directory::Directory() {
  enable_freelist();
}

Directory::~Directory() {
  std::unique_lock guard(mutex);
  for (auto [name, item] : *this) {
    (void)name;
    delete_inode(item);
  }
}

void delete_inode(InodeBase* item) noexcept {
  if (item == nullptr) {
    return;
  }
  if (item->regular()) {
    delete static_cast<InodeFile*>(item);
  } else {
    delete static_cast<InodeDir*>(item);
  }
}

unsigned fixed_decimal(std::string_view s, size_t p, size_t n) {
  unsigned value = 0;
  if (p + n > s.size()) {
    throw std::runtime_error("truncated S3 LastModified");
  }
  for (size_t i = 0; i < n; ++i) {
    const unsigned char c = u_char(s[p + i]);
    if (!isdigit(c)) {
      throw std::runtime_error("invalid S3 LastModified digit");
    }
    value = value * 10 + unsigned(c - '0');
  }
  return value;
}

bool leap_year(unsigned y) noexcept {
  return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
}

time_t parse_s3_mtime(std::string_view s) {
  if (s.size() < 20 || s[4] != '-' || s[7] != '-' ||
      s[10] != 'T' || s[13] != ':' || s[16] != ':') {
    throw std::runtime_error("invalid S3 LastModified");
  }
  if (s[19] == '.') {
    if (s.back() != 'Z' || s.size() == 21) {
      throw std::runtime_error("invalid S3 LastModified fraction");
    }
    for (size_t i = 20; i + 1 < s.size(); ++i) {
      if (!isdigit(u_char(s[i]))) {
        throw std::runtime_error("invalid S3 LastModified fraction");
      }
    }
  } else if (s.size() != 20 || s[19] != 'Z') {
    throw std::runtime_error("invalid S3 LastModified timezone");
  }

  const unsigned y = fixed_decimal(s, 0, 4);
  const unsigned m = fixed_decimal(s, 5, 2);
  const unsigned d = fixed_decimal(s, 8, 2);
  const unsigned h = fixed_decimal(s, 11, 2);
  const unsigned n = fixed_decimal(s, 14, 2);
  const unsigned z = fixed_decimal(s, 17, 2);
  static constexpr unsigned month_days[] = {
      31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (m == 0 || m > 12 || d == 0 ||
      d > month_days[m - 1] + unsigned(m == 2 && leap_year(y)) ||
      h > 23 || n > 59 || z > 59) {
    throw std::runtime_error("out-of-range S3 LastModified");
  }

  const int64_t adjusted_year = int64_t(y) - int64_t(m <= 2);
  const int64_t era = (adjusted_year >= 0
                           ? adjusted_year
                           : adjusted_year - 399) /
                      400;
  const unsigned year_of_era =
      unsigned(adjusted_year - era * 400);
  const unsigned adjusted_month = m > 2 ? m - 3 : m + 9;
  const unsigned day_of_year =
      (153 * adjusted_month + 2) / 5 + d - 1;
  const unsigned day_of_era =
      year_of_era * 365 + year_of_era / 4 - year_of_era / 100 +
      day_of_year;
  const int64_t days = era * 146097 + int64_t(day_of_era) - 719468;
  const int64_t seconds =
      days * 86400 + int64_t(h * 3600 + n * 60 + z);
  if constexpr (std::numeric_limits<time_t>::is_signed) {
    if (seconds < int64_t(std::numeric_limits<time_t>::min()) ||
        seconds > int64_t(std::numeric_limits<time_t>::max())) {
      throw std::runtime_error("S3 LastModified is outside time_t");
    }
  } else {
    if (seconds < 0 ||
        uint64_t(seconds) > uint64_t(std::numeric_limits<time_t>::max())) {
      throw std::runtime_error("S3 LastModified is outside time_t");
    }
  }
  return time_t(seconds);
}

// Request-path and CopyObject helpers.
bool is_encoded_request_path(std::string_view p) {
  if (p.empty() || p.front() != '/' || p.find('?') != std::string_view::npos) {
    return false;
  }
  for (size_t i = 0; i < p.size(); ++i) {
    const unsigned char c = u_char(p[i]);
    if (c <= 0x20U || c == 0x7fU) {
      return false;
    }
    if (p[i] == '%') {
      if (i + 2 >= p.size() || isxdigit(u_char(p[i + 1])) == 0 ||
          isxdigit(u_char(p[i + 2])) == 0) {
        return false;
      }
      i += 2;
    }
  }
  return true;
}

bool request_path_is_directory_marker(std::string_view p) {
  p = p.substr(0, p.find('?'));
  if (p.ends_with('/')) {
    return true;
  }
  if (p.size() < 3 || p[p.size() - 3] != '%') {
    return false;
  }
  return p[p.size() - 2] == '2' && tolower(u_char(p.back())) == 'f';
}

std::string request_path_without_query(std::string_view p) {
  return std::string(p.substr(0, p.find('?')));
}

std::string make_copy_source(std::string_view bucket,
                             std::string_view source_path,
                             std::string_view version_id,
                             bool virtual_hosted) {
  if (bucket.empty()) {
    throw std::invalid_argument("CopyObject source bucket is empty");
  }
  const std::string source = request_path_without_query(source_path);
  const std::string encoded_bucket = uri_encode(bucket, false);
  const std::string path_style_prefix = '/' + encoded_bucket;
  std::string result;
  if (!virtual_hosted &&
      (source == path_style_prefix ||
      (source.size() > path_style_prefix.size() &&
       source.starts_with(path_style_prefix) &&
       source[path_style_prefix.size()] == '/'))) {
    result = source;
  } else {
    result = path_style_prefix + source;
  }
  if (!version_id.empty()) {
    result += "?versionId=" + uri_encode(version_id, false);
  }
  return result;
}

bool authority_uses_virtual_bucket(std::string_view authority,
                                   std::string_view bucket) {
  if (bucket.empty()) {
    return false;
  }
  std::string_view host = authority;
  if (host.starts_with('[')) {
    const size_t bracket = host.find(']');
    if (bracket == std::string_view::npos) {
      return false;
    }
    host = host.substr(1, bracket - 1);
  } else {
    const size_t first_colon = host.find(':');
    if (first_colon != std::string_view::npos &&
        first_colon == host.rfind(':')) {
      host = host.substr(0, first_colon);
    }
  }
  if (host.size() < bucket.size()) {
    return false;
  }
  for (size_t i = 0; i < bucket.size(); ++i) {
    const auto left = u_char(host[i]);
    const auto right = u_char(bucket[i]);
    if (tolower(left) != tolower(right)) {
      return false;
    }
  }
  return host.size() == bucket.size() || host[bucket.size()] == '.';
}

// AWS Signature Version 4.
inline constexpr size_t kSha256Bytes = 32;
using Bytes = std::array<unsigned char, kSha256Bytes>;

void hex(std::span<const unsigned char> bytes, char* output) {
  constexpr auto pairs = [] {
    constexpr char digits[] = "0123456789abcdef";
    std::array<std::array<char, 2>, 256> result{};
    for (size_t i = 0; i < result.size(); ++i) {
      result[i][0] = digits[i >> 4];
      result[i][1] = digits[i & 0x0fU];
    }
    return result;
  }();
  for (unsigned char byte : bytes) {
    *output++ = pairs[byte][0];
    *output++ = pairs[byte][1];
  }
}

Bytes sha256(std::string_view value) {
  SHA256_CTX context;
  Bytes digest;
  if (SHA256_Init(&context) != 1 ||
      SHA256_Update(&context, value.data(), value.size()) != 1 ||
      SHA256_Final(digest.data(), &context) != 1) {
    throw std::runtime_error("OpenSSL SHA-256 failed");
  }
  return digest;
}

struct HmacSha256Key {
  SHA256_CTX inner;
  SHA256_CTX outer;
};

HmacSha256Key make_hmac_key(std::span<const unsigned char> key) {
  Bytes hashed_key;
  if (key.size() > SHA256_CBLOCK) {
    hashed_key = sha256(std::string_view(
        reinterpret_cast<const char*>(key.data()), key.size()));
    key = hashed_key;
  }

  std::array<unsigned char, SHA256_CBLOCK> block{};
  std::copy(key.begin(), key.end(), block.begin());
  for (unsigned char& byte : block) {
    byte ^= 0x36;
  }

  HmacSha256Key result;
  if (SHA256_Init(&result.inner) != 1 ||
      SHA256_Update(&result.inner, block.data(), block.size()) != 1) {
    throw std::runtime_error("OpenSSL HMAC-SHA256 inner key failed");
  }
  for (unsigned char& byte : block) {
    byte ^= 0x36 ^ 0x5c;
  }
  if (SHA256_Init(&result.outer) != 1 ||
      SHA256_Update(&result.outer, block.data(), block.size()) != 1) {
    throw std::runtime_error("OpenSSL HMAC-SHA256 outer key failed");
  }
  return result;
}

Bytes hmac(const HmacSha256Key& key, std::string_view value) {
  SHA256_CTX inner = key.inner;
  Bytes digest;
  if (SHA256_Update(&inner, value.data(), value.size()) != 1 ||
      SHA256_Final(digest.data(), &inner) != 1) {
    throw std::runtime_error("OpenSSL HMAC-SHA256 inner digest failed");
  }

  SHA256_CTX outer = key.outer;
  if (SHA256_Update(&outer, digest.data(), digest.size()) != 1 ||
      SHA256_Final(digest.data(), &outer) != 1) {
    throw std::runtime_error("OpenSSL HMAC-SHA256 outer digest failed");
  }
  return digest;
}

Bytes hmac(std::span<const unsigned char> key, std::string_view value) {
  return hmac(make_hmac_key(key), value);
}

Bytes hmac(std::string_view key, std::string_view value) {
  return hmac(std::span(
                  reinterpret_cast<const unsigned char*>(key.data()),
                  key.size()),
              value);
}

struct SigningKeyCache {
  std::string secret_access_key;
  std::string date;
  std::string region;
  std::string service;
  HmacSha256Key hmac_key{};
  bool valid = false;
};

Bytes signing_hmac(SigningKeyCache& cache,
                   std::string_view secret_access_key,
                   std::string_view date, std::string_view region,
                   std::string_view service, std::string_view value) {
  if (!cache.valid || cache.secret_access_key != secret_access_key ||
      cache.date != date || cache.region != region ||
      cache.service != service) {
    const Bytes date_key = hmac("AWS4" + std::string(secret_access_key), date);
    const Bytes region_key = hmac(date_key, region);
    const Bytes service_key = hmac(region_key, service);
    const Bytes signing_key = hmac(service_key, "aws4_request");
    cache.secret_access_key.assign(secret_access_key);
    cache.date.assign(date);
    cache.region.assign(region);
    cache.service.assign(service);
    cache.hmac_key = make_hmac_key(signing_key);
    cache.valid = true;
  }

  return hmac(cache.hmac_key, value);
}

ssostr<32> lowercase(std::string_view value) {
  ssostr<32> output(value.data(), value.size());
  for (char& ch : output) {
    if (ch >= 'A' && ch <= 'Z') {
      ch += 'a' - 'A';
    }
  }
  return output;
}

bool has_header_whitespace(std::string_view value) {
  for (char ch : value) {
    if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n') {
      return true;
    }
  }
  return false;
}

ssostr<32> normalize_header_value(std::string_view value) {
  if (!has_header_whitespace(value)) {
    return ssostr<32>(value.data(), value.size());
  }
  ssostr<32> output;
  output.reserve(value.size());
  bool pending_space = false;
  for (char item : value) {
    if (item == ' ' || item == '\t' || item == '\r' || item == '\n') {
      if (!output.empty()) {
        pending_space = true;
      }
      continue;
    }
    if (pending_space) {
      output.push_back(' ');
      pending_space = false;
    }
    output.push_back(item);
  }
  return output;
}

struct CanonicalHeader {
  std::string_view name;
  std::string_view value;
  ssostr<32>* owned_name = nullptr;
  ssostr<32>* owned_value = nullptr;
};

struct SigningWorkspace {
  SigningKeyCache key;
  std::vector<CanonicalHeader> headers;
  std::vector<ssostr<32>> normalized_names;
  std::vector<ssostr<32>> normalized_values;
  std::string canonical;
  std::string scope;
  std::string signed_names;
  std::string to_sign;
};

SigningWorkspace& signing_workspace() {
  thread_local SigningWorkspace workspace;
  return workspace;
}

void add_canonical_header(std::vector<CanonicalHeader>& headers,
                          std::vector<ssostr<32>>& normalized_names,
                          std::vector<ssostr<32>>& normalized_values,
                          std::string_view name, std::string_view value) {
  ssostr<32>* owned_name = nullptr;
  if (name == ":authority") {
    name = "host";
  } else if (std::ranges::any_of(name, [](char ch) {
               return ch >= 'A' && ch <= 'Z';
             })) {
    normalized_names.push_back(lowercase(name));
    owned_name = &normalized_names.back();
    name = sso_view(*owned_name);
  }
  const auto position = std::lower_bound(
      headers.begin(), headers.end(), name,
      [](const CanonicalHeader& header, std::string_view target) {
        return header.name < target;
      });
  if (position != headers.end() && position->name == name) {
    if (position->owned_value == nullptr) {
      normalized_values.emplace_back(position->value.data(),
                                     position->value.size());
      position->owned_value = &normalized_values.back();
    }
    position->owned_value->push_back(',');
    const ssostr<32> canonical_value = normalize_header_value(value);
    position->owned_value->append(canonical_value.data(),
                                  canonical_value.size());
    position->value = sso_view(*position->owned_value);
    return;
  }

  ssostr<32>* owned_value = nullptr;
  if (has_header_whitespace(value)) {
    normalized_values.push_back(normalize_header_value(value));
    owned_value = &normalized_values.back();
    value = sso_view(*owned_value);
  }
  headers.insert(position, CanonicalHeader{
                               .name = name,
                               .value = value,
                               .owned_name = owned_name,
                               .owned_value = owned_value,
                           });
}

std::string uri_encode(std::string_view value, bool preserve_slashes) {
  constexpr char digits[] = "0123456789ABCDEF";
  std::string output;
  output.reserve(value.size());
  for (const unsigned char item : value) {
    const bool unreserved =
        isalnum(item) != 0 || item == '-' || item == '.' || item == '_' ||
        item == '~' || (preserve_slashes && item == '/');
    if (unreserved) {
      output.push_back(static_cast<char>(item));
    } else {
      output.push_back('%');
      output.push_back(digits[item >> 4]);
      output.push_back(digits[item & 0x0fU]);
    }
  }
  return output;
}

void sign_v4_impl(const Credentials& credentials,
                  const SignRequest& request, Signature* diagnostics,
                  HeaderList& result_headers) {
  if (credentials.access_key_id.empty() ||
      credentials.secret_access_key.empty()) {
    throw std::invalid_argument("SigV4 credentials are incomplete");
  }
  if (request.amz_datetime.size() != 16 ||
      request.amz_datetime[8] != 'T' || request.amz_datetime.back() != 'Z') {
    throw std::invalid_argument("SigV4 timestamp must be YYYYMMDDTHHMMSSZ");
  }
  if (request.payload_hash.empty() || request.authority.empty() ||
      request.region.empty()) {
    throw std::invalid_argument("SigV4 request fields are incomplete");
  }

  SigningWorkspace& workspace = signing_workspace();
  result_headers.clear();
  result_headers.reserve(credentials.session_token.empty() ? 4 : 5);
  std::string& signed_names = workspace.signed_names;
  signed_names.clear();

  std::string& canonical = workspace.canonical;
  canonical.clear();
  canonical.reserve(request.method.size() + request.canonical_uri.size() +
                    request.canonical_query.size() +
                    request.payload_hash.size() +
                    request.headers.size() * 64 + 256);
  canonical.append(request.method);
  canonical.push_back('\n');
  canonical.append(request.canonical_uri);
  canonical.push_back('\n');
  canonical.append(request.canonical_query);
  canonical.push_back('\n');

  const Header* if_match_header = nullptr;
  const Header* range_header    = nullptr;
  bool fast_get_headers = diagnostics == nullptr && request.fast_get &&
                          request.headers.size() <= 2;
  if (fast_get_headers) {
    for (const Header& header : request.headers) {
      const std::string_view name = sso_view(header.name);
      if (name == "if-match" && if_match_header == nullptr) {
        if_match_header = &header;
      } else if (name == "range" && range_header == nullptr) {
        range_header = &header;
      } else {
        fast_get_headers = false;
      }
    }
  }

  if (fast_get_headers) {
    constexpr std::string_view host_field = "host:";
    constexpr std::string_view host_name = "host";
    constexpr std::string_view if_match_field = "if-match:";
    constexpr std::string_view if_match_name = ";if-match";
    constexpr std::string_view range_field = "range:";
    constexpr std::string_view range_name = ";range";
    constexpr std::string_view fixed_names =
        ";x-amz-content-sha256;x-amz-date";
    constexpr std::string_view payload_hash_field =
        "x-amz-content-sha256:";
    constexpr std::string_view date_field = "\nx-amz-date:";
    constexpr std::string_view token_field = "\nx-amz-security-token:";
    constexpr std::string_view token_name = ";x-amz-security-token";
    canonical.append(host_field);
    canonical.append(request.authority);
    canonical.push_back('\n');
    signed_names.assign(host_name);
    if (if_match_header != nullptr) {
      canonical.append(if_match_field);
      canonical.append(sso_view(if_match_header->value));
      canonical.push_back('\n');
      signed_names.append(if_match_name);
    }
    if (range_header != nullptr) {
      canonical.append(range_field);
      canonical.append(sso_view(range_header->value));
      canonical.push_back('\n');
      signed_names.append(range_name);
    }
    canonical.append(payload_hash_field);
    canonical.append(request.payload_hash);
    canonical.append(date_field);
    canonical.append(request.amz_datetime);
    signed_names.append(fixed_names);
    if (!credentials.session_token.empty()) {
      canonical.append(token_field);
      canonical.append(credentials.session_token);
      signed_names.append(token_name);
    }
    canonical.push_back('\n');
  } else {
    std::vector<CanonicalHeader>& headers = workspace.headers;
    headers.clear();
    std::vector<ssostr<32>>& normalized_names = workspace.normalized_names;
    std::vector<ssostr<32>>& normalized_values = workspace.normalized_values;
    normalized_names.clear();
    normalized_values.clear();
    const size_t maximum_header_count =
        request.headers.size() +
        (credentials.session_token.empty() ? 3 : 4);
    headers.reserve(maximum_header_count);
    normalized_names.reserve(maximum_header_count);
    normalized_values.reserve(maximum_header_count);
    add_canonical_header(headers, normalized_names, normalized_values,
                         "host", request.authority);
    for (const Header& header : request.headers) {
      add_canonical_header(
          headers, normalized_names, normalized_values,
          std::string_view(header.name.data(), header.name.size()),
          sso_view(header.value));
    }
    add_canonical_header(headers, normalized_names, normalized_values,
                         "x-amz-content-sha256", request.payload_hash);
    add_canonical_header(headers, normalized_names, normalized_values,
                         "x-amz-date", request.amz_datetime);
    if (!credentials.session_token.empty()) {
      add_canonical_header(headers, normalized_names, normalized_values,
                           "x-amz-security-token",
                           credentials.session_token);
    }
    signed_names.reserve(headers.size() * 24);
    for (const CanonicalHeader& header : headers) {
      canonical.append(header.name);
      canonical.push_back(':');
      canonical.append(header.value);
      canonical.push_back('\n');
      if (!signed_names.empty()) {
        signed_names.push_back(';');
      }
      signed_names.append(header.name);
    }
  }
  canonical.push_back('\n');
  canonical.append(signed_names);
  canonical.push_back('\n');
  canonical.append(request.payload_hash);

  std::array<char, kSha256Bytes * 2> canonical_hash;
  hex(sha256(canonical), canonical_hash.data());
  if (diagnostics != nullptr) {
    diagnostics->canonical_request_hash.assign(canonical_hash.data(),
                                                canonical_hash.size());
  }
  const std::string_view date = request.amz_datetime.substr(0, 8);
  std::string& scope = workspace.scope;
  scope.clear();
  scope.reserve(date.size() + request.region.size() +
                request.service.size() + 15);
  scope.append(date);
  scope.push_back('/');
  scope.append(request.region);
  scope.push_back('/');
  scope.append(request.service);
  constexpr std::string_view scope_suffix = "/aws4_request";
  scope.append(scope_suffix);

  constexpr std::string_view algorithm = "AWS4-HMAC-SHA256";
  std::string& to_sign = workspace.to_sign;
  to_sign.clear();
  to_sign.reserve(algorithm.size() + request.amz_datetime.size() +
                  scope.size() + canonical_hash.size() + 3);
  to_sign.append(algorithm);
  to_sign.push_back('\n');
  to_sign.append(request.amz_datetime);
  to_sign.push_back('\n');
  to_sign.append(scope);
  to_sign.push_back('\n');
  to_sign.append(canonical_hash.data(), canonical_hash.size());

  std::array<char, kSha256Bytes * 2> signature;
  hex(signing_hmac(workspace.key, credentials.secret_access_key, date,
                   request.region, request.service, to_sign),
      signature.data());

  constexpr std::string_view credential = " Credential=";
  constexpr std::string_view signed_headers = ",SignedHeaders=";
  constexpr std::string_view signature_field = ",Signature=";
  const auto add_result_header = [&](std::string_view name,
                                     std::string_view value) {
    Header& header = result_headers.emplace_back();
    header.name.assign(name.data(), name.size());
    header.value.assign(value.data(), value.size());
  };
  add_result_header("x-amz-content-sha256", request.payload_hash);
  add_result_header("x-amz-date", request.amz_datetime);
  if (!credentials.session_token.empty()) {
    add_result_header("x-amz-security-token", credentials.session_token);
  }
  Header& authorization_header = result_headers.emplace_back();
  authorization_header.name.assign("authorization", 13);
  ssostr<32>& authorization = authorization_header.value;
  authorization.clear();
  authorization.reserve(
      algorithm.size() + credential.size() + credentials.access_key_id.size() +
      1 + scope.size() + signed_headers.size() + signed_names.size() +
      signature_field.size() + signature.size());
  authorization.append(algorithm);
  authorization.append(credential);
  authorization.append(credentials.access_key_id);
  authorization.push_back('/');
  authorization.append(scope);
  authorization.append(signed_headers);
  authorization.append(signed_names);
  authorization.append(signature_field);
  authorization.append(signature.data(), signature.size());

  if (diagnostics != nullptr) {
    diagnostics->signed_header_names = signed_names;
  }
}

Signature sign_v4(const Credentials& credentials, const SignRequest& request) {
  Signature result;
  sign_v4_impl(credentials, request, &result, result.headers);
  return result;
}

HeaderList sign_v4_headers(const Credentials& credentials,
                           const SignRequest& request) {
  HeaderList headers;
  sign_v4_impl(credentials, request, nullptr, headers);
  return headers;
}

void sign_v4_headers(HeaderList& headers, const Credentials& credentials,
                     const SignRequest& request) {
  sign_v4_impl(credentials, request, nullptr, headers);
}

void write_2digits(char* p, unsigned n) noexcept {
  p[0] = char('0' + n / 10);
  p[1] = char('0' + n % 10);
}

void write_4digits(char* p, unsigned n) noexcept {
  write_2digits(p, n / 100);
  write_2digits(p + 2, n % 100);
}

ssostr<32> amz_datetime(int64_t epoch_seconds) {
  const std::chrono::sys_seconds point{
      std::chrono::seconds{epoch_seconds}};
  const std::chrono::sys_days day =
      std::chrono::floor<std::chrono::days>(point);
  const std::chrono::year_month_day date{day};
  const std::chrono::hh_mm_ss time{point - day};
  const int year = int(date.year());
  if (year < 0 || year > 9999) {
    throw std::out_of_range("SigV4 year is outside YYYY range");
  }

  std::array<char, 16> out;
  write_4digits(out.data(), unsigned(year));
  write_2digits(out.data() + 4, unsigned(date.month()));
  write_2digits(out.data() + 6, unsigned(date.day()));
  out[8] = 'T';
  write_2digits(out.data() + 9, unsigned(time.hours().count()));
  write_2digits(out.data() + 11, unsigned(time.minutes().count()));
  write_2digits(out.data() + 13, unsigned(time.seconds().count()));
  out[15] = 'Z';
  return ssostr<32>(out.data(), out.size());
}

ssostr<32> amz_datetime_now() {
  timespec now{};
  if (::clock_gettime(CLOCK_REALTIME_COARSE, &now) != 0) {
    throw std::runtime_error("clock_gettime(CLOCK_REALTIME_COARSE) failed");
  }
  const int64_t second = now.tv_sec;
  struct Cache {
    int64_t second = std::numeric_limits<int64_t>::min();
    ssostr<32> value;
  };
  thread_local Cache cache;
  if (cache.second != second) {
    cache.second = second;
    cache.value = amz_datetime(second);
  }
  return cache.value;
}
