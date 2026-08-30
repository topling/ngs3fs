#include "http.hpp"
#include "io.hpp"
#include "s3.hpp"

#include <fcntl.h>
#include <sys/socket.h>

#include <array>
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <stddef.h>
#include <iostream>
#include <span>
#include <vector>

static_assert(sizeof(ssostr<32>) == 32);

template<class Function>
void assert_throws(Function function) {
  bool threw = false;
  try {
    function();
  } catch (const std::exception&) {
    threw = true;
  }
  assert(threw);
}

void test_s3_xml() {
  constexpr std::string_view document =
      "<?xml version=\"1.0\"?>"
      "<s3:ListBucketResult xmlns:s3=\"urn:s3\">"
      "<s3:Contents><s3:Key>a&amp;<!-- split -->"
      "<![CDATA[b]]>&#x43;</s3:Key><s3:Size>7</s3:Size></s3:Contents>"
      "<s3:IsTruncated> 1 </s3:IsTruncated>"
      "<s3:NextContinuationToken>next</s3:NextContinuationToken>"
      "</s3:ListBucketResult>";
  S3Xml xml(document, "ListObjectsV2");
  const tinyxml2::XMLElement& root = xml.root("ListBucketResult");
  const tinyxml2::XMLElement* contents =
      xml.first_child(root, "Contents");
  assert(contents != nullptr);
  assert(xml.required_text(*contents, "Key") == "a&bC");
  assert(xml.required_text(*contents, "Size") == "7");
  assert(xml.optional_text(*contents, "ETag").empty());
  assert(xml.required_bool(root, "IsTruncated"));
  assert(xml.required_text(root, "NextContinuationToken") == "next");
  assert(xml.next_sibling(*contents, "Contents") == nullptr);

  assert_throws([] {
    S3Xml xml("<ListBucketResult><IsTruncated>", "malformed");
  });
  assert_throws([] {
    const std::string text("<Root>ok</Root>\0junk", 20);
    S3Xml xml(text, "embedded-NUL");
  });
  assert_throws([] {
    S3Xml xml("<Root><Key>a</Key><Key>b</Key></Root>", "duplicate");
    xml.required_text(xml.root("Root"), "Key");
  });
  assert_throws([] {
    S3Xml xml("<Root><Key><Nested/></Key></Root>", "nested");
    xml.required_text(xml.root("Root"), "Key");
  });
  assert_throws([] {
    S3Xml xml("<ListBucketResult/>", "missing-pagination");
    xml.required_bool(xml.root("ListBucketResult"), "IsTruncated");
  });
  assert_throws([] {
    S3Xml xml(
        "<ListBucketResult><IsTruncated>true</IsTruncated>"
        "</ListBucketResult>",
        "missing-token");
    const tinyxml2::XMLElement& root = xml.root("ListBucketResult");
    if (xml.required_bool(root, "IsTruncated")) {
      xml.required_text(root, "NextContinuationToken");
    }
  });
  assert_throws([] {
    S3Xml xml(
        "<ListBucketResult><IsTruncated>maybe</IsTruncated>"
        "</ListBucketResult>",
        "invalid-pagination");
    xml.required_bool(xml.root("ListBucketResult"), "IsTruncated");
  });
  assert_throws([] {
    S3Xml xml("<Wrong/>", "wrong-root");
    xml.root("ListBucketResult");
  });
  assert_throws([] {
    S3Xml xml(
        "<s3:Error xmlns:s3=\"urn:s3\">"
        "<s3:Code>InternalError</s3:Code></s3:Error>",
        "embedded-error");
    xml.result_root("CompleteMultipartUploadResult");
  });
}

void test_ssostr_header_names() {
  const ssostr<32> known("x-amz-rename-source-if-match");
  assert(known.size() == 28);
  assert(known.is_local());

  terark::gold_hash_map<ssostr<32>, ssostr<32>> headers;
  headers.insert_or_assign(known, ssostr<32>("value"));
  assert(headers.at("x-amz-rename-source-if-match") == "value");

  const ssostr<32> unknown_name(64, 'x');
  const ssostr<32> unknown_value(80, 'y');
  assert(!unknown_name.is_local());
  assert(!unknown_value.is_local());

  headers.insert_or_assign(unknown_name, ssostr<32>(unknown_value));
  assert(headers.at(unknown_name) == unknown_value);
}

void test_inode_dentry_slots() {
  Directory directory;
  std::vector<InodeBase*> items;
  std::vector<std::string> names;
  items.reserve(1024);
  names.reserve(1024);

  for (size_t i = 0; i < 512; ++i) {
    names.push_back("item-" + std::to_string(i));
    auto* item = new InodeFile;
    const auto inserted = directory.insert_i(
        terark::fstring(names.back().data(), names.back().size()), item);
    assert(inserted.second);
    item->dentry_slot = uint32_t(inserted.first);
    items.push_back(item);
  }

  for (size_t i = 0; i < items.size(); i += 3) {
    directory.erase_i(items[i]->dentry_slot);
    items[i]->dentry_slot = UINT32_MAX;
    delete_inode(items[i]);
    items[i] = nullptr;
  }
  for (size_t i = 0; i < 256; ++i) {
    std::string name = "new-item-" + std::to_string(i);
    auto* item = new InodeFile;
    const auto inserted = directory.insert_i(
        terark::fstring(name.data(), name.size()), item);
    assert(inserted.second);
    item->dentry_slot = uint32_t(inserted.first);
  }

  for (size_t i = 0; i < items.size(); ++i) {
    InodeBase* item = items[i];
    if (item == nullptr) {
      continue;
    }
    assert(directory.val(item->dentry_slot) == item);
    assert(directory.key(item->dentry_slot) == names[i]);
  }
}

void test_inode_tagged_parent() {
  InodeDir first;
  InodeDir second;
  InodeFile file(&first);
  assert((uintptr_t(&first) & InodeBase::kFlagMask) == 0);
  assert((uintptr_t(&second) & InodeBase::kFlagMask) == 0);
  assert((uintptr_t(&file) & InodeBase::kFlagMask) == 0);
  assert(file.parent() == &first);
  assert(file.regular());
  file.set_detached(true);
  file.set_pending(true);
  file.set_truncate_pending(true);
  assert((file.parent_flags & InodeBase::kFlagMask) == 0b1111);
  file.set_parent(&second);
  assert(file.parent() == &second);
  assert(file.regular());
  assert(file.detached());
  assert(file.pending());
  assert(file.truncate_pending());
  file.set_detached(false);
  file.set_pending(false);
  file.set_truncate_pending(false);
  assert((file.parent_flags & InodeBase::kFlagMask) == 0b0001);
}

void test_pipe_splice() {
  std::array<int, 2> sockets{-1, -1};
  assert(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0,
                      sockets.data()) == 0);
  UniqueFd sender(sockets[0]);
  UniqueFd receiver(sockets[1]);

  std::vector<std::byte> expected(64U * 1024U);
  for (size_t i = 0; i < expected.size(); ++i) {
    expected[i] = static_cast<std::byte>(i & 0xffU);
  }
  write_all(sender.get(), expected);

  Pipe pipe = Pipe::create();
  assert(pipe.capacity() >= 64U * 1024U);
  assert(splice_exact(receiver.get(), pipe.write_fd(),
                                  expected.size(), SPLICE_F_MOVE) ==
         expected.size());

  std::vector<std::byte> actual(expected.size());
  read_all(pipe.read_fd(), actual);
  assert(actual == expected);
}

void test_s3_control_paths() {
  assert(is_encoded_request_path("/key%20with%20spaces"));
  assert(!is_encoded_request_path("key"));
  assert(!is_encoded_request_path("/key with spaces"));
  assert(!is_encoded_request_path("/bad%2"));
  assert(!is_encoded_request_path("/key?versionId=x"));
  assert(!request_path_is_directory_marker("/empty-object"));
  assert(request_path_is_directory_marker("/directory/"));
  assert(request_path_is_directory_marker("/directory%2F"));

  assert(make_copy_source(
             "bucket", "/key%20with%20spaces", "version/1") ==
         "/bucket/key%20with%20spaces?versionId=version%2F1");
  assert(make_copy_source(
             "bucket", "/bucket/already%20path-style", {}) ==
         "/bucket/already%20path-style");
  assert(make_copy_source(
             "bucket", "/bucket/is%20part%20of%20the%20key", {}, true) ==
         "/bucket/bucket/is%20part%20of%20the%20key");
  assert(authority_uses_virtual_bucket(
      "bucket.s3.example:9000", "bucket"));
  assert(!authority_uses_virtual_bucket(
      "s3.example:9000", "bucket"));
}

ChecksumValue checksum_of(ChecksumAlgorithm algorithm,
                          std::string_view text, size_t split) {
  DataChecksum checksum(algorithm);
  const auto bytes = std::span(
      reinterpret_cast<const std::byte*>(text.data()), text.size());
  checksum.update(bytes.first(split));
  checksum.update(bytes.subspan(split));
  return checksum.finish();
}

void test_data_checksums() {
  ChecksumAlgorithm parsed = CHECKSUM_AUTO;
  assert(parse_checksum_algorithm("XXHash128", parsed));
  assert(parsed == CHECKSUM_XXHASH128);
  assert(parse_checksum_algorithm("crc64-xz", parsed));
  assert(parsed == CHECKSUM_CRC64XZ);
  assert(!parse_checksum_algorithm("crc128", parsed));
  assert(checksum_s3_name(CHECKSUM_XXHASH128) == "XXHASH128");
  assert(checksum_header_name(CHECKSUM_CRC64XZ) ==
         "x-oss-hash-crc64ecma");
  assert(checksum_xml_name(CHECKSUM_SHA512) == "ChecksumSHA512");
  assert(checksum_multipart_type(CHECKSUM_CRC64NVME) == "FULL_OBJECT");
  assert(checksum_multipart_type(CHECKSUM_XXHASH128) == "COMPOSITE");

  constexpr std::string_view text = "123456789";
  const ChecksumValue crc32 = checksum_of(CHECKSUM_CRC32, text, 4);
  const ChecksumValue crc32c = checksum_of(CHECKSUM_CRC32C, text, 5);
  const ChecksumValue nvme = checksum_of(CHECKSUM_CRC64NVME, text, 3);
  const ChecksumValue xz = checksum_of(CHECKSUM_CRC64XZ, text, 7);
  assert(crc32.integer == 0xcbf43926);
  assert(sso_view(crc32.base64) == "y/Q5Jg==");
  assert(crc32c.integer == 0xe3069283);
  assert(sso_view(crc32c.base64) == "4waSgw==");
  assert(nvme.integer == 0xae8b14860a799888);
  assert(sso_view(nvme.base64) == "rosUhgp5mIg=");
  assert(xz.integer == 0x995dc9bbdf1939fa);
  assert(sso_view(xz.base64) == "mV3Ju98ZOfo=");
  assert(crc64_checksum_value(nvme.integer).base64 == nvme.base64);
  assert(sso_view(checksum_of(CHECKSUM_MD5, text, 4).base64) ==
         "JfnnlDI7RTiF9RgfG2JNCw==");
  assert(sso_view(checksum_of(CHECKSUM_SHA1, text, 4).base64) ==
         "98O8HYCOBHMq32eZZczDTKeuNEE=");
  assert(sso_view(checksum_of(CHECKSUM_SHA256, text, 4).base64) ==
         "FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU=");
  assert(sso_view(checksum_of(CHECKSUM_SHA512, text, 4).base64) ==
         "2eZ2LdHI6vbWGzxhkvxAjU1tXxF20MKRabwk5xw/J0rSf81YEbMT1oH35V7A"
         "LXPUmclUVba1u1A6z1dPuo/+hQ==");
  assert(sso_view(checksum_of(CHECKSUM_XXHASH64, text, 4).base64) ==
         "jLhB20DmroM=");
  assert(sso_view(checksum_of(CHECKSUM_XXHASH3, text, 4).base64) ==
         "ctyxi2ehff8=");
  assert(sso_view(checksum_of(CHECKSUM_XXHASH128, text, 4).base64) ==
         "MxGUd+3l3NXpcWQnaB1YYA==");

  for (const ChecksumAlgorithm algorithm :
       {CHECKSUM_CRC64NVME, CHECKSUM_CRC64XZ}) {
    const ChecksumValue whole = checksum_of(algorithm, text, 3);
    for (size_t split = 0; split <= text.size(); ++split) {
      const std::string_view first_text = text.substr(0, split);
      const std::string_view second_text = text.substr(split);
      const ChecksumValue first = checksum_of(
          algorithm, first_text, first_text.size() / 2);
      const ChecksumValue second = checksum_of(
          algorithm, second_text, second_text.size() / 2);
      assert(combine_crc64(algorithm, first.integer, second.integer,
                           second_text.size()) == whole.integer);
    }
  }

  constexpr std::array algorithms{
      CHECKSUM_CRC32,
      CHECKSUM_CRC32C,
      CHECKSUM_CRC64NVME,
      CHECKSUM_SHA1,
      CHECKSUM_SHA256,
      CHECKSUM_MD5,
      CHECKSUM_XXHASH64,
      CHECKSUM_XXHASH3,
      CHECKSUM_XXHASH128,
      CHECKSUM_SHA512,
      CHECKSUM_CRC64XZ,
  };
  for (const ChecksumAlgorithm algorithm : algorithms) {
    assert(checksum_of(algorithm, text, 0).base64 ==
           checksum_of(algorithm, text, 4).base64);
  }
}

void test_aws_sigv4_reference_vector() {
  const Credentials credentials{
      .access_key_id = "AKIAIOSFODNN7EXAMPLE",
      .secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  };
  const std::array signed_headers{
      Header{"range", "bytes=0-9"},
  };
  const auto signature = sign_v4(
      credentials,
      SignRequest{
          .method = "GET",
          .canonical_uri = "/test.txt",
          .canonical_query = "",
          .authority = "examplebucket.s3.amazonaws.com",
          .headers = signed_headers,
          .payload_hash = kEmptyPayloadSha256,
          .amz_datetime = "20130524T000000Z",
          .region = "us-east-1",
          .fast_get = true,
      });
  assert(signature.canonical_request_hash ==
         "7344ae5b7ee6c3e7e6b0fe0640412a37"
         "625d1fbfff95c48bbb2dc43964946972");
  assert(signature.signed_header_names ==
         "host;range;x-amz-content-sha256;x-amz-date");
  assert(signature.headers.back().value ==
         "AWS4-HMAC-SHA256 "
         "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/"
         "aws4_request,SignedHeaders=host;range;x-amz-content-sha256;"
         "x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f017"
         "0aba48dd91039c6036bdb41");
  const auto fast_headers = sign_v4_headers(
      credentials,
      SignRequest{
          .method = "GET",
          .canonical_uri = "/test.txt",
          .canonical_query = "",
          .authority = "examplebucket.s3.amazonaws.com",
          .headers = signed_headers,
          .payload_hash = kEmptyPayloadSha256,
          .amz_datetime = "20130524T000000Z",
          .region = "us-east-1",
          .fast_get = true,
      });
  assert(fast_headers.back().value == signature.headers.back().value);

  Credentials token_credentials = credentials;
  token_credentials.session_token = "session-token";
  const std::array conditional_headers{
      Header{"range", "bytes=0-9"},
      Header{"if-match", "\"etag\""},
  };
  const SignRequest conditional_request{
      .method = "GET",
      .canonical_uri = "/test.txt",
      .canonical_query = "",
      .authority = "examplebucket.s3.amazonaws.com",
      .headers = conditional_headers,
      .payload_hash = kEmptyPayloadSha256,
      .amz_datetime = "20130524T000000Z",
      .region = "us-east-1",
  };
  SignRequest fast_conditional_request = conditional_request;
  fast_conditional_request.fast_get = true;
  assert(sign_v4_headers(token_credentials,
                         fast_conditional_request).back().value ==
         sign_v4(token_credentials, conditional_request).headers.back().value);
  SignRequest unsigned_range_request = conditional_request;
  unsigned_range_request.headers = {};
  unsigned_range_request.fast_get = true;
  SignRequest generic_unsigned_range_request = unsigned_range_request;
  generic_unsigned_range_request.fast_get = false;
  assert(sign_v4_headers(token_credentials,
                         unsigned_range_request).back().value ==
         sign_v4_headers(token_credentials,
                         generic_unsigned_range_request).back().value);

  const auto sign = [&](std::string_view timestamp,
                        std::string_view region,
                        const Credentials& selected_credentials) {
    return sign_v4(
        selected_credentials,
        SignRequest{
            .method = "GET",
            .canonical_uri = "/test.txt",
            .canonical_query = "",
            .authority = "examplebucket.s3.amazonaws.com",
            .headers = signed_headers,
            .payload_hash = kEmptyPayloadSha256,
            .amz_datetime = timestamp,
            .region = region,
        });
  };
  assert(sign("20130524T000000Z", "us-east-1", credentials)
             .headers.back().value == signature.headers.back().value);

  Credentials changed_credentials = credentials;
  changed_credentials.secret_access_key.push_back('x');
  assert(sign("20130524T000000Z", "us-east-1", changed_credentials)
             .headers.back().value != signature.headers.back().value);
  assert(sign("20130525T000000Z", "us-east-1", credentials)
             .headers.back().value != signature.headers.back().value);
  assert(sign("20130524T000000Z", "us-west-2", credentials)
             .headers.back().value != signature.headers.back().value);
  assert(sign("20130524T000000Z", "us-east-1", credentials)
             .headers.back().value == signature.headers.back().value);
}

void test_amz_datetime() {
  assert(sso_view(amz_datetime(-1)) == "19691231T235959Z");
  assert(sso_view(amz_datetime(0)) == "19700101T000000Z");
  assert(sso_view(amz_datetime(951782400)) == "20000229T000000Z");
  assert(sso_view(amz_datetime(2147483647)) == "20380119T031407Z");
  assert(sso_view(amz_datetime(4102444799)) == "20991231T235959Z");

  const ssostr<32> now = amz_datetime_now();
  assert(now.size() == 16);
  assert(now[8] == 'T');
  assert(now[15] == 'Z');
}

void test_s3_mtime() {
  assert(parse_s3_mtime("1969-12-31T23:59:59Z") == -1);
  assert(parse_s3_mtime("1970-01-01T00:00:00Z") == 0);
  assert(parse_s3_mtime("2000-02-29T00:00:00.000Z") == 951782400);
  assert(parse_s3_mtime("2038-01-19T03:14:07.123456Z") == 2147483647);
}

int main() {
  test_s3_xml();
  test_ssostr_header_names();
  test_inode_dentry_slots();
  test_inode_tagged_parent();
  test_pipe_splice();
  test_s3_control_paths();
  test_data_checksums();
  test_aws_sigv4_reference_vector();
  test_amz_datetime();
  test_s3_mtime();
  std::cout << "ngs3fs core tests passed\n";
  return 0;
}
