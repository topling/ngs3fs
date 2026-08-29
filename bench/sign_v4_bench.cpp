#include "s3.hpp"

#include <time.h>

#include <algorithm>
#include <array>
#include <iostream>
#include <stddef.h>
#include <stdint.h>
#include <stdexcept>
#include <vector>

uint64_t monotonic_ns() {
  timespec t{};
  if (clock_gettime(CLOCK_MONOTONIC, &t) != 0) {
    throw std::runtime_error("clock_gettime failed");
  }
  return uint64_t(t.tv_sec) * 1'000'000'000 + uint64_t(t.tv_nsec);
}

int main() {
  constexpr size_t iterations = 200'000;
  constexpr size_t repetitions = 7;
  constexpr size_t time_iterations = 1'000'000;
  const Credentials credentials{
      .access_key_id = "AKIAIOSFODNN7EXAMPLE",
      .secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      .session_token = "",
  };
  const std::array<Header, 0> headers{};
  const SignRequest request{
      .method = "GET",
      .canonical_uri = "/benchmark-object",
      .canonical_query = "",
      .authority = "examplebucket.s3.amazonaws.com",
      .headers = headers,
      .payload_hash = kEmptyPayloadSha256,
      .amz_datetime = "20260829T000000Z",
      .region = "us-east-1",
      .fast_get = true,
  };

  size_t checksum = 0;
  HeaderList signed_headers;
  for (size_t i = 0; i < 10'000; ++i) {
    sign_v4_headers(signed_headers, credentials, request);
    checksum += signed_headers.back().value[0];
  }

  std::vector<uint64_t> samples;
  samples.reserve(repetitions);
  for (size_t repetition = 0; repetition < repetitions; ++repetition) {
    const uint64_t start = monotonic_ns();
    for (size_t i = 0; i < iterations; ++i) {
      sign_v4_headers(signed_headers, credentials, request);
      checksum += signed_headers.back().value[0];
    }
    samples.push_back(monotonic_ns() - start);
  }
  std::sort(samples.begin(), samples.end());
  const double median_ns = double(samples[repetitions / 2]) / iterations;

  samples.clear();
  for (size_t i = 0; i < 10'000; ++i) {
    checksum += amz_datetime_now()[0];
  }
  for (size_t repetition = 0; repetition < repetitions; ++repetition) {
    const uint64_t start = monotonic_ns();
    for (size_t i = 0; i < time_iterations; ++i) {
      checksum += amz_datetime_now()[0];
    }
    samples.push_back(monotonic_ns() - start);
  }
  std::sort(samples.begin(), samples.end());
  const double time_ns =
      double(samples[repetitions / 2]) / time_iterations;
  std::cout << "{\"iterations\":" << iterations
            << ",\"median_ns_per_sign\":" << median_ns
            << ",\"amz_datetime_ns\":" << time_ns
            << ",\"checksum\":" << checksum << "}\n";
}
