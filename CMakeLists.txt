cmake_minimum_required(VERSION 3.25)

project(ngs3fs VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)

option(NGS3FS_BUILD_TESTS "Build unit and integration tests" ON)
option(NGS3FS_ENABLE_SANITIZERS "Enable ASan and UBSan" OFF)
option(NGS3FS_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(NGS3FS_BUILD_BENCHMARKS "Build latency benchmark tools" ON)
set(TOPLING_ZIP_SOURCE_DIR
  "" CACHE PATH "Path to an external topling-zip source tree")

if(TOPLING_ZIP_SOURCE_DIR)
  message(STATUS "Using external topling-zip: ${TOPLING_ZIP_SOURCE_DIR}")
else()
  FetchContent_Declare(ngs3fs_topling_zip
    GIT_REPOSITORY https://github.com/topling/topling-zip.git
    GIT_TAG 60d2c9a3b37249ceef04b608ef7dd4402ec44553
    GIT_SUBMODULES boost-include
    GIT_SUBMODULES_RECURSE TRUE)
  FetchContent_GetProperties(ngs3fs_topling_zip)
  if(NOT ngs3fs_topling_zip_POPULATED)
    message(STATUS "TOPLING_ZIP_SOURCE_DIR is unset; cloning topling-zip")
    FetchContent_Populate(ngs3fs_topling_zip)
  endif()
  set(TOPLING_ZIP_SOURCE_DIR "${ngs3fs_topling_zip_SOURCE_DIR}")
endif()

if(NOT EXISTS "${TOPLING_ZIP_SOURCE_DIR}/src/terark/sso.hpp" OR
   NOT EXISTS "${TOPLING_ZIP_SOURCE_DIR}/src/terark/gold_hash_map.hpp" OR
   NOT EXISTS "${TOPLING_ZIP_SOURCE_DIR}/src/terark/hash_common.cpp" OR
   NOT EXISTS "${TOPLING_ZIP_SOURCE_DIR}/boost-include/boost/current_function.hpp")
  message(FATAL_ERROR "required Topling headers or sources not found under TOPLING_ZIP_SOURCE_DIR")
endif()

if(NGS3FS_ENABLE_SANITIZERS)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

if(NGS3FS_ENABLE_TSAN)
  if(NGS3FS_ENABLE_SANITIZERS)
    message(FATAL_ERROR "ASan/UBSan and TSan cannot be enabled together")
  endif()
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
endif()

if(NGS3FS_BUILD_BENCHMARKS)
  add_executable(mmap_fault_bench bench/mmap_fault_bench.cpp)
  target_compile_options(mmap_fault_bench PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(Dependencies)
find_package(OpenSSL REQUIRED COMPONENTS Crypto SSL)
ngs3fs_find_fuse3()

set(tinyxml2_BUILD_TESTING OFF CACHE BOOL "" FORCE)
FetchContent_Declare(ngs3fs_tinyxml2
  GIT_REPOSITORY https://github.com/leethomason/tinyxml2.git
  GIT_TAG 11.0.0
  GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(ngs3fs_tinyxml2)

FetchContent_Declare(ngs3fs_xxhash
  URL https://github.com/Cyan4973/xxHash/archive/refs/tags/v0.8.3.tar.gz
  URL_HASH SHA256=aae608dfe8213dfd05d909a57718ef82f30722c392344583d3f39050c7f29a80
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_GetProperties(ngs3fs_xxhash)
if(NOT ngs3fs_xxhash_POPULATED)
  FetchContent_Populate(ngs3fs_xxhash)
endif()
add_library(ngs3fs_xxhash STATIC
  "${ngs3fs_xxhash_SOURCE_DIR}/xxhash.c")
target_include_directories(ngs3fs_xxhash SYSTEM PUBLIC
  "${ngs3fs_xxhash_SOURCE_DIR}")

add_library(ngs3fs_core
  ${TOPLING_ZIP_SOURCE_DIR}/src/terark/hash_common.cpp
  src/http.cpp
  src/io.cpp
  src/s3.cpp
)
target_include_directories(ngs3fs_core PUBLIC src)
target_include_directories(ngs3fs_core SYSTEM PUBLIC
  "${TOPLING_ZIP_SOURCE_DIR}/src"
  "${TOPLING_ZIP_SOURCE_DIR}/boost-include")
target_compile_options(ngs3fs_core PRIVATE
  -Wall -Wextra -Wpedantic -Wconversion)
target_compile_definitions(ngs3fs_core PUBLIC TOPLING_DISABLE_JEMALLOC)
target_link_libraries(ngs3fs_core PUBLIC
  Threads::Threads
  OpenSSL::Crypto
  OpenSSL::SSL
  nghttp2_static
  llhttp::llhttp
  tinyxml2::tinyxml2
  ngs3fs_xxhash
)

if(NGS3FS_BUILD_BENCHMARKS)
  add_executable(sign_v4_bench bench/sign_v4_bench.cpp)
  target_link_libraries(sign_v4_bench PRIVATE ngs3fs_core)
  target_compile_options(sign_v4_bench PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion)
endif()

add_executable(ngs3fs
  src/main.cpp
  src/fuse.cpp
)
target_link_libraries(ngs3fs PRIVATE ngs3fs_core FUSE3::fuse3)
target_compile_definitions(ngs3fs PRIVATE FUSE_USE_VERSION=35)
target_compile_options(ngs3fs PRIVATE
  -Wall -Wextra -Wpedantic -Wconversion)

if(NGS3FS_BUILD_TESTS)
  enable_testing()
  add_compile_options(-UNDEBUG)

  add_executable(mounted_e2e_client tests/mounted_e2e_client.cpp)
  target_compile_options(mounted_e2e_client PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion)

  add_executable(random_read_stress tests/random_read_stress.c)
  target_link_libraries(random_read_stress PRIVATE Threads::Threads)
  target_compile_options(random_read_stress PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion)

  add_executable(ngs3fs_core_tests tests/core_tests.cpp)
  target_link_libraries(ngs3fs_core_tests PRIVATE ngs3fs_core)
  add_test(NAME ngs3fs_core_tests COMMAND ngs3fs_core_tests)

  add_executable(nghttp2_external_data_test
    tests/nghttp2_external_data_test.cpp)
  target_link_libraries(nghttp2_external_data_test PRIVATE ngs3fs_core)
  add_test(NAME nghttp2_external_data_test COMMAND nghttp2_external_data_test)

  add_executable(h2_range_client_test tests/h2_range_client_test.cpp)
  target_link_libraries(h2_range_client_test PRIVATE ngs3fs_core)
  add_test(NAME h2_range_client_test COMMAND h2_range_client_test)

  add_executable(h2_put_client_test tests/h2_put_client_test.cpp)
  target_link_libraries(h2_put_client_test PRIVATE ngs3fs_core)
  add_test(NAME h2_put_client_test COMMAND h2_put_client_test)

  add_executable(fuse_mmap_integration_test
    tests/fuse_mmap_integration_test.cpp)
  target_link_libraries(fuse_mmap_integration_test PRIVATE ngs3fs_core)
  add_test(NAME fuse_mmap_integration_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>)
  set_tests_properties(fuse_mmap_integration_test PROPERTIES TIMEOUT 60)
  add_test(NAME fuse_crc64nvme_integration_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs> crc64nvme)
  set_tests_properties(fuse_crc64nvme_integration_test PROPERTIES TIMEOUT 60)
  add_test(NAME fuse_oss_crc64_integration_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs> crc64xz)
  set_tests_properties(fuse_oss_crc64_integration_test PROPERTIES TIMEOUT 60)

  add_test(NAME read_ahead_option_test
    COMMAND ngs3fs -R 1MiB --help)
  add_test(NAME read_ahead_alignment_test
    COMMAND ngs3fs --read-ahead 257KiB --help)
  set_tests_properties(read_ahead_alignment_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME io_size_option_test
    COMMAND ngs3fs --io-size 1MiB --help)
  add_test(NAME io_size_zero_test
    COMMAND ngs3fs --io-size 0 --help)
  set_tests_properties(io_size_zero_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME checksum_options_test
    COMMAND ngs3fs --checksum xxhash128 --verify-read-checksum --help)
  add_test(NAME checksum_invalid_test
    COMMAND ngs3fs --checksum not-a-checksum --help)
  set_tests_properties(checksum_invalid_test PROPERTIES WILL_FAIL TRUE)

  add_test(NAME write_options_test
    COMMAND ngs3fs --part-size 8MiB --max-uploads 4
                   --max-connections 8 --max-pinned-memory 256MiB --help)
  add_test(NAME part_size_minimum_test
    COMMAND ngs3fs --part-size 4MiB --help)
  set_tests_properties(part_size_minimum_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME connection_budget_test
    COMMAND ngs3fs --max-uploads 4 --max-connections 4 --help)
  set_tests_properties(connection_budget_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME pinned_memory_budget_test
    COMMAND ngs3fs --part-size 8MiB --max-pinned-memory 5MiB --help)
  set_tests_properties(pinned_memory_budget_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME writeback_cache_rejected_test
    COMMAND ngs3fs -o writeback_cache --help)
  set_tests_properties(writeback_cache_rejected_test PROPERTIES
    WILL_FAIL TRUE)
  add_test(NAME directory_cache_option_test
    COMMAND ngs3fs --dir-cache-timeout 250 --help)
  add_test(NAME inode_cache_option_test
    COMMAND ngs3fs --max-cached-inodes 2000000 --help)
  add_test(NAME inode_cache_zero_test
    COMMAND ngs3fs --max-cached-inodes 0 --help)
  set_tests_properties(inode_cache_zero_test PROPERTIES WILL_FAIL TRUE)

  add_executable(h1_client_test tests/h1_client_test.cpp)
  target_link_libraries(h1_client_test PRIVATE ngs3fs_core)
  add_test(NAME h1_client_test COMMAND h1_client_test)

  add_executable(protocol_probe_timeout_test
    tests/protocol_probe_timeout_test.cpp)
  target_link_libraries(protocol_probe_timeout_test PRIVATE ngs3fs_core)
  add_test(NAME protocol_probe_timeout_test COMMAND protocol_probe_timeout_test)
  set_tests_properties(protocol_probe_timeout_test PROPERTIES TIMEOUT 5)

  add_executable(h2_reconnect_client_test
    tests/h2_reconnect_client_test.cpp)
  target_link_libraries(h2_reconnect_client_test PRIVATE ngs3fs_core)
  add_test(NAME h2_reconnect_client_test COMMAND h2_reconnect_client_test)
  set_tests_properties(h2_reconnect_client_test PROPERTIES TIMEOUT 5)

  add_executable(tls_client_test tests/tls_client_test.cpp)
  target_link_libraries(tls_client_test PRIVATE ngs3fs_core)
  add_test(NAME tls_client_test COMMAND tls_client_test)
  set_tests_properties(tls_client_test PROPERTIES TIMEOUT 10)
endif()
