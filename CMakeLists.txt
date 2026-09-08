cmake_minimum_required(VERSION 3.25)

project(ngs3fs VERSION 0.1.0 LANGUAGES C CXX)

include(GNUInstallDirs)

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
ngs3fs_find_liburing()

FetchContent_Declare(ngs3fs_tinyxml2
  URL https://github.com/leethomason/tinyxml2/archive/refs/tags/11.0.0.tar.gz
  URL_HASH SHA256=5556deb5081fb246ee92afae73efd943c889cef0cafea92b0b82422d6a18f289
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_GetProperties(ngs3fs_tinyxml2)
if(NOT ngs3fs_tinyxml2_POPULATED)
  FetchContent_Populate(ngs3fs_tinyxml2)
endif()
add_library(ngs3fs_tinyxml2 STATIC
  "${ngs3fs_tinyxml2_SOURCE_DIR}/tinyxml2.cpp")
target_include_directories(ngs3fs_tinyxml2 SYSTEM PUBLIC
  "${ngs3fs_tinyxml2_SOURCE_DIR}")
target_compile_definitions(ngs3fs_tinyxml2 PUBLIC _FILE_OFFSET_BITS=64)

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

FetchContent_Declare(ngs3fs_yyjson
  URL https://github.com/ibireme/yyjson/archive/refs/tags/0.12.0.tar.gz
  URL_HASH SHA256=b16246f617b2a136c78d73e5e2647c6f1de1313e46678062985bdcf1f40bb75d
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_GetProperties(ngs3fs_yyjson)
if(NOT ngs3fs_yyjson_POPULATED)
  FetchContent_Populate(ngs3fs_yyjson)
endif()
add_library(ngs3fs_yyjson STATIC
  "${ngs3fs_yyjson_SOURCE_DIR}/src/yyjson.c")
target_include_directories(ngs3fs_yyjson SYSTEM PUBLIC
  "${ngs3fs_yyjson_SOURCE_DIR}/src")

add_library(ngs3fs_core
  ${TOPLING_ZIP_SOURCE_DIR}/src/terark/hash_common.cpp
  src/cache.cpp
  src/credentials.cpp
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
  ngs3fs_tinyxml2
  ngs3fs_xxhash
  ngs3fs_yyjson
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
  src/fuse_reactor.cpp
)
target_link_libraries(ngs3fs PRIVATE ngs3fs_core FUSE3::fuse3 LIBURING::uring)
target_compile_definitions(ngs3fs PRIVATE FUSE_USE_VERSION=318)
target_compile_options(ngs3fs PRIVATE
  -Wall -Wextra -Wpedantic -Wconversion)

install(TARGETS ngs3fs
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES docs/ngs3fs.1
  DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
install(FILES README.md LICENSE docs/operations.md docs/local-cache-spec.md
  DESTINATION ${CMAKE_INSTALL_DOCDIR})
install(FILES packaging/ngs3fs@.service
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/systemd/system)
install(FILES packaging/ngs3fs.env.example
  DESTINATION ${CMAKE_INSTALL_DOCDIR}/examples)

set(CPACK_PACKAGE_NAME ngs3fs)
set(CPACK_PACKAGE_VENDOR Topling)
set(CPACK_PACKAGE_CONTACT "Topling <support@topling.cn>")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
  "Low-latency libnghttp2/llhttp S3 FUSE client")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_PACKAGING_INSTALL_PREFIX /usr)
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
set(CPACK_GENERATOR "TGZ;DEB")
include(CPack)

if(NGS3FS_BUILD_TESTS)
  enable_testing()
  add_compile_options(-UNDEBUG)
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  add_test(NAME random_read_memory_test
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/random_read_memory_test.py)
  add_test(NAME profile_measurement_test
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/profile_measurement_test.py)
  add_test(NAME download_reference_binaries_test
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/download_reference_binaries_test.py)
  add_test(NAME summarize_random_read_test
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/summarize_random_read_test.py)
  add_test(NAME summarize_io_engine_test
    COMMAND ${Python3_EXECUTABLE}
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/summarize_io_engine_test.py)

  add_executable(reactor_io_test tests/reactor_io_test.cpp src/fuse_reactor.cpp)
  target_link_libraries(reactor_io_test PRIVATE ngs3fs_core FUSE3::fuse3 LIBURING::uring)
  target_compile_definitions(reactor_io_test PRIVATE FUSE_USE_VERSION=318)
  target_compile_options(reactor_io_test PRIVATE -Wall -Wextra -Wpedantic -Wconversion)
  add_test(NAME reactor_io_test COMMAND reactor_io_test)
  add_test(NAME reactor_sqpoll_io_test COMMAND reactor_io_test --sqpoll)
  add_test(NAME reactor_inode_dispatch_test COMMAND reactor_io_test --dispatch)
  add_test(NAME reactor_dispatch_freelist_test
    COMMAND reactor_io_test --dispatch-freelist)
  set_tests_properties(reactor_inode_dispatch_test reactor_dispatch_freelist_test
    PROPERTIES TIMEOUT 10)
  set_tests_properties(reactor_dispatch_freelist_test
    PROPERTIES SKIP_RETURN_CODE 77)
  set_tests_properties(reactor_io_test reactor_sqpoll_io_test
    PROPERTIES TIMEOUT 20 SKIP_RETURN_CODE 77)

  add_executable(libfuse_prefix_test tests/libfuse_prefix_test.cpp)
  target_link_libraries(libfuse_prefix_test PRIVATE FUSE3::fuse3)
  target_compile_definitions(libfuse_prefix_test PRIVATE FUSE_USE_VERSION=318)
  target_compile_options(libfuse_prefix_test PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion)
  add_test(NAME libfuse_prefix_test COMMAND libfuse_prefix_test)
  set_tests_properties(libfuse_prefix_test PROPERTIES TIMEOUT 10)

  add_executable(mounted_e2e_client tests/mounted_e2e_client.cpp)
  target_compile_options(mounted_e2e_client PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion)

  add_executable(random_read_stress tests/random_read_stress.c)
  target_link_libraries(random_read_stress PRIVATE Threads::Threads)
  target_compile_options(random_read_stress PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion)

  add_executable(cache_recovery_fixture tests/cache_recovery_fixture.c)
  target_compile_options(cache_recovery_fixture PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion)

  add_executable(cross_mount_race tests/cross_mount_race.cpp)
  target_compile_options(cross_mount_race PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion)
  target_link_libraries(cross_mount_race PRIVATE Threads::Threads)

  add_executable(provider_smoke tests/provider_smoke.cpp)
  target_compile_options(provider_smoke PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion)
  target_link_libraries(provider_smoke PRIVATE Threads::Threads)

  add_executable(ngs3fs_core_tests tests/core_tests.cpp)
  target_link_libraries(ngs3fs_core_tests PRIVATE ngs3fs_core)
  add_test(NAME ngs3fs_core_tests COMMAND ngs3fs_core_tests)

  add_executable(cache_test tests/cache_test.cpp)
  target_link_libraries(cache_test PRIVATE ngs3fs_core)
  add_test(NAME cache_test COMMAND cache_test)

  add_executable(nghttp2_external_data_test
    tests/nghttp2_external_data_test.cpp)
  target_link_libraries(nghttp2_external_data_test PRIVATE ngs3fs_core)
  add_test(NAME nghttp2_external_data_test COMMAND nghttp2_external_data_test)

  add_executable(h2_range_client_test tests/h2_range_client_test.cpp)
  target_link_libraries(h2_range_client_test PRIVATE ngs3fs_core)
  add_test(NAME h2_range_client_test COMMAND h2_range_client_test)
  add_test(NAME h2_range_client_async_test
    COMMAND h2_range_client_test --async)
  add_test(NAME h2_range_client_async_memory_test
    COMMAND h2_range_client_test --async-memory)
  add_test(NAME h2_range_client_async_memory_padded_test
    COMMAND h2_range_client_test --async-memory-padded)

  add_executable(h2_put_client_test tests/h2_put_client_test.cpp)
  target_link_libraries(h2_put_client_test PRIVATE ngs3fs_core)
  add_test(NAME h2_put_client_test COMMAND h2_put_client_test)
  add_test(NAME h2_put_client_async_test
    COMMAND h2_put_client_test --async)
  add_test(NAME h2_put_client_async_segments_test
    COMMAND h2_put_client_test --async-segments)

  add_executable(fuse_mmap_integration_test
    tests/fuse_mmap_integration_test.cpp)
  target_link_libraries(fuse_mmap_integration_test PRIVATE ngs3fs_core)

  find_program(STRACE_EXECUTABLE strace)
  if(STRACE_EXECUTABLE)
    add_test(NAME fuse_native_passthrough_wiring_test
      COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
              xxhash128 passthrough auto 1 ${STRACE_EXECUTABLE})
    set_tests_properties(fuse_native_passthrough_wiring_test
      PROPERTIES TIMEOUT 90 RESOURCE_LOCK fuse_mounts SKIP_RETURN_CODE 77)
  endif()
  add_test(NAME fuse_passthrough_budget_fallback_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 passthrough-budget auto 1)
  set_tests_properties(fuse_passthrough_budget_fallback_test
    PROPERTIES TIMEOUT 90 RESOURCE_LOCK fuse_mounts)

  add_executable(native_passthrough_integration_test
    tests/native_passthrough_integration_test.cpp)
  target_link_libraries(native_passthrough_integration_test PRIVATE
    FUSE3::fuse3)
  target_compile_definitions(native_passthrough_integration_test PRIVATE
    FUSE_USE_VERSION=318)
  target_compile_options(native_passthrough_integration_test PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion)
  add_test(NAME native_passthrough_fallback_test
    COMMAND native_passthrough_integration_test)
  add_test(NAME native_passthrough_real_test
    COMMAND native_passthrough_integration_test --require-passthrough)
  set_tests_properties(
    native_passthrough_fallback_test
    native_passthrough_real_test
    PROPERTIES TIMEOUT 30 RESOURCE_LOCK fuse_mounts SKIP_RETURN_CODE 77)

  foreach(engine legacy uring)
    foreach(blocks cache-blocks cache-blocks-96k)
      add_test(NAME fuse_${engine}_${blocks}_test
        COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
                xxhash128 ${blocks} ${engine} 2)
      set_tests_properties(fuse_${engine}_${blocks}_test PROPERTIES
        TIMEOUT 60 RESOURCE_LOCK fuse_mounts)
    endforeach()
    add_test(NAME fuse_${engine}_hardlink_copy_test
      COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
              xxhash128 hardlink-copy ${engine} 1)
    add_test(NAME fuse_${engine}_cached_hardlink_copy_test
      COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
              xxhash128 hardlink-copy-cache ${engine} 1)
    set_tests_properties(
      fuse_${engine}_hardlink_copy_test
      fuse_${engine}_cached_hardlink_copy_test
      PROPERTIES TIMEOUT 90 RESOURCE_LOCK fuse_mounts)
  endforeach()
  foreach(engine legacy uring)
    add_test(NAME fuse_${engine}_prefetch_shutdown_test
      COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
              xxhash128 prefetch-shutdown ${engine} 2)
    set_tests_properties(fuse_${engine}_prefetch_shutdown_test PROPERTIES
      TIMEOUT 60 RESOURCE_LOCK fuse_mounts)
    add_test(NAME fuse_${engine}_prefetch_budget_test
      COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
              xxhash128 prefetch-budget ${engine} 2)
    set_tests_properties(fuse_${engine}_prefetch_budget_test PROPERTIES
      TIMEOUT 60 RESOURCE_LOCK fuse_mounts)
  endforeach()
  add_test(NAME fuse_legacy_prefetch_no_store_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 prefetch legacy)
  set_tests_properties(fuse_legacy_prefetch_no_store_test PROPERTIES
    TIMEOUT 60 RESOURCE_LOCK fuse_mounts)
  add_test(NAME fuse_mmap_integration_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>)
  set_tests_properties(fuse_mmap_integration_test PROPERTIES TIMEOUT 60)
  add_test(NAME fuse_uring_integration_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 plain uring)
  set_tests_properties(fuse_uring_integration_test PROPERTIES TIMEOUT 60)
  add_test(NAME fuse_legacy_integration_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 plain legacy)
  set_tests_properties(fuse_legacy_integration_test PROPERTIES TIMEOUT 60)
  add_test(NAME fuse_sqpoll_plain_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 plain uring-sqpoll)
  add_test(NAME fuse_sqpoll_cached_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 cache uring-sqpoll)
  set_tests_properties(
    fuse_sqpoll_plain_test
    fuse_sqpoll_cached_test
    PROPERTIES TIMEOUT 90 RESOURCE_LOCK fuse_mounts)
  add_test(NAME fuse_multi_reactor_integration_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 plain uring 2)
  set_tests_properties(fuse_multi_reactor_integration_test PROPERTIES
    TIMEOUT 60)
  add_test(NAME fuse_multi_reactor_cached_integration_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 cache uring 2)
  set_tests_properties(fuse_multi_reactor_cached_integration_test PROPERTIES
    TIMEOUT 60)
  add_test(NAME fuse_crc64nvme_integration_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs> crc64nvme)
  set_tests_properties(fuse_crc64nvme_integration_test PROPERTIES TIMEOUT 60)
  add_test(NAME fuse_oss_crc64_integration_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs> crc64xz)
  set_tests_properties(fuse_oss_crc64_integration_test PROPERTIES TIMEOUT 60)
  add_test(NAME fuse_cached_mmap_integration_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs> xxhash128 cache)
  set_tests_properties(fuse_cached_mmap_integration_test PROPERTIES TIMEOUT 90)
  add_test(NAME fuse_uncached_read_ahead_integration_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 prefetch legacy)
  set_tests_properties(fuse_uncached_read_ahead_integration_test
    PROPERTIES TIMEOUT 60)
  add_test(NAME fuse_uring_prefetch_pool_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 prefetch-pool uring)
  add_test(NAME fuse_multi_reactor_prefetch_pool_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 prefetch-pool uring 2)
  add_test(NAME fuse_sqpoll_prefetch_pool_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 prefetch-pool uring-sqpoll)
  add_test(NAME fuse_sqpoll_prefetch_shutdown_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 prefetch-shutdown uring-sqpoll 2)
  add_test(NAME fuse_sqpoll_prefetch_budget_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 prefetch-budget uring-sqpoll 2)
  add_test(NAME fuse_uring_prefetch_pool_limited_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 prefetch-pool-limited uring)
  add_test(NAME fuse_multi_reactor_prefetch_pool_pressure_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 prefetch-pool-pressure uring 2)
  set_tests_properties(fuse_multi_reactor_prefetch_pool_pressure_test PROPERTIES
    TIMEOUT 60 RESOURCE_LOCK fuse_mounts)
  # The receive-pool path intentionally omits uncached checksum verification
  # and never STOREs. Keep the legacy checksum gates instead of running those
  # obsolete requirements against the new uring contract; cached verification
  # remains covered by both cached integration engine variants above.
  # Old uring connection-pressure checks expected 256 KiB windows; bounded
  # progress is now exercised by the 4 MiB uring prefetch-budget test above.
  add_test(NAME fuse_legacy_prefetch_verified_clean_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 prefetch-verified-clean legacy)
  add_test(NAME fuse_legacy_prefetch_retry_test
    COMMAND fuse_mmap_integration_test $<TARGET_FILE:ngs3fs>
            xxhash128 prefetch-verified legacy)
  set_tests_properties(fuse_uring_prefetch_pool_test
    fuse_multi_reactor_prefetch_pool_test
    fuse_sqpoll_prefetch_pool_test
    fuse_sqpoll_prefetch_shutdown_test
    fuse_sqpoll_prefetch_budget_test
    fuse_uring_prefetch_pool_limited_test
    fuse_legacy_prefetch_verified_clean_test
    fuse_legacy_prefetch_retry_test PROPERTIES TIMEOUT 60)

  # Each mounted test already exercises concurrent operations. Do not run
  # independent mounts against the same per-user kernel pipe budget at once.
  set_tests_properties(
    fuse_mmap_integration_test
    fuse_uring_integration_test
    fuse_legacy_integration_test
    fuse_sqpoll_plain_test
    fuse_sqpoll_cached_test
    fuse_multi_reactor_integration_test
    fuse_multi_reactor_cached_integration_test
    fuse_crc64nvme_integration_test
    fuse_oss_crc64_integration_test
    fuse_cached_mmap_integration_test
    fuse_uncached_read_ahead_integration_test
    fuse_uring_prefetch_pool_test
    fuse_multi_reactor_prefetch_pool_test
    fuse_sqpoll_prefetch_pool_test
    fuse_sqpoll_prefetch_shutdown_test
    fuse_sqpoll_prefetch_budget_test
    fuse_uring_prefetch_pool_limited_test
    fuse_legacy_prefetch_verified_clean_test
    fuse_legacy_prefetch_retry_test
    PROPERTIES RESOURCE_LOCK fuse_mounts)

  add_test(NAME prefetch_window_environment_test
    COMMAND ngs3fs --help)
  set_tests_properties(prefetch_window_environment_test PROPERTIES
    ENVIRONMENT "UNSTABLE_NGS3FS_MAX_PREFETCH_WINDOW_SIZE=67108864")
  add_test(NAME invalid_prefetch_window_environment_test
    COMMAND ngs3fs --io-engine legacy --help)
  set_tests_properties(invalid_prefetch_window_environment_test PROPERTIES
    ENVIRONMENT "UNSTABLE_NGS3FS_MAX_PREFETCH_WINDOW_SIZE=1048577")
  foreach(size 2097152 134217728)
    add_test(NAME uring_prefetch_window_${size}_test
      COMMAND ngs3fs --io-engine uring --help)
    set_tests_properties(uring_prefetch_window_${size}_test PROPERTIES
      ENVIRONMENT "UNSTABLE_NGS3FS_MAX_PREFETCH_WINDOW_SIZE=${size}")
  endforeach()
  foreach(size 1048576 3145728)
    add_test(NAME uring_prefetch_window_${size}_test
      COMMAND ngs3fs --io-engine uring --help)
    set_tests_properties(uring_prefetch_window_${size}_test PROPERTIES
      ENVIRONMENT "UNSTABLE_NGS3FS_MAX_PREFETCH_WINDOW_SIZE=${size}"
      WILL_FAIL TRUE)
  endforeach()
  add_test(NAME auto_prefetch_window_subblock_test
    COMMAND ngs3fs --io-engine auto --help)
  set_tests_properties(auto_prefetch_window_subblock_test PROPERTIES
    ENVIRONMENT "UNSTABLE_NGS3FS_MAX_PREFETCH_WINDOW_SIZE=1048576"
    WILL_FAIL TRUE)
  # CLI validation complements the real-mount pooled budget tests above.
  add_test(NAME max_prefetch_memory_option_test
    COMMAND ngs3fs --max-prefetch-memory 256MiB --help)
  add_test(NAME max_prefetch_memory_zero_option_test
    COMMAND ngs3fs --max-prefetch-memory 0 --help)
  add_test(NAME max_prefetch_memory_unaligned_test
    COMMAND ngs3fs --max-prefetch-memory 262145 --help)
  set_tests_properties(max_prefetch_memory_unaligned_test PROPERTIES
    WILL_FAIL TRUE)
  add_test(NAME max_prefetch_memory_too_small_test
    COMMAND ngs3fs --max-prefetch-memory 128KiB --help)
  set_tests_properties(max_prefetch_memory_too_small_test PROPERTIES
    WILL_FAIL TRUE)
  add_test(NAME max_file_prefetch_memory_option_test
    COMMAND ngs3fs --max-file-prefetch-memory 512MiB --help)
  add_test(NAME max_file_prefetch_memory_zero_option_test
    COMMAND ngs3fs --max-file-prefetch-memory 0 --help)
  add_test(NAME max_file_prefetch_memory_unaligned_test
    COMMAND ngs3fs --max-file-prefetch-memory 262145 --help)
  set_tests_properties(max_file_prefetch_memory_unaligned_test PROPERTIES
    WILL_FAIL TRUE)
  add_test(NAME max_file_prefetch_memory_too_small_test
    COMMAND ngs3fs --max-file-prefetch-memory 128KiB --help)
  set_tests_properties(max_file_prefetch_memory_too_small_test PROPERTIES
    WILL_FAIL TRUE)
  foreach(option max-prefetch-memory max-file-prefetch-memory)
    string(REPLACE "-" "_" test_option "${option}")
    add_test(NAME uring_${test_option}_minimum_test
      COMMAND ngs3fs --io-engine uring --${option} 4MiB --help)
    add_test(NAME uring_${test_option}_single_block_test
      COMMAND ngs3fs --io-engine uring --${option} 2MiB --help)
    add_test(NAME uring_${test_option}_subblock_test
      COMMAND ngs3fs --io-engine uring --${option} 1MiB --help)
    add_test(NAME uring_${test_option}_fractional_block_test
      COMMAND ngs3fs --io-engine uring --${option} 3MiB --help)
    set_tests_properties(uring_${test_option}_single_block_test
      uring_${test_option}_subblock_test
      uring_${test_option}_fractional_block_test PROPERTIES WILL_FAIL TRUE)
  endforeach()
  add_test(NAME removed_tail_read_ahead_option_test
    COMMAND ngs3fs --tail-read-ahead 64KiB --help)
  set_tests_properties(removed_tail_read_ahead_option_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME removed_read_ahead_option_test
    COMMAND ngs3fs --read-ahead 1MiB --help)
  set_tests_properties(removed_read_ahead_option_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME removed_io_size_option_test
    COMMAND ngs3fs --io-size 1MiB --help)
  set_tests_properties(removed_io_size_option_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME removed_cache_fetch_option_test
    COMMAND ngs3fs --max-cache-fetch-size 8MiB --help)
  set_tests_properties(removed_cache_fetch_option_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME socket_buffer_size_option_test
    COMMAND ngs3fs --socket-buffer-size 4MiB --help)
  add_test(NAME socket_buffer_size_auto_test
    COMMAND ngs3fs --socket-buffer-size 0 --help)
  add_test(NAME socket_buffer_size_invalid_test
    COMMAND ngs3fs --socket-buffer-size nope --help)
  set_tests_properties(socket_buffer_size_invalid_test
    PROPERTIES WILL_FAIL TRUE)
  add_test(NAME socket_buffer_size_oversized_test
    COMMAND ngs3fs --socket-buffer-size 2GiB --help)
  set_tests_properties(socket_buffer_size_oversized_test
    PROPERTIES WILL_FAIL TRUE)
  add_test(NAME checksum_options_test
    COMMAND ngs3fs --checksum xxhash128 --verify-read-checksum --help)
  add_test(NAME checksum_invalid_test
    COMMAND ngs3fs --checksum not-a-checksum --help)
  set_tests_properties(checksum_invalid_test PROPERTIES WILL_FAIL TRUE)

  add_test(NAME io_engine_options_test COMMAND ngs3fs --io-engine auto --reactors 1 --help)
  add_test(NAME io_engine_legacy_option_test COMMAND ngs3fs --io-engine legacy --help)
  add_test(NAME io_engine_uring_option_test COMMAND ngs3fs --io-engine uring --help)
  add_test(NAME io_engine_sqpoll_option_test COMMAND ngs3fs --io-engine uring-sqpoll --help)
  add_test(NAME io_engine_sqpoll_tls_test COMMAND ngs3fs --io-engine uring-sqpoll --tls --help)
  add_test(NAME io_engine_sqpoll_https_test COMMAND ngs3fs --io-engine uring-sqpoll -p 443 --help)
  set_tests_properties(io_engine_sqpoll_tls_test io_engine_sqpoll_https_test
    PROPERTIES WILL_FAIL TRUE)
  add_test(NAME io_engine_invalid_test COMMAND ngs3fs --io-engine nope --help)
  set_tests_properties(io_engine_invalid_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME reactors_invalid_test COMMAND ngs3fs --reactors nope --help)
  set_tests_properties(reactors_invalid_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME reactors_zero_test COMMAND ngs3fs --reactors 0 --help)
  set_tests_properties(reactors_zero_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME reactors_oversized_test COMMAND ngs3fs --reactors 257 --help)
  set_tests_properties(reactors_oversized_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME cache_options_test
    COMMAND ngs3fs -L /tmp/ngs3fs-option-test --cache-size 1GiB
                   --cache-reserve 5% --help)
  add_test(NAME cache_reserve_size_test
    COMMAND ngs3fs -L /tmp/ngs3fs-option-test --cache-reserve 256MiB --help)
  add_test(NAME cache_reserve_invalid_test
    COMMAND ngs3fs --cache-reserve 100% --help)
  set_tests_properties(cache_reserve_invalid_test PROPERTIES WILL_FAIL TRUE)
  add_test(NAME cache_block_size_option_test
    COMMAND ngs3fs -L /tmp/ngs3fs-option-test
            --cache-block-size 2MiB --help)
  add_test(NAME cache_block_size_minimum_test
    COMMAND ngs3fs -L /tmp/ngs3fs-option-test
            --cache-block-size 32KiB --help)
  add_test(NAME cache_block_size_maximum_test
    COMMAND ngs3fs -L /tmp/ngs3fs-option-test
            --cache-block-size 128MiB --help)
  foreach(value 0 32769 129MiB)
    string(REPLACE "MiB" "mib" test_value "${value}")
    add_test(NAME cache_block_size_${test_value}_invalid_test
      COMMAND ngs3fs -L /tmp/ngs3fs-option-test
              --cache-block-size ${value} --help)
    set_tests_properties(cache_block_size_${test_value}_invalid_test
      PROPERTIES WILL_FAIL TRUE)
  endforeach()
  add_test(NAME cache_block_size_requires_dir_test
    COMMAND ngs3fs --cache-block-size 2MiB --help)
  set_tests_properties(cache_block_size_requires_dir_test
    PROPERTIES WILL_FAIL TRUE)
  add_test(NAME cache_unlimited_option_test
    COMMAND ngs3fs -L /tmp/ngs3fs-option-test --cache-unlimited --help)
  add_test(NAME cache_unlimited_requires_dir_test
    COMMAND ngs3fs --cache-unlimited --help)
  add_test(NAME cache_unlimited_size_conflict_test
    COMMAND ngs3fs -L /tmp/ngs3fs-option-test --cache-unlimited
            --cache-size 0 --help)
  add_test(NAME cache_unlimited_reserve_conflict_test
    COMMAND ngs3fs -L /tmp/ngs3fs-option-test --cache-unlimited
            --cache-reserve 5% --help)
  set_tests_properties(
    cache_unlimited_requires_dir_test
    cache_unlimited_size_conflict_test
    cache_unlimited_reserve_conflict_test
    PROPERTIES WILL_FAIL TRUE)

  add_test(NAME hardlink_as_copy_option_test
    COMMAND ngs3fs --hardlink-as-copy --help)
  add_test(NAME hardlink_as_copy_cached_option_test
    COMMAND ngs3fs -L /tmp/ngs3fs-option-test --hardlink-as-copy --help)
  add_test(NAME hardlink_as_copy_argument_rejected_test
    COMMAND ngs3fs --hardlink-as-copy=no --help)
  set_tests_properties(hardlink_as_copy_argument_rejected_test
    PROPERTIES WILL_FAIL TRUE)

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
  add_executable(async_http_test tests/async_http_test.cpp)
  target_link_libraries(async_http_test PRIVATE ngs3fs_core)
  add_test(NAME async_http_test COMMAND async_http_test)
  set_tests_properties(async_http_test PROPERTIES TIMEOUT 10)

  add_executable(protocol_probe_timeout_test
    tests/protocol_probe_timeout_test.cpp)
  target_link_libraries(protocol_probe_timeout_test PRIVATE ngs3fs_core)
  add_test(NAME protocol_probe_timeout_test COMMAND protocol_probe_timeout_test)
  set_tests_properties(protocol_probe_timeout_test PROPERTIES TIMEOUT 5)

  add_executable(credential_process_helper
    tests/credential_process_helper.cpp)
  target_compile_options(credential_process_helper PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion)

  add_executable(credential_provider_test
    tests/credential_provider_test.cpp)
  target_link_libraries(credential_provider_test PRIVATE ngs3fs_core)
  add_dependencies(credential_provider_test credential_process_helper)
  add_test(NAME credential_provider_test
    COMMAND credential_provider_test $<TARGET_FILE:credential_process_helper>)
  set_tests_properties(credential_provider_test PROPERTIES TIMEOUT 30)

  add_test(NAME timeout_options_test
    COMMAND ngs3fs --connect-timeout 1000 --request-timeout 2000
                   --protocol-probe-timeout 100 --metadata-timeout 500
                   --stats-interval 30 --help)
  add_test(NAME timeout_options_zero_test
    COMMAND ngs3fs --request-timeout 0 --help)
  set_tests_properties(timeout_options_zero_test PROPERTIES WILL_FAIL TRUE)

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
