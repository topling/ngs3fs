include_guard(GLOBAL)

find_package(Threads REQUIRED)

set(NGS3FS_DEPS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/.deps" CACHE PATH
    "Directory containing locally bootstrapped dependencies")
set(_ngs3fs_nghttp2_default_source_dir
    "${NGS3FS_DEPS_DIR}/src/nghttp2-1.70.0")
if(NOT DEFINED NGS3FS_NGHTTP2_SOURCE_DIR OR
   NGS3FS_NGHTTP2_SOURCE_DIR STREQUAL
     "${NGS3FS_DEPS_DIR}/src/nghttp2-1.59.0")
  set(NGS3FS_NGHTTP2_SOURCE_DIR
      "${_ngs3fs_nghttp2_default_source_dir}" CACHE PATH
      "Path to the nghttp2 v1.70.0 source tree" FORCE)
else()
  set(NGS3FS_NGHTTP2_SOURCE_DIR
      "${_ngs3fs_nghttp2_default_source_dir}" CACHE PATH
      "Path to the nghttp2 v1.70.0 source tree")
endif()

if(NOT EXISTS "${NGS3FS_NGHTTP2_SOURCE_DIR}/lib/includes/nghttp2/nghttp2.h" OR
   NOT EXISTS "${NGS3FS_NGHTTP2_SOURCE_DIR}/lib/includes/nghttp2/nghttp2ver.h")
  message(FATAL_ERROR "nghttp2 is missing; run scripts/bootstrap.sh")
endif()
file(READ
  "${NGS3FS_NGHTTP2_SOURCE_DIR}/lib/includes/nghttp2/nghttp2.h"
  _nghttp2_public_header)
file(READ
  "${NGS3FS_NGHTTP2_SOURCE_DIR}/lib/includes/nghttp2/nghttp2ver.h"
  _nghttp2_version_header)
string(FIND "${_nghttp2_version_header}"
  "#define NGHTTP2_VERSION \"1.70.0\"" _nghttp2_v170)
string(FIND "${_nghttp2_public_header}"
  "nghttp2_session_mem_recv_data_external" _obsolete_external_data_api)
if(_nghttp2_v170 EQUAL -1)
  message(FATAL_ERROR "ngs3fs requires the pinned nghttp2 v1.70.0 source")
endif()
if(NOT _obsolete_external_data_api EQUAL -1)
  message(FATAL_ERROR
    "obsolete patched nghttp2 source detected; rerun scripts/bootstrap.sh")
endif()

set(ENABLE_LIB_ONLY ON CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
set(ENABLE_APP OFF CACHE BOOL "" FORCE)
set(ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
set(ENABLE_HPACK_TOOLS OFF CACHE BOOL "" FORCE)
set(ENABLE_DOC OFF CACHE BOOL "" FORCE)
set(ENABLE_PYTHON_BINDINGS OFF CACHE BOOL "" FORCE)
set(ENABLE_FAILMALLOC OFF CACHE BOOL "" FORCE)
set(WITH_JEMALLOC OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
add_subdirectory(
  "${NGS3FS_NGHTTP2_SOURCE_DIR}"
  "${CMAKE_BINARY_DIR}/_deps/nghttp2-1.70.0"
  EXCLUDE_FROM_ALL)

set(_ngs3fs_llhttp_source_dir
    "${NGS3FS_DEPS_DIR}/src/llhttp-release-v9.4.3")
if(NOT EXISTS "${_ngs3fs_llhttp_source_dir}/include/llhttp.h")
  message(FATAL_ERROR "llhttp is missing; run scripts/bootstrap.sh")
endif()
file(READ "${_ngs3fs_llhttp_source_dir}/include/llhttp.h"
  _llhttp_public_header)
string(FIND "${_llhttp_public_header}"
  "#define LLHTTP_VERSION_MAJOR 9" _llhttp_major)
string(FIND "${_llhttp_public_header}"
  "#define LLHTTP_VERSION_MINOR 4" _llhttp_minor)
string(FIND "${_llhttp_public_header}"
  "#define LLHTTP_VERSION_PATCH 3" _llhttp_patch)
if(_llhttp_major EQUAL -1 OR _llhttp_minor EQUAL -1 OR
   _llhttp_patch EQUAL -1)
  message(FATAL_ERROR "ngs3fs requires the pinned llhttp v9.4.3 source")
endif()

set(LLHTTP_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(LLHTTP_BUILD_STATIC_LIBS ON CACHE BOOL "" FORCE)
add_subdirectory(
  "${_ngs3fs_llhttp_source_dir}"
  "${CMAKE_BINARY_DIR}/_deps/llhttp-v9.4.3"
  EXCLUDE_FROM_ALL)

function(ngs3fs_find_fuse3)
  find_path(FUSE3_INCLUDE_DIR
    NAMES fuse_lowlevel.h
    HINTS
      "${NGS3FS_DEPS_DIR}/sysroot/usr/include/fuse3"
      "/usr/include/fuse3")
  find_library(FUSE3_LIBRARY
    NAMES fuse3 libfuse3.so.3 fuse3.so.3
    HINTS
      "${NGS3FS_DEPS_DIR}/sysroot/usr/lib/x86_64-linux-gnu"
      "/lib/x86_64-linux-gnu"
      "/usr/lib/x86_64-linux-gnu")
  if(NOT FUSE3_INCLUDE_DIR OR NOT FUSE3_LIBRARY)
    message(FATAL_ERROR "libfuse3 is required; run scripts/bootstrap.sh")
  endif()
  if(NOT TARGET FUSE3::fuse3)
    add_library(FUSE3::fuse3 UNKNOWN IMPORTED)
    set_target_properties(FUSE3::fuse3 PROPERTIES
      IMPORTED_LOCATION "${FUSE3_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${FUSE3_INCLUDE_DIR}")
  endif()
endfunction()
