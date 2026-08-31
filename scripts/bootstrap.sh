#!/usr/bin/env bash
set -euo pipefail

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
preset=${NGS3FS_PRESET:-dev}
deps_dir="${project_dir}/.deps"
nghttp2_version="1.70.0"
source_dir="${deps_dir}/src/nghttp2-${nghttp2_version}"
archive="${deps_dir}/nghttp2-${nghttp2_version}.tar.xz"
archive_url="https://github.com/nghttp2/nghttp2/releases/download/v${nghttp2_version}/nghttp2-${nghttp2_version}.tar.xz"
archive_sha256="e05cb1388eaca3830aded4ccf20044b6e1ac1a61411dcca11b0437c4285c8bc2"
llhttp_version="9.4.3"
llhttp_source_dir="${deps_dir}/src/llhttp-release-v${llhttp_version}"
llhttp_archive="${deps_dir}/llhttp-v${llhttp_version}.tar.gz"
llhttp_archive_url="https://github.com/nodejs/llhttp/archive/refs/tags/release/v${llhttp_version}.tar.gz"
llhttp_archive_sha256="1eb813c7437b31a87496a1cd3ed79f00746720f5e7e29c79b42c02cb69f36c39"

mkdir -p "${deps_dir}/src" "${deps_dir}/debs" "${deps_dir}/sysroot"

if [[ ! -f "${archive}" ]]; then
  curl --fail --location "${archive_url}" --output "${archive}"
fi

printf '%s  %s\n' "${archive_sha256}" "${archive}" | sha256sum --check --status

if [[ ! -d "${source_dir}" ]]; then
  tar -xJf "${archive}" -C "${deps_dir}/src"
fi

if grep -q nghttp2_session_mem_recv_data_external \
    "${source_dir}/lib/includes/nghttp2/nghttp2.h"; then
  echo "restoring stock nghttp2 ${nghttp2_version} over the obsolete patched tree"
  tar -xJf "${archive}" -C "${deps_dir}/src"
fi

if [[ ! -f "${llhttp_archive}" ]]; then
  curl --fail --location "${llhttp_archive_url}" --output "${llhttp_archive}"
fi

printf '%s  %s\n' "${llhttp_archive_sha256}" "${llhttp_archive}" | \
  sha256sum --check --status

if [[ ! -d "${llhttp_source_dir}" ]]; then
  tar -xzf "${llhttp_archive}" -C "${deps_dir}/src"
fi

if [[ ! -f "${deps_dir}/sysroot/usr/include/fuse3/fuse_lowlevel.h" ]]; then
  (
    cd "${deps_dir}/debs"
    apt-get download libfuse3-dev libfuse3-3
  )
  while IFS= read -r package; do
    dpkg-deb -x "${package}" "${deps_dir}/sysroot"
  done < <(find "${deps_dir}/debs" -maxdepth 1 -type f -name 'libfuse3*.deb' -print)
fi

cmake --preset "$preset"
cmake --build --preset "$preset"
ctest --preset "$preset"
