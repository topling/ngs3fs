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
libfuse_version="3.18.2"
libfuse_commit="033844748010a3b8265bf1c90b9ae8ffe4cd9ca7"
libfuse_source_dir="${deps_dir}/src/fuse-${libfuse_version}"
libfuse_build_dir="${deps_dir}/build/fuse-${libfuse_version}"
libfuse_prefix="${deps_dir}/fuse-${libfuse_version}"
libfuse_patch="${project_dir}/scripts/libfuse-3.18.2-async-reply.patch"
libfuse_stamp="${libfuse_prefix}/.ngs3fs-libfuse-patch"
libfuse_config_stamp="${libfuse_prefix}/.ngs3fs-libfuse-config"
libfuse_config="classic-async-v1;static;uring=false"
meson_bin=${MESON:-}
if [[ -z "${meson_bin}" ]]; then
  meson_bin=$(command -v meson || true)
fi
if [[ -z "${meson_bin}" && -x "${HOME}/.local/bin/meson" ]]; then
  meson_bin="${HOME}/.local/bin/meson"
fi
if [[ -z "${meson_bin}" ]]; then
  echo "meson is required to build the pinned libfuse" >&2
  exit 1
fi

mkdir -p "${deps_dir}/src" "${deps_dir}/build"

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

if [[ ! -d "${libfuse_source_dir}/.git" ]]; then
  git clone --no-checkout https://github.com/libfuse/libfuse.git \
    "${libfuse_source_dir}"
fi
git -C "${libfuse_source_dir}" fetch --quiet origin "${libfuse_commit}"
git -C "${libfuse_source_dir}" checkout --detach --quiet "${libfuse_commit}"
if [[ ! -f "${libfuse_stamp}" ]] ||
    ! cmp --silent "${libfuse_patch}" "${libfuse_stamp}" ||
    [[ ! -f "${libfuse_config_stamp}" ]] ||
    [[ "$(<"${libfuse_config_stamp}")" != "${libfuse_config}" ]] ||
    [[ ! -f "${libfuse_prefix}/include/fuse3/fuse_lowlevel.h" ]] ||
    [[ ! -f "${libfuse_prefix}/lib/libfuse3.a" ]]; then
  git -C "${libfuse_source_dir}" restore --source="${libfuse_commit}" \
    --staged --worktree -- .
  git -C "${libfuse_source_dir}" apply --check "${libfuse_patch}"
  git -C "${libfuse_source_dir}" apply "${libfuse_patch}"
  if [[ -f "${libfuse_build_dir}/meson-private/coredata.dat" ]]; then
    "${meson_bin}" setup --wipe "${libfuse_build_dir}" \
      "${libfuse_source_dir}" \
      --prefix "${libfuse_prefix}" --libdir lib --buildtype release \
      --default-library static \
      -Dtests=false -Dexamples=false -Dutils=false -Denable-io-uring=false
  else
    "${meson_bin}" setup "${libfuse_build_dir}" \
      "${libfuse_source_dir}" \
      --prefix "${libfuse_prefix}" --libdir lib --buildtype release \
      --default-library static \
      -Dtests=false -Dexamples=false -Dutils=false -Denable-io-uring=false
  fi
  "${meson_bin}" compile -C "${libfuse_build_dir}"
  "${meson_bin}" install -C "${libfuse_build_dir}"
  cp "${libfuse_patch}" "${libfuse_stamp}"
  printf '%s\n' "${libfuse_config}" >"${libfuse_config_stamp}"
fi
cmake --preset "$preset"
cmake --build --preset "$preset"
ctest --preset "$preset"
