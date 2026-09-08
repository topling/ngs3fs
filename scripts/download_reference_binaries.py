#!/usr/bin/env python3
"""Download official x86_64 release binaries used by CI reference benchmarks."""

import hashlib
import json
import os
import pathlib
import platform
import re
import shutil
import stat
import sys
import tarfile
import urllib.request


MOUNTPOINT_API = "https://api.github.com/repos/awslabs/mountpoint-s3/releases/latest"
GOOFYS_API = "https://api.github.com/repos/kahing/goofys/releases/latest"
HTTP_TIMEOUT = 60
PROCESS_TIMEOUT = 30


def get_json(url):
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "ngs3fs-ci"}
    if os.environ.get("GH_TOKEN"):
        headers["Authorization"] = f"Bearer {os.environ['GH_TOKEN']}"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
        return json.load(response)


def download(url, path):
    request = urllib.request.Request(url, headers={"User-Agent": "ngs3fs-ci"})
    digest = hashlib.sha256()
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response, path.open("wb") as output:
        while chunk := response.read(1024 * 1024):
            digest.update(chunk)
            output.write(chunk)
    return digest.hexdigest()


def verify_digest(computed, official_digest):
    if not official_digest:
        return False
    if not official_digest.startswith("sha256:"):
        raise ValueError(f"unsupported official digest: {official_digest!r}")
    expected = official_digest.removeprefix("sha256:")
    if computed != expected:
        raise ValueError(f"official SHA256 mismatch: expected {expected}, got {computed}")
    return True


def parse_mountpoint_release(release):
    tag = release.get("tag_name", "")
    if not tag.startswith("mountpoint-s3-"):
        raise ValueError(f"unexpected Mountpoint release tag: {tag!r}")
    version = tag.removeprefix("mountpoint-s3-")
    urls = set(re.findall(r"https://[^\s)>]+", release.get("body", "")))
    candidates = [
        url.rstrip(".,") for url in urls
        if url.startswith("https://s3.amazonaws.com/mountpoint-s3-release/")
        and "/x86_64/" in url
        and url.endswith(".tar.gz")
        and f"mount-s3-{version}-x86_64.tar.gz" in url
    ]
    if len(candidates) != 1:
        raise ValueError(f"expected one official x86_64 Mountpoint tarball for {tag}, found {len(candidates)}")
    return {"tag": tag, "version": version, "url": candidates[0], "signature_url": candidates[0] + ".asc"}


def extract_mount_binary(archive_path, output_path):
    with tarfile.open(archive_path, "r:gz") as archive:
        matches = [
            item for item in archive.getmembers()
            if pathlib.PurePosixPath(item.name).name == "mount-s3"
        ]
        if len(matches) != 1 or not matches[0].isfile():
            raise ValueError("Mountpoint archive must contain exactly one regular mount-s3 file")
        source = archive.extractfile(matches[0])
        if source is None:
            raise ValueError("unable to read Mountpoint mount-s3 file")
        with source, output_path.open("wb") as destination:
            shutil.copyfileobj(source, destination)
    output_path.chmod(output_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def version(path):
    import subprocess

    result = subprocess.run([str(path), "--version"], check=True, capture_output=True, text=True, timeout=PROCESS_TIMEOUT)
    return result.stdout.strip() or result.stderr.strip()


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: download_reference_binaries.py OUTPUT_DIR MANIFEST")
    if platform.machine() not in ("x86_64", "amd64"):
        raise SystemExit(f"unsupported runner architecture: {platform.machine()} (x86_64 required)")
    output_dir = pathlib.Path(sys.argv[1])
    manifest_path = pathlib.Path(sys.argv[2])
    output_dir.mkdir(parents=True, exist_ok=True)
    mount_release = get_json(MOUNTPOINT_API)
    mount = parse_mountpoint_release(mount_release)
    goofys_release = get_json(GOOFYS_API)
    goofys_tag = goofys_release.get("tag_name", "")
    goofys_assets = goofys_release.get("assets", [])
    goofys_asset = next((asset for asset in goofys_assets if asset.get("name") == "goofys"), None)
    if goofys_asset is None:
        raise SystemExit(f"latest goofys release {goofys_tag!r} has no official goofys binary asset")
    goofys_url = goofys_asset["browser_download_url"]
    temp_mount = output_dir / "mount-s3.tar.gz"
    mount_sha256 = download(mount["url"], temp_mount)
    mount_verified = False
    mount_binary = output_dir / "mount-s3"
    extract_mount_binary(temp_mount, mount_binary)
    temp_mount.unlink()
    goofys_binary = output_dir / "goofys"
    goofys_sha256 = download(goofys_url, goofys_binary)
    goofys_verified = verify_digest(goofys_sha256, goofys_asset.get("digest"))
    goofys_binary.chmod(goofys_binary.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    manifest = {
        "architecture": "x86_64",
        "mountpoint": {**mount, "archive_sha256_computed": mount_sha256, "executable_sha256_computed": hashlib.sha256(mount_binary.read_bytes()).hexdigest(), "sha256_official": None, "sha256_verified": mount_verified, "signature_verified": False, "version_output": version(mount_binary)},
        "goofys": {"tag": goofys_tag, "version": goofys_release.get("name") or goofys_tag, "url": goofys_url, "sha256_official": goofys_asset.get("digest"), "sha256_computed": goofys_sha256, "sha256_verified": goofys_verified, "version_output": version(goofys_binary)},
        "digest_note": "sha256_verified is true only when the official release metadata supplied a matching sha256 digest; signature URLs are recorded but not treated as verified signatures.",
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
