import hashlib
import importlib.util
import json
import io
import pathlib
import sys
import tarfile
import tempfile
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "download_reference_binaries.py"
SPEC = importlib.util.spec_from_file_location("download_reference_binaries", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class DownloadReferenceBinariesTest(unittest.TestCase):
    def test_mountpoint_url_is_selected_from_release_body(self):
        release = {
            "tag_name": "mountpoint-s3-1.24.0",
            "body": "* tar.gz: https://s3.amazonaws.com/mountpoint-s3-release/1.24.0/x86_64/mount-s3-1.24.0-x86_64.tar.gz\n"
            "* arm: https://s3.amazonaws.com/mountpoint-s3-release/1.24.0/arm64/mount-s3-1.24.0-arm64.tar.gz",
        }
        parsed = MODULE.parse_mountpoint_release(release)
        self.assertEqual(parsed["version"], "1.24.0")
        self.assertIn("/x86_64/", parsed["url"])

    def test_mountpoint_url_must_be_unique(self):
        release = {"tag_name": "mountpoint-s3-1.24.0", "body": "https://mirror.example/mountpoint-s3-release/1.24.0/x86_64/mount-s3-1.24.0-x86_64.tar.gz"}
        with self.assertRaises(ValueError):
            MODULE.parse_mountpoint_release(release)

    def test_extract_rejects_non_regular_mount_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            archive_path = pathlib.Path(directory) / "mount.tar.gz"
            with tarfile.open(archive_path, "w:gz") as archive:
                info = tarfile.TarInfo("nested/mount-s3")
                info.type = tarfile.SYMTYPE
                info.linkname = "other"
                archive.addfile(info)
            with self.assertRaises(ValueError):
                MODULE.extract_mount_binary(archive_path, pathlib.Path(directory) / "mount-s3")

    def test_extract_copies_only_regular_mount_binary(self):
        payload = b"mock mountpoint"
        with tempfile.TemporaryDirectory() as directory:
            archive_path = pathlib.Path(directory) / "mount.tar.gz"
            output_path = pathlib.Path(directory) / "mount-s3"
            with tarfile.open(archive_path, "w:gz") as archive:
                info = tarfile.TarInfo("nested/mount-s3")
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
            MODULE.extract_mount_binary(archive_path, output_path)
            self.assertEqual(output_path.read_bytes(), payload)

    def test_digest_is_verified_only_when_official_digest_matches(self):
        digest = hashlib.sha256(b"mock").hexdigest()
        self.assertTrue(MODULE.verify_digest(digest, "sha256:" + digest))
        self.assertFalse(MODULE.verify_digest(digest, None))
        with self.assertRaises(ValueError):
            MODULE.verify_digest(digest, "sha256:" + ("0" * 64))

    def test_main_is_fully_mocked_and_writes_manifest(self):
        mount_url = "https://s3.amazonaws.com/mountpoint-s3-release/1.24.0/x86_64/mount-s3-1.24.0-x86_64.tar.gz"
        mount_payload = io.BytesIO()
        with tarfile.open(fileobj=mount_payload, mode="w:gz") as archive:
            payload = b"mock mount binary"
            info = tarfile.TarInfo("mount-s3")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
        mount_archive = mount_payload.getvalue()
        goofys_payload = b"mock goofys binary"
        goofys_digest = "sha256:" + hashlib.sha256(goofys_payload).hexdigest()
        releases = {
            MODULE.MOUNTPOINT_API: {
                "tag_name": "mountpoint-s3-1.24.0",
                "body": f"tar.gz: {mount_url}",
            },
            MODULE.GOOFYS_API: {
                "tag_name": "v0.24.0",
                "name": "v0.24.0",
                "assets": [{"name": "goofys", "browser_download_url": "https://github.com/kahing/goofys/releases/download/v0.24.0/goofys", "digest": goofys_digest}],
            },
        }

        def fake_download(url, path):
            payload = mount_archive if url == mount_url else goofys_payload
            path.write_bytes(payload)
            return hashlib.sha256(payload).hexdigest()

        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "tools"
            manifest_path = output / "reference-binaries.json"
            with mock.patch.object(MODULE, "get_json", side_effect=lambda url: releases[url]), \
                 mock.patch.object(MODULE.platform, "machine", return_value="x86_64"), \
                 mock.patch.object(MODULE, "download", side_effect=fake_download), \
                 mock.patch.object(MODULE, "version", side_effect=lambda path: f"{path.name} mock-version"), \
                 mock.patch.object(sys, "argv", ["download_reference_binaries.py", str(output), str(manifest_path)]):
                MODULE.main()
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["mountpoint"]["archive_sha256_computed"], hashlib.sha256(mount_archive).hexdigest())
            self.assertEqual(manifest["mountpoint"]["executable_sha256_computed"], hashlib.sha256(b"mock mount binary").hexdigest())
            self.assertFalse(manifest["mountpoint"]["signature_verified"])
            self.assertTrue(manifest["goofys"]["sha256_verified"])
            self.assertEqual((output / "mount-s3").read_bytes(), b"mock mount binary")
            self.assertEqual((output / "goofys").read_bytes(), goofys_payload)

    def test_unsupported_architecture_is_rejected_before_network(self):
        with mock.patch.object(MODULE.platform, "machine", return_value="aarch64"), \
             mock.patch.object(MODULE, "get_json") as request, \
             mock.patch.object(sys, "argv", ["download_reference_binaries.py", "unused", "unused.json"]):
            with self.assertRaisesRegex(SystemExit, "unsupported runner architecture: aarch64"):
                MODULE.main()
            request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
