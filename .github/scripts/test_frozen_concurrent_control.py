import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from frozen_concurrent_control import verify


class FrozenInventoryTests(unittest.TestCase):
    def test_verifies_content_and_rejects_same_length_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = root / "image.dll"
            payload.write_bytes(b"original")
            manifest = {"commit": "frozen", "files": [{"path": "image.dll", "length": 8,
                        "sha256": hashlib.sha256(payload.read_bytes()).hexdigest()}]}
            (root / "inventory-sha256.json").write_text(json.dumps(manifest))
            self.assertEqual(verify(root), "frozen")
            payload.write_bytes(b"modified")
            with self.assertRaisesRegex(ValueError, "Retained input changed"):
                verify(root)

    def test_rejects_parent_path_before_reading_outside_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "inventory-sha256.json").write_text(json.dumps({"files": [
                {"path": "../outside", "length": 0, "sha256": ""}]}))
            with self.assertRaisesRegex(ValueError, "escapes archive"):
                verify(root)

    def test_missing_executable_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "inventory-sha256.json").write_text(json.dumps({"files": [
                {"path": "missing.dll", "length": 0, "sha256": ""}]}))
            with self.assertRaises(FileNotFoundError):
                verify(root)


if __name__ == "__main__":
    unittest.main()
