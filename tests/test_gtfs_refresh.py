import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from app.services.gtfs_refresh import GTFSRefreshService


class GTFSRefreshValidationTests(unittest.TestCase):
    @staticmethod
    def _write_minimal_gtfs(path: Path) -> None:
        with zipfile.ZipFile(path, "w") as archive:
            for name in ("agency.txt", "routes.txt", "stops.txt", "trips.txt", "stop_times.txt"):
                archive.writestr(name, "header\n")

    def test_valid_minimal_gtfs_zip(self):
        with tempfile.TemporaryDirectory() as directory:
            archive_path = Path(directory) / "gtfs.zip"
            self._write_minimal_gtfs(archive_path)
            GTFSRefreshService.validate(archive_path)

    def test_missing_required_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            archive_path = Path(directory) / "gtfs.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("agency.txt", "header\n")
            with self.assertRaisesRegex(ValueError, "missing required files"):
                GTFSRefreshService.validate(archive_path)

    def test_refresh_downloads_validates_and_replaces_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.zip"
            target = root / "target.zip"
            self._write_minimal_gtfs(source)
            payload = source.read_bytes()
            callbacks = []
            service = GTFSRefreshService(
                source_url="https://example.invalid/gtfs.zip",
                target_path=target,
                interval_seconds=300,
                timeout_seconds=5,
                max_download_bytes=1_048_576,
                enabled=True,
                on_changed=lambda: callbacks.append(True),
            )
            response = io.BytesIO(payload)
            response.headers = {"Content-Length": str(len(payload))}
            with patch("app.services.gtfs_refresh.urllib.request.urlopen", return_value=response):
                self.assertTrue(service.refresh())
            self.assertEqual(source.read_bytes(), target.read_bytes())
            self.assertEqual(callbacks, [True])
            self.assertIsNone(service.snapshot()["last_error"])


if __name__ == "__main__":
    unittest.main()
