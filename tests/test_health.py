import unittest

from app.main import app, gtfs_refresh_status


class HealthTests(unittest.TestCase):
    def test_expected_routes_are_registered(self):
        paths = {route.path for route in app.routes}
        self.assertIn("/health", paths)
        self.assertIn("/api/status", paths)
        self.assertIn("/api/gtfs/refresh/status", paths)
        self.assertIn("/", paths)

    def test_refresh_status_does_not_expose_source_url(self):
        status = gtfs_refresh_status()
        self.assertIn("enabled", status)
        self.assertNotIn("source_url", status)


if __name__ == "__main__":
    unittest.main()
