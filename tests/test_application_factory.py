import asyncio
import importlib
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import app.main as package_main
from app.factory import create_app
from app.runtime import create_runtime
from app.services.gtfs_loader import GTFSStore
from tests.route_helpers import application_routes


KNOWN_PATHS = (
    "/health", "/api/status", "/api/search",
    "/api/stops/{stop_id}/departures", "/api/trips/{trip_id}",
    "/api/routes/{line}", "/api/vehicles", "/api/map",
    "/api/gtfs/refresh/status", "/", "/{path:path}",
)


async def asgi_get(app, path):
    messages = []
    received = False

    async def receive():
        nonlocal received
        if not received:
            received = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message):
        messages.append(message)

    scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "GET", "scheme": "http", "path": path, "raw_path": path.encode("ascii"), "query_string": b"", "headers": [(b"host", b"test")], "client": ("127.0.0.1", 1), "server": ("test", 80), "root_path": ""}
    await app(scope, receive, send)
    status = next(message["status"] for message in messages if message["type"] == "http.response.start")
    body = b"".join(message.get("body", b"") for message in messages if message["type"] == "http.response.body")
    return status, body


class ApplicationFactoryTests(unittest.TestCase):
    def test_factory_returns_distinct_apps_runtimes_and_providers(self):
        first_runtime, second_runtime = create_runtime(), create_runtime()
        first, second = create_app(first_runtime), create_app(second_runtime)
        self.assertIsInstance(first, FastAPI)
        self.assertIsNot(first, second)
        self.assertIsNot(first.state.runtime, second.state.runtime)
        self.assertIsNot(first_runtime.gtfs_provider, second_runtime.gtfs_provider)
        self.assertIsNot(first_runtime.live_provider, second_runtime.live_provider)
        self.assertEqual(first.title, "Bluestar Unilink Menetrend")
        self.assertIs(first.state.gtfs_refresh, first_runtime.gtfs_refresh)
        self.assertIs(first.state.live_refresh, first_runtime.live_refresh)
        self.assertTrue(any(item.cls is CORSMiddleware for item in first.user_middleware))

    def test_factory_does_not_start_services(self):
        runtime = create_runtime()
        with patch.object(runtime.gtfs_refresh, "start") as gtfs_start, patch.object(runtime.live_refresh, "start") as live_start:
            create_app(runtime)
        gtfs_start.assert_not_called()
        live_start.assert_not_called()

    def test_routes_are_unique_ordered_and_openapi_complete(self):
        routes = application_routes(package_main.app)
        paths = [getattr(route, "path", None) for route in routes]
        for path in KNOWN_PATHS:
            self.assertEqual(paths.count(path), 1, path)
        fallback_index = paths.index("/{path:path}")
        self.assertEqual(fallback_index, len(paths) - 1)
        self.assertLess(paths.index("/"), fallback_index)
        self.assertTrue(all(index < fallback_index for index, path in enumerate(paths) if str(path).startswith("/api/") or path == "/health"))
        for path in KNOWN_PATHS[:-2]:
            self.assertEqual(list(package_main.app.openapi()["paths"]).count(path), 1)
        self.assertEqual(paths.count("/static"), 1 if package_main.runtime.settings.static_path.exists() else 0)

    def test_lifespan_initializes_starts_and_stops_once_in_order(self):
        runtime = create_runtime()
        app = create_app(runtime)
        events = []
        with patch.object(runtime, "initialize_local_gtfs", side_effect=lambda: events.append("load")), patch.object(runtime.gtfs_refresh, "start", side_effect=lambda: events.append("gtfs-start")), patch.object(runtime.live_refresh, "start", side_effect=lambda: events.append("live-start")), patch.object(runtime.live_refresh, "stop", side_effect=lambda: events.append("live-stop")), patch.object(runtime.gtfs_refresh, "stop", side_effect=lambda: events.append("gtfs-stop")):
            async def exercise():
                async with app.router.lifespan_context(app):
                    events.append("serve")
            asyncio.run(exercise())
        self.assertEqual(events, ["load", "gtfs-start", "live-start", "serve", "live-stop", "gtfs-stop"])

    def test_local_candidate_replaces_only_when_loaded(self):
        runtime = create_runtime()
        previous = runtime.gtfs_provider.get()
        loaded = GTFSStore(); loaded.loaded = True
        unavailable = GTFSStore(); unavailable.error = "missing"
        loaded.load = lambda: loaded
        unavailable.load = lambda: unavailable
        with patch("app.runtime.GTFSStore", return_value=loaded):
            runtime.initialize_local_gtfs()
        self.assertIs(runtime.gtfs_provider.get(), loaded)
        with patch("app.runtime.GTFSStore", return_value=unavailable):
            runtime.initialize_local_gtfs()
        self.assertIs(runtime.gtfs_provider.get(), loaded)
        self.assertIsNot(runtime.gtfs_provider.get(), previous)

    def test_startup_failure_is_controlled_and_other_service_runs(self):
        runtime = create_runtime()
        app = create_app(runtime)
        with patch.object(runtime, "initialize_local_gtfs", side_effect=RuntimeError("load failed")), patch.object(runtime.gtfs_refresh, "start", side_effect=RuntimeError("start failed")), patch.object(runtime.live_refresh, "start") as live_start, patch.object(runtime.live_refresh, "stop") as live_stop:
            async def exercise():
                async with app.router.lifespan_context(app):
                    pass
            asyncio.run(exercise())
        live_start.assert_called_once_with()
        live_stop.assert_called_once_with()

    def test_root_shim_identity_and_no_reverse_import(self):
        root_main = importlib.import_module("main")
        before = len(application_routes(package_main.app))
        importlib.reload(root_main)
        self.assertIs(root_main.app, package_main.app)
        self.assertEqual(len(application_routes(package_main.app)), before)
        app_root = Path(__file__).resolve().parents[1] / "app"
        offenders = []
        for path in app_root.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if "import main" in text or "from main import" in text:
                offenders.append(path)
        self.assertEqual(offenders, [])

    def test_frontend_root_fallback_and_static_contracts(self):
        root_status, root_body = asyncio.run(asgi_get(package_main.app, "/"))
        fallback_status, fallback_body = asyncio.run(asgi_get(package_main.app, "/some/spa/path"))
        static_status, static_body = asyncio.run(asgi_get(package_main.app, "/static/app.css"))
        self.assertEqual((root_status, fallback_status, static_status), (200, 200, 200))
        self.assertEqual(root_body, fallback_body)
        self.assertTrue(root_body)
        self.assertTrue(static_body)


if __name__ == "__main__":
    unittest.main()
