from __future__ import annotations

from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.dependencies import APIRuntime, RouteRuntime, SearchRuntime, StatusRuntime, StopRuntime, TripRuntime
from app.api.errors import api_error
from app.api.frontend import create_frontend_router
from app.api.gtfs import create_gtfs_router
from app.api.health import create_health_router
from app.api.map import create_map_router
from app.api.routes import create_routes_router
from app.api.search import create_search_router
from app.api.status import create_status_router
from app.api.stops import create_stops_router
from app.api.trips import create_trips_router
from app.api.vehicles import create_vehicles_router
from app.runtime import ApplicationRuntime, create_runtime


logger = logging.getLogger(__name__)


def _lifespan(runtime: ApplicationRuntime):
    @asynccontextmanager
    async def lifespan(_app):
        try:
            runtime.initialize_local_gtfs()
        except Exception:
            logger.exception("Local GTFS initialization failed")
        gtfs_started = live_started = fleet_started = False
        try:
            try:
                runtime.gtfs_refresh.start()
                gtfs_started = True
            except Exception:
                logger.exception("GTFS refresh service failed to start")
            try:
                runtime.live_refresh.start()
                live_started = True
            except Exception:
                logger.exception("Live refresh service failed to start")
            try:
                runtime.fleet_refresh.start()
                fleet_started = True
            except Exception:
                logger.exception("Fleet metadata refresh service failed to start")
            yield
        finally:
            if fleet_started:
                try:
                    runtime.fleet_refresh.stop()
                except Exception:
                    logger.exception("Fleet metadata refresh service failed to stop")
            if live_started:
                try:
                    runtime.live_refresh.stop()
                except Exception:
                    logger.exception("Live refresh service failed to stop")
            if gtfs_started:
                try:
                    runtime.gtfs_refresh.stop()
                except Exception:
                    logger.exception("GTFS refresh service failed to stop")

    return lifespan


def create_app(runtime: ApplicationRuntime | None = None) -> FastAPI:
    runtime = runtime or create_runtime()
    settings = runtime.settings
    app = FastAPI(title=settings.app_name, lifespan=_lifespan(runtime))
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.runtime = runtime
    app.state.gtfs_refresh = runtime.gtfs_refresh
    app.state.live_refresh = runtime.live_refresh
    app.state.fleet_refresh = runtime.fleet_refresh
    if settings.static_path.exists():
        app.mount("/static", StaticFiles(directory=str(settings.static_path)), name="static")

    common = APIRuntime(
        runtime.gtfs_provider,
        runtime.live_provider,
        runtime.gtfs_zip_path,
        runtime.gtfs_directory_path,
        runtime.now,
        runtime.fleet_provider,
        settings.fleet_metadata_operator_id,
    )
    routers_and_endpoints = [
        create_health_router(app_name=settings.app_name, now=runtime.now),
        create_status_router(runtime=StatusRuntime(runtime.gtfs_provider, runtime.live_provider, lambda: runtime.gtfs_refresh.snapshot(), runtime.now, settings.live_max_age_seconds, settings.live_operator_filter, settings.live_cache_ttl_seconds, lambda: runtime.fleet_refresh.snapshot())),
        create_search_router(runtime=SearchRuntime(runtime.gtfs_provider), unavailable_response=api_error),
        create_stops_router(runtime=StopRuntime(runtime.gtfs_provider, runtime.live_provider, runtime.now, settings.departure_window_minutes, settings.departure_limit, settings.live_match_minutes, runtime.fleet_provider, settings.fleet_metadata_operator_id), unavailable_response=api_error),
        create_trips_router(runtime=TripRuntime(runtime.gtfs_provider, runtime.live_provider, runtime.now, runtime.fleet_provider, settings.fleet_metadata_operator_id), unavailable_response=api_error),
        create_routes_router(runtime=RouteRuntime(runtime.gtfs_provider, runtime.live_provider, runtime.now), unavailable_response=api_error),
        create_vehicles_router(runtime=common),
        create_map_router(runtime=common, unavailable_response=api_error),
        create_gtfs_router(snapshot=runtime.gtfs_refresh.snapshot),
    ]
    endpoint_names = ["health", "status", "api_search", "api_stop_departures", "api_trip", "api_route", "api_vehicles", "api_map", "gtfs_refresh_status"]
    for name, (router, endpoint) in zip(endpoint_names, routers_and_endpoints):
        app.include_router(router)
        runtime.endpoints[name] = endpoint

    frontend_router, index, spa_fallback = create_frontend_router(
        templates_path=settings.templates_path
    )
    app.include_router(frontend_router)
    runtime.endpoints.update({"index": index, "spa_fallback": spa_fallback})
    return app
