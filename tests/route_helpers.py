def application_routes(app):
    """Flatten FastAPI include_router groups while preserving registration order."""
    flattened = []
    for route in app.routes:
        original_router = getattr(route, "original_router", None)
        if original_router is None:
            flattened.append(route)
        else:
            flattened.extend(original_router.routes)
    return flattened
