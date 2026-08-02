from fastapi.responses import JSONResponse


def api_error(text: str, status: int = 400) -> JSONResponse:
    return JSONResponse({"ok": False, "error": text}, status_code=status)
