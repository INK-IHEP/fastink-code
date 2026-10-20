from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from fastink.auth.apikey import validate_api_key
from fastink.auth.session import validate_session
from fastink.common.config import get_config
from fastink.storage.utils import DEFAULT_MAX_FILE_SIZE


class UploadSizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject oversized uploads before Starlette spools the multipart body.

    Chunked requests (no Content-Length) are rejected: the backend cannot
    bound their body without a proxy/deployment limit.
    """

    def __init__(self, app, path: str = "/api/v3/files/upload", overhead: int = 1024 * 1024):
        super().__init__(app)
        self.path = path
        self.overhead = overhead

    async def dispatch(self, request: Request, call_next):
        if request.url.path == self.path:
            try:
                limit = int(
                    get_config(
                        "storage", "max_file_size", fallback=DEFAULT_MAX_FILE_SIZE
                    )
                    or DEFAULT_MAX_FILE_SIZE
                )
            except (TypeError, ValueError):
                return JSONResponse(
                    status_code=503,
                    content={"detail": "Invalid storage.max_file_size configuration"},
                )
            if limit <= 0:
                limit = DEFAULT_MAX_FILE_SIZE
            raw_length = request.headers.get("content-length")
            if not raw_length:
                return JSONResponse(
                    status_code=411,
                    content={"detail": "Content-Length is required for uploads"},
                )
            try:
                length = int(raw_length)
            except ValueError:
                return JSONResponse(
                    status_code=413, content={"detail": "Invalid Content-Length"}
                )
            if length > limit + self.overhead:
                return JSONResponse(
                    status_code=413,
                    content={"detail": "File exceeds max_file_size"},
                )
        return await call_next(request)


class BearerAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, public_paths: list[str] | None = None):
        super().__init__(app)
        self.public_paths = public_paths or []

    async def dispatch(self, request: Request, call_next):
        if not request.url.path.startswith("/api/v3"):
            return await call_next(request)
        if request.url.path in self.public_paths:
            return await call_next(request)

        authorization = request.headers.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid Authorization header"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        try:
            token = authorization[7:].strip()
            principal = await validate_session(token)
            if principal is None:
                principal = await validate_api_key(token)
        except (RuntimeError, ValueError, KeyError):
            return JSONResponse(
                status_code=503,
                content={"detail": "Authentication service unavailable"},
            )
        if principal is None:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        request.state.principal = principal
        return await call_next(request)
