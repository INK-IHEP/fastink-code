from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from fastink.auth.oidc.bearer import validate_bearer


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
            )

        principal = validate_bearer(authorization[7:])
        if principal is None:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"},
            )

        request.state.principal = principal
        return await call_next(request)
