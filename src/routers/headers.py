import importlib
import ipaddress
import time
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from src.common.config import get_config
from src.common.logger import logger
from src.routers.status import InkStatus


class UserValidationMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, skip_routers: list = list()):
        super().__init__(app)
        self.skip_routers = skip_routers

    async def dispatch(self, request: Request, call_next):
        # only execute for API requests
        if not request.url.path.startswith("/api"):
            logger.debug(f"Not an API request, skip middleware")
            return await call_next(request)

        if request.url.path.startswith(tuple(self.skip_routers)):
            logger.debug(f"Skip authentication for {request.url.path}")
            return await call_next(request)

        # extract username and token from headers
        username = request.headers.get("Ink-Username")
        token = request.headers.get("Ink-Token")

        # TODO: option-in in next version
        if not username or not token:
            logger.warning("No username or token provided")
            return JSONResponse(
                status_code=401,
                content={
                    "status": InkStatus.TOKEN_INVALID,
                    "msg": "Ink-Username or Ink-Token is missing in request headers",
                    "data": None,
                },
            )

        # request.state.username = username
        # request.state.token = token

        # validate user
        if not validate_token(username, token):
            logger.warning(f"Invalid user {username} with token {token}")
            return JSONResponse(
                status_code=403,
                content={
                    "status": InkStatus.USER_INVALID,
                    "msg": "Invalid username or token",
                    "data": None,
                },
            )
        return await call_next(request)


class IPWhitelistMiddleware(BaseHTTPMiddleware):
    def __init__(
        self, app, ip_whitelist: list = list(), forbidden_routers: list = list()
    ):
        super().__init__(app)
        self.allowed_networks = list()
        self.forbidden_routers = tuple(forbidden_routers)
        for entry in ip_whitelist:
            if "/" in entry:
                self.allowed_networks.append(ipaddress.ip_network(entry, strict=False))
            else:
                self.allowed_networks.append(ipaddress.ip_address(entry))

    async def dispatch(self, request: Request, call_next):
        # only works on skip_routers
        if not request.url.path.startswith(self.forbidden_routers):
            logger.debug(f"IP whitelist will not be applied to {request.url.path}")
            return await call_next(request)

        # get client ip
        client_ip = request.headers.get("X-Real-IP") or request.client.host
        logger.debug(f"client ip: {client_ip}")

        # skip testclient
        if client_ip == "testclient":
            return await call_next(request)

        ip_obj = ipaddress.ip_address(client_ip)

        allowed = False
        for entry in self.allowed_networks:
            if isinstance(entry, ipaddress.IPv4Network) or isinstance(
                entry, ipaddress.IPv6Network
            ):
                if ip_obj in entry:
                    allowed = True
                    break
            elif ip_obj == entry:
                allowed = True
                break

        if not allowed:
            return JSONResponse(
                status_code=403,
                content={
                    "status": InkStatus.IP_BANNED,
                    "msg": "IP address not allowed",
                    "data": None,
                },
            )

        return await call_next(request)


class TimerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as e:
            process_time = time.perf_counter() - start_time
            logger.error(
                f"Exception {e} | {process_time:.4f}s | Request: {request.method} {request.url.path}"
            )
            raise
        process_time = time.perf_counter() - start_time
        # response.headers["X-Process-Time"] = f"{process_time:.4f}"
        logger.debug(
            f"{process_time:.4f}s | Request: {request.method} {response.status_code} {request.url}"
        )
        return response


def validate_token(username: str, token: str) -> bool:
    issuer = get_config("auth", "issuer")
    client_id = get_config("auth", "client_id")
    client_secret = get_config("auth", "client_secret")
    type = get_config("auth", "type")
    logger.debug(f"Validating user {username}, issuer {issuer}, type {type}")
    # using plugins
    logger.debug(f"validating user {username} with {type} plugin")
    plugin = importlib.import_module(f"src.auth.plugins.{type}")
    try:
        if plugin.validate_token(
            username=username,
            token=token,
            client_id=client_id,
            client_secret=client_secret,
            issuer=issuer,
        ):
            return True
    except Exception as e:
        logger.error(f"User validation failed: {e}")
        return False


def get_username(request: Request) -> str:
    # return request.state.username
    return request.headers.get("Ink-Username")


def get_token(request: Request) -> str:
    # return request.state.token
    return request.headers.get("Ink-Token")
