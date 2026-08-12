import ipaddress
import time
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from fastink.auth.backends.registry import get_auth_backend
from fastink.common.config import get_config
from fastink.common.logger import logger
from fastink.routers.status import InkStatus


def _path_matches(path: str, patterns: list) -> bool:
    """Match a request path against a list of router patterns.

    Two matching modes, distinguished by a trailing slash so that the two
    security config lists (skip_routers, ip_controlled_routers) share one
    predictable semantics:

      - pattern ending with "/"  -> PREFIX match. "/api/v1/" matches
        "/api/v1/foo" and also the bare "/api/v1".
      - pattern without trailing "/" -> EXACT match. "/api/v2/auth/get_token"
        matches only that exact path, never "/api/v2/auth/get_token_x" or
        "/api/v2/auth/get_token/y".
    """
    for pattern in patterns:
        if pattern.endswith("/"):
            # prefix match; also treat the bare path (pattern minus the
            # trailing slash) as a match so "/api/v1/" covers "/api/v1"
            if path.startswith(pattern) or path == pattern.rstrip("/"):
                return True
        else:
            if path == pattern:
                return True
    return False


class UserValidationMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, skip_routers: list = list()):
        super().__init__(app)
        self.skip_routers = skip_routers

    async def dispatch(self, request: Request, call_next):
        # only execute for API requests
        if not request.url.path.startswith("/api"):
            logger.debug("Not an API request, skip middleware")
            return await call_next(request)

        if _path_matches(request.url.path, self.skip_routers):
            logger.debug("Skip authentication for %s", request.url.path)
            return await call_next(request)

        # extract username and token from headers
        username = request.headers.get("Ink-Username")
        token = request.headers.get("Ink-Token")

        # TODO: option-in in next version
        if not username or not token:
            logger.warning("No username or token provided")
            return JSONResponse(
                status_code=200,
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
            logger.warning("Invalid user %s with token %s", username, token)
            return JSONResponse(
                status_code=200,
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
        self.forbidden_routers = list(forbidden_routers)
        for entry in ip_whitelist:
            if "/" in entry:
                self.allowed_networks.append(ipaddress.ip_network(entry, strict=False))
            else:
                self.allowed_networks.append(ipaddress.ip_address(entry))

    async def dispatch(self, request: Request, call_next):
        # only works on ip_controlled_routers
        if not _path_matches(request.url.path, self.forbidden_routers):
            logger.debug("IP whitelist will not be applied to %s", request.url.path)
            return await call_next(request)

        # get client ip
        client_ip = request.headers.get("X-Real-IP") or request.client.host
        logger.debug("client ip: %s", client_ip)

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
                status_code=200,
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
            url = mask_url_query(str(request.url), {"password"})
            logger.error(
                "Exception %s | %.4fs | Request: %s %s", e, process_time, request.method, url
            )
            raise
        process_time = time.perf_counter() - start_time
        # response.headers["X-Process-Time"] = f"{process_time:.4f}"
        url = mask_url_query(str(request.url), {"password"})
        logger.debug(
            "%.4fs | Request: %s %s %s", process_time, request.method, response.status_code, url
        )
        return response


def validate_token(username: str, token: str) -> bool:
    issuer = get_config("auth", "issuer")
    client_id = get_config("auth", "client_id")
    client_secret = get_config("auth", "client_secret")
    type = get_config("auth", "type")
    logger.debug("Validating user %s, issuer %s, type %s", username, issuer, type)
    try:
        backend = get_auth_backend(type)
        if backend.validate_token(
            username=username,
            token=token,
            client_id=client_id,
            client_secret=client_secret,
            issuer=issuer,
        ):
            return True
    except Exception as e:
        logger.error("User validation failed: %s", e)
        return False


def mask_url_query(url: str, sensitive_keys: set[str]) -> str:
    parsed = urlparse(url)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)

    masked_pairs = list()
    for k, v in query_pairs:
        if k in sensitive_keys and v:
            masked_pairs.append((k, "*" * len(v)))
        else:
            masked_pairs.append((k, v))
    new_query = urlencode(masked_pairs)

    return urlunparse(parsed._replace(query=new_query))


def get_username(request: Request) -> str:
    # return request.state.username
    return request.headers.get("Ink-Username")


def get_token(request: Request) -> str:
    # return request.state.token
    return request.headers.get("Ink-Token")
