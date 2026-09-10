import hashlib
import ipaddress
import time
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from fastink.auth.backends.registry import get_auth_backend
from fastink.auth.oidc import get_auth_mode
from fastink.auth.oidc.bearer import validate_bearer
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


def _parse_whitelist(entries: list) -> list:
    """Parse whitelist entries (exact IPs or CIDR networks) into ipaddress objects."""
    networks = list()
    for entry in entries:
        if "/" in entry:
            networks.append(ipaddress.ip_network(entry, strict=False))
        else:
            networks.append(ipaddress.ip_address(entry))
    return networks


def _ip_allowed(ip_obj, networks: list) -> bool:
    """True if *ip_obj* matches any parsed whitelist entry."""
    for entry in networks:
        if isinstance(entry, (ipaddress.IPv4Network, ipaddress.IPv6Network)):
            if ip_obj in entry:
                return True
        elif ip_obj == entry:
            return True
    return False


def ip_is_whitelisted(client_ip: str, entries: list) -> bool:
    """True if *client_ip* is in *entries* (exact IP or CIDR).

    Unparseable input (e.g. "testclient" or a malformed whitelist entry)
    returns False rather than raising.
    """
    try:
        ip_obj = ipaddress.ip_address(client_ip)
        networks = _parse_whitelist(entries)
    except ValueError:
        return False
    return _ip_allowed(ip_obj, networks)


class UserValidationMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, skip_routers: list = list()):
        super().__init__(app)
        self.skip_routers = skip_routers

    async def dispatch(self, request: Request, call_next):
        # only execute for API requests
        if not request.url.path.startswith("/api"):
            logger.debug("Not an API request, skip middleware")
            return await call_next(request)

        if request.url.path.startswith("/api/v3"):
            return await call_next(request)

        if _path_matches(request.url.path, self.skip_routers):
            logger.debug("Skip authentication for %s", request.url.path)
            return await call_next(request)

        mode = get_auth_mode()
        if mode in ("dual", "oidc"):
            authorization = request.headers.get("Authorization")
            if authorization is not None:
                token = authorization[7:] if authorization.startswith("Bearer ") else ""
                principal = validate_bearer(token) if token else None
                if principal is None:
                    return JSONResponse(
                        status_code=200,
                        content={
                            "status": InkStatus.USER_INVALID,
                            "msg": "Invalid username or token",
                            "data": None,
                        },
                    )
                request.state.principal = principal
                return await call_next(request)
            if mode == "oidc":
                return JSONResponse(
                    status_code=200,
                    content={
                        "status": InkStatus.USER_INVALID,
                        "msg": "Invalid username or token",
                        "data": None,
                    },
                )

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
            fingerprint = hashlib.sha256(token.encode()).hexdigest()[:12]
            logger.warning(
                "Invalid user %s with token fingerprint %s", username, fingerprint
            )
            return JSONResponse(
                status_code=200,
                content={
                    "status": InkStatus.USER_INVALID,
                    "msg": "Invalid username or token",
                    "data": None,
                },
            )

        from fastink.auth.oidc.principal import Principal
        request.state.principal = Principal(
            username=username,
            issuer="legacy",
            subject=username,
            auth_method="legacy",
        )
        return await call_next(request)


class IPWhitelistMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        ip_whitelist: list = list(),
        forbidden_routers: list = list(),
        token_bypass_routers: list = list(),
    ):
        super().__init__(app)
        self.allowed_networks = _parse_whitelist(ip_whitelist)
        self.forbidden_routers = list(forbidden_routers)
        self.token_bypass_routers = list(token_bypass_routers)
        for router in self.token_bypass_routers:
            if not _path_matches(router, self.forbidden_routers):
                logger.warning(
                    "Token bypass router %s is not IP-controlled; the "
                    "bypass will never take effect",
                    router,
                )

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
        if _ip_allowed(ip_obj, self.allowed_networks):
            return await call_next(request)

        # Non-whitelisted IPs may still pass on token_bypass_routers when
        # the request carries a VALID Ink-Username/Ink-Token pair (e.g.
        # CLI clients outside the intranet calling get_permission). The
        # token must be validated, not merely present: header presence
        # alone would let anyone bypass the whitelist with garbage
        # credentials. The queried identity must also match the validated
        # header identity, otherwise any valid account could enumerate
        # other users' permissions. Whitelisted IPs are allowed above
        # without paying the token-validation cost (krb5 validation
        # parses the ccache binary in-process).
        if _path_matches(request.url.path, self.token_bypass_routers):
            username = request.headers.get("Ink-Username")
            token = request.headers.get("Ink-Token")
            if username and token and validate_token(username, token):
                if request.query_params.get("username") == username:
                    logger.debug(
                        "IP whitelist bypassed for authenticated user %s on %s",
                        username,
                        request.url.path,
                    )
                    return await call_next(request)
                logger.warning(
                    "Bypass denied for %s on %s: queried username %r does not "
                    "match the validated identity",
                    username,
                    request.url.path,
                    request.query_params.get("username"),
                )
            else:
                logger.warning(
                    "Bypass denied on %s: missing or invalid credentials",
                    request.url.path,
                )

        return JSONResponse(
                status_code=200,
                content={
                    "status": InkStatus.IP_BANNED,
                    "msg": "IP address not allowed",
                    "data": None,
                },
            )


class TimerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as e:
            process_time = time.perf_counter() - start_time
            url = mask_url_query(str(request.url), _SENSITIVE_QUERY_KEYS)
            logger.error(
                "Exception %s | %.4fs | Request: %s %s", e, process_time, request.method, url
            )
            raise
        process_time = time.perf_counter() - start_time
        # response.headers["X-Process-Time"] = f"{process_time:.4f}"
        url = mask_url_query(str(request.url), _SENSITIVE_QUERY_KEYS)
        logger.info(
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


_SENSITIVE_QUERY_KEYS = {"password", "jwtToken"}


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
    principal = getattr(request.state, "principal", None)
    if principal is not None and getattr(principal, "username", None):
        return principal.username
    return request.headers.get("Ink-Username")


def request_authenticated(request: Request) -> bool:
    return getattr(request.state, "principal", None) is not None


def get_token(request: Request) -> str:
    principal = getattr(request.state, "principal", None)
    if principal is not None and principal.auth_method == "oidc":
        authorization = request.headers.get("Authorization", "")
        if authorization.startswith("Bearer "):
            return authorization[7:]
    return request.headers.get("Ink-Token")
