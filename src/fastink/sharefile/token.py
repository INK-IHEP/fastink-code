import posixpath
import re
import urllib.parse
from typing import Any, Dict

import jwt


FILE_SHARE_ISSUER = "inkfront-file-share"
FILE_SHARE_AUDIENCE = "fastink-file-share"
FILE_SHARE_PERMISSIONS = {"preview", "download"}
FILE_SHARE_OWNER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def validate_file_share_path(file_path: Any) -> str:
    """Return an unambiguous absolute POSIX path or reject the share."""
    if not isinstance(file_path, str) or not file_path or len(file_path) > 4096:
        raise jwt.InvalidTokenError("Invalid file-share path")
    if any(ord(char) < 32 or ord(char) == 127 for char in file_path):
        raise jwt.InvalidTokenError("Invalid file-share path")
    if "\\" in file_path or not file_path.startswith("/"):
        raise jwt.InvalidTokenError("Invalid file-share path")

    # The ASGI server already URL-decodes query parameters once.  Encoded path
    # material in a signed claim would make later decoding ambiguous and could
    # turn an apparently safe path into a traversal path.
    if urllib.parse.unquote(file_path) != file_path:
        raise jwt.InvalidTokenError("Encoded file-share paths are not allowed")

    segments = file_path.split("/")
    if any(segment in {".", ".."} for segment in segments):
        raise jwt.InvalidTokenError("File-share traversal is not allowed")
    if posixpath.normpath(file_path) != file_path:
        raise jwt.InvalidTokenError("File-share path is not canonical")
    return file_path


def validate_file_share_claims(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate security-sensitive claims after signature verification."""
    owner = payload.get("owner")
    if (
        not isinstance(owner, str)
        or owner in {".", ".."}
        or not FILE_SHARE_OWNER_PATTERN.fullmatch(owner)
    ):
        raise jwt.InvalidTokenError("Invalid file-share owner")

    permissions = payload.get("permissions")
    if (
        not isinstance(permissions, list)
        or not permissions
        or any(not isinstance(item, str) for item in permissions)
        or not set(permissions).issubset(FILE_SHARE_PERMISSIONS)
    ):
        raise jwt.InvalidTokenError("Invalid file-share permissions")

    payload["filePath"] = validate_file_share_path(payload.get("filePath"))
    return payload


def decode_file_share_token(token: str, secret_key: str) -> Dict[str, Any]:
    """Validate an InkFront file-share token and return its scoped claims."""
    payload = jwt.decode(
        token,
        key=secret_key,
        algorithms=["HS256"],
        issuer=FILE_SHARE_ISSUER,
        audience=FILE_SHARE_AUDIENCE,
        options={
            "require": [
                "exp",
                "iat",
                "iss",
                "aud",
                "filePath",
                "owner",
                "permissions",
            ],
        },
    )
    return validate_file_share_claims(payload)
