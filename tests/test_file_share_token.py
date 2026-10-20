from datetime import datetime, timedelta, timezone

import jwt
import pytest

from fastink.sharefile.token import decode_file_share_token, validate_file_share_path


SECRET = "a" * 64


def _token(**overrides):
    now = datetime.now(timezone.utc)
    payload = {
        "iat": now,
        "exp": now + timedelta(minutes=30),
        "iss": "inkfront-file-share",
        "aud": "fastink-file-share",
        "filePath": "/home/user/file.txt",
        "owner": "user",
        "permissions": ["download"],
        **overrides,
    }
    return jwt.encode(payload, SECRET, algorithm="HS256")


def test_decode_file_share_token_accepts_scoped_token():
    payload = decode_file_share_token(_token(), SECRET)
    assert payload["filePath"] == "/home/user/file.txt"
    assert payload["permissions"] == ["download"]


def test_decode_file_share_token_rejects_wrong_secret():
    with pytest.raises(jwt.InvalidSignatureError):
        decode_file_share_token(_token(), "b" * 64)


def test_decode_file_share_token_requires_permissions():
    now = datetime.now(timezone.utc)
    token = jwt.encode({
        "iat": now,
        "exp": now + timedelta(minutes=30),
        "iss": "inkfront-file-share",
        "aud": "fastink-file-share",
        "filePath": "/home/user/file.txt",
        "owner": "user",
    }, SECRET, algorithm="HS256")
    with pytest.raises(jwt.MissingRequiredClaimError):
        decode_file_share_token(token, SECRET)


@pytest.mark.parametrize(
    "file_path",
    [
        "home/user/file.txt",
        "/home/user/../admin/secret.txt",
        "/home/user/%2e%2e/admin/secret.txt",
        "/home/user/%252e%252e/admin/secret.txt",
        "/home//user/file.txt",
        "/home/user/file.txt\x00ignored",
        "\\home\\user\\file.txt",
    ],
)
def test_decode_file_share_token_rejects_ambiguous_paths(file_path):
    with pytest.raises(jwt.InvalidTokenError):
        decode_file_share_token(_token(filePath=file_path), SECRET)


@pytest.mark.parametrize("owner", ["", "../admin", "user/name", "user name"])
def test_decode_file_share_token_rejects_invalid_owner(owner):
    with pytest.raises(jwt.InvalidTokenError):
        decode_file_share_token(_token(owner=owner), SECRET)


@pytest.mark.parametrize(
    "permissions",
    [[], ["delete"], "download", ["download", 1]],
)
def test_decode_file_share_token_rejects_invalid_permissions(permissions):
    with pytest.raises(jwt.InvalidTokenError):
        decode_file_share_token(_token(permissions=permissions), SECRET)


@pytest.mark.parametrize(
    "file_path",
    [
        "/home/user/file.txt",
        "/home/user/file with spaces.txt",
        "/home/user/中文文件.txt",
    ],
)
def test_validate_file_share_path_accepts_canonical_absolute_path(file_path):
    assert validate_file_share_path(file_path) == file_path
