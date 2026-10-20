import asyncio
import os
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request

from fastink.auth.principal import Principal
from fastink.routers.v3 import files as files_router
from fastink.routers.v3.middleware import BearerAuthMiddleware, UploadSizeLimitMiddleware
from fastink.storage.utils import PathType


def _principal(scopes=("files:read", "files:write")):
    return Principal(
        username="alice",
        issuer="session",
        subject="alice",
        auth_method="krb5",
        scopes=list(scopes),
    )


def _app():
    app = FastAPI()
    app.add_middleware(BearerAuthMiddleware, public_paths=[])
    app.include_router(files_router.router, prefix="/api/v3")
    return app


def _request(method, url, principal=None, **kwargs):
    async def fake_session(token):
        return principal

    with patch("fastink.routers.v3.middleware.validate_session", fake_session):
        return getattr(TestClient(_app()), method)(
            url, headers={"Authorization": "Bearer test"}, **kwargs
        )


def test_upload_streams_to_path():
    content = b"echo hello\n"
    seen = {}

    async def path_exist(name, username="", mgm=None):
        if name == "/tmp/x":
            return True, PathType.DIR
        return False, PathType.UNKNOWN

    async def mkdir(**kwargs):
        return True

    async def upload_from_path(src_path, dst, username="", mgm=None, overwrite=True):
        with open(src_path, "rb") as handle:
            seen["content"] = handle.read()
        seen["src"] = src_path
        seen["dst"] = dst
        seen["overwrite"] = overwrite

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router.storage, "mkdir", mkdir
    ), patch.object(
        files_router.storage, "upload_file_from_path", upload_from_path
    ):
        response = _request(
            "post",
            "/api/v3/files/upload",
            _principal(),
            data={"upload_dir": "/tmp/x"},
            files={"file": ("job.sh", content, "text/plain")},
        )

    assert response.status_code == 201
    assert response.json() == {"path": "/tmp/x/job.sh", "bytes": len(content), "uploaded": True}
    assert seen["content"] == content
    assert seen["dst"] == "/tmp/x/job.sh"
    assert seen["overwrite"] is False
    assert not os.path.exists(seen["src"])


def test_upload_rejects_traversal():
    called = []

    async def upload_from_path(**kwargs):
        called.append(kwargs)

    with patch.object(files_router.storage, "upload_file_from_path", upload_from_path):
        response = _request(
            "post",
            "/api/v3/files/upload",
            _principal(),
            data={"upload_dir": "/tmp/x"},
            files={"file": ("../evil", b"x", "text/plain")},
        )

    assert response.status_code == 400
    assert called == []


def test_upload_refuses_overwrite_by_default():
    async def path_exist(name, username="", mgm=None):
        if name == "/tmp/x":
            return True, PathType.DIR
        return True, PathType.FILE

    with patch.object(files_router.storage, "path_exist", path_exist):
        response = _request(
            "post",
            "/api/v3/files/upload",
            _principal(),
            data={"upload_dir": "/tmp/x"},
            files={"file": ("job.sh", b"new", "text/plain")},
        )

    assert response.status_code == 409


def test_unquote_expand_user_strips_quotes_with_url():
    from fastink.storage.utils import unquote_expand_user

    assert (
        unquote_expand_user("'/tmp/x'", "alice", url=True)
        == "/tmp/x"
    )


def test_upload_rejects_quote_only_dir():
    response = _request(
        "post",
        "/api/v3/files/upload",
        _principal(),
        data={"upload_dir": '"'},
        files={"file": ("job.sh", b"x", "text/plain")},
    )
    assert response.status_code == 400


def test_download_rejects_quote_only_path():
    response = _request(
        "get",
        "/api/v3/files/download",
        _principal(),
        params={"path": '"'},
    )
    assert response.status_code == 400


def test_upload_maps_storage_exists_race_to_409():
    async def path_exist(name, username="", mgm=None):
        if name == "/tmp/x":
            return True, PathType.DIR
        return False, PathType.UNKNOWN

    async def upload_from_path(**kwargs):
        raise FileExistsError("exists")

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router.storage, "upload_file_from_path", upload_from_path
    ):
        response = _request(
            "post",
            "/api/v3/files/upload",
            _principal(),
            data={"upload_dir": "/tmp/x"},
            files={"file": ("job.sh", b"x", "text/plain")},
        )

    assert response.status_code == 409


def test_upload_enforces_max_file_size():
    async def path_exist(name, username="", mgm=None):
        if name == "/tmp/x":
            return True, PathType.DIR
        return False, PathType.UNKNOWN

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router, "get_config", lambda *a, **k: 4
    ):
        response = _request(
            "post",
            "/api/v3/files/upload",
            _principal(),
            data={"upload_dir": "/tmp/x"},
            files={"file": ("large.bin", b"12345", "application/octet-stream")},
        )

    assert response.status_code == 413


def test_upload_maps_preflight_permission_error_to_403():
    async def path_exist(name, username="", mgm=None):
        raise PermissionError("denied")

    with patch.object(files_router.storage, "path_exist", path_exist):
        response = _request(
            "post",
            "/api/v3/files/upload",
            _principal(),
            data={"upload_dir": "/tmp/x"},
            files={"file": ("job.sh", b"x", "text/plain")},
        )

    assert response.status_code == 403


def test_upload_overwrite_true_is_forwarded():
    seen = {}

    async def path_exist(name, username="", mgm=None):
        if name == "/tmp/x":
            return True, PathType.DIR
        return True, PathType.FILE

    async def upload_from_path(src_path, dst, username="", mgm=None, overwrite=True):
        seen["overwrite"] = overwrite

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router.storage, "upload_file_from_path", upload_from_path
    ):
        response = _request(
            "post",
            "/api/v3/files/upload",
            _principal(),
            data={"upload_dir": "/tmp/x", "overWrite": "true"},
            files={"file": ("job.sh", b"x", "text/plain")},
        )

    assert response.status_code == 201
    assert seen["overwrite"] is True


def test_upload_cleans_temp_on_permission_error():
    seen = {}

    async def path_exist(name, username="", mgm=None):
        if name == "/tmp/x":
            return True, PathType.DIR
        return False, PathType.UNKNOWN

    async def upload_from_path(src_path, **kwargs):
        seen["src"] = src_path
        raise PermissionError("denied")

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router.storage, "upload_file_from_path", upload_from_path
    ):
        response = _request(
            "post",
            "/api/v3/files/upload",
            _principal(),
            data={"upload_dir": "/tmp/x"},
            files={"file": ("job.sh", b"x", "text/plain")},
        )

    assert response.status_code == 403
    assert not os.path.exists(seen["src"])


def test_upload_requires_write_scope():
    response = _request(
        "post",
        "/api/v3/files/upload",
        _principal(scopes=["files:read"]),
        data={"upload_dir": "/tmp/x"},
        files={"file": ("job.sh", b"x", "text/plain")},
    )
    assert response.status_code == 403


def test_upload_rejects_api_key():
    api_key = Principal(
        username="service:svc",
        issuer="apikey",
        subject="svc",
        auth_method="apikey",
        scopes=["files:write"],
    )

    async def no_session(token):
        return None

    async def as_key(token):
        return api_key

    with patch("fastink.routers.v3.middleware.validate_session", no_session), patch(
        "fastink.routers.v3.middleware.validate_api_key", as_key
    ):
        response = TestClient(_app()).post(
            "/api/v3/files/upload",
            data={"upload_dir": "/tmp/x"},
            files={"file": ("job.sh", b"x", "text/plain")},
            headers={"Authorization": "Bearer ink_test"},
        )
    assert response.status_code == 403


def test_download_streams_file():
    async def path_exist(name, username="", mgm=None):
        return True, PathType.FILE

    async def stream(fname, username="", mgm=None):
        yield b"hello "
        yield b"world"

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router.storage, "get_file_stream", stream
    ):
        response = _request(
            "get",
            "/api/v3/files/download",
            _principal(),
            params={"path": "/tmp/x/file.txt"},
        )

    assert response.status_code == 200
    assert response.content == b"hello world"
    assert "file.txt" in response.headers["content-disposition"]


def test_download_missing_returns_404():
    async def path_exist(name, username="", mgm=None):
        return False, PathType.UNKNOWN

    with patch.object(files_router.storage, "path_exist", path_exist):
        response = _request(
            "get",
            "/api/v3/files/download",
            _principal(),
            params={"path": "/tmp/x/missing.txt"},
        )
    assert response.status_code == 404


def test_download_maps_other_oserror_to_502():
    async def path_exist(name, username="", mgm=None):
        return True, PathType.FILE

    async def stream(fname, username="", mgm=None):
        raise IOError("network down")
        yield b""

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router.storage, "get_file_stream", stream
    ):
        response = _request(
            "get",
            "/api/v3/files/download",
            _principal(),
            params={"path": "/tmp/x/slow.bin"},
        )

    assert response.status_code == 502


def test_download_maps_timeout_to_504():
    async def path_exist(name, username="", mgm=None):
        return True, PathType.FILE

    async def stream(fname, username="", mgm=None):
        raise TimeoutError("timed out")
        yield b""

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router.storage, "get_file_stream", stream
    ):
        response = _request(
            "get",
            "/api/v3/files/download",
            _principal(),
            params={"path": "/tmp/x/slow.bin"},
        )

    assert response.status_code == 504


def test_download_maps_stream_size_error_to_413():
    async def path_exist(name, username="", mgm=None):
        return True, PathType.FILE

    async def stream(fname, username="", mgm=None):
        raise IOError("too large")
        yield b""

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router.storage, "get_file_stream", stream
    ):
        response = _request(
            "get",
            "/api/v3/files/download",
            _principal(),
            params={"path": "/tmp/x/large.bin"},
        )

    assert response.status_code == 413


def test_download_empty_stream():
    async def path_exist(name, username="", mgm=None):
        return True, PathType.FILE

    async def stream(fname, username="", mgm=None):
        return
        yield b""

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router.storage, "get_file_stream", stream
    ):
        response = _request(
            "get",
            "/api/v3/files/download",
            _principal(),
            params={"path": "/tmp/x/empty.txt"},
        )

    assert response.status_code == 200
    assert response.content == b""


@pytest.mark.parametrize(
    "error,status",
    [(PermissionError, 404), (TypeError, 400), (RuntimeError, 502)],
)
def test_download_maps_first_chunk_errors(error, status):
    async def path_exist(name, username="", mgm=None):
        return True, PathType.FILE

    async def stream(fname, username="", mgm=None):
        raise error("boom")
        yield b""

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router.storage, "get_file_stream", stream
    ):
        response = _request(
            "get",
            "/api/v3/files/download",
            _principal(),
            params={"path": "/tmp/x/file.txt"},
        )

    assert response.status_code == status


def test_delete_passes_name_to_storage():
    seen = {}

    async def delete_path(**kwargs):
        seen.update(kwargs)

    with patch.object(files_router.storage, "delete_path", delete_path):
        response = _request(
            "delete",
            "/api/v3/files",
            _principal(scopes=["files:delete"]),
            params={"path": "/workfs2/u07/guochaoqi/v3-test.txt"},
        )

    assert response.status_code == 200
    assert seen["name"] == "/workfs2/u07/guochaoqi/v3-test.txt"


def test_download_filename_is_rfc5987_encoded():
    async def path_exist(name, username="", mgm=None):
        return True, PathType.FILE

    async def stream(fname, username="", mgm=None):
        yield b"x"

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router.storage, "get_file_stream", stream
    ):
        response = _request(
            "get",
            "/api/v3/files/download",
            _principal(),
            params={"path": "/tmp/x/a b%.txt"},
        )

    assert "filename*=UTF-8''a%20b%25.txt" in response.headers["content-disposition"]


def test_upload_negative_limit_falls_back():
    async def path_exist(name, username="", mgm=None):
        if name == "/tmp/x":
            return True, PathType.DIR
        return False, PathType.UNKNOWN

    async def upload_from_path(**kwargs):
        return True, "ok"

    with patch.object(files_router.storage, "path_exist", path_exist), patch.object(
        files_router.storage, "upload_file_from_path", upload_from_path
    ), patch.object(files_router, "get_config", lambda *a, **k: -1):
        response = _request(
            "post",
            "/api/v3/files/upload",
            _principal(),
            data={"upload_dir": "/tmp/x"},
            files={"file": ("job.sh", b"x", "text/plain")},
        )

    assert response.status_code == 201


def test_upload_size_middleware_rejects_oversized_body():
    app = FastAPI()
    app.add_middleware(UploadSizeLimitMiddleware, overhead=0)

    @app.post("/api/v3/files/upload")
    async def upload():
        return {"ok": True}

    with patch("fastink.routers.v3.middleware.get_config", lambda *a, **k: 4):
        response = TestClient(app).post(
            "/api/v3/files/upload", content=b"12345"
        )

    assert response.status_code == 413


def test_upload_size_middleware_requires_content_length():
    middleware = UploadSizeLimitMiddleware(app=None, overhead=0)

    async def call_next(request):
        raise AssertionError("must not reach the app")

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v3/files/upload",
            "headers": [],
            "query_string": b"",
            "scheme": "http",
            "server": ("test", 80),
            "client": ("127.0.0.1", 1234),
            "root_path": "",
        }
    )
    response = asyncio.run(middleware.dispatch(request, call_next))
    assert response.status_code == 411


def test_upload_size_middleware_rejects_invalid_config():
    app = FastAPI()
    app.add_middleware(UploadSizeLimitMiddleware, overhead=0)

    @app.post("/api/v3/files/upload")
    async def upload():
        return {"ok": True}

    with patch(
        "fastink.routers.v3.middleware.get_config", lambda *a, **k: "not-a-number"
    ):
        response = TestClient(app).post(
            "/api/v3/files/upload", content=b"12345"
        )

    assert response.status_code == 503


def test_download_directory_returns_400():
    async def path_exist(name, username="", mgm=None):
        return True, PathType.DIR

    with patch.object(files_router.storage, "path_exist", path_exist):
        response = _request(
            "get",
            "/api/v3/files/download",
            _principal(),
            params={"path": "/tmp/x"},
        )
    assert response.status_code == 400
