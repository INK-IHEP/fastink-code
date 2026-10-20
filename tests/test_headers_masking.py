"""Query-string masking: credentials must never reach request logs."""
from urllib.parse import unquote

from fastink.routers import headers


def test_sensitive_query_keys_include_jwt_token_and_password():
    assert "jwtToken" in headers._SENSITIVE_QUERY_KEYS
    assert "password" in headers._SENSITIVE_QUERY_KEYS


def test_mask_url_query_redacts_credentials():
    token = "eyJhbGciOiIUzI1"
    url = (
        "https://ink.ihep.ac.cn/api/v2/joblens/service/launch"
        f"?jwtToken={token}&password=sekrit&cluster=slurm&os=AlmaLinux9"
    )
    masked = headers.mask_url_query(url, headers._SENSITIVE_QUERY_KEYS)
    assert token not in masked
    assert "sekrit" not in masked
    assert "cluster=slurm" in masked
    assert "os=AlmaLinux9" in masked
    assert "*" * len(token) in unquote(masked)


def test_mask_url_query_keeps_empty_and_unlisted_values():
    masked = headers.mask_url_query("https://h/api?password=&x=1", {"password"})
    assert "password=" in masked
    assert "x=1" in masked
