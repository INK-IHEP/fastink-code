import pytest
from fastink.common.config import get_config


@pytest.fixture(autouse=True)
def _clear_rate_limit():
    """Keep credential-failure budgets isolated between tests."""
    try:
        from fastink.auth import ratelimit

        client = ratelimit._redis()
        for key in client.scan_iter("ink:rl:pw:*"):
            client.delete(key)
    except Exception:
        pass
    yield


@pytest.fixture(scope="session")
def test_username():
    return get_config("test", "username")


@pytest.fixture(scope="session")
def test_password():
    return get_config("test", "password")


def pytest_configure(config):
    config.addinivalue_line("markers", "krb5: marks tests that require krb5 authentication")
