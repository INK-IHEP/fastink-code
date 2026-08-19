import pytest
from fastink.common.config import get_config


@pytest.fixture(scope="session")
def test_username():
    return get_config("test", "username")


@pytest.fixture(scope="session")
def test_password():
    return get_config("test", "password")


def pytest_configure(config):
    config.addinivalue_line("markers", "krb5: marks tests that require krb5 authentication")
