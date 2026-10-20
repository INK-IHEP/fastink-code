from fastink.routers.status import InkStatus


def test_status_codes():
    assert InkStatus.PASSWORD_EXPIRED == "A09"
    assert InkStatus.ACCOUNT_EXPIRED == "A10"
    assert InkStatus.USER_NOT_FOUND == "A11"
