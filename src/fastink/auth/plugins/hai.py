import requests
from dataclasses import dataclass
from typing import Optional


@dataclass
class ValidatedResult:
    api_key: str
    username: str
    is_valid: bool
    alias: str
    allowed_models: list[str]
    remarks: str


def validate_token(
    username: str,
    token: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    issuer: Optional[str] = None,
) -> bool:
    if issuer is not None:
        base_url = f"{issuer}".rstrip("/")
    else:
        base_url = "https://aiapi.ihep.ac.cn/apiv2"

    if not client_secret:
        raise Exception("No client secret provided")

    resp = requests.post(
        f"{base_url}/key/verify_api_key",
        headers={"Authorization": f"Bearer {client_secret}"},
        json={"api_key": token},
    )
    try:
        resp.raise_for_status()
        resp_data = resp.json()
    except requests.RequestException as e:
        raise Exception(f"Failed: {e}: {resp.text}") from e

    validated_result = ValidatedResult(**resp_data)
    email = username_to_email(username)
    if email != validated_result.username:
        raise Exception("Username does not match the API key owner")
    if not validated_result.is_valid:
        raise Exception("API key is not valid")
    return True


def username_to_email(username: str) -> str:
    url = f"https://login.ihep.ac.cn/umt/api/APIafsToemail?afsAccount={username}"
    response = requests.get(url)
    return response.json()["result"][0].get("email")
