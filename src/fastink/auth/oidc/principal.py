from dataclasses import dataclass

from fastapi import HTTPException, Request


@dataclass
class Principal:
    username: str
    issuer: str
    subject: str
    groups: list[str] | None = None
    scopes: list[str] | None = None


def get_current_principal(request: Request) -> Principal:
    principal = getattr(request.state, "principal", None)
    if principal is None:
        raise HTTPException(status_code=401, detail="invalid_token")
    return principal


__all__ = ["Principal", "get_current_principal"]
