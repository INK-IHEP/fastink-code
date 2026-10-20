from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.exc import IntegrityError

from fastink.auth.common import add_user, get_user
from fastink.auth.external_identity import (
    map_external_identity,
    resolve_external_identity,
)
from fastink.database.sqla import models
from fastink.database.sqla.session import get_session


def _cleanup(issuer: str, subject: str, user_id=None) -> None:
    session_scoped = get_session()
    session = session_scoped()
    try:
        session.begin()
        session.execute(
            delete(models.ExternalIdentities).where(
                models.ExternalIdentities.issuer == issuer,
                models.ExternalIdentities.subject == subject,
            )
        )
        if user_id is not None:
            session.execute(delete(models.Users).where(models.Users.id == user_id))
        session.commit()
    finally:
        session_scoped.remove()


def test_resolve_unmapped_returns_none():
    issuer = f"https://issuer.example/mr4a/{uuid4().hex}"
    subject = f"unmapped-{uuid4().hex}"

    assert resolve_external_identity(issuer, subject) is None


def test_map_then_resolve_roundtrip():
    issuer = f"https://issuer.example/mr4a/{uuid4().hex}"
    subject = f"subject-{uuid4().hex}"
    username = f"external_identity_{uuid4().hex}"
    user_id = None

    try:
        assert add_user(username=username)
        user_id = get_user(username=username)["id"]
        map_external_identity(issuer, subject, user_id)

        resolved = resolve_external_identity(issuer, subject)

        assert resolved is not None
        assert resolved.id == user_id
        assert resolved.username == username
    finally:
        _cleanup(issuer, subject, user_id)


def test_duplicate_issuer_subject_rejected():
    issuer = f"https://issuer.example/mr4a/{uuid4().hex}"
    subject = f"duplicate-{uuid4().hex}"
    username = f"eid_duplicate_{uuid4().hex}"
    user_id = None

    try:
        assert add_user(username=username)
        user_id = get_user(username=username)["id"]
        map_external_identity(issuer, subject, user_id)

        with pytest.raises(IntegrityError):
            map_external_identity(issuer, subject, user_id)
    finally:
        _cleanup(issuer, subject, user_id)
