import uuid
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from fastink.common.hooks import hookable
from fastink.database.sqla import models
from fastink.database.sqla.session import read_session, transactional_session


@transactional_session
def map_external_identity(
    issuer: str,
    subject: str,
    user_id: uuid.UUID,
    *,
    session: Session,
) -> models.ExternalIdentities:
    external_identity = models.ExternalIdentities(
        issuer=issuer,
        subject=subject,
        user_id=user_id,
    )
    external_identity.save(session=session)
    return external_identity


@hookable
@read_session
def resolve_external_identity(
    issuer: str,
    subject: str,
    *,
    session: Session,
) -> Optional[models.Users]:
    statement = (
        select(models.Users)
        .join(
            models.ExternalIdentities,
            models.ExternalIdentities.user_id == models.Users.id,
        )
        .where(
            models.ExternalIdentities.issuer == issuer,
            models.ExternalIdentities.subject == subject,
        )
    )
    return session.execute(statement).scalar_one_or_none()
