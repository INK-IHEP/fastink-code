from pprint import pprint
from typing import Optional, Any
from datetime import datetime
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import update, select, delete

from src.database.sqla import models
from src.database.sqla.session import read_session, transactional_session
from src.common.logger import logger

@transactional_session
def add_link(
    username: str, path: Optional[str] = None, owner: Optional[str] = None, *, session: Session
) -> bool:
    try:
        new_link = models.ShareLink(
            username=username,
            filename=filename,
            owner=owner,
            path=path
        )
        new_link.save(session=session, flush=True)
        return True
    except IntegrityError:
        raise
@read_session
def get_link(
    path, 
    *,
    session: Session,
):
    stmt = select(models.ShareLink)
    msg = ''

    if path:
        stmt = stmt.where(models.ShareLink.file_path == path)
    
    try:
        results = session.execute(stmt).scalar()
        results = results.to_dict()
    except AttributeError as e:
        raise NoResultFound
    return results

    
