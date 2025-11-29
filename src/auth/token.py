import importlib
from datetime import datetime, timedelta

from src.auth.common import (
    add_token,
    get_token,
    update_token,
    get_user,
    get_authentication,
)
from src.common.config import get_config
from src.common.logger import logger

AUTH_TYPE = get_config("auth", "type")
AUTH_PLUGIN = importlib.import_module(f"src.auth.plugins.{AUTH_TYPE}")


def create_token(username: str, password: str, expire_in: int = 86400) -> bool:
    user_id = get_user(username=username)["id"]
    authentication_id = get_authentication(authentication=AUTH_TYPE)["id"]
    token = AUTH_PLUGIN.create_token(
        username=username, password=password, expire_in=expire_in
    )
    logger.debug(f"Create token for {username} = {user_id}")
    try:
        # if token not exists, create a new one
        get_token(user_id=user_id)
    except:
        logger.debug(f"No token found for {username}, add a new one")
        try:
            add_token(
                user_id=user_id,
                authentication_id=authentication_id,
                token=token["token"],
                generated_at=token["generated_at"],
                expired_at=token["expired_at"],
            )
        except:
            logger.error(f"Failed to create token for {username}")
            raise Exception(f"Failed to create token for {username}")
    else:
        # if token exists, update it
        logger.debug("Token exists, need to update it")
        try:
            update_token(
                user_id=user_id,
                authentication_id=authentication_id,
                token=token["token"],
                generated_at=token["generated_at"],
                expired_at=token["expired_at"],
            )
        except:
            logger.error(f"Failed to update token for {username}")
            raise Exception(f"Failed to update token for {username}")
    return True


def query_token(username: str, expire_in: int = 3600) -> str:
    user_id = get_user(username=username)["id"]
    token = get_token(user_id=user_id)
    logger.debug(f"Query token for {username} = {user_id}")
    # if token is expired, raise exception
    if token is None or token["expired_at"] < datetime.now():
        logger.debug(f"Token for {username} is expired.")
        raise ValueError("Token expired")
    # if token is expiring soon, update it
    elif token["expired_at"] - datetime.now() < timedelta(seconds=expire_in):
        logger.debug(f"Token for {username} is expiring soon.")
        updated_token = AUTH_PLUGIN.update_token(
            username=username, token=token["token"], expire_in=expire_in
        )
        update_token(
            user_id=user_id,
            authentication_id=updated_token["authentication_id"],
            token=updated_token["token"],
            generated_at=updated_token["generated_at"],
            expired_at=updated_token["expired_at"],
        )
        return updated_token["token"]
    else: 
        # if token is valid, return it
        logger.debug(f"Token for {username} is valid.")
        return token["token"]
