#!/usr/bin/env python3
from sqlalchemy import create_engine

from src.common.config import get_config
from src.common.logger import logger
from src.database.sqla.models import BASE
from src.auth import permission, common


def init_db():
    db_config = get_config("database")

    host = db_config["host"]
    port = db_config["port"]
    user = db_config["user"]
    password = db_config["password"]
    database = db_config["dbname"]

    db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    logger.debug(f"Connecting to database: {db_url}")

    engine = create_engine(db_url)
    BASE.metadata.create_all(engine)

    logger.info("Database and tables initialized successfully.")

    perm_list = [
        "cpu",
        "gpu",
        "CentOS7",
        "AlmaLinux9",
        "admin",
        "ink_special",
        "elk",
        "compile",
        "lhaasogpu",
    ]
    for perm in perm_list:
        try:
            common.get_permission(perm)
            continue
        except:
            pass
        try:
            permission.add_permission(perm)
        except:
            pass

    userlist = [
        {"username": "guochaoqi", "email": "guochaoqi@ihep.ac.cn"},
        {"username": "qinguoyong", "email": "305039273@qq.com"},
        {"username": "oug", "email": "oug@ihep.ac.cn"},
        {"username": "shijy", "email": "shijy@ihep.ac.cn"},
        {"username": "guocq", "email": "guocq@ihep.ac.cn"},
        {"username": "hanx", "email": "hanx@ihep.ac.cn"},
    ]
    for useritem in userlist:
        try:
            common.get_user(**useritem)
            continue
        except:
            pass
        try:
            common.add_user(**useritem)
        except:
            pass
        try:
            permission.add_user_permission(useritem["username"], "admin")
        except:
            pass
        try:
            permission.add_user_permission(useritem["username"], "ink_special")
        except:
            pass
        try:
            permission.add_user_permission(useritem["username"], "elk")
        except:
            pass

    authenticationlist = ["password", "krb5"]
    for authentication in authenticationlist:
        try:
            common.get_authentication(authentication)
        except:
            pass
        try:
            common.add_authentication(authentication)
        except:
            pass


if __name__ == "__main__":
    init_db()
