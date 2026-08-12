#!/usr/bin/env python3
"""Create tables and seed the minimal rows the portable tests need.

Open-source clean: no IHEP users/permissions/groups. This is the CI
counterpart to tools/init_database.py, which still seeds IHEP-specific
users/permissions (see tools/README.md TODO). Here we only create the
schema plus the rows the portable test layer depends on.

Run inside the CI test container before pytest:
    python deploy/tests/ci/init_ci_db.py
"""
from sqlalchemy import create_engine

from fastink.common.config import get_config
from fastink.database.sqla.models import BASE
from fastink.auth import common, permission


def main() -> None:
    db = get_config("database")
    url = (
        f"mysql+pymysql://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['dbname']}"
    )
    engine = create_engine(url)
    BASE.metadata.create_all(engine)
    print("tables created")

    # Seed only what the portable tests look up. Open-source clean: no IHEP
    # users/permissions/groups.
    username = get_config("test", "username")
    try:
        common.get_user(username=username)
    except Exception:
        common.add_user(username=username)
        print(f"seeded user {username}")

    # test_get_permission expects the test user to hold at least one
    # permission (an empty permission list makes the endpoint return
    # PERMISSION_QUERY_FAILURE). Grant a baseline permission.
    permission.add_permission("admin")
    permission.add_user_permission(username, "admin")

    for auth in ("password", "krb5"):
        try:
            common.get_authentication(authentication=auth)
        except Exception:
            try:
                common.add_authentication(auth)
                print(f"seeded authentication {auth}")
            except Exception:
                pass


if __name__ == "__main__":
    main()
