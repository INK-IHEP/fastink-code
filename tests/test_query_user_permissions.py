from uuid import uuid4

from sqlalchemy.exc import NoResultFound

from fastink.auth import common, permission


def test_query_user_permissions_preserves_permission_order(monkeypatch):
    username = f"qup_char_user_{uuid4().hex}"
    group_name = f"qup_char_group_{uuid4().hex}"
    user_id = None
    read_id = None
    centos_id = None
    almalinux_id = None

    try:
        common.add_user(username=username)
        user_id = common.get_user(username=username)["id"]

        for permission_name in ("read", "CentOS7", "AlmaLinux9"):
            try:
                common.get_permission(permission=permission_name)
            except NoResultFound:
                assert permission.add_permission(permission_name)
        read_id = common.get_permission(permission="read")["id"]
        centos_id = common.get_permission(permission="CentOS7")["id"]
        almalinux_id = common.get_permission(permission="AlmaLinux9")["id"]

        permission.add_user_permission(username, "read")
        permission.add_user_permission(username, "CentOS7")
        common.add_group_permission(
            group_name=group_name, permission_id=almalinux_id
        )
        common.add_group_permission(group_name=group_name, permission_id=read_id)

        monkeypatch.setattr(
            "fastink.auth.permission.get_user_groups", lambda user: [group_name]
        )

        permissions = permission.query_user_permissions(username=username)

        assert permissions == ["CentOS7", "AlmaLinux9", "read"]
        assert type(permissions) is list
    finally:
        if user_id is not None:
            if read_id is not None:
                common.delete_user_permission(
                    user_id=user_id, permission_id=read_id
                )
            if centos_id is not None:
                common.delete_user_permission(
                    user_id=user_id, permission_id=centos_id
                )
        if almalinux_id is not None:
            common.delete_group_permission(
                group_name=group_name, permission_id=almalinux_id
            )
        if read_id is not None:
            common.delete_group_permission(group_name=group_name, permission_id=read_id)
        if user_id is not None:
            common.delete_user(user_id)
