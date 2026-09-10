from sqlalchemy.exc import NoResultFound

from fastink.auth import permission


def test_can_remove_group_admin_source_allows_direct_admin(monkeypatch):
    monkeypatch.setattr(
        permission.common,
        "get_user",
        lambda **kwargs: {"id": "caller-id"},
    )
    monkeypatch.setattr(
        permission.common,
        "get_permission",
        lambda **kwargs: {"id": "admin-id"},
    )
    monkeypatch.setattr(
        permission.common,
        "get_user_permission",
        lambda **kwargs: {"user_id": "caller-id", "permission_id": "admin-id"},
    )

    assert permission.can_remove_group_admin_source("caller", "group") is True


def test_can_remove_group_admin_source_blocks_callers_group_admin_source(monkeypatch):
    monkeypatch.setattr(
        permission.common,
        "get_user",
        lambda **kwargs: {"id": "caller-id"},
    )
    monkeypatch.setattr(
        permission.common,
        "get_permission",
        lambda **kwargs: {"id": "admin-id"},
    )

    def no_direct_permission(**kwargs):
        raise NoResultFound

    monkeypatch.setattr(
        permission.common,
        "get_user_permission",
        no_direct_permission,
    )
    monkeypatch.setattr(permission, "get_user_groups", lambda username: [" Group "])

    assert permission.can_remove_group_admin_source("caller", "group") is False


def test_get_users_by_permission_including_groups_unions_and_deduplicates(
    monkeypatch,
):
    direct_user = {"id": "direct-id", "username": "direct"}
    group_user = {"id": "group-id", "username": "group"}
    monkeypatch.setattr(
        permission.common,
        "get_permission",
        lambda **kwargs: {"id": "admin-id"},
    )
    monkeypatch.setattr(
        permission.common,
        "get_users_by_permission",
        lambda **kwargs: [direct_user],
    )
    monkeypatch.setattr(
        permission.common,
        "get_group_names_by_permission",
        lambda **kwargs: ["research"],
    )
    monkeypatch.setattr(
        permission.common,
        "get_users",
        lambda: [direct_user, group_user],
    )
    monkeypatch.setattr(
        "fastink.auth.groups.get_users_groups",
        lambda usernames: {
            "group": ["research"],
        },
    )

    assert permission.get_users_by_permission_including_groups("admin") == [
        direct_user,
        group_user,
    ]
