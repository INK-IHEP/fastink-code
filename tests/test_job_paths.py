from fastink.computing.tools.common import utils


def test_get_user_ink_dir_uses_account_home(monkeypatch):
    monkeypatch.setattr(
        utils,
        "get_config",
        lambda *args, **kwargs: "~",
    )
    monkeypatch.setattr(
        utils.os.path,
        "expanduser",
        lambda value: "/users/alice" if value == "~alice" else value,
    )

    assert utils.get_user_ink_dir("alice", 1001) == "/users/alice"


def test_get_user_ink_dir_formats_username_without_group_lookup(monkeypatch):
    monkeypatch.setattr(
        utils,
        "get_config",
        lambda *args, **kwargs: "/home/{username}",
    )
    monkeypatch.setattr(
        utils,
        "get_user_exp_group",
        lambda _uid: (_ for _ in ()).throw(
            AssertionError("group lookup should not run")
        ),
    )

    assert utils.get_user_ink_dir("alice", 1001) == "/home/alice"


def test_get_user_ink_dir_supports_site_group_templates(monkeypatch):
    monkeypatch.setattr(
        utils,
        "get_config",
        lambda *args, **kwargs: (
            "/work/{user_group}/{experiment_group_lower}/{username}"
        ),
    )
    monkeypatch.setattr(
        utils,
        "get_user_exp_group",
        lambda uid: ("ExampleExperiment", "physics")
        if uid == 1001
        else (None, None),
    )

    assert (
        utils.get_user_ink_dir("alice", 1001)
        == "/work/physics/exampleexperiment/alice"
    )


def test_get_user_jobs_dir_appends_fastink_jobs_tree(monkeypatch):
    monkeypatch.setattr(
        utils,
        "get_user_ink_dir",
        lambda username, uid=None: f"/srv/{username}-{uid}",
    )

    assert (
        utils.get_user_jobs_dir("alice", 1001)
        == "/srv/alice-1001/.ink/Jobs"
    )
