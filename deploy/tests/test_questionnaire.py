"""detect_deploy_user: initial-admin auto-detection for the installer."""
import pytest
from deploy.lib.questionnaire import detect_deploy_user


def test_sudo_user_wins(monkeypatch):
    monkeypatch.setenv("SUDO_USER", "alice")
    monkeypatch.setattr("deploy.lib.questionnaire.getpass.getuser", lambda: "root")
    assert detect_deploy_user() == "alice"


def test_sudo_root_falls_through(monkeypatch):
    monkeypatch.setenv("SUDO_USER", "root")
    monkeypatch.setattr("deploy.lib.questionnaire.getpass.getuser", lambda: "bob")
    assert detect_deploy_user() == "bob"


def test_current_user_when_not_root(monkeypatch):
    monkeypatch.delenv("SUDO_USER", raising=False)
    monkeypatch.setattr("deploy.lib.questionnaire.getpass.getuser", lambda: "carol")
    assert detect_deploy_user() == "carol"


def test_root_returns_empty(monkeypatch):
    monkeypatch.delenv("SUDO_USER", raising=False)
    monkeypatch.setattr("deploy.lib.questionnaire.getpass.getuser", lambda: "root")
    assert detect_deploy_user() == ""


def test_getuser_failure_returns_empty(monkeypatch):
    monkeypatch.delenv("SUDO_USER", raising=False)

    def boom():
        raise OSError("no uid mapping")

    monkeypatch.setattr("deploy.lib.questionnaire.getpass.getuser", boom)
    assert detect_deploy_user() == ""


def _stub_prompts(monkeypatch):
    # Patch through questionnaire's own cli_ui reference: deploy tests can
    # load the module as either deploy.lib.* or lib.*, and patching the
    # wrong instance would leave the real (stdin-reading) prompts active.
    from deploy.lib import questionnaire

    cli_ui = questionnaire.cli_ui
    monkeypatch.setattr(cli_ui, "step", lambda *a, **k: None)
    monkeypatch.setattr(cli_ui, "choice_prompt", lambda text, options, default="", reuse=None: default or options[0])
    monkeypatch.setattr(cli_ui, "text_prompt", lambda text, default="", reuse=None: reuse if reuse is not None else default)
    monkeypatch.setattr(cli_ui, "confirm_prompt", lambda text, default=True, reuse=None: default)
    monkeypatch.setattr(cli_ui, "int_prompt", lambda text, default=0, reuse=None: default)
    monkeypatch.setattr(cli_ui, "secret_prompt", lambda text, default="", reuse=None: default)


def test_custom_preserves_saved_empty_admin_username(monkeypatch, tmp_path):
    """A saved explicit opt-out (admin_username: "") must survive the
    questionnaire instead of being replaced by auto-detection."""
    from deploy.lib import questionnaire

    _stub_prompts(monkeypatch)
    # Non-empty detection result so the assertion discriminates: without
    # the is-not-None preservation the detected value would leak through.
    monkeypatch.setattr(questionnaire, "detect_deploy_user", lambda: "deployer")
    answers = questionnaire.collect_answers_custom(
        previous_answers={"admin_username": ""}, deploy_dir=tmp_path
    )
    assert answers["admin_username"] == ""


def test_custom_preserves_saved_admin_username(monkeypatch, tmp_path):
    from deploy.lib import questionnaire

    _stub_prompts(monkeypatch)
    monkeypatch.setattr(questionnaire, "detect_deploy_user", lambda: "deployer")
    answers = questionnaire.collect_answers_custom(
        previous_answers={"admin_username": "alice"}, deploy_dir=tmp_path
    )
    assert answers["admin_username"] == "alice"


def test_basic_text_prompt_reuse_convention(monkeypatch):
    """Dependency-free basic mode matches questionary semantics: empty
    input returns the saved reuse value when present (else the default),
    'r' remains an explicit alias, and typed input wins."""
    from deploy.lib import cli_ui

    monkeypatch.setattr(cli_ui, "_HAS_DEPS", False)
    inputs = iter(["", "", "", "r", "typed"])
    monkeypatch.setattr("builtins.input", lambda prompt="": next(inputs))

    # Enter with no saved value -> computed default
    assert cli_ui.text_prompt("Field", "deployer", None) == "deployer"
    # Enter with a saved empty opt-out -> empty survives (regression the
    # review bot flagged: basic mode used to return the default here)
    assert cli_ui.text_prompt("Field", "deployer", "") == ""
    # Enter with a saved value -> saved value
    assert cli_ui.text_prompt("Field", "deployer", "alice") == "alice"
    # 'r' remains an explicit alias for the saved value
    assert cli_ui.text_prompt("Field", "deployer", "alice") == "alice"
    # Typed input wins
    assert cli_ui.text_prompt("Field", "deployer", "alice") == "typed"


def test_quickstart_reuses_saved_secrets(tmp_path):
    """A re-run must keep the saved secrets: the MariaDB datadir keeps the
    passwords it was first initialized with (entrypoint ignores MYSQL_* env
    on a non-empty datadir), so freshly generated ones would leave the
    server unable to authenticate to a surviving database (user-reported
    regression on partial-failure re-deploy)."""
    from deploy.lib.questionnaire import build_quickstart_answers, default_answers

    previous = {
        "db_root_password": "saved-root-pw",
        "db_password": "saved-user-pw",
        "redis_password": "saved-redis-pw",
        "file_share_jwt_secret": "saved-jwt-secret",
    }
    defaults = default_answers("quickstart", tmp_path)
    answers = build_quickstart_answers(defaults, tmp_path, previous_answers=previous)
    assert answers["db_root_password"] == "saved-root-pw"
    assert answers["db_password"] == "saved-user-pw"
    assert answers["redis_password"] == "saved-redis-pw"
    assert answers["file_share_jwt_secret"] == "saved-jwt-secret"


def test_quickstart_generates_fresh_secrets_without_saved(tmp_path):
    from deploy.lib.questionnaire import build_quickstart_answers, default_answers

    defaults = default_answers("quickstart", tmp_path)
    first = build_quickstart_answers(defaults, tmp_path)
    second = build_quickstart_answers(defaults, tmp_path)
    for key in ("db_root_password", "db_password", "redis_password", "file_share_jwt_secret"):
        assert first[key]
        assert first[key] != second[key]


def test_quickstart_secret_edge_values(tmp_path):
    """saved_secret contract: a partial dict reuses what is saved, an
    empty saved value regenerates, and non-string values stringify."""
    from deploy.lib.questionnaire import build_quickstart_answers, default_answers

    previous = {
        "db_root_password": "saved-root",
        "db_password": "",
        "redis_password": 12345,
    }
    defaults = default_answers("quickstart", tmp_path)
    answers = build_quickstart_answers(defaults, tmp_path, previous_answers=previous)
    assert answers["db_root_password"] == "saved-root"
    assert answers["db_password"]
    assert answers["redis_password"] == "12345"
    assert answers["file_share_jwt_secret"]


def test_try_load_saved_answers_rejects_non_object_json(tmp_path):
    """A parseable but non-object answers.json degrades to None instead
    of crashing a re-run with AttributeError (review round finding)."""
    from deploy.lib.questionnaire import try_load_saved_answers

    (tmp_path / "answers.json").write_text("[]")
    assert try_load_saved_answers(tmp_path) is None
