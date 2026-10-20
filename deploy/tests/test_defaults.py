"""Default values and answer normalization tests: default_answers, default_image_answers, parse_override_value.

Note: parse_override_value only applies type inference for keys in
BOOL_FIELDS/INT_FIELDS; other keys are returned as strings.
"""
import re

import pytest
from deploy.lib.defaults import default_answers, default_image_answers, parse_override_value


def test_default_answers_has_required_keys() -> None:
    answers = default_answers()
    for key in ("profile", "image_source", "project_name"):
        assert key in answers, f"missing default key: {key}"
    for key in ("db_image", "redis_image", "nginx_image"):
        assert key in answers, f"missing default key: {key}"
        assert str(answers[key]).startswith("dockerhub.ihep.ac.cn/ink/")


def test_security_access_defaults_to_true() -> None:
    """Secure-by-default guard: a missing key normalizes to True
    (enforcement on) and an explicit opt-out survives normalization —
    prevents a silent regression to the unauthenticated default."""
    from deploy.lib.defaults import normalize_answers

    assert default_answers()["security_access"] is True
    filled = normalize_answers({"profile": "quickstart"})
    assert filled["security_access"] is True
    opted_out = normalize_answers({"profile": "quickstart", "security_access": False})
    assert opted_out["security_access"] is False


class TestDefaultImageAnswers:
    def test_pull(self) -> None:
        images = default_image_answers("pull")
        for key in ("server_image", "cron_image", "rootbrowse_image", "xrootd_image", "init_image", "htcondor_image"):
            assert key in images, f"missing image key: {key}"

    def test_build_returns_local_tags(self) -> None:
        images = default_image_answers("build")
        # build mode returns locally built :local image tags
        assert images["server_image"] == "fastink-server:local"
        assert images["cron_image"] == "fastink-redis-cron:local"
        # xrootd is not in the build list, falls back to the default pull image
        assert images["xrootd_image"] == "dockerhub.ihep.ac.cn/ink/xrootd-multiuser:5.9.0-3"

    def test_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported image source"):
            default_image_answers("invalid")


class TestParseOverrideValue:
    @pytest.mark.parametrize(["key", "raw", "expected"], [
        ("enable_nginx", "true", True),      # BOOL_FIELDS
        ("enable_krb5", "false", False),     # BOOL_FIELDS
        ("security_access", "false", False), # BOOL_FIELDS
        ("host_port", "8080", 8080),         # INT_FIELDS
        ("workers", "4", 4),                 # INT_FIELDS
        ("profile", "custom", "custom"),    # plain field → str
        ("db_name", "mydb", "mydb"),        # plain field → str
    ])
    def test_type_inference(self, key: str, raw: str, expected: object) -> None:
        assert parse_override_value(key, raw) == expected

    def test_bool_field_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid boolean"):
            parse_override_value("enable_nginx", "not-a-bool")


def test_dockerfile_base_images_use_site_registry():
    """Every FROM under deploy/images must resolve — through its ARG defaults —
    to an approved site-registry base or a locally-built image; build mode
    must work without a docker.io proxy on mainland hosts (pull mode covered
    by the compose render test). Bumping a mirror means updating the
    allowlist below deliberately."""
    from pathlib import Path

    approved_registry_bases = {
        "dockerhub.ihep.ac.cn/ink/base-almalinux:9",
        "dockerhub.ihep.ac.cn/ink/base-htcondor:23.0.28-el9",
    }
    images_dir = Path(__file__).resolve().parent.parent / "images"
    dockerfiles = sorted(images_dir.rglob("Dockerfile"))
    assert dockerfiles, "no Dockerfiles found — test layout drifted"
    for dockerfile in dockerfiles:
        text = dockerfile.read_text(encoding="utf-8")
        arg_defaults = dict(re.findall(r"^[ \t]*ARG[ \t]+(\w+)=(.*)$", text, flags=re.MULTILINE))
        froms = re.findall(r"^[ \t]*FROM[ \t]+(\S+)", text, flags=re.MULTILINE)
        loose_froms = re.findall(r"(?im)^[ \t]*FROM\b", text)
        assert len(froms) == len(loose_froms), (
            f"{dockerfile.parent.name}/Dockerfile: FROM scanner missed a directive"
        )
        assert froms, f"{dockerfile.parent.name}/Dockerfile: no FROM — test layout drifted"
        for ref in froms:
            resolved = ref
            for _ in range(8):  # expand ${VAR} chains until stable
                expanded = re.sub(
                    r"\$\{(\w+)\}", lambda m: arg_defaults.get(m.group(1), m.group(0)), resolved
                )
                if expanded == resolved:
                    break
                resolved = expanded
            assert "${" not in resolved, (
                f"{dockerfile.parent.name}/Dockerfile: FROM {ref!r} references an ARG "
                "with no default — the one-click build passes no build args"
            )
            assert resolved in approved_registry_bases or resolved.startswith("fastink-"), (
                f"{dockerfile.parent.name}/Dockerfile: FROM {ref!r} resolves to "
                f"{resolved!r}, which is not an approved base (allowed: "
                f"{sorted(approved_registry_bases)} or locally-built fastink-*)"
            )
