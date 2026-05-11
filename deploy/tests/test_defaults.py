"""Default values and answer normalization tests: default_answers, default_image_answers, parse_override_value.

Note: parse_override_value only applies type inference for keys in
BOOL_FIELDS/INT_FIELDS; other keys are returned as strings.
"""
import pytest
from deploy.lib.defaults import default_answers, default_image_answers, parse_override_value


def test_default_answers_has_required_keys() -> None:
    answers = default_answers()
    for key in ("profile", "image_source", "project_name"):
        assert key in answers, f"missing default key: {key}"


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
