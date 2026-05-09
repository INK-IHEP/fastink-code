"""默认值和答案规范化测试：default_answers, default_image_answers, parse_override_value。

注意：parse_override_value 只对 BOOL_FIELDS/INT_FIELDS 中的 key
做类型推断，其他 key 原样返回字符串。
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
        # build 模式返回本地构建的 :local 镜像名
        assert images["server_image"] == "fastink-server:local"
        assert images["cron_image"] == "fastink-redis-cron:local"
        # xrootd 不在 build 列表中，使用默认 pull 镜像
        assert images["xrootd_image"] == "dockerhub.ihep.ac.cn/ink/xrootd-multiuser:5.9.0-3"

    def test_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported image source"):
            default_image_answers("invalid")


class TestParseOverrideValue:
    @pytest.mark.parametrize(["key", "raw", "expected"], [
        ("enable_nginx", "true", True),      # BOOL_FIELDS →
        ("enable_krb5", "false", False),     # BOOL_FIELDS
        ("host_port", "8080", 8080),         # INT_FIELDS → int
        ("workers", "4", 4),                 # INT_FIELDS → int
        ("profile", "custom", "custom"),    # 普通字段 → str
        ("db_name", "mydb", "mydb"),        # 普通字段 → str
    ])
    def test_type_inference(self, key: str, raw: str, expected: object) -> None:
        assert parse_override_value(key, raw) == expected

    def test_bool_field_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid boolean"):
            parse_override_value("enable_nginx", "not-a-bool")
