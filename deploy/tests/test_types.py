"""Type-safe accessor tests: get_bool, get_str, get_int."""
import pytest
from deploy.lib.types import get_bool, get_str, get_int


class TestGetBool:
    @pytest.mark.parametrize(["key", "expected"], [
        ("enabled", True),   ("disabled", False),
        ("missing", False),  ("defaulted", True),
    ])
    def test_values(self, key: str, expected: bool) -> None:
        answers: dict[str, object] = {"enabled": True, "disabled": False}
        default = True if key == "defaulted" else False
        assert get_bool(answers, key, default) is expected

    @pytest.mark.parametrize(["raw", "expected"], [
        ("true", True),    ("TRUE", True),    ("yes", True),
        ("1", True),       ("y", True),       ("on", True),
        ("false", False),  ("no", False),     ("0", False),
    ])
    def test_string_values(self, raw: str, expected: bool) -> None:
        assert get_bool({"v": raw}, "v") is expected

    def test_none_returns_default(self) -> None:
        assert get_bool({"v": None}, "v") is False
        assert get_bool({"v": None}, "v", True) is True


class TestGetStr:
    @pytest.mark.parametrize(["key", "expected"], [
        ("name", "hello"),   ("empty", ""),
        ("missing", ""),     ("defaulted", "fallback"),
    ])
    def test_values(self, key: str, expected: str) -> None:
        answers: dict[str, object] = {"name": "hello", "empty": ""}
        default = "fallback" if key == "defaulted" else ""
        assert get_str(answers, key, default) == expected

    def test_non_string_casts(self) -> None:
        assert get_str({"n": 42}, "n") == "42"
        assert get_str({"b": True}, "b") == "True"


class TestGetInt:
    @pytest.mark.parametrize(["key", "expected"], [
        ("count", 5),       ("zero", 0),
        ("missing", 0),     ("defaulted", 10),
    ])
    def test_values(self, key: str, expected: int) -> None:
        answers: dict[str, object] = {"count": 5, "zero": 0}
        default = 10 if key == "defaulted" else 0
        assert get_int(answers, key, default) == expected

    @pytest.mark.parametrize(["raw", "expected"], [
        ("42", 42),   ("0", 0),   ("-1", -1),
    ])
    def test_string_to_int(self, raw: str, expected: int) -> None:
        assert get_int({"v": raw}, "v") == expected
