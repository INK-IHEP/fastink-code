"""Render utility tests: YAML block generation, deep_merge, profile_chain, etc."""
from typing import Optional

import pytest
import yaml
from deploy.lib.render import (
    render_volume_block,
    render_optional_single_volume_block,
    render_yaml_list_block,
    default_jobtype_config_block,
    yaml_string,
    profile_chain,
    deep_merge,
    build_xrootd_vo_entries,
)


class TestRenderVolumeBlock:
    @pytest.mark.parametrize(["entries", "expected"], [
        ([], ""),
        (["/a:/b"], '\n      - /a:/b'),
        (["/a:/b", "/c:/d:ro"], '\n      - /a:/b\n      - /c:/d:ro'),
    ])
    def test_output(self, entries: list[str], expected: str) -> None:
        assert render_volume_block(entries) == expected

    def test_special_chars_use_yaml_quoting(self) -> None:
        result = render_volume_block(["/path with spaces:/c:ro"])
        parsed = yaml.safe_load(result.strip())
        assert parsed == ["/path with spaces:/c:ro"]


class TestRenderOptionalSingleVolumeBlock:
    @pytest.mark.parametrize(["entry", "expected"], [
        (None, ""),
        ("", ""),
        ("/a:/b:ro", '\n      - /a:/b:ro'),
    ])
    def test_output(self, entry: Optional[str], expected: str) -> None:
        assert render_optional_single_volume_block(entry) == expected


class TestRenderYamlListBlock:
    def test_indent_0_gives_valid_yaml(self) -> None:
        """indent=0 output is directly parseable as a YAML list."""
        result = render_yaml_list_block(["a", "b"], indent=0)
        assert yaml.safe_load(result) == ["a", "b"]

    def test_with_indent(self) -> None:
        result = render_yaml_list_block(["x"], indent=4)
        assert result.startswith("    -")


class TestDefaultJobtypeConfigBlock:
    def test_structure(self) -> None:
        result = default_jobtype_config_block("schedd@h1", "h1", 4, 8192, indent=0)
        parsed = yaml.safe_load(result)
        for jt in ("vscode", "jupyter", "vnc", "rootbrowse"):
            assert jt in parsed, f"missing jobtype: {jt}"
            assert parsed[jt]["htc"]["RequestMemory"] == 8192
            assert parsed[jt]["htc"]["RequestCpus"] == 4


class TestYamlString:
    @pytest.mark.parametrize(["raw", "expected"], [
        ("hello", '"hello"'),
        ('a"b', '"a\\"b"'),
        ("", '""'),
    ])
    def test_quoting(self, raw: str, expected: str) -> None:
        assert yaml_string(raw) == expected


class TestProfileChain:
    def test_quickstart(self) -> None:
        assert profile_chain("quickstart") == ["quickstart"]

    def test_custom(self) -> None:
        assert profile_chain("custom") == ["quickstart", "custom"]


class TestDeepMerge:
    def test_scalar_overwrite(self) -> None:
        assert deep_merge({"a": 1}, {"a": 2}) == {"a": 2}

    def test_recursive_dict(self) -> None:
        result = deep_merge({"a": {"b": 1}}, {"a": {"c": 2}})
        assert result == {"a": {"b": 1, "c": 2}}

    def test_list_overwrite(self) -> None:
        # deep_merge overwrites lists (latter wins), it does not append
        assert deep_merge({"a": [1]}, {"a": [2]}) == {"a": [2]}

    def test_new_keys_added(self) -> None:
        assert deep_merge({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}


class TestBuildXrootdVoEntries:
    @pytest.mark.parametrize(["mounts", "expected"], [
        ([], []),
        (["/home/:/mnt/home"], ["/mnt/home/"]),
        (["/data/:/mnt/data:ro"], ["/mnt/data/"]),
        (["/a:/x", "/b:/y"], ["/x/", "/y/"]),
    ])
    def test_vo_entries(self, mounts: list[str], expected: list[str]) -> None:
        assert build_xrootd_vo_entries(mounts) == expected
