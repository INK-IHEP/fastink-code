"""Render utility tests: YAML block generation, deep_merge, profile_chain, etc."""
import pytest
import yaml
from deploy.lib.render import (
    render_yaml_list_block,
    default_jobtype_config_block,
    yaml_string,
    profile_chain,
    deep_merge,
    build_xrootd_vo_entries,
)


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

    def test_list_extend(self) -> None:
        # list_strategy="extend" concatenates lists
        assert deep_merge({"a": [1]}, {"a": [2]}, list_strategy="extend") == {"a": [1, 2]}

    def test_list_extend_recursive(self) -> None:
        # extend strategy propagates through nested dicts
        base = {"services": {"srv": {"volumes": ["/a:/a"]}}}
        overlay = {"services": {"srv": {"volumes": ["/b:/b"]}}}
        result = deep_merge(base, overlay, list_strategy="extend")
        assert result == {"services": {"srv": {"volumes": ["/a:/a", "/b:/b"]}}}

    def test_list_extend_new_service(self) -> None:
        # extend with a new key works like regular merge
        base = {"services": {"srv1": {"volumes": ["/a:/a"]}}}
        overlay = {"services": {"srv2": {"volumes": ["/b:/b"]}}}
        result = deep_merge(base, overlay, list_strategy="extend")
        assert result == {
            "services": {
                "srv1": {"volumes": ["/a:/a"]},
                "srv2": {"volumes": ["/b:/b"]},
            }
        }

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
