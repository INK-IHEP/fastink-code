import os
from pathlib import Path

import pytest
import yaml

from deploy.lib.paths import build_runtime_paths
from deploy.lib.render import build_mapping, render_config


EXPECTED_KRB5_VALIDATE = {
    "service_principal": "",
    "realm": "",
    "cache_ttl": 600,
    "negative_backoff": 5,
    "timeout": 5,
    "max_concurrency": 8,
}


@pytest.fixture
def sample_paths(tmp_path: Path) -> dict[str, Path]:
    _, paths = build_runtime_paths(
        output_dir=tmp_path / "output",
        data_root=tmp_path / "data",
        enable_nginx=False,
        enable_xrootd=False,
    )
    return paths


def render_test_config(
    sample_answers: dict[str, object],
    sample_paths: dict[str, Path],
    deploy_dir: Path,
    extra_overlays: list[Path] | None = None,
) -> dict:
    mapping = build_mapping("quickstart", sample_answers, sample_paths, deploy_dir)
    rendered = render_config("quickstart", mapping, extra_overlays=extra_overlays)
    return yaml.safe_load(rendered)


def test_krb5_validate_defaults(sample_answers, sample_paths, tmp_path):
    config = render_test_config(sample_answers, sample_paths, tmp_path / "deploy")

    section = config["auth"]["krb5_validate"]
    assert section == EXPECTED_KRB5_VALIDATE
    assert type(section["service_principal"]) is str
    assert type(section["realm"]) is str
    assert type(section["cache_ttl"]) is int
    assert type(section["negative_backoff"]) is int
    assert type(section["timeout"]) is int
    assert type(section["max_concurrency"]) is int


def test_krb5_validate_overlay_preserves_numeric_defaults(
    sample_answers, sample_paths, tmp_path
):
    overlay = tmp_path / "auth-overlay.yml"
    overlay.write_text(
        "auth:\n"
        "  krb5_validate:\n"
        "    service_principal: service/example@REALM\n"
        "    realm: REALM\n",
        encoding="utf-8",
    )

    config = render_test_config(
        sample_answers,
        sample_paths,
        tmp_path / "deploy",
        extra_overlays=[overlay],
    )

    assert config["auth"]["krb5_validate"] == {
        "service_principal": "service/example@REALM",
        "realm": "REALM",
        "cache_ttl": 600,
        "negative_backoff": 5,
        "timeout": 5,
        "max_concurrency": 8,
    }


def test_krb5_validate_ihep_overlay(sample_answers, sample_paths, tmp_path):
    overlay_path = os.environ.get("FASTINK_TEST_IHEP_OVERLAY")
    if not overlay_path:
        pytest.skip("FASTINK_TEST_IHEP_OVERLAY is not set")

    config = render_test_config(
        sample_answers,
        sample_paths,
        tmp_path / "deploy",
        extra_overlays=[Path(overlay_path)],
    )

    section = config["auth"]["krb5_validate"]
    assert section["service_principal"] == "xrootd/besproxy01.ihep.ac.cn@IHEPKRB5"
    assert section["realm"] == "IHEPKRB5"
