"""Render utility tests: deep_merge, profile_chain, Jinja2 rendering pipeline."""
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from deploy.lib.render import (
    deep_merge,
    profile_chain,
    build_xrootd_vo_entries,
    build_mapping,
    render_config,
    render_compose,
    render_bundle,
    build_compose_port_overlay,
    build_compose_volume_overlay,
    render_template_text,
)


class TestDeepMerge:
    def test_scalar_overwrite(self) -> None:
        assert deep_merge({"a": 1}, {"a": 2}) == {"a": 2}

    def test_recursive_dict(self) -> None:
        result = deep_merge({"a": {"b": 1}}, {"a": {"c": 2}})
        assert result == {"a": {"b": 1, "c": 2}}

    def test_list_overwrite(self) -> None:
        assert deep_merge({"a": [1]}, {"a": [2]}) == {"a": [2]}

    def test_list_extend(self) -> None:
        assert deep_merge({"a": [1]}, {"a": [2]}, list_strategy="extend") == {"a": [1, 2]}

    def test_list_extend_recursive(self) -> None:
        base = {"services": {"srv": {"volumes": ["/a:/a"]}}}
        overlay = {"services": {"srv": {"volumes": ["/b:/b"]}}}
        result = deep_merge(base, overlay, list_strategy="extend")
        assert result == {"services": {"srv": {"volumes": ["/a:/a", "/b:/b"]}}}

    def test_list_extend_new_service(self) -> None:
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


class TestProfileChain:
    def test_quickstart(self) -> None:
        assert profile_chain("quickstart") == ["quickstart"]

    def test_custom(self) -> None:
        assert profile_chain("custom") == ["quickstart", "custom"]


class TestBuildXrootdVoEntries:
    @pytest.mark.parametrize(["mounts", "expected"], [
        ([], []),
        (["/home/:/mnt/home"], ["/mnt/home/"]),
        (["/data/:/mnt/data:ro"], ["/mnt/data/"]),
        (["/a:/x", "/b:/y"], ["/x/", "/y/"]),
    ])
    def test_vo_entries(self, mounts: list[str], expected: list[str]) -> None:
        assert build_xrootd_vo_entries(mounts) == expected


# ---------------------------------------------------------------------------
# Rendering pipeline tests
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_paths(tmp_path: Path) -> dict[str, Path]:
    data_root = tmp_path / "data"
    data_root.mkdir()
    keys_dir = tmp_path / "keys"
    keys_dir.mkdir(parents=True)
    (keys_dir / "ssh-client").mkdir(parents=True)
    preload = data_root / "preload"
    (preload / "server").mkdir(parents=True)
    (preload / "cron").mkdir(parents=True)
    (preload / "rootbrowse").mkdir(parents=True)
    xrootd_data_dir = data_root / "xrootd"
    xrootd_data_dir.mkdir(parents=True)
    return {
        "data_root": data_root,
        "db_data_dir": data_root / "db",
        "redis_data_dir": data_root / "redis",
        "etc_init_dir": data_root / "etc-init",
        "tmp_dir": data_root / "tmp",
        "plugins_dir": data_root / "plugins",
        "keys_dir": keys_dir,
        "preload_server_dir": preload / "server",
        "preload_cron_dir": preload / "cron",
        "preload_rootbrowse_dir": preload / "rootbrowse",
        "xrootd_data_dir": xrootd_data_dir,
    }


@pytest.fixture
def sample_answers() -> dict:
    return {
        "host_name": "localhost",
        "host_port": "8000",
        "rootbrowse_port": "2001",
        "public_base_url": "http://localhost:8000",
        "image_source": "pull",
        "server_image": "dockerhub.ihep.ac.cn/ink/fastink-server:latest",
        "cron_image": "dockerhub.ihep.ac.cn/ink/fastink-cron:latest",
        "rootbrowse_image": "dockerhub.ihep.ac.cn/ink/fastink-rootbrowse:latest",
        "htcondor_image": "dockerhub.ihep.ac.cn/ink/fastink-htcondor:latest",
        "xrootd_image": "dockerhub.ihep.ac.cn/ink/xrootd-multiuser:5.9.0-3",
        "project_name": "fastink",
        "db_name": "fastink",
        "db_user": "fastink",
        "db_password": "secret",
        "db_root_password": "rootsecret",
        "redis_password": "redispass",
        "workers": "4",
        "ink_production": "false",
        "init_database": "true",
        "enable_nginx": "true",
        "enable_xrootd": "true",
        "enable_krb5": "false",
        "enable_local_htcondor": "false",
        "schedd_host": "localhost",
        "cm_host": "localhost",
        "htcondor_internal_domain": "local",
        "xrootd_krb5_keytab_source_path": "",
        "xrootd_krb5_principal": "",
        "krb5_conf_host_path": "/etc/krb5.conf",
        "enable_host_slurm_client": "false",
        "slurm_conf_host_path": "/etc/slurm/slurm.conf",
        "munge_socket_dir": "/var/run/munge",
        "extra_mounts_file": "",
        "server_preload_script_dirs": "",
        "server_preload_scripts": "",
        "cron_preload_script_dirs": "",
        "cron_preload_scripts": "",
        "rootbrowse_preload_script_dirs": "",
        "rootbrowse_preload_scripts": "",
        "plugin_pip_packages": "",
        "plugin_editable_dirs": "",
        "xrootd_port": "1094",
    }


class TestBuildMapping:
    def test_produces_expected_keys(self, sample_answers, sample_paths):
        mapping = build_mapping("quickstart", sample_answers, sample_paths, Path(tempfile.mkdtemp()))
        assert mapping["server_image"] == "dockerhub.ihep.ac.cn/ink/fastink-server:latest"
        assert mapping["db_name"] == "fastink"
        assert mapping["enable_nginx"] == "true"
        assert mapping["krb5_enabled"] == "false"
        # No legacy suffixes
        assert "host_name_yaml" not in mapping
        assert "server_image_raw" not in mapping
        assert "db_name_yaml" not in mapping

    def test_with_krb5(self, sample_answers, sample_paths):
        sample_answers["enable_krb5"] = "true"
        mapping = build_mapping("quickstart", sample_answers, sample_paths, Path(tempfile.mkdtemp()))
        assert mapping["auth_type"] == "krb5"

    def test_cluster_list(self, sample_answers, sample_paths):
        mapping = build_mapping("quickstart", sample_answers, sample_paths, Path(tempfile.mkdtemp()))
        assert mapping["cluster_list"] == ["htcondor"]
        # noenv_jobtype / start_keywords / jobtype_defaults are no longer
        # part of the render mapping -- they are now owned by the JobApp
        # classes under fastink.computing.apps and read at runtime through
        # fastink.computing.apps.registry.
        assert "noenv_jobtype" not in mapping
        assert "start_keywords" not in mapping
        assert "jobtype_defaults" not in mapping


class TestRenderConfig:
    def test_produces_valid_yaml(self, sample_answers, sample_paths):
        mapping = build_mapping("quickstart", sample_answers, sample_paths, Path(tempfile.mkdtemp()))
        result = render_config("quickstart", mapping)
        data = yaml.safe_load(result)
        assert data["common"]["krb5_enabled"] is False
        assert data["database"]["user"] == "fastink"
        assert "htcondor" in data["computing"]["cluster_list"]

    def test_with_profile_chain_custom(self, sample_answers, sample_paths):
        mapping = build_mapping("custom", sample_answers, sample_paths, Path(tempfile.mkdtemp()))
        result = render_config("custom", mapping)
        data = yaml.safe_load(result)
        assert data is not None


class TestRenderCompose:
    def test_produces_valid_yaml(self, sample_answers, sample_paths):
        mapping = build_mapping("quickstart", sample_answers, sample_paths, Path(tempfile.mkdtemp()))
        port_overlay = build_compose_port_overlay(enable_nginx=False, host_port=8000)
        result = render_compose(
            "quickstart", mapping,
            enable_nginx=False, enable_xrootd=False,
            port_overlay=port_overlay,
        )
        data = yaml.safe_load(result)
        assert "fastink-db" in data["services"]
        assert "fastink-redis" in data["services"]
        assert "fastink-server" in data["services"]

    def test_with_nginx(self, sample_answers, sample_paths):
        mapping = build_mapping("quickstart", sample_answers, sample_paths, Path(tempfile.mkdtemp()))
        port_overlay = build_compose_port_overlay(enable_nginx=True, host_port=443)
        result = render_compose(
            "quickstart", mapping,
            enable_nginx=True, enable_xrootd=False,
            port_overlay=port_overlay,
        )
        data = yaml.safe_load(result)
        assert "fastink-nginx" in data["services"]
        assert "expose" in data["services"]["fastink-server"]


class TestRenderBundle:
    def test_basic(self, sample_answers, sample_paths, tmp_path):
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        bundle = render_bundle(
            "quickstart", sample_answers, sample_paths, deploy_dir,
            initialize_host_assets=False,
        )
        assert "config.yml" in bundle
        assert ".env" in bundle
        assert "docker-compose.yml" in bundle
        assert "condor/ink.conf" in bundle
        assert bundle[".env"].startswith("PROFILE=quickstart")

    def test_jinja2_renders_yaml_quoting(self, sample_answers, sample_paths, tmp_path):
        """Verify Jinja2 |to_yaml filter correctly quotes YAML values."""
        sample_answers["db_password"] = "secret!with@special#chars"
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        bundle = render_bundle(
            "quickstart", sample_answers, sample_paths, deploy_dir,
            initialize_host_assets=False,
        )
        config_yml = bundle["config.yml"]
        assert "secret!with@special#chars" in config_yml
        data = yaml.safe_load(config_yml)
        assert data["database"]["password"] == "secret!with@special#chars"

    def test_without_nginx(self, sample_answers, sample_paths, tmp_path):
        sample_answers["enable_nginx"] = "false"
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        bundle = render_bundle(
            "quickstart", sample_answers, sample_paths, deploy_dir,
            initialize_host_assets=False,
        )
        assert "nginx/default.conf" not in bundle

    def test_node_domain_suffix_default_empty(self, sample_answers, sample_paths, tmp_path):
        """Without a suffix, nginx upstreams use the bare node name."""
        sample_answers["enable_nginx"] = "true"
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        bundle = render_bundle(
            "quickstart", sample_answers, sample_paths, deploy_dir,
            initialize_host_assets=False,
        )
        vscode_conf = bundle["nginx/locations/vscode.conf"]
        assert "proxy_pass http://$node:$port" in vscode_conf
        assert ".ihep.ac.cn" not in vscode_conf
        data = yaml.safe_load(bundle["config.yml"])
        assert data["computing"]["node_domain_suffix"] == ""

    def test_node_domain_suffix_applied(self, sample_answers, sample_paths, tmp_path):
        """A configured suffix is appended to nginx upstream node names."""
        sample_answers["enable_nginx"] = "true"
        sample_answers["node_domain_suffix"] = ".example.org"
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        bundle = render_bundle(
            "quickstart", sample_answers, sample_paths, deploy_dir,
            initialize_host_assets=False,
        )
        assert "proxy_pass http://$node.example.org:$port" in bundle["nginx/locations/vscode.conf"]
        assert "proxy_pass http://$node.example.org:$port" in bundle["nginx/locations/jupyter.conf"]
        data = yaml.safe_load(bundle["config.yml"])
        assert data["computing"]["node_domain_suffix"] == ".example.org"

    def test_jobtype_defaults_in_config(self, sample_answers, sample_paths, tmp_path):
        """The jobtype: block in config.yml is now an empty site-override
        hook -- per-app defaults live on the JobApp classes and are merged
        at submit time by generate_condor_submit.
        """
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        bundle = render_bundle(
            "quickstart", sample_answers, sample_paths, deploy_dir,
            initialize_host_assets=False,
        )
        data = yaml.safe_load(bundle["config.yml"])
        assert data["jobtype"] == {}

    def test_apps_nginx_snippets_bundled(self, sample_answers, sample_paths, tmp_path):
        """Per-app nginx locations are pulled from src/fastink/computing/apps."""
        sample_answers["enable_nginx"] = "true"
        deploy_dir = tmp_path / "deploy"
        deploy_dir.mkdir()
        bundle = render_bundle(
            "quickstart", sample_answers, sample_paths, deploy_dir,
            initialize_host_assets=False,
        )
        # Built-in apps only: IHEP-specific apps (openclaw, herddisplay, ...)
        # live in fastink-plugins-ihep; their nginx snippets are provided by
        # the fastink-dev locations-overlay extension point.
        for name in ("vscode", "vnc", "jupyter", "rootbrowse"):
            key = f"nginx/locations/{name}.conf"
            assert key in bundle, f"missing {key} in rendered nginx bundle"
        for name in ("openclaw", "herddisplay"):
            assert f"nginx/locations/{name}.conf" not in bundle


class TestRenderComposeVolumeOverlay:
    def test_volume_overlay_extends_existing_volumes(self, sample_answers, sample_paths):
        mapping = build_mapping("quickstart", sample_answers, sample_paths, Path(tempfile.mkdtemp()))
        volume_overlay = build_compose_volume_overlay(
            extra_mount_entries=["/host/data:/container/data:ro"],
            enable_krb5=False,
            krb5_conf_host_path="/etc/krb5.conf",
            enable_host_slurm_client=False,
            slurm_conf_host_path="/etc/slurm/slurm.conf",
            munge_socket_dir="/var/run/munge",
            enable_xrootd=False,
            enable_local_htcondor=False,
        )
        port_overlay = build_compose_port_overlay(enable_nginx=False, host_port=8000)
        result = render_compose(
            "quickstart", mapping,
            enable_nginx=False, enable_xrootd=False,
            volume_overlay=volume_overlay,
            port_overlay=port_overlay,
        )
        data = yaml.safe_load(result)
        server_vols = data["services"]["fastink-server"]["volumes"]
        assert "/container/data:ro" in server_vols[-1]
        assert len(server_vols) >= 6  # base volumes plus extra mount


class TestStrictUndefined:
    def test_missing_variable_raises_undefined_error(self, tmp_path):
        tpl = tmp_path / "test.tpl"
        tpl.write_text("hello {{ name }}")
        with pytest.raises(Exception) as exc_info:
            render_template_text(tpl, {"not_name": "world"})
        assert "name" in str(exc_info.value) or "Undefined" in str(type(exc_info.value).__name__)
