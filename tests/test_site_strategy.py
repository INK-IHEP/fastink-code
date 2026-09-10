"""Unit tests for the site strategy registry and its generic defaults."""

import pytest

from fastink.computing.site.strategy import (
    get_site,
    get_submitter,
    register_site,
    register_submitter,
)
from fastink.computing.tools.common.utils import (
    get_extra_job_config,
    map_group_to_experiment,
)


class TestSiteRegistry:
    def test_generic_site_registered(self):
        assert callable(get_site("generic"))

    def test_generic_submitters_registered(self):
        assert callable(get_submitter("generic", "slurm"))
        assert callable(get_submitter("generic", "htcondor"))

    def test_unknown_site_raises_lookup_error_with_hint(self):
        with pytest.raises(LookupError) as excinfo:
            get_site("no-such-site")
        message = str(excinfo.value)
        assert "no-such-site" in message
        assert "generic" in message  # lists available sites

    def test_submitter_falls_back_to_generic(self):
        # A site with no registered submitters resolves to generic ones.
        fallback = get_submitter("no-such-site", "slurm")
        assert fallback is get_submitter("generic", "slurm")

    def test_site_specific_submitter_wins_over_generic(self):
        @register_submitter("unit-test-site", "slurm")
        async def _submit(*args):  # pragma: no cover - never called
            return None

        assert get_submitter("unit-test-site", "slurm") is _submit

    def test_unknown_scheduler_raises(self):
        with pytest.raises(LookupError):
            get_submitter("generic", "no-such-scheduler")


class TestHookDefaults:
    def test_map_group_to_experiment_defaults_to_none(self):
        # Without a site plugin no experiment mapping exists.
        assert map_group_to_experiment._original_func("juno") is None

    def test_get_extra_job_config_default(self):
        config = get_extra_job_config._original_func("alice", "users", "jupyter")
        assert config == {"concurrency_limits": "inkjob_alice_jupyter"}

    def test_get_extra_job_config_default_ignores_request_os(self):
        config = get_extra_job_config._original_func(
            "alice", "users", "jupyter", request_os="AlmaLinux9"
        )
        assert "+HepJob_RequestOS" not in config
