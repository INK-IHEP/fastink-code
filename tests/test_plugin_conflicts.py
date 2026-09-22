"""Unit tests for the uniform plugin conflict semantics.

One rule for hooks / computing apps / plugin routes:
registration order = load order, LAST registration wins,
every override leaves a warning in the logs.
"""

import logging

from fastapi import FastAPI, APIRouter
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from fastink.common import hooks as hooks_mod
from fastink.common.hooks import register_hook, get_hook
from fastink.common.plugin_interface import PluginManager


class TestHookOverrideWarning:
    def test_last_registration_wins_with_warning(self, caplog):
        name = "unit.test.conflict_semantics.hook_a"

        @register_hook(name)
        def first():
            return "first"

        with caplog.at_level(logging.WARNING, logger="ink"):
            @register_hook(name)
            def second():
                return "second"

        assert get_hook(name)() == "second"
        assert any("Hook override" in r.message for r in caplog.records)
        # cleanup
        hooks_mod._HOOKS_REGISTRY.pop(name, None)

    def test_reregistering_same_function_is_silent(self, caplog):
        name = "unit.test.conflict_semantics.hook_b"

        @register_hook(name)
        def fn():
            return 1

        with caplog.at_level(logging.WARNING, logger="ink"):
            register_hook(name)(fn)  # same object again (module re-import)

        assert not any("Hook override" in r.message for r in caplog.records)
        hooks_mod._HOOKS_REGISTRY.pop(name, None)


class TestRouteDedupLastWins:
    def _build_app_with_duplicates(self):
        app = FastAPI()
        builtin = APIRouter()

        @builtin.get("/api/v2/demo/get_value")
        def builtin_get_value():
            return {"who": "builtin"}

        app.include_router(builtin)

        plugin = APIRouter()

        @plugin.get("/api/v2/demo/get_value")
        def plugin_get_value():
            return {"who": "plugin"}

        @plugin.get("/api/v2/demo/get_other")
        def plugin_get_other():
            return {"who": "plugin-other"}

        app.include_router(plugin)
        return app

    @staticmethod
    def _collect_api_routes(container):
        """Recursively collect APIRoutes (handles _IncludedRouter wrappers)."""
        found = []
        for route in list(container):
            if isinstance(route, APIRoute):
                found.append(route)
                continue
            inner = getattr(route, "original_router", None)
            if inner is not None and hasattr(inner, "routes"):
                found.extend(TestRouteDedupLastWins._collect_api_routes(inner.routes))
        return found

    def test_last_route_wins_and_earlier_is_removed(self, caplog):
        app = self._build_app_with_duplicates()
        with caplog.at_level(logging.WARNING, logger="ink"):
            PluginManager.dedup_routes_last_wins(app)

        matching = [
            r for r in self._collect_api_routes(app.routes)
            if r.path == "/api/v2/demo/get_value"
        ]
        assert len(matching) == 1
        assert matching[0].endpoint.__name__ == "plugin_get_value"
        assert any("Route override" in r.message for r in caplog.records)
        # Runtime proof: the request actually reaches the plugin endpoint.
        client = TestClient(app)
        assert client.get("/api/v2/demo/get_value").json() == {"who": "plugin"}

    def test_non_duplicate_routes_untouched(self):
        app = self._build_app_with_duplicates()
        before_other = [
            r for r in self._collect_api_routes(app.routes)
            if r.path == "/api/v2/demo/get_other"
        ]
        PluginManager.dedup_routes_last_wins(app)
        after_other = [
            r for r in self._collect_api_routes(app.routes)
            if r.path == "/api/v2/demo/get_other"
        ]
        assert before_other == after_other
        assert len(after_other) == 1

    def test_partial_method_overlap_keeps_other_methods(self):
        """A GET+POST route losing only its GET to a later GET-only route
        must keep serving POST (methods are subtracted, not the route)."""
        app = FastAPI()
        early = APIRouter()

        @early.api_route("/api/v2/demo/do_multi", methods=["GET", "POST"])
        def early_multi():
            return {"who": "early"}

        app.include_router(early)
        late = APIRouter()

        @late.get("/api/v2/demo/do_multi")
        def late_get():
            return {"who": "late"}

        app.include_router(late)
        PluginManager.dedup_routes_last_wins(app)

        client = TestClient(app)
        assert client.get("/api/v2/demo/do_multi").json() == {"who": "late"}
        assert client.post("/api/v2/demo/do_multi").json() == {"who": "early"}


    def test_dedup_is_idempotent(self):
        """Running the pass twice must not remove anything further
        (main.py runs a second pass after direct routes register)."""
        app = self._build_app_with_duplicates()
        PluginManager.dedup_routes_last_wins(app)
        first = [r.path for r in self._collect_api_routes(app.routes)]
        PluginManager.dedup_routes_last_wins(app)
        second = [r.path for r in self._collect_api_routes(app.routes)]
        assert first == second

    def test_partial_overlap_regenerates_operation_id(self):
        """After method subtraction the OpenAPI operationId must not
        advertise the removed method."""
        app = FastAPI()
        early = APIRouter()

        @early.api_route("/api/v2/demo/do_stale_id", methods=["GET", "POST"])
        def early_multi2():
            return {}

        app.include_router(early)
        late = APIRouter()

        @late.get("/api/v2/demo/do_stale_id")
        def late_get2():
            return {}

        app.include_router(late)
        PluginManager.dedup_routes_last_wins(app)
        survivor = [
            r for r in self._collect_api_routes(app.routes)
            if r.path == "/api/v2/demo/do_stale_id"
            and r.endpoint.__name__ == "early_multi2"
        ]
        assert len(survivor) == 1
        assert survivor[0].methods == {"POST"}
        assert "get" not in survivor[0].unique_id.lower() or "post" in survivor[0].unique_id.lower()

    def test_different_methods_same_path_not_deduped(self):
        app = FastAPI()
        r = APIRouter()

        @r.get("/api/v2/demo/do_thing")
        def get_thing():
            return {}

        @r.post("/api/v2/demo/do_thing")
        def post_thing():
            return {}

        app.include_router(r)
        PluginManager.dedup_routes_last_wins(app)
        matching = [
            x for x in self._collect_api_routes(app.routes)
            if x.path == "/api/v2/demo/do_thing"
        ]
        assert len(matching) == 2


class TestAppRegistryEagerDiscover:
    def test_builtin_discover_then_plugin_override_wins(self, caplog):
        """Simulates the bootstrap order: eager discover() pins built-ins
        first, so a plugin app registered afterwards wins the name."""
        from fastink.computing.apps import registry
        from fastink.computing.apps.base import JobApp  # noqa: F401

        registry.discover()
        builtin_jupyter = registry.get("jupyter")

        with caplog.at_level(logging.WARNING, logger="ink"):
            @registry.register
            class FakePluginJupyter(JobApp):
                name = "jupyter"

        try:
            assert type(registry.get("jupyter")).__name__ == "FakePluginJupyter"
            assert any("collision" in r.message.lower() for r in caplog.records)
        finally:
            # restore the built-in
            registry._APPS["jupyter"] = builtin_jupyter
