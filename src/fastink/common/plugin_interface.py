"""
Unified plugin interface for FastINK.

Plugins are pip-installable packages that can add custom routers and hooks.
"""

from abc import ABC, abstractmethod
from typing import Optional
from fastapi import FastAPI

from fastink.common.logger import logger


class FastINKPlugin(ABC):
    """
    Abstract base class for FastINK plugins.

    To create a plugin:
    1. Inherit from this class
    2. Implement get_name() and get_version()
    3. Override register_routers() to add custom API endpoints
    4. Override register_hooks() to replace hookable functions
    """

    @abstractmethod
    def get_name(self) -> str:
        """Return the plugin name."""
        pass

    @abstractmethod
    def get_version(self) -> str:
        """Return the plugin version."""
        pass

    def register_routers(self, app: FastAPI) -> None:
        """
        Register custom routers with the FastAPI application.

        Example:
            from fastapi import APIRouter
            router = APIRouter()

            @router.get("/hello")
            def hello():
                return {"msg": "Hello"}

            app.include_router(router, prefix="/api/v2/myplugin")
        """
        pass

    def register_hooks(self) -> None:
        """
        Register custom hook functions.

        Use register_hook() to override hookable functions in fastink.

        Example:
            from fastink.common.hooks import register_hook

            def my_custom_permission_check(username: str, permission: str) -> bool:
                # Custom logic here
                return True

            register_hook("fastink.auth.permission.check_user_permission")(my_custom_permission_check)
        """
        pass

    def initialize(self) -> None:
        """
        Perform initialization tasks.

        Called after plugin is loaded, before routers and hooks are registered.
        """
        pass


class PluginManager:
    """Manages loading and registration of plugins."""

    def __init__(self):
        self.loaded_plugins: list[FastINKPlugin] = []

    def load_plugin_from_package(self, package_name: str) -> Optional[FastINKPlugin]:
        """
        Load a plugin from an installed package.

        The package should have a Plugin class that inherits from FastINKPlugin.
        """
        import importlib
        import sys
        import os

        try:
            # Try to import the package
            plugin_module = importlib.import_module(package_name)
        except ImportError:
            # Try path-based import for development
            plugin_base_path = '/ink/fastink-plugins'
            if os.path.exists(plugin_base_path) and plugin_base_path not in sys.path:
                sys.path.insert(0, plugin_base_path)

            try:
                plugin_module = importlib.import_module(package_name)
            except ImportError as e:
                logger.error(f"Failed to import plugin '{package_name}': {e}")
                return None

        # Look for a Plugin class
        plugin_class = getattr(plugin_module, 'Plugin', None)
        if plugin_class and isinstance(plugin_class, type) and issubclass(plugin_class, FastINKPlugin):
            try:
                return plugin_class()
            except Exception as e:
                logger.error(f"Failed to instantiate Plugin class from '{package_name}': {e}")
                return None

        # Fallback: look for any class that inherits from FastINKPlugin
        for attr_name in dir(plugin_module):
            attr = getattr(plugin_module, attr_name)
            if (isinstance(attr, type) and
                issubclass(attr, FastINKPlugin) and
                attr is not FastINKPlugin):
                try:
                    return attr()
                except Exception:
                    continue

        logger.error(f"No valid Plugin class found in '{package_name}'")
        return None

    def load_plugins_from_config(self, config_section: str = "unified_plugins") -> None:
        """Load plugins based on configuration."""
        from fastink.common.config import get_config

        plugin_packages_str = get_config(config_section, "packages", "")

        if not plugin_packages_str:
            logger.info("No plugins configured")
            return

        plugin_packages = [name.strip() for name in plugin_packages_str.split(",") if name.strip()]

        for package_name in plugin_packages:
            logger.info(f"Loading plugin: {package_name}")

            plugin = self.load_plugin_from_package(package_name)
            if plugin:
                try:
                    plugin.initialize()
                    self.loaded_plugins.append(plugin)
                    logger.info(f"Loaded plugin: {plugin.get_name()} v{plugin.get_version()}")
                except Exception as e:
                    logger.error(f"Failed to initialize plugin '{package_name}': {e}")
            else:
                logger.error(f"Failed to load plugin: {package_name}")

    def register_plugin_routers(self, app: FastAPI) -> None:
        """Register all plugin routers.

        After all plugins have added their routes, duplicate API routes
        (same path + overlapping HTTP methods) are deduplicated so that
        the LAST registration wins — matching the conflict semantics of
        hooks and computing apps (load order = priority, override is
        logged). Without this, Starlette matches routes in registration
        order, so an earlier (e.g. built-in) route would silently shadow
        a plugin route that was meant to replace it.
        """

        for plugin in self.loaded_plugins:
            try:
                plugin.register_routers(app)
                logger.info(f"Registered routers for: {plugin.get_name()}")
            except Exception as e:
                logger.error(f"Failed to register routers for '{plugin.get_name()}': {e}")

        # First pass: resolve conflicts among built-in + plugin routes
        # known at this point. main.py runs a second pass after its
        # directly-decorated routes (/health, /version, ...) are added,
        # because those register AFTER plugin routers. Idempotent.
        self.dedup_routes_last_wins(app)

    @staticmethod
    def dedup_routes_last_wins(app: FastAPI) -> None:
        """Drop earlier duplicates of (path, method) pairs, keeping the last.

        Only APIRoute entries are considered (mounts/websockets untouched).
        Every removal is logged as a warning with both endpoints so route
        overrides are visible in production logs.

        Handles both routing layouts:
        - classic FastAPI: APIRoute objects sit directly in app.routes;
        - newer FastAPI (>= 0.13x _IncludedRouter): include_router() adds
          a lazy wrapper and the APIRoutes stay inside the wrapped
          router's own .routes list. We walk wrappers recursively and
          remove shadowed routes from whichever container owns them.
        """
        from fastapi.routing import APIRoute

        def collect(container) -> list:
            """Yield (route, owning_list) for every APIRoute, in
            registration order, recursing into included routers."""
            found = []
            for route in list(container):
                if isinstance(route, APIRoute):
                    found.append((route, container))
                    continue
                inner = getattr(route, "original_router", None)
                if inner is not None and hasattr(inner, "routes"):
                    found.extend(collect(inner.routes))
            return found

        entries = collect(app.routes)

        seen: dict = {}
        # route -> set of methods that are shadowed by a later registration
        shadowed_methods: dict = {}
        # Walk in reverse so the LAST registered route claims each key.
        for route, owner in reversed(entries):
            for method in sorted(route.methods or []):
                key = (route.path, method)
                winner = seen.get(key)
                if winner is None:
                    seen[key] = route
                elif winner is not route:
                    logger.warning(
                        "Route override: %s %s from %s.%s is shadowed; "
                        "%s.%s wins (last registration wins)",
                        method, route.path,
                        route.endpoint.__module__, route.endpoint.__name__,
                        winner.endpoint.__module__, winner.endpoint.__name__,
                    )
                    shadowed_methods.setdefault(id(route), (route, owner, set()))[2].add(method)

        for route, owner, methods in shadowed_methods.values():
            remaining = set(route.methods or []) - methods
            if remaining:
                # Partial overlap: only subtract the conflicting methods so
                # the route keeps serving its non-shadowed methods
                # (e.g. a GET+POST route loses only its GET to a later
                # GET-only registration).
                route.methods = remaining
                # Recompute the OpenAPI operationId: it was derived from the
                # original method set at construction time and would
                # otherwise advertise a removed method.
                from fastapi.datastructures import DefaultPlaceholder

                generator = getattr(route, "generate_unique_id_function", None)
                if route.operation_id is None and generator is not None:
                    if isinstance(generator, DefaultPlaceholder):
                        fn = generator.value
                    else:
                        fn = generator
                    try:
                        route.unique_id = fn(route)
                    except Exception as exc:  # pragma: no cover - defensive
                        logger.warning(
                            "Route dedup: failed to regenerate operationId "
                            "for %s after method subtraction: %s",
                            route.path, exc,
                        )
                continue
            try:
                owner.remove(route)
            except ValueError:
                logger.error(
                    "Route dedup: could not remove shadowed route %s", route.path
                )

    def register_plugin_hooks(self) -> None:
        """Register all plugin hooks."""

        for plugin in self.loaded_plugins:
            try:
                plugin.register_hooks()
                logger.info(f"Registered hooks for: {plugin.get_name()}")
            except Exception as e:
                logger.error(f"Failed to register hooks for '{plugin.get_name()}': {e}")


# Global plugin manager instance
plugin_manager = PluginManager()
