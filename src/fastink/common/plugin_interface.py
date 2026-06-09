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
        """Register all plugin routers."""

        for plugin in self.loaded_plugins:
            try:
                plugin.register_routers(app)
                logger.info(f"Registered routers for: {plugin.get_name()}")
            except Exception as e:
                logger.error(f"Failed to register routers for '{plugin.get_name()}': {e}")

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
