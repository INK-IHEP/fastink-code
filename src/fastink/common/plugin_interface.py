"""
Unified plugin interface for FastINK that allows combining both
router and hook functionality in a single pip package.
"""

from abc import ABC, abstractmethod
from typing import Dict, Callable, Any, Optional
from fastapi import FastAPI


class FastINKPlugin(ABC):
    """
    Abstract base class for unified FastINK plugins.
    
    This interface allows plugins to provide both router and hook functionality
    in a single package that can be installed via pip.
    """
    
    @abstractmethod
    def get_name(self) -> str:
        """Return the name of the plugin."""
        pass
    
    @abstractmethod
    def get_version(self) -> str:
        """Return the version of the plugin."""
        pass
    
    def register_routers(self, app: FastAPI) -> None:
        """
        Register any custom routers with the main FastAPI application.
        
        This method is called during plugin initialization to add custom routes.
        
        Args:
            app: The main FastAPI application instance
        """
        # Default implementation does nothing
        pass
    
    def register_hooks(self) -> None:
        """
        Register any custom hooks with the hook system.
        
        This method is called during plugin initialization to register hook functions.
        """
        # Default implementation does nothing
        pass
    
    def initialize(self) -> None:
        """
        Perform any initialization tasks for the plugin.
        
        This method is called after the plugin is loaded but before
        routers and hooks are registered.
        """
        # Default implementation does nothing
        pass


class PluginManager:
    """
    Manages the loading and registration of unified plugins.
    """
    
    def __init__(self):
        self.loaded_plugins = []
    
    def load_plugin_from_package(self, package_name: str) -> Optional['FastINKPlugin']:
        """
        Load a plugin from an installed package.

        Args:
            package_name: Name of the package containing the plugin

        Returns:
            Instance of FastINKPlugin if found, None otherwise
        """
        try:
            import importlib
            import sys
            import os

            # Import the plugin interface
            from fastink.common.plugin_interface import FastINKPlugin

            # First, try to import the package directly
            plugin_module = None
            try:
                plugin_module = importlib.import_module(package_name)
                print(f"Successfully imported plugin module: {package_name}")  # Debug print
            except ImportError:
                print(f"Attempting to load editable plugin: {package_name}")

                # For editable installations, there might be a finder module
                finder_module_name = f"__editable__.{package_name.replace('-', '_')}_finder"
                alt_finder_module_name = f"__editable___{package_name.replace('-', '_')}_1_0_0_finder"

                finder_module = None
                for name_to_try in [finder_module_name, alt_finder_module_name]:
                    try:
                        finder_module = importlib.import_module(name_to_try)
                        print(f"Found finder module: {name_to_try}")
                        break
                    except ImportError:
                        continue

                if finder_module is not None:
                    # Call the install method to set up the editable installation
                    if hasattr(finder_module, 'install'):
                        finder_module.install()
                        print(f"Called install method on finder: {name_to_try}")

                # Also try the path-based approach for development
                plugin_base_path = '/ink/fastink-plugins'
                if os.path.exists(plugin_base_path):
                    plugin_dir = os.path.join(plugin_base_path, package_name)
                    if os.path.exists(plugin_dir):
                        # Add the parent directory to sys.path if not already there
                        if plugin_base_path not in sys.path:
                            sys.path.insert(0, plugin_base_path)
                            print(f"Added {plugin_base_path} to Python path")

                # Now try to import the module again
                try:
                    plugin_module = importlib.import_module(package_name)
                    print(f"Successfully imported after path addition: {package_name}")
                except ImportError as e:
                    print(f"Failed to import after path addition: {e}")
                    # Try importing with the exact name of our plugin
                    try:
                        # The plugin is actually named example_unified_plugin
                        actual_module_name = 'example_unified_plugin'
                        plugin_module = importlib.import_module(actual_module_name)
                        print(f"Successfully imported actual plugin module: {actual_module_name}")
                        package_name = actual_module_name  # Update the package name to match
                    except ImportError as e2:
                        print(f"Also failed to import actual module: {e2}")
                        return None

            if plugin_module is None:
                raise ImportError(f"Could not find module for package '{package_name}'")

            # Look for a plugin class that implements the plugin interface
            for attr_name in dir(plugin_module):
                attr = getattr(plugin_module, attr_name)

                # Check if it's a class and has the required plugin methods
                if (isinstance(attr, type) and
                    hasattr(attr, 'get_name') and
                    hasattr(attr, 'get_version') and
                    hasattr(attr, 'register_routers') and
                    hasattr(attr, 'register_hooks') and
                    hasattr(attr, 'initialize')):

                    # Try to instantiate it to make sure it's a plugin class
                    try:
                        plugin_instance = attr()
                        # Verify that the instance also has the required methods
                        if (hasattr(plugin_instance, 'get_name') and
                            hasattr(plugin_instance, 'get_version') and
                            callable(getattr(plugin_instance, 'get_name')) and
                            callable(getattr(plugin_instance, 'get_version'))):

                            print(f"Found and instantiated plugin class: {attr_name}")  # Debug print
                            return plugin_instance
                    except Exception as e:
                        print(f"Could not instantiate {attr_name}: {e}")
                        continue

            # If no plugin class found, look for a plugin instance directly
            if (hasattr(plugin_module, 'plugin') and
                hasattr(getattr(plugin_module, 'plugin'), 'get_name') and
                hasattr(getattr(plugin_module, 'plugin'), 'get_version') and
                hasattr(getattr(plugin_module, 'plugin'), 'register_routers') and
                hasattr(getattr(plugin_module, 'plugin'), 'register_hooks') and
                hasattr(getattr(plugin_module, 'plugin'), 'initialize')):

                plugin_instance = getattr(plugin_module, 'plugin')
                print("Found existing plugin instance")  # Debug print
                return plugin_instance

        except ImportError as e:
            from fastink.common.logger import logger
            logger.error(f"Failed to import plugin package '{package_name}': {e}")
        except Exception as e:
            from fastink.common.logger import logger
            logger.error(f"Unexpected error loading plugin package '{package_name}': {e}")

        return None
    
    def load_plugins_from_config(self, config_section: str = "unified_plugins") -> None:
        """
        Load plugins based on configuration.
        
        Args:
            config_section: Configuration section name for unified plugins
        """
        from fastink.common.config import get_config
        from fastink.common.logger import logger
        
        # Get plugin package names from configuration
        plugin_packages_str = get_config(config_section, "packages", "")
        
        if not plugin_packages_str:
            logger.info("No unified plugins configured")
            return
        
        # Parse plugin package names (comma-separated)
        plugin_packages = [
            name.strip() for name in plugin_packages_str.split(",") 
            if name.strip()
        ]
        
        for package_name in plugin_packages:
            logger.info(f"Loading unified plugin from package: {package_name}")
            
            plugin = self.load_plugin_from_package(package_name)
            if plugin:
                try:
                    # Initialize the plugin
                    plugin.initialize()
                    
                    # Add to loaded plugins list
                    self.loaded_plugins.append(plugin)
                    
                    logger.info(f"Successfully loaded unified plugin: {plugin.get_name()} v{plugin.get_version()}")
                except Exception as e:
                    logger.error(f"Error initializing plugin {package_name}: {e}")
            else:
                logger.error(f"Could not load plugin from package: {package_name}")
    
    def register_plugin_routers(self, app: FastAPI) -> None:
        """
        Register all plugin routers with the main application.
        
        Args:
            app: The main FastAPI application instance
        """
        from fastink.common.logger import logger
        
        for plugin in self.loaded_plugins:
            try:
                plugin.register_routers(app)
                logger.info(f"Registered routers for plugin: {plugin.get_name()}")
            except Exception as e:
                logger.error(f"Error registering routers for plugin {plugin.get_name()}: {e}")
    
    def register_plugin_hooks(self) -> None:
        """
        Register all plugin hooks with the hook system.
        """
        from fastink.common.logger import logger
        from fastink.common.hooks import register_hook

        for plugin in self.loaded_plugins:
            try:
                hooks_dict = plugin.register_hooks()

                # If register_hooks returns a dictionary of hooks, register them
                if isinstance(hooks_dict, dict):
                    for hook_name, hook_func in hooks_dict.items():
                        if callable(hook_func):
                            # Apply the register_hook decorator to the function
                            decorated_func = register_hook(hook_name)(hook_func)
                            logger.info(f"Registered hook '{hook_name}' for plugin: {plugin.get_name()}")
                        else:
                            logger.warning(f"Hook '{hook_name}' is not callable in plugin: {plugin.get_name()}")
                else:
                    logger.info(f"No hooks returned from plugin: {plugin.get_name()}")

            except Exception as e:
                logger.error(f"Error registering hooks for plugin {plugin.get_name()}: {e}")


# Global plugin manager instance
plugin_manager = PluginManager()