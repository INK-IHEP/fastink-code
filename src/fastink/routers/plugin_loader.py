"""Dynamic plugin loader for custom routers in FastINK."""

import importlib
from pathlib import Path
from typing import List

from fastapi import FastAPI
from fastink.common.logger import logger
from fastink.common.config import get_config


def load_router_plugins(app: FastAPI, config_section: str = "plugins") -> List[str]:
    """
    Dynamically load router plugins from the plugins directory based on configuration.
    
    Args:
        app: FastAPI application instance
        config_section: Configuration section name for plugins
        
    Returns:
        List of loaded plugin names
    """
    # Get the plugin names from configuration
    plugin_names_str = get_config(config_section, "router_plugins", "")
    
    if not plugin_names_str:
        logger.info("No router plugins configured")
        return []
    
    # Parse plugin names (comma-separated)
    plugin_names = [name.strip() for name in plugin_names_str.split(",") if name.strip()]
    
    if not plugin_names:
        logger.info("No router plugins to load")
        return []
    
    loaded_plugins = []
    
    # Get the plugins directory
    plugins_dir = Path(__file__).parent / "plugins"
    
    for plugin_name in plugin_names:
        try:
            # Import the plugin module
            module_path = f"fastink.routers.plugins.{plugin_name}"
            plugin_module = importlib.import_module(module_path)
            
            # Check if the module has a 'router' attribute
            if hasattr(plugin_module, 'router'):
                router = plugin_module.router
                
                # Include the router in the main app with a prefix
                app.include_router(router, prefix=f"/api/v2/{plugin_name}")
                
                logger.info(f"Successfully loaded router plugin: {plugin_name}")
                loaded_plugins.append(plugin_name)
            else:
                logger.warning(f"Plugin {plugin_name} does not have a 'router' attribute")
                
        except ImportError as e:
            logger.error(f"Failed to import router plugin '{plugin_name}': {e}")
        except Exception as e:
            logger.error(f"Unexpected error loading router plugin '{plugin_name}': {e}")
    
    return loaded_plugins


def discover_available_plugins() -> List[str]:
    """
    Discover all available router plugins in the plugins directory.
    
    Returns:
        List of available plugin names
    """
    plugins_dir = Path(__file__).parent / "plugins"
    available_plugins = []
    
    if plugins_dir.exists():
        for file_path in plugins_dir.iterdir():
            if (file_path.is_file() and 
                file_path.suffix == '.py' and 
                file_path.name != '__init__.py'):
                plugin_name = file_path.stem
                available_plugins.append(plugin_name)
    
    return available_plugins