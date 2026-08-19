"""Runtime plugin/hook bootstrap shared by the API server and cron runner.

The FastAPI server (main.py) and the redis-cron runner both need
unified plugins loaded and their hooks registered before any business
logic runs; only router registration is server-specific and stays in
main.py.
"""

from fastink.common.hooks import load_instance_hooks
from fastink.common.logger import logger
from fastink.common.plugin_interface import plugin_manager

_initialized = False


def init_plugins() -> None:
    """Load unified plugins and activate their hooks. Idempotent."""
    global _initialized
    if _initialized:
        return

    # Eagerly register the built-in computing apps BEFORE plugins load.
    # The apps registry normally discovers built-ins lazily on first
    # lookup; if a plugin registers an app with the same name during
    # initialize(), a later lazy discover() would silently re-register
    # the built-in on top of it. Discovering first pins the order to
    # "built-ins first, plugins after", so the uniform conflict rule
    # (last registration wins, with a warning) actually holds.
    from fastink.computing.apps import registry as computing_registry
    computing_registry.discover()

    # Same eager-discovery for built-in auth backends (password, krb5)
    # so a plugin backend (e.g. IHEP apikey) registered during plugin
    # load applies the uniform "last registration wins" rule correctly.
    from fastink.auth.backends import registry as auth_registry
    auth_registry.discover()

    # Load unified plugins (which may include both hooks and routers)
    plugin_manager.load_plugins_from_config()

    # Load custom function hooks (hooks.modules config)
    load_instance_hooks()

    # Register hooks from unified plugins
    plugin_manager.register_plugin_hooks()

    _initialized = True
    logger.info("Plugin/hook bootstrap completed")
