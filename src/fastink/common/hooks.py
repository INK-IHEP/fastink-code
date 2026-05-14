"""Function hook system for FastINK to allow instance administrators
to override core functionality."""

import asyncio
import importlib
import sys
from typing import Callable, Any, Dict, Optional
from functools import wraps
from fastink.common.logger import logger
from fastink.common.config import get_config


# Global registry for hooks
_HOOKS_REGISTRY: Dict[str, Callable] = {}


def register_hook(hook_name: str):
    """
    Decorator to register a function as a hook.

    Args:
        hook_name: Name of the hook to register
    """
    def decorator(func: Callable) -> Callable:
        _HOOKS_REGISTRY[hook_name] = func
        logger.info(f"Registered hook: {hook_name} -> {func.__module__}.{func.__name__}")
        return func
    return decorator


def get_hook(hook_name: str, default_func: Optional[Callable] = None) -> Callable:
    """
    Retrieve a hook function by name, falling back to default if not found.

    Args:
        hook_name: Name of the hook to retrieve
        default_func: Default function to return if hook is not found

    Returns:
        Hook function or default function
    """
    return _HOOKS_REGISTRY.get(hook_name, default_func)


def load_instance_hooks():
    """
    Load instance-specific hooks based on configuration.
    Hooks are loaded from the 'hooks' section in the configuration.
    """
    try:
        # Get hook modules from configuration
        hook_modules_str = get_config("hooks", "modules", "")

        if not hook_modules_str:
            logger.info("No hook modules configured")
            return

        # Parse hook modules (comma-separated)
        hook_modules = [name.strip() for name in hook_modules_str.split(",") if name.strip()]

        for module_name in hook_modules:
            try:
                # Import the hook module
                hook_module = importlib.import_module(f"fastink.hooks.{module_name}")
                logger.info(f"Successfully loaded hook module: {module_name}")
            except ImportError as e:
                logger.error(f"Failed to import hook module '{module_name}': {e}")

    except Exception as e:
        logger.error(f"Error loading instance hooks: {e}")


def hookable(default_func: Callable) -> Callable:
    """
    Decorator to make a function hookable.

    Supports both sync and async functions. The wrapper detects the
    original function's type and returns a coroutine or value accordingly.

    Args:
        default_func: The default implementation of the function

    Returns:
        The function that will use hooks if available
    """
    hook_name = f"{default_func.__module__}.{default_func.__qualname__}"

    if asyncio.iscoroutinefunction(default_func):
        @wraps(default_func)
        async def wrapper(*args, **kwargs):
            func = get_hook(hook_name, default_func)
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            return func(*args, **kwargs)
    else:
        @wraps(default_func)
        def wrapper(*args, **kwargs):
            func = get_hook(hook_name, default_func)
            return func(*args, **kwargs)

    # Store reference to the original function
    wrapper._original_func = default_func
    wrapper._hook_name = hook_name

    return wrapper


def get_all_registered_hooks() -> Dict[str, Callable]:
    """
    Get all currently registered hooks.

    Returns:
        Dictionary mapping hook names to functions
    """
    return _HOOKS_REGISTRY.copy()