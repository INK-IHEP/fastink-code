"""Platform API client — hookable functions for third-party platform integration.

Default implementations return None, allowing sites without specific
platform plugins to function normally. Plugins override these hooks
via the standard hook registration mechanism.
"""

from fastink.common.hooks import hookable
from fastink.common.logger import logger


@hookable
async def get_user_api_key(email: str) -> str | None:
    """Fetch a user's personal API key from an external AI platform.

    The default implementation returns None. IHEP's plugin overrides this
    hook to call the HEP AI platform API.

    Args:
        email: User email address.

    Returns:
        The API key string, or None if no platform is configured.
    """
    logger.debug("No platform plugin available for get_user_api_key(%s)", email)
    return None
