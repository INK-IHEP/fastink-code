"""Group discovery for FastINK permission system.

Provides a hookable function to resolve a user's Linux group memberships
(primary + supplemental). The default implementation uses OS calls.
IHEP's plugin overrides this hook to query the CCS database instead.
"""

import grp
import os
import pwd

from fastink.common.hooks import hookable
from fastink.common.logger import logger


@hookable
def get_user_groups(username: str) -> list[str]:
    """Return ALL Linux groups a user belongs to (primary + supplemental).

    Default implementation uses os.getgrouplist(). Plugins (e.g., IHEP)
    can override this via register_hook("fastink.auth.groups.get_user_groups")
    to use their own group database (CCS DB).

    Args:
        username: The Unix username.

    Returns:
        List of group name strings. Returns empty list on failure.
    """
    try:
        pw = pwd.getpwnam(username)
        uid = pw.pw_uid
        primary_gid = pw.pw_gid
        group_ids = os.getgrouplist(username, primary_gid)
    except (KeyError, OSError) as e:
        logger.warning(
            "Failed to resolve groups for user %s: %s", username, e
        )
        return []

    groups = []
    for gid in group_ids:
        try:
            groups.append(grp.getgrgid(gid).gr_name)
        except KeyError:
            continue

    return list(dict.fromkeys(groups))
