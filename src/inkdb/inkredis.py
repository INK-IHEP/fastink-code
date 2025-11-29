from redis.asyncio import Redis
from src.common.config import get_config

"""
Author:         guocq@ihep.ac.cn
Created:        2024-12-18
Last Modified:  2025-09-02
"""


def redis_connect():
    redis_config = get_config("redis")
    r = Redis(
        host=redis_config.get("host", "localhost"),
        port=redis_config.get("port", 6379),
        password=redis_config.get("password", None),
        decode_responses=True,
    )
    return r
