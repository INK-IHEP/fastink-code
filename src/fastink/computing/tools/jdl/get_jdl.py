import os

class _EnvFallbackDict(dict):
    """用于 str.format_map：key 不在 ctx 时改查环境变量（同名或大写），
       都没有则保留 {key} 原样。"""
    def __missing__(self, key):
        # 同名环境变量
        val = os.environ.get(key)
        if val is None:
            # 大写再试一次（cc_user -> CC_USER）
            val = os.environ.get(key.upper())
        if val is None:
            return "{" + key + "}"
        # 缓存结果，避免重复查
        self[key] = val
        return val


def render_value(val, ctx: dict):
    """仅渲染 {xxx}；不会影响 Condor 的 $(ClusterId) 等宏。"""
    if isinstance(val, str):
        return val.format_map(_EnvFallbackDict(ctx))
    return val
