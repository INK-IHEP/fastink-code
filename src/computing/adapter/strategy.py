from typing import Callable
from src.computing.tools.common.utils import change_username_to_uid
from src.computing.adapter.baseadapter import SchedulerBase

scheduler_registry: dict[str, type[SchedulerBase]] = {}

def scheduler(clusterid: str):
    def decorator(cls: type[SchedulerBase]):
        scheduler_registry[clusterid] = cls
        return cls
    return decorator


def _ensure_loaded():
    import src.computing.adapter.htcadapter

def get_scheduler(clusterid: str, username: str):

    uid = change_username_to_uid(username)

    if not scheduler_registry:
        _ensure_loaded()
    
    try:
        cls = scheduler_registry[clusterid]
    except KeyError:
        raise ValueError(f"Unknown scheduler '{clusterid}'. Registered={list(scheduler_registry)}")
    
    return cls(uid)


jdlenv_registry: dict[str, Callable] = {}

def register_jdlformat(name: str):
    def deco(fn):
        jdlenv_registry[name] = fn
        return fn
    return deco

def get_jdlformat(name: str):
    return jdlenv_registry[name]