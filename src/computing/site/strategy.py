from typing import Callable


_buildsite_registry: dict[str, Callable] = {}

def register_site(name: str):
    def deco(fn):
        _buildsite_registry[name] = fn
        return fn
    return deco

def get_site(name: str):
    return _buildsite_registry[name]



_submitters: dict[tuple[str,str], Callable] = {}

def register_submitter(site: str, mode: str):
    def deco(fn):
        _submitters[(site, mode)] = fn
        return fn
    return deco

def get_submitter(site: str, mode: str): 
    return _submitters[(site,mode)]

import src.computing.site.ihep
import src.computing.site.hai
import src.computing.site.heps
