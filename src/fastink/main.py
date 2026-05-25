# main.py
import json
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from fastink.common.utils import get_version, get_version_date
from fastink.routers.headers import (
    IPWhitelistMiddleware,
    TimerMiddleware,
    UserValidationMiddleware,
)
from fastink.common.config import get_config
from fastink.routers.plugin_loader import load_router_plugins
from fastink.common.hooks import load_instance_hooks
from fastink.common.logger import logger
from fastink.common.plugin_interface import plugin_manager

app = FastAPI()
if get_config("common", "security_access") is True:
    skip_routers = get_config("security", "skip_routers")
    app.add_middleware(UserValidationMiddleware, skip_routers=skip_routers)
if get_config("common", "ip_whitelist_access") is True:
    ip_whitelist = get_config("security", "ip_whitelist")
    forbidden_routers = get_config("security", "ip_controlled_routers")
    app.add_middleware(
        IPWhitelistMiddleware,
        ip_whitelist=ip_whitelist,
        forbidden_routers=forbidden_routers,
    )
app.add_middleware(TimerMiddleware)

# Load unified plugins (which may include both hooks and routers)
plugin_manager.load_plugins_from_config()

# Load custom function hooks before initializing routes
load_instance_hooks()

# Register hooks from unified plugins
plugin_manager.register_plugin_hooks()

# 路由添加到主应用中
from fastink.routers.v2 import auth_manager as auth_manager_v2
from fastink.routers.v2 import compute_resources as compute_resources_v2
from fastink.routers.v2 import app_manager as app_manager_v2
from fastink.routers.v2 import assistant_manager as assistant_manager_v2
from fastink.routers.v2 import fs_manager as fs_manager_v2
from fastink.routers.v2 import get_ccs_info as get_ccs_info_v2
from fastink.routers.v2 import service_manager as service_manager_v2
from fastink.routers.v2 import elk_onlinemon_router as elk_onlinemon_router
from fastink.routers.v2 import share_manager as share_manager_v2

app.include_router(auth_manager_v2.router, prefix="/api/v2/auth")
app.include_router(compute_resources_v2.router, prefix="/api/v2/cr")
app.include_router(app_manager_v2.router, prefix="/api/v2/app")
app.include_router(assistant_manager_v2.router, prefix="/api/v2")
app.include_router(fs_manager_v2.router, prefix="/api/v2/fs")
app.include_router(get_ccs_info_v2.router, prefix="/api/v2")
app.include_router(service_manager_v2.router, prefix="/api/v2")
app.include_router(elk_onlinemon_router.router, prefix="/api/v2")
app.include_router(share_manager_v2.router, prefix="/api/v2")


# Load custom router plugins dynamically
loaded_plugins = load_router_plugins(app, "plugins")

# Register routers from unified plugins
plugin_manager.register_plugin_routers(app)


# 参数验证失败，自定义错误响应
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    custom_errors = []
    for error in exc.errors():
        custom_errors.append(
            {
                "field": "->".join(str(loc) for loc in error["loc"]),
                "msg": error["msg"],
                "error_type": error["type"],
            }
        )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"status": "422", "msg": "参数验证失败", "data": custom_errors},
    )


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/version")
def version():
    ver = get_version()
    date = get_version_date()
    return {"version": ver, "date": date}


@app.get("/check")
def check():
    logger.debug("directly check debug log")
    logger.info("directly check info log")
    logger.warning("directly check warning log")
    logger.error("directly check error log")
    logger.critical("directly check critical log")
    return {"message": "how do you find me?"}


@app.get("/")
async def root():
    return {"message": "Welcome to the main API!!"}


with open("src/fastink/misc/openapi_schema.json", "w") as f:
    json.dump(app.openapi(), f)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
