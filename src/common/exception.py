from starlette.exceptions import HTTPException  # 官方推荐注册异常处理器时，应该注册到来自 Starlette 的 HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

# 全局异常       
async def global_exception_handler(request, exc):
    err_msg = exc.detail
    return JSONResponse({
        'status': exc.status_code,
        'msg': err_msg
    })

# 请求数据无效时的错误处理
async def validate_exception_handler(request, exc):
    err = exc.errors()[0]
    return JSONResponse({
        'code': 400,
        'msg': err['msg']
    })

golbal_exception_handlers = {
    HTTPException: global_exception_handler,
    RequestValidationError: validate_exception_handler
}

class BaseAPIException(HTTPException):
    status_code = 400
    detail = 'api error'
    def __init__(self, detail: str = None, status_code: int = None):
        self.detail = detail or self.detail
        self.status_code = status_code or self.status_code
