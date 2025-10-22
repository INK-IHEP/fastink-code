from fastapi import APIRouter, Query, Body
from pydantic import BaseModel

from src.common.logger import logger

router = APIRouter()


# /api/v2/example/get_example?example_para=xxx
@router.get("/example/get_example")
def get_example(example_para: str = Query(None)) -> dict:
    """
    GET类型的接口，不得传入body，必须用query参数，
    用法：
        curl -X GET http://<hostname>:<port>/api/v2/example/get_example?example_para=xxx
    返回值必须三段式：
        {"status": "状态码", "msg": "消息" "data": json格式}
    """
    logger.info("This is an example for GET in api_v2")
    if example_para:
        return {
            "status": "E00",
            "msg": "This is an example for GET in api_v2",
            "data": {"example_para": example_para},
        }
    else:
        return {
            "status": "E01",
            "msg": "This is an example for GET in api_v2",
            "data": None,
        }


# 不推荐使用pydantic的BaseModel，
# 因为pydantic的BaseModel会检查参数的格式，
# 如果参数格式不正确，会直接返回HTTP 422错误，
# 导致返回格式不按照约定的三段式，导致前端无法解析。
class ExamplePost(BaseModel):
    """不推荐这个方法"""

    example_para1: str
    example_para2: str


@router.post("/example/post_example_not_recommended")
async def post_example_not_recommended(example_post: ExamplePost):
    """不推荐这个方法"""
    para1 = example_post.example_para1
    para2 = example_post.example_para2
    return {
        "status": "E01",
        "msg": "This is a example for POST in api_v2",
        "data": {"para1": para1, "para2": para2},
    }


@router.post("/example/post_example_recommended")
async def post_example_recommanded(para1=Body(...), para2=Body(...)):
    """推荐这个方法"""
    return {
        "status": "E01",
        "msg": "This is a example for POST in api_v2",
        "data": {"para1": para1, "para2": para2},
    }
