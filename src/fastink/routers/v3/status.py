from fastapi import APIRouter

router = APIRouter(tags=["status"])


@router.get("/status/health")
async def health():
    return {"status": "ok"}
