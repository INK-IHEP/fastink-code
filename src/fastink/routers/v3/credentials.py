from fastapi import APIRouter, HTTPException

from fastink.auth.credential import export_credential
from fastink.auth.oidc.principal import Principal
from fastink.auth.scopes import require_scope
from fastink.common.logger import logger

router = APIRouter(tags=["credentials"])


@router.post("/credentials/export", status_code=200)
async def export(principal: Principal = require_scope("credentials:export")):
    try:
        return export_credential(principal)
    except Exception:
        logger.exception("Credential export failed for %s", principal.username)
        raise HTTPException(status_code=500, detail="Credential export failed")
