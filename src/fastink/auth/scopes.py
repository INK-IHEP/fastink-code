from fastapi import Depends, HTTPException, Request

from fastink.auth.oidc.principal import Principal, get_current_principal


def require_scope(*required: str):
    async def checker(request: Request) -> Principal:
        principal = get_current_principal(request)
        if not principal.scopes:
            raise HTTPException(status_code=403, detail="No scopes in token")
        missing = [s for s in required if s not in principal.scopes]
        if missing:
            raise HTTPException(
                status_code=403,
                detail=f"Missing scope: {', '.join(missing)}",
            )
        return principal
    return Depends(checker)
