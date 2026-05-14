from fastapi import APIRouter, Depends, Query

from fastink.routers.headers import get_username
from fastink.routers.status import InkStatus
from fastink.service.assistant.schemas import (
    AssistantActionConfirmRequest,
    AssistantMessageCreateRequest,
    AssistantSessionCreateRequest,
)
from fastink.service.assistant.service import AssistantService

router = APIRouter()


def _assistant_disabled_response() -> dict:
    return {
        "status": InkStatus.RESOURCE_NOT_SUPPORT,
        "msg": "Assistant feature is disabled in current deployment.",
        "data": None,
    }


@router.post("/assistant/create_session")
async def create_assistant_session(
    payload: AssistantSessionCreateRequest,
    username: str = Depends(get_username),
) -> dict:
    service = AssistantService()
    if not service.is_enabled():
        return _assistant_disabled_response()
    try:
        session = await service.create_session(username, payload.title)
        return {
            "status": InkStatus.SUCCESS,
            "msg": "Create assistant session successfully.",
            "data": session,
        }
    except Exception as exc:
        return {
            "status": InkStatus.INTERNAL_ERROR,
            "msg": f"Create assistant session failed: {exc}",
            "data": None,
        }


@router.get("/assistant/get_sessions")
async def get_assistant_sessions(
    username: str = Depends(get_username),
) -> dict:
    service = AssistantService()
    if not service.is_enabled():
        return _assistant_disabled_response()
    try:
        sessions = await service.list_sessions(username)
        return {
            "status": InkStatus.SUCCESS,
            "msg": "Get assistant sessions successfully.",
            "data": {"sessions": sessions},
        }
    except Exception as exc:
        return {
            "status": InkStatus.INTERNAL_ERROR,
            "msg": f"Get assistant sessions failed: {exc}",
            "data": None,
        }


@router.get("/assistant/get_messages")
async def get_assistant_messages(
    session_id: str = Query(..., description="Assistant session id"),
    username: str = Depends(get_username),
) -> dict:
    service = AssistantService()
    if not service.is_enabled():
        return _assistant_disabled_response()
    try:
        messages = await service.list_messages(username, session_id)
        return {
            "status": InkStatus.SUCCESS,
            "msg": "Get assistant messages successfully.",
            "data": {"messages": messages},
        }
    except Exception as exc:
        return {
            "status": InkStatus.RESOURCE_NOT_FOUND,
            "msg": f"Get assistant messages failed: {exc}",
            "data": None,
        }


@router.post("/assistant/create_message")
async def create_assistant_message(
    payload: AssistantMessageCreateRequest,
    username: str = Depends(get_username),
) -> dict:
    service = AssistantService()
    if not service.is_enabled():
        return _assistant_disabled_response()
    try:
        result = await service.create_message(username, payload.session_id, payload.message)
        return {
            "status": InkStatus.SUCCESS,
            "msg": "Create assistant message successfully.",
            "data": result,
        }
    except Exception as exc:
        return {
            "status": InkStatus.INTERNAL_ERROR,
            "msg": f"Create assistant message failed: {exc}",
            "data": None,
        }


@router.get("/assistant/get_turn")
async def get_assistant_turn(
    session_id: str = Query(..., description="Assistant session id"),
    turn_id: str = Query(..., description="Assistant turn id"),
    username: str = Depends(get_username),
) -> dict:
    service = AssistantService()
    if not service.is_enabled():
        return _assistant_disabled_response()
    try:
        turn = await service.get_turn(username, session_id, turn_id)
        return {
            "status": InkStatus.SUCCESS,
            "msg": "Get assistant turn successfully.",
            "data": turn,
        }
    except Exception as exc:
        return {
            "status": InkStatus.RESOURCE_NOT_FOUND,
            "msg": f"Get assistant turn failed: {exc}",
            "data": None,
        }


@router.post("/assistant/confirm_action")
async def confirm_assistant_action(
    payload: AssistantActionConfirmRequest,
    username: str = Depends(get_username),
) -> dict:
    service = AssistantService()
    if not service.is_enabled():
        return _assistant_disabled_response()
    try:
        turn = await service.confirm_action(
            username,
            payload.session_id,
            payload.turn_id,
            payload.action_id,
            payload.confirmed,
        )
        return {
            "status": InkStatus.SUCCESS,
            "msg": "Confirm assistant action successfully.",
            "data": turn,
        }
    except Exception as exc:
        return {
            "status": InkStatus.INTERNAL_ERROR,
            "msg": f"Confirm assistant action failed: {exc}",
            "data": None,
        }
