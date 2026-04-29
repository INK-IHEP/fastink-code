from pydantic import BaseModel, Field


class AssistantSessionCreateRequest(BaseModel):
    title: str | None = Field(default=None, description="Optional session title")


class AssistantMessageCreateRequest(BaseModel):
    session_id: str = Field(..., description="Assistant session id")
    message: str = Field(..., min_length=1, description="User message")


class AssistantActionConfirmRequest(BaseModel):
    session_id: str = Field(..., description="Assistant session id")
    turn_id: str = Field(..., description="Assistant turn id")
    action_id: str = Field(..., description="Pending action id")
    confirmed: bool = Field(..., description="Whether to execute the pending action")
