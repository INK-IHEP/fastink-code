#! /usr/bin/python3

from pydantic import BaseModel, Field


class OpenClawSyncRequest(BaseModel):
    base_url: str = Field(..., min_length=1)
    api_key: str = Field(..., min_length=1)
    api_name: str = Field(..., min_length=1)

    model_id: str = Field(..., min_length=1)
    model_name: str | None = None
    model_reasoning: bool | None = None
    model_input: list[str] | None = None
    model_cost_input: float | int | None = None
    model_cost_output: float | int | None = None
    model_cost_cache_read: float | int | None = None
    model_cost_cache_write: float | int | None = None
    model_context_window: int | None = None
    model_max_tokens: int | None = None
