#! /usr/bin/python3

from pydantic import BaseModel, Field


class OpenClawModelCostRequest(BaseModel):
    input: float | int | None = None
    output: float | int | None = None
    cacheRead: float | int | None = None
    cacheWrite: float | int | None = None


class OpenClawModelRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    reasoning: bool | None = None
    input: list[str] | None = None
    cost: OpenClawModelCostRequest | None = None
    contextWindow: int | None = None
    maxTokens: int | None = None


class OpenClawSyncRequest(BaseModel):
    baseUrl: str = Field(..., min_length=1)
    apiKey: str = Field(..., min_length=1)
    api: str = Field(..., min_length=1)
    models: list[OpenClawModelRequest] | None = None
