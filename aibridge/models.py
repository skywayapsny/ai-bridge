"""Pydantic schemas for ai-bridge HTTP surface."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class ChatRequest(BaseModel):
    """OpenAI /v1/chat/completions request (минимальная валидация).

    Любые дополнительные поля (response_format, stop, seed и т.д.) проходят
    в очередь as-is — агент решает, что с ними делать.
    """
    model: str = "ai-bridge"
    messages: list[dict[str, Any]]
    tools: list[dict[str, Any]] | None = None
    tool_choice: Any = None
    temperature: float | None = None
    max_tokens: int | None = None
    stream: bool = False
    stream_options: dict[str, Any] | None = None

    model_config = {"extra": "allow"}


class AgentResponse(BaseModel):
    """Ответ агента — content + tool_calls в OpenAI-формате."""
    content: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    finish_reason: str | None = None  # stop | tool_calls | length
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    error: str | None = None


    @field_validator("tool_calls")
    @classmethod
    def validate_tool_calls(cls, calls: list[dict[str, Any]] | None):
        for call in calls or []:
            function = call.get("function")
            if function is not None and not isinstance(function, dict):
                raise ValueError("tool call function must be an object")
            name = (function or {}).get("name") or call.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError("tool calls require a function name")
        return calls


# ─── Brain protocol (claim/lease) ─────────────────────────────────────────────


class ClaimRequest(BaseModel):
    client_id: str = Field(max_length=128)
    model: str | None = None       # consumer isolation: только запросы с этим тегом
    request_id: str | None = None  # забрать конкретный, не самый старый
    wait: float = Field(default=0, allow_inf_nan=False)                # long-poll: ждать запрос до N сек (0 → 204 сразу)


class LeaseBody(BaseModel):
    lease_token: str


class BrainResponse(AgentResponse):
    """Agent result plus proof of lease ownership."""
    lease_token: str | None = None
