"""OpenAI wire format helpers: responses, errors, SSE chunks."""
from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator
from typing import Any

from fastapi.responses import JSONResponse, StreamingResponse


def openai_error(status: int, message: str, err_type: str = "server_error",
                 code: str | None = None) -> JSONResponse:
    """Ошибка в формате OpenAI API — SDK корректно её парсит и ретраит."""
    return JSONResponse(
        status_code=status,
        content={"error": {"message": message, "type": err_type, "code": code}},
    )


def normalize_tool_calls(tool_calls: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Привести tool_calls агента к строгому OpenAI-виду.

    Принимает и плоский вид (name/arguments на верхнем уровне), и полный
    OpenAI-вид (function.name/function.arguments).
    """
    normalized = []
    for i, tc in enumerate(tool_calls or []):
        fn = tc.get("function") or {}
        name = fn.get("name") or tc.get("name", "")
        arguments = fn.get("arguments", tc.get("arguments", ""))
        if not isinstance(arguments, str):
            arguments = json.dumps(arguments, ensure_ascii=False)
        normalized.append({
            "id": tc.get("id") or f"call_{i}",
            "type": "function",
            "function": {"name": name, "arguments": arguments},
        })
    return normalized


def build_chat_response(req_id: str, model: str, agent_resp: dict) -> dict:
    """Собрать OpenAI-format response из ответа агента."""
    content = agent_resp.get("content")
    tool_calls = normalize_tool_calls(agent_resp.get("tool_calls"))
    finish_reason = (
        agent_resp.get("finish_reason")
        or ("tool_calls" if tool_calls else "stop")
    )
    message: dict[str, Any] = {"role": "assistant", "content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls

    prompt_tokens = int(agent_resp.get("input_tokens") or 0)
    completion_tokens = int(agent_resp.get("output_tokens") or 0)
    return {
        "id": f"chatcmpl-{req_id}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason,
        }],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }


def _sse_chunk(req_id: str, model: str, created: int, payload: dict | str) -> str:
    if payload == "[DONE]":
        return "data: [DONE]\n\n"
    chunk = {
        "id": f"chatcmpl-{req_id}",
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        **payload,
    }
    return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"


def stream_chat_response(req_id: str, model: str, agent_resp: dict,
                         include_usage: bool) -> StreamingResponse:
    """Обернуть цельный ответ агента в OpenAI-совместимый SSE-поток.

    Мост не стримит токены по мере генерации — агент отвечает целиком,
    поэтому поток состоит из одного content-chunk + finish-chunk + [DONE].
    Этого достаточно, чтобы клиенты со stream=True работали корректно.
    """
    tool_calls = normalize_tool_calls(agent_resp.get("tool_calls"))
    finish_reason = (
        agent_resp.get("finish_reason")
        or ("tool_calls" if tool_calls else "stop")
    )
    prompt_tokens = int(agent_resp.get("input_tokens") or 0)
    completion_tokens = int(agent_resp.get("output_tokens") or 0)

    created = int(time.time())
    usage_marker = {"usage": None} if include_usage else {}

    async def events() -> AsyncIterator[str]:
        delta: dict[str, Any] = {"role": "assistant"}
        if agent_resp.get("content") is not None:
            delta["content"] = agent_resp["content"]
        if tool_calls:
            delta["tool_calls"] = [
                {"index": i, **tc} for i, tc in enumerate(tool_calls)
            ]
        yield _sse_chunk(req_id, model, created, {
            "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
            **usage_marker,
        })
        yield _sse_chunk(req_id, model, created, {
            "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
            **usage_marker,
        })
        if include_usage:
            yield _sse_chunk(req_id, model, created, {
                "choices": [],
                "usage": {
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens,
                },
            })
        yield _sse_chunk(req_id, model, created, "[DONE]")

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
