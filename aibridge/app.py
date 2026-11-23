"""FastAPI application factory for ai-bridge.

create_app(config) собирает приложение с замкнутым конфигом — никаких
module-globals: несколько мостов могут жить в одном процессе, а тесты
поднимают изолированные инстансы.
"""
from __future__ import annotations

import asyncio
import logging
import secrets
import time
import uuid
from contextlib import asynccontextmanager, suppress
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.exception_handlers import http_exception_handler
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import __version__

from . import brain, models, openai, queueio
from .config import BridgeConfig

log = logging.getLogger("ai-bridge")

RESP_READ_RETRIES = 3  # попытки прочитать resp (страховка от не-атомарных писателей)


def create_app(config: BridgeConfig | None = None) -> FastAPI:
    config = config or BridgeConfig.from_env()
    config.validate()
    stats = brain.BrainStats()
    available = asyncio.Semaphore(config.max_inflight)

    async def maintenance() -> None:
        while True:
            await asyncio.sleep(60)
            try:
                queueio.sweep_timed_out_requests()
                queueio.sweep_orphans()
            except OSError:
                log.exception("Queue maintenance failed")

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        queueio.sweep_timed_out_requests()
        task = asyncio.create_task(maintenance())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    app = FastAPI(title="AI Bridge", version=__version__, lifespan=lifespan)
    app.state.config = config
    app.state.brain_stats = stats
    def check_req_id(req_id: str) -> None:
        if not queueio.is_valid_req_id(req_id):
            raise HTTPException(422, "invalid request id")

    def require_key(request: Request, key: str) -> None:
        if key:
            expected = f"Bearer {key}".encode()
            supplied = request.headers.get("authorization", "").encode()
            if not secrets.compare_digest(supplied, expected):
                raise HTTPException(401, "Invalid or missing API key",
                                    headers={"WWW-Authenticate": "Bearer"})
            return

        # Unauthenticated routes are for same-host clients only. Reject
        # browser cross-origin requests and DNS-rebinding Host headers.
        host = request.headers.get("host", "")
        origin = request.headers.get("origin")
        try:
            local_hosts = {"localhost", "127.0.0.1", "::1", config.host}
            local_host = urlsplit(f"//{host}").hostname in local_hosts
        except ValueError:
            local_host = False
        cross_site = request.headers.get("sec-fetch-site") == "cross-site"
        if not local_host or cross_site or (origin and origin != f"{request.url.scheme}://{host}"):
            raise HTTPException(403, "Local access only")

    async def require_api_key(request: Request) -> None:
        require_key(request, config.api_key)

    async def require_agent_key(request: Request) -> None:
        require_key(request, config.effective_agent_key)

    queue_api = APIRouter(prefix="/queue", dependencies=[Depends(require_agent_key)])

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, exc: StarletteHTTPException):
        if exc.status_code == 401:
            response = openai.openai_error(401, str(exc.detail), "authentication_error")
            response.headers.update(exc.headers or {})
            return response
        return await http_exception_handler(request, exc)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        # Validation errors can carry the original messages in Pydantic's input field.
        return openai.openai_error(422, "Invalid request body or parameters", "invalid_request_error")

    async def await_response_file(req_id: str, raw: Request) -> dict:
        """Ждать появления resp_<id>.json, прочитать и вернуть ответ агента.

        Следим за disconnect клиента (JARVIS/Katana могут оборвать ожидание):
        сиротский запрос сразу чистится, мозг не тратит силы на ответ в пустоту.
        """
        resp_file = queueio.resp_path(req_id)
        deadline = time.monotonic() + config.timeout
        attempts = 0
        while time.monotonic() < deadline:
            if resp_file.exists():
                try:
                    return models.AgentResponse.model_validate(
                        queueio.read_json(resp_file)).model_dump(exclude_none=True)
                except (OSError, ValueError):
                    attempts += 1
                    if attempts >= RESP_READ_RETRIES:
                        raise HTTPException(500, "Agent wrote an unreadable response")
            if await raw.is_disconnected():
                raise HTTPException(499, "Client disconnected")
            if not queueio.request_exists(req_id):
                raise HTTPException(409, "Request cancelled")
            await asyncio.sleep(config.poll_interval)
        stats.timeouts += 1
        raise HTTPException(504, "Agent did not respond in time")

    # ─── /v1 OpenAI-compatible ───────────────────────────────────────────────

    @app.post("/v1/chat/completions", dependencies=[Depends(require_api_key)])
    async def chat_completions(request: models.ChatRequest, raw: Request):
        """Принять OpenAI-запрос, записать в queue, дождаться ответа агента."""
        if available.locked():
            return openai.openai_error(429, "Bridge is at capacity", "rate_limit_error")
        async with available:
            req_id = uuid.uuid4().hex
            model = request.model if "model" in request.model_fields_set else config.model
            payload = request.model_dump()
            now = time.time()
            # Store the deadline under a reserved key for recovery after a restart.
            payload.update({
                "id": req_id, "created_at": now, "deadline_at": now + config.timeout,
                "model": model,
            })
            try:
                queueio.write_request(req_id, payload)
            except (OSError, ValueError) as exc:
                log.error("failed to enqueue %s: %s", req_id, type(exc).__name__)
                return openai.openai_error(503, "Queue storage is unavailable")
            log.info("queued %s (%d messages, stream=%s)",
                     req_id, len(request.messages), request.stream)
            try:
                agent_resp = await await_response_file(req_id, raw)
            except HTTPException as exc:
                return openai.openai_error(
                    exc.status_code, str(exc.detail),
                    err_type="timeout" if exc.status_code == 504 else "server_error")
            finally:
                queueio.cleanup_pair(req_id)

        if agent_resp.get("error"):
            log.warning("agent error for %s", req_id)
            return openai.openai_error(502, agent_resp["error"], err_type="agent_error")

        if request.stream:
            include_usage = bool((request.stream_options or {}).get("include_usage"))
            return openai.stream_chat_response(req_id, model, agent_resp, include_usage)

        return openai.build_chat_response(req_id, model, agent_resp)

    @app.get("/v1/models", dependencies=[Depends(require_api_key)])
    async def list_models():
        """Модели, видимые мостом: конфиг + AI_BRIDGE_MODELS + теги очереди.

        Проект может выбрать модель как у облачного провайдера — model-тег
        запроса роутит его к нужному мозгу/worker'у.
        """
        ids = {config.model, *config.models, *queueio.known_models()}
        return {
            "object": "list",
            "data": [{
                "id": mid,
                "object": "model",
                "created": 1700000000,
                "owned_by": "local",
            } for mid in sorted(ids)],
        }

    @app.get("/")
    @app.get("/health")
    async def health():
        return {"status": "ok", "service": "ai-bridge"}

    # ─── /queue management (для агента) ──────────────────────────────────────

    @queue_api.get("/pending")
    async def queue_pending():
        queueio.restore_expired_claims()
        pending = queueio.list_pending()
        return {"pending": pending, "count": len(pending)}

    @queue_api.get("/next")
    async def queue_next(raw: Request, timeout: float = Query(30.0, ge=0, le=120),
                         model: str | None = Query(None)):
        """Long-poll: ждать и вернуть самый старый pending запрос.

        204 = за timeout новых запросов не появилось.
        `model` — изоляция потребителей (только запросы с этим model-тегом).
        NB: для мозга предпочтителен /brain/claim — он выдаёт lease.
        """
        deadline = time.monotonic() + timeout
        while True:
            if await raw.is_disconnected():
                return Response(status_code=499)
            # Истёкшие lease восстанавливаем и здесь — иначе claimed-запрос
            # без мозга висел бы вечно.
            queueio.restore_expired_claims()
            item = queueio.next_pending(model=model)
            if item is not None:
                req_id, payload = item
                return {"id": req_id, "request": payload}
            if time.monotonic() >= deadline:
                return Response(status_code=204)  # JSONResponse(null) would emit a body
            await asyncio.sleep(config.poll_interval)

    @queue_api.get("/request/{req_id}")
    async def queue_get_request(req_id: str):
        check_req_id(req_id)
        for path in (queueio.req_path(req_id), queueio.claimed_path(req_id)):
            data = queueio.load_json(path)
            if data is not None:
                return data
        raise HTTPException(404, f"Request {req_id} not found")

    @queue_api.post("/respond/{req_id}")
    async def queue_post_response(req_id: str, response: models.BrainResponse):
        """Legacy queue endpoint. A leased request still requires its token."""
        check_req_id(req_id)
        try:
            queueio.complete_request(
                req_id, response.model_dump(exclude_none=True, exclude={"lease_token"}),
                lease_token=response.lease_token,
            )
        except queueio.QueueConflict as exc:
            raise HTTPException(exc.status, str(exc)) from exc
        return {"status": "ok", "id": req_id}

    @queue_api.get("/stats")
    async def queue_stats():
        queueio.restore_expired_claims()
        return queueio.stats()

    @queue_api.delete("/request/{req_id}")
    async def queue_cancel_request(req_id: str):
        """Отменить/очистить запрос."""
        check_req_id(req_id)
        existed = queueio.cleanup_pair(req_id)
        return {"status": "cancelled" if existed else "not_found",
                "id": req_id}

    # ─── /brain protocol (claim/lease) ────────────────────────────────────────
    app.include_router(queue_api)
    app.include_router(brain.create_router(config, stats),
                       dependencies=[Depends(require_agent_key)])

    return app
