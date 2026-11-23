"""Brain protocol — claim/lease endpoints.

Внешний агент (Devin-сессия, любой другой мозг) атомарно claim'ит запрос,
думает (продлевая lease через /brain/renew), отвечает с lease_token.
Lease истекает → запрос сам возвращается в очередь. `model` в claim —
изоляция потребителей: мозг забирает только запросы с этим model-тегом.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import aclosing

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

from . import queueio
from .config import BridgeConfig
from .models import BrainResponse, ClaimRequest, LeaseBody

log = logging.getLogger("ai-bridge.brain")

MAX_CLAIM_WAIT = 120.0     # потолок long-poll ожидания в /brain/claim
STREAM_HEARTBEAT = 15.0    # интервал SSE heartbeat-комментария, сек


class BrainStats:
    """Per-client counters: какие мозги реально отвечают, а какие
    только сжигают lease."""

    def __init__(self) -> None:
        self.by_client: dict[str, dict[str, int]] = {}
        self.timeouts = 0
        self.lease_expired = 0

    def bump(self, client_id: str, key: str) -> None:
        entry = self.by_client.setdefault(str(client_id), {"claims": 0, "answers": 0})
        entry[key] = entry.get(key, 0) + 1

    def restore_expired(self) -> None:
        self.lease_expired += queueio.restore_expired_claims()

    def snapshot(self, lease_seconds: float) -> dict:
        # Файловые worker-claim'ы живут в queueio.client_stats() — мержим,
        # иначе /brain/stats слеп к самой массовой группе обработчиков.
        merged = {cid: dict(e) for cid, e in queueio.client_stats().items()}
        for cid, entry in self.by_client.items():
            dst = merged.setdefault(str(cid), {"claims": 0, "answers": 0})
            for key, val in entry.items():
                dst[key] = dst.get(key, 0) + val
        return {
            "by_client": merged,
            "timeouts": self.timeouts,
            "lease_expired": self.lease_expired,
            "lease_seconds": lease_seconds,
        }


def _check_req_id(req_id: str) -> None:
    if not queueio.is_valid_req_id(req_id):
        raise HTTPException(422, f"Invalid request id: {req_id!r}")


def _claimable_candidates(request_id: str | None) -> list[tuple[str, dict]]:
    """Pending-запросы, доступные для claim (unanswered + unclaimed)."""
    if request_id:
        data = queueio.load_json(queueio.req_path(request_id))
        return [(request_id, data)] if data is not None else []
    candidates = []
    for f in queueio.iter_request_files():
        req_id = f.stem.removeprefix(queueio.REQ_PREFIX)
        if queueio.resp_path(req_id).exists() or queueio.claim_path(req_id).exists():
            continue
        data = queueio.load_json(f)
        if data is not None:
            candidates.append((req_id, data))
    return candidates


def _try_claim(client_id: str, model: str | None, request_id: str | None,
               lease_seconds: float) -> dict | None:
    """Атомарно claim'нуть первый подходящий запрос. None — нечего брать."""
    for req_id, payload in _claimable_candidates(request_id):
        if model is not None and payload.get("model") != model:
            continue
        claim = queueio.claim(req_id, client_id, lease_seconds)
        if claim is None:
            continue
        log.info("brain claim %s by %s%s", req_id, client_id,
                 f" (model={model})" if model else "")
        return {
            "ok": True,
            "request": payload,
            "lease_token": claim["lease_token"],
            "lease_expires_at": claim["expires_at"],
        }
    return None


async def stream_claims(config: BridgeConfig, stats: BrainStats,
                        client_id: str, model: str | None,
                        max_inflight: int) -> AsyncIterator[dict]:
    """Сессионная аренда: генератор claim-событий для SSE-подписчика.

    Пока подписчик держит соединение:
    - каждый подходящий pending-запрос claim'ится от его client_id и
      выдаётся событием (lease_token внутри — отвечать обычным /brain/respond);
    - невыполненные lease продлеваются автоматически (думать можно дольше
      lease_seconds — лимит только у клиента, ждущего ответ);
    - при обрыве соединения все невыполненные claim'ы мгновенно
      возвращаются в очередь — не ждём lease-истечения.
    """
    outstanding: dict[str, str] = {}
    last_renew = time.monotonic()
    last_hb = 0.0
    try:
        yield {"event": "ready",
               "data": {"client_id": client_id, "model": model,
                        "lease_seconds": config.lease_seconds}}
        while True:
            stats.restore_expired()
            outstanding = {rid: token for rid, token in outstanding.items()
                           if queueio.verify_lease(rid, token)}
            while len(outstanding) < max_inflight:
                claimed = _try_claim(client_id, model, None, config.lease_seconds)
                if claimed is None:
                    break
                req_id = claimed["request"]["id"]
                outstanding[req_id] = claimed["lease_token"]
                stats.bump(client_id, "claims")
                yield {"event": "claim", "data": claimed}
            now = time.monotonic()
            if outstanding and now - last_renew >= config.lease_seconds / 2:
                for rid, token in outstanding.items():
                    try:
                        queueio.renew_claim(rid, config.lease_seconds, lease_token=token)
                    except queueio.QueueConflict:
                        continue
                last_renew = now
            if now - last_hb >= STREAM_HEARTBEAT:
                last_hb = now
                yield {"event": "hb"}
            await asyncio.sleep(config.poll_interval)
    finally:
        # Сессия закрыта: невыполненное немедленно обратно в очередь.
        for rid, token in outstanding.items():
            try:
                queueio.release_claim(rid, lease_token=token)
            except queueio.QueueConflict:
                continue
            log.info("brain stream released %s on disconnect", rid)


def create_router(config: BridgeConfig, stats: BrainStats) -> APIRouter:
    """APIRouter /brain/* bound to a bridge config and shared stats."""
    router = APIRouter()

    @router.post("/brain/claim")
    async def brain_claim(body: ClaimRequest, raw: Request):
        """Атомарно забрать один pending-запрос. 204 = очередь пуста.

        Claimed-запрос исчезает из /brain/pending и недоступен другим мозгам,
        пока не истечёт lease (тогда возвращается сам). `wait`>0 включает
        long-poll: держим соединение до появления запроса (потолок 120с).
        """
        client_id = body.client_id.strip()
        if not client_id:
            raise HTTPException(400, "client_id is required")
        if body.request_id is not None and not queueio.is_valid_req_id(body.request_id):
            raise HTTPException(400, "invalid request id")

        wait = max(0.0, min(float(body.wait or 0), MAX_CLAIM_WAIT))
        deadline = time.monotonic() + wait
        while True:
            if await raw.is_disconnected():
                return Response(status_code=499)
            stats.restore_expired()
            claimed = _try_claim(client_id, body.model, body.request_id,
                                 config.lease_seconds)
            if claimed is not None:
                stats.bump(client_id, "claims")
                return claimed
            if time.monotonic() >= deadline:
                return Response(status_code=204)
            await asyncio.sleep(config.poll_interval)

    @router.get("/brain/stream")
    async def brain_stream(client_id: str = Query(..., max_length=128),
                           model: str | None = Query(None),
                           max_inflight: int = Query(8, ge=1, le=64)):
        """SSE-сессия мозга: claim-события вместо поллинга /brain/claim.

        Каждое `event: claim` — уже claim'нутый запрос (lease_token в data);
        ответ — обычным POST /brain/respond/<id>. Пока стрим жив, lease'ы
        продлеваются сами; при обрыве невыполненное сразу возвращается
        в очередь. `max_inflight` ограничивает одновременно удерживаемые
        claim'ы, чтобы один стрим не монополизировал всплеск очереди.
        """
        cid = client_id.strip()
        if not cid:
            raise HTTPException(400, "client_id is required")

        async def events() -> AsyncIterator[str]:
            async with aclosing(stream_claims(config, stats, cid, model, max_inflight)) as claims:
                async for ev in claims:
                    if ev["event"] == "hb":
                        yield ": hb\n\n"
                    else:
                        yield (f"event: {ev['event']}\n"
                               f"data: {json.dumps(ev['data'], ensure_ascii=False)}\n\n")

        return StreamingResponse(events(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache",
                                          "X-Accel-Buffering": "no"})

    @router.post("/brain/respond/{req_id}")
    async def brain_respond(req_id: str, body: BrainResponse):
        """Ответ мозга. Если запрос claimed — lease_token обязателен."""
        _check_req_id(req_id)
        try:
            claim = queueio.complete_request(
                req_id, body.model_dump(exclude_none=True, exclude={"lease_token"}),
                lease_token=body.lease_token, require_claim=config.require_claim,
            )
        except queueio.QueueConflict as exc:
            raise HTTPException(exc.status, str(exc)) from exc
        if claim is not None:
            stats.bump(str(claim.get("client_id", "unknown")), "answers")
        log.info("brain respond %s (client=%s)", req_id,
                 (claim or {}).get("client_id", "unclaimed"))
        return {"ok": True, "id": req_id}

    @router.post("/brain/renew/{req_id}")
    async def brain_renew(req_id: str, body: LeaseBody):
        """Extend the current owner's lease."""
        _check_req_id(req_id)
        try:
            expires_at = queueio.renew_claim(req_id, config.lease_seconds,
                                            lease_token=body.lease_token)
        except queueio.QueueConflict as exc:
            raise HTTPException(exc.status, str(exc)) from exc
        return {"ok": True, "id": req_id, "lease_expires_at": expires_at}

    @router.post("/brain/release/{req_id}")
    async def brain_release(req_id: str, body: LeaseBody):
        """Return the current owner's request to the queue."""
        _check_req_id(req_id)
        try:
            queueio.release_claim(req_id, lease_token=body.lease_token)
        except queueio.QueueConflict as exc:
            raise HTTPException(exc.status, str(exc)) from exc
        log.info("brain release %s", req_id)
        return {"ok": True, "id": req_id}

    @router.get("/brain/pending")
    async def brain_pending():
        """Unclaimed unanswered запросы — полные payload'ы."""
        stats.restore_expired()
        requests = []
        count = 0
        for f in queueio.iter_request_files():
            req_id = f.stem.removeprefix(queueio.REQ_PREFIX)
            if queueio.resp_path(req_id).exists() or queueio.claim_path(req_id).exists():
                continue
            count += 1
            data = queueio.load_json(f)
            if data is not None and len(requests) < 20:
                requests.append(data)
        return {"requests": requests, "count": count}

    @router.get("/brain/stats")
    async def brain_stats():
        stats.restore_expired()
        return stats.snapshot(config.lease_seconds)

    return router
