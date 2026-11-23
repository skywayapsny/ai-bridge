#!/usr/bin/env python3
"""Autonomous queue worker for ai-bridge.

Delegates pending OpenAI-compatible requests from the bridge file-queue to a
configured upstream LLM endpoint and writes responses back atomically.

Never fabricates answers: without AI_WORKER_BASE_URL/API_KEY it refuses to
start, and after exhausting retries it writes an explicit error response so
the client gets a 502 instead of hanging until the bridge timeout.

Настройки берутся из переменных окружения или из файла `.env` рядом со скриптом
(пример — `.env.example`):

    AI_WORKER_BASE_URL      — базовый URL OpenAI-compatible API (обязательно)
    AI_WORKER_API_KEY       — ключ провайдера (обязательно)
    AI_WORKER_MODEL         — модель (default: модель из запроса)
    AI_WORKER_TIMEOUT       — таймаут upstream-запроса, сек (default: 60)
    AI_WORKER_MAX_ATTEMPTS  — попыток на запрос до error-ответа (default: 3)
    AI_WORKER_WORKERS       — параллельных upstream-вызовов (default: 1)
    AI_WORKER_CLAIM_MODEL   — брать только запросы с этим model-тегом
    AI_WORKER_THINKING      — enabled|disabled — reasoning-режим провайдера
    AI_WORKER_REASONING_EFFORT — high|max (при thinking=enabled)
    AI_WORKER_EXTRA_HEADERS — JSON-объект с доп. HTTP-заголовками

CLI:
    python3 worker.py                 # бесконечный цикл
    python3 worker.py --once          # один проход по очереди
    python3 worker.py --workers 4     # параллельная обработка
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import math
import os
import time
from threading import Lock
import urllib.error
import urllib.request
from pathlib import Path

from . import queueio
from .config import WorkerConfig, load_env_file
from .models import AgentResponse

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"

log = logging.getLogger("ai-bridge.worker")

# Поля, которые не передаём upstream как есть (служебные / переопределяемые).
_INTERNAL_FIELDS = {"id", "created_at", "deadline_at"}
# Поля, которые апстриму передавать нельзя / бессмысленно.
# thinking/reasoning_effort — инъекция предыдущего worker'а в цепочке:
# чужой reasoning-режим не должен течь дальше (Gemini и др. дают 400).
_STRIP_FIELDS = {"stream", "stream_options", "thinking", "reasoning_effort",
                 "received_at"}


def build_upstream_body(payload: dict, config: WorkerConfig) -> dict:
    """Собрать тело upstream-запроса: поля клиента проходят сквозь мост.

    Всё, что клиент положил в /v1/chat/completions (response_format, stop,
    seed, presence_penalty и т.д.), доезжает до провайдера. Служебные поля
    очереди вырезаются, stream принудительно выключен.
    """
    body = {
        key: value for key, value in payload.items()
        if key not in _INTERNAL_FIELDS | _STRIP_FIELDS and value is not None
    }
    body["model"] = config.upstream_model(payload)
    body["stream"] = False
    body.update(config.thinking_payload())
    return body


def call_model(payload: dict, config: WorkerConfig,
               api_key: str | None = None) -> dict:
    """Вызвать upstream LLM и нормализовать ответ под формат очереди.

    ``api_key`` — конкретный ключ из пула ротации (``config.key_pool()``);
    None → первый ключ пула / одиночный api_key.
    """
    url = f"{config.base_url.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key or next(iter(config.key_pool()), '')}",
        **config.extra_headers,
    }
    request = urllib.request.Request(
        url,
        data=json.dumps(build_upstream_body(payload, config), ensure_ascii=False).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    for name, value in headers.items():
        request.add_unredirected_header(name, value)
    with urllib.request.urlopen(request, timeout=config.timeout) as response:
        result = json.loads(response.read().decode())

    choices = result["choices"]
    message = choices[0]["message"]
    tool_calls = []
    for call in message.get("tool_calls") or []:
        function = call.get("function") or {}
        arguments = function.get("arguments", "{}")
        if not isinstance(arguments, str):
            arguments = json.dumps(arguments, ensure_ascii=False)
        tool_calls.append({
            "id": call.get("id") or f"call_{len(tool_calls)}",
            "name": function.get("name", ""),
            "arguments": arguments,
        })

    usage = result.get("usage") or {}
    response = {
        "content": message.get("content"),
        "tool_calls": tool_calls,
        "finish_reason": choices[0].get("finish_reason")
            or ("tool_calls" if tool_calls else "stop"),
        "input_tokens": usage.get("prompt_tokens", 0),
        "output_tokens": usage.get("completion_tokens", 0),
    }
    return AgentResponse.model_validate(response).model_dump(exclude_none=True)


class Worker:
    """Пул обработчиков очереди с claim-файлами и ретраями."""

    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self._client_id = f"worker:{os.getpid()}"
        self._sweep_at = 0.0
        self._key_lock = Lock()
        self._key_idx = 0

    def _next_key(self) -> str | None:
        pool = self.config.key_pool()
        if not pool:
            return None
        with self._key_lock:
            key = pool[self._key_idx % len(pool)]
            self._key_idx += 1
            return key

    def _eligible(self) -> list[tuple[str, dict]]:
        queueio.restore_expired_claims()
        eligible = []
        for path in queueio.iter_request_files():
            req_id = path.stem.removeprefix(queueio.REQ_PREFIX)
            if queueio.resp_path(req_id).exists() or queueio.claim_path(req_id).exists():
                continue
            payload = queueio.load_json(path)
            if payload is None:
                continue
            if self.config.claim_model is None or payload.get("model") == self.config.claim_model:
                eligible.append((req_id, payload))
        return eligible

    def _call_with_retries(self, req_id: str, payload: dict, token: str, lease: float) -> dict:
        for attempt in range(1, self.config.max_attempts + 1):
            queueio.renew_claim(req_id, lease, lease_token=token)
            rate_limited = False
            try:
                return call_model(payload, self.config, api_key=self._next_key())
            except urllib.error.HTTPError as exc:
                reason = f"HTTP {exc.code}"
                rate_limited = exc.code == 429
                if 400 <= exc.code < 500 and not rate_limited:
                    return {"error": f"upstream {reason}"}
            except (OSError, ValueError, KeyError, TypeError, IndexError, AttributeError) as exc:
                reason = type(exc).__name__

            if attempt == self.config.max_attempts:
                return {"error": f"upstream failed after {attempt} attempts: {reason}"}
            backoff = min(25 * attempt, 120) if rate_limited else min(2 ** min(attempt, 5), 30)
            log.warning("%s: attempt %d failed (%s), retry in %ds",
                        req_id, attempt, reason, backoff)
            time.sleep(backoff)

        raise ValueError("max_attempts must be positive")

    def _process(self, req_id: str, payload: dict) -> bool:
        lease = max(300.0, self.config.timeout + 150.0)
        record = queueio.claim(req_id, self._client_id, lease)
        if record is None:
            return False
        token = record["lease_token"]
        queueio.bump_client_stat(self._client_id, "claims")
        try:
            response = self._call_with_retries(req_id, payload, token, lease)
            queueio.complete_request(req_id, response, lease_token=token)
        except queueio.QueueConflict:
            log.info("%s: request ended before upstream reply", req_id)
        except Exception:
            log.exception("%s: worker failed", req_id)
            try:
                queueio.release_claim(req_id, lease_token=token)
            except queueio.QueueConflict:
                pass
        else:
            queueio.bump_client_stat(self._client_id, "answers")
            log.info("responded %s (in=%s out=%s)", req_id,
                     response.get("input_tokens"), response.get("output_tokens"))
        return True

    def run_once(self) -> int:
        now = time.monotonic()
        if now >= self._sweep_at:
            queueio.sweep_orphans()
            self._sweep_at = now + 60

        jobs = self._eligible()
        if self.config.workers == 1 or len(jobs) < 2:
            return sum(self._process(req_id, payload) for req_id, payload in jobs)
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(self.config.workers, len(jobs)),
            thread_name_prefix="ai-bridge-worker",
        ) as pool:
            futures = [pool.submit(self._process, req_id, payload) for req_id, payload in jobs]
            return sum(future.result() for future in futures)


def main() -> int:
    parser = argparse.ArgumentParser(description="ai-bridge queue worker")
    parser.add_argument("--interval", type=float, default=0.5,
                        help="Pause between queue scans, seconds")
    parser.add_argument("--once", action="store_true",
                        help="Process the queue once and exit")
    parser.add_argument("--workers", type=int, default=None,
                        help="Concurrent upstream calls "
                             "(default: AI_WORKER_WORKERS или 1)")
    parser.add_argument("--env-file", default=str(ENV_FILE),
                        help="Путь к .env с настройками LLM")
    parser.add_argument("--timeout", type=float, default=None,
                        help="Upstream request timeout, seconds")
    parser.add_argument("--max-attempts", type=int, default=None,
                        help="Upstream retry budget "
                             "(default: AI_WORKER_MAX_ATTEMPTS или 3)")
    parser.add_argument("--claim-model", default=None,
                        help="Consumer isolation: брать только запросы с этим "
                             "model-тегом (как model в /brain/claim; "
                             "default: AI_WORKER_CLAIM_MODEL)")
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    loaded = load_env_file(Path(args.env_file))
    config = WorkerConfig.from_env()
    # CLI побеждает только при явном значении: argparse-default'ы читают
    # os.getenv ДО load_env_file — иначе .env никогда не срабатывает.
    if args.workers is not None:
        config.workers = args.workers
    if args.max_attempts is not None:
        config.max_attempts = args.max_attempts
    if args.claim_model is not None:
        config.claim_model = args.claim_model
    config.interval = args.interval
    if args.timeout is not None:
        config.timeout = args.timeout

    if config.workers < 1 or config.max_attempts < 1:
        parser.error("workers and max-attempts must be positive")
    if not math.isfinite(config.timeout) or config.timeout <= 0:
        parser.error("timeout must be finite and positive")
    if not config.base_url or not config.key_pool():
        raise SystemExit(
            "Set AI_WORKER_BASE_URL and AI_WORKER_API_KEY (или API_KEYS "
            f"через запятую) before starting worker (env vars or {args.env_file})"
        )

    log.info("worker: %s @ %s (.env: %d values, workers=%d, max_attempts=%d, "
             "claim_model=%s, keys=%d)",
             config.model or "ai-bridge", config.base_url, loaded,
             config.workers, config.max_attempts, config.claim_model or "*",
             len(config.key_pool()))

    worker = Worker(config)
    try:
        while True:
            try:
                worker.run_once()
            except Exception:
                log.exception("queue scan failed — continuing")
            if args.once:
                return 0
            time.sleep(max(0.1, config.interval))
    except KeyboardInterrupt:
        log.info("stopped")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
