#!/usr/bin/env python3
"""Universal AI Bridge — OpenAI-compatible HTTP server с file-queue бэкендом.

Любой проект, использующий OpenAI SDK (или OpenAI-compatible API), подключается
к этому мосту вместо облака. Мост паркует запросы в file-queue, внешний агент
(worker.py, Devin-сессия через /brain/claim, любой другой) отвечает.

Подключение к проекту:
    LLM_PROVIDER=openai-compatible
    LLM_BASE_URL=http://127.0.0.1:8777/v1
    LLM_API_KEY=ai-bridge
    LLM_MODEL=ai-bridge

Запуск:
    python3 bridge.py                    # :8777 (шим → aibridge.server)
    python3 -m aibridge                  # то же самое
    ai-bridge                            # console script (pip install -e .)
    python3 bridge.py --port 8777 --host 127.0.0.1 --timeout 300

Endpoints: см. README.md — /v1/chat/completions (+SSE), /queue/*, /brain/*.

Конфигурация (env, перекрывается CLI):
    AI_BRIDGE_QUEUE_DIR     — каталог очереди (default: ./queue)
    AI_BRIDGE_TIMEOUT       — сколько ждать ответа агента, сек (default: 300)
    AI_BRIDGE_POLL_INTERVAL — интервал проверки resp-файла, сек (default: 0.3)
    AI_BRIDGE_MODEL         — имя модели в /v1/models (default: ai-bridge)
    AI_BRIDGE_API_KEY       — ключ /v1/* и резервный ключ /brain/* и /queue/*
    AI_BRIDGE_AGENT_KEY     — отдельный ключ /brain/* и /queue/*
    AI_BRIDGE_LEASE_SECONDS — TTL claim-аренды (default: 300)
    AI_BRIDGE_REQUIRE_CLAIM — "0": /brain/respond без claim (default: 1)
"""
from __future__ import annotations

import argparse
import logging

from . import queueio
from .config import BridgeConfig


def main() -> None:
    parser = argparse.ArgumentParser(description="Universal AI Bridge")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--model", help="Model name to report")
    parser.add_argument("--timeout", type=float,
                        help="Seconds to wait for the agent response")
    parser.add_argument("--poll-interval", type=float,
                        help="Seconds between response-file checks")
    parser.add_argument("--lease", type=float,
                        help="Brain claim lease TTL, seconds")
    parser.add_argument("--log-level")
    args = parser.parse_args()

    try:
        config = BridgeConfig.from_env()
        config.apply_args(args)
        config.validate()
    except ValueError as exc:
        parser.error(str(exc))

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    from .app import create_app  # поздний импорт: fastapi не нужна для --help
    app = create_app(config)

    print(f"Universal AI Bridge on {config.host}:{config.port}")
    print(f"  Queue dir:      {queueio.QUEUE_DIR}")
    print(f"  Model:          {config.model}")
    print(f"  Timeout:        {config.timeout:.0f}s")
    print(f"  Client auth:    {'Bearer' if config.api_key else 'local'}")
    print(f"  Agent auth:     {'Bearer' if config.effective_agent_key else 'local'}")
    print("  Endpoints:")
    print("    POST   /v1/chat/completions  (OpenAI-compatible, stream=true → SSE)")
    print("    GET    /v1/models")
    print("    GET    /health")
    print("    GET    /queue/pending | /queue/next?timeout=30 | /queue/request/{id}")
    print("    POST   /queue/respond/{id}")
    print("    GET    /queue/stats")
    print("    DELETE /queue/request/{id}")
    print(f"  Brain protocol (lease={config.lease_seconds:.0f}s, require_claim={config.require_claim}):")
    print("    POST   /brain/claim            {client_id, model?, wait?}")
    print("    GET    /brain/stream           SSE-сессия мозга (авто-lease)")
    print("    POST   /brain/respond/{id}    {lease_token, content, tool_calls}")
    print("    POST   /brain/renew/{id}      {lease_token}")
    print("    POST   /brain/release/{id}    {lease_token}")
    print("    GET    /brain/pending | /brain/stats")

    import uvicorn
    uvicorn.run(app, host=config.host, port=config.port, log_level=config.log_level)


if __name__ == "__main__":
    main()
