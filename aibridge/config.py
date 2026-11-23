"""Centralized configuration for ai-bridge components.

BridgeConfig / WorkerConfig собираются из env → CLI-аргументы перекрывают env.
Никаких module-level globals, которые main() мутирует через `global`:
конфиг создаётся один раз и передаётся в create_app()/Worker.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from ipaddress import ip_address
from pathlib import Path

ENV_PREFIX = "AI_BRIDGE_"
WORKER_PREFIX = "AI_WORKER_"


def _env(name: str, default: str = "") -> str:
    return os.getenv(ENV_PREFIX + name, default)


def _fenv(name: str, default: float) -> float:
    try:
        return float(_env(name)) if _env(name) else default
    except ValueError:
        return default


@dataclass
class BridgeConfig:
    """Настройки моста (aibridge.server)."""
    host: str = "127.0.0.1"
    port: int = 8777
    model: str = "ai-bridge"
    models: tuple[str, ...] = ()    # рекламируемые id в /v1/models (AI_BRIDGE_MODELS, csv)
    timeout: float = 300.0          # сколько ждать ответа мозга/worker'а
    poll_interval: float = 0.3      # период проверки resp-файла
    lease_seconds: float = 300.0    # TTL claim-аренды
    api_key: str = ""               # пусто = /v1/* без авторизации
    agent_key: str = ""             # /brain/* and /queue/*; falls back to api_key
    max_inflight: int = 32
    require_claim: bool = True      # /brain/respond только по живому claim
    log_level: str = "info"

    @classmethod
    def from_env(cls) -> "BridgeConfig":
        models = tuple(
            m.strip() for m in _env("MODELS").split(",") if m.strip()
        )
        return cls(
            host=_env("HOST", cls.host),
            port=int(_env("PORT", str(cls.port))),
            model=_env("MODEL", cls.model),
            models=models,
            timeout=_fenv("TIMEOUT", cls.timeout),
            poll_interval=_fenv("POLL_INTERVAL", cls.poll_interval),
            lease_seconds=_fenv("LEASE_SECONDS", cls.lease_seconds),
            api_key=_env("API_KEY").strip(),
            agent_key=_env("AGENT_KEY").strip(),
            max_inflight=int(_env("MAX_INFLIGHT", str(cls.max_inflight))),
            log_level=_env("LOG_LEVEL", cls.log_level),
            require_claim=_env("REQUIRE_CLAIM", "1") == "1",
        )

    @property
    def effective_agent_key(self) -> str:
        return self.agent_key or self.api_key

    @property
    def is_loopback(self) -> bool:
        if self.host == "localhost":
            return True
        try:
            return ip_address(self.host).is_loopback
        except ValueError:
            return False

    def validate(self) -> None:
        if not self.is_loopback and not (self.api_key and self.effective_agent_key):
            raise ValueError("A non-loopback bind requires AI_BRIDGE_API_KEY")
        if not 1 <= self.port <= 65535:
            raise ValueError("Bridge port must be between 1 and 65535")
        if self.max_inflight < 1:
            raise ValueError("AI_BRIDGE_MAX_INFLIGHT must be positive")
        for name in ("timeout", "poll_interval", "lease_seconds"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.poll_interval >= self.lease_seconds / 2:
            raise ValueError("poll_interval must be less than half the lease duration")

    def apply_args(self, args) -> None:
        """CLI-флаги перекрывают env/defaults (только явно заданные)."""
        for attr, arg in (("host", "host"), ("port", "port"), ("model", "model"),
                          ("timeout", "timeout"), ("poll_interval", "poll_interval"),
                          ("lease_seconds", "lease"), ("log_level", "log_level")):
            value = getattr(args, arg, None)
            if value is not None:
                setattr(self, attr, value)


@dataclass
class WorkerConfig:
    """Настройки worker'а (aibridge.worker). Все поля — из env/CLI."""
    base_url: str = ""
    api_key: str = ""
    api_keys: tuple[str, ...] = ()  # ротация: AI_WORKER_API_KEYS (через запятую)
    model: str = ""                # форсировать модель upstream ("" = из запроса)
    timeout: float = 60.0
    max_attempts: int = 3
    workers: int = 1
    claim_model: str | None = None
    interval: float = 0.5
    thinking: str = ""             # enabled|disabled|""
    reasoning_effort: str = ""     # high|max|""
    extra_headers: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        headers: dict = {}
        raw = os.getenv(WORKER_PREFIX + "EXTRA_HEADERS", "").strip()
        if raw:
            import json
            try:
                headers = {str(k): str(v) for k, v in json.loads(raw).items()}
            except (json.JSONDecodeError, AttributeError):
                pass
        keys = tuple(
            k.strip() for k in
            os.getenv(WORKER_PREFIX + "API_KEYS", "").split(",")
            if k.strip()
        )
        return cls(
            base_url=os.getenv(WORKER_PREFIX + "BASE_URL", "").strip(),
            api_key=os.getenv(WORKER_PREFIX + "API_KEY", "").strip(),
            api_keys=keys,
            model=os.getenv(WORKER_PREFIX + "MODEL", "").strip(),
            timeout=float(os.getenv(WORKER_PREFIX + "TIMEOUT", "60")),
            max_attempts=int(os.getenv(WORKER_PREFIX + "MAX_ATTEMPTS", "3")),
            workers=int(os.getenv(WORKER_PREFIX + "WORKERS", "1")),
            claim_model=os.getenv(WORKER_PREFIX + "CLAIM_MODEL") or None,
            thinking=os.getenv(WORKER_PREFIX + "THINKING", "").strip().lower(),
            reasoning_effort=os.getenv(WORKER_PREFIX + "REASONING_EFFORT", "").strip().lower(),
            extra_headers=headers,
        )

    def key_pool(self) -> tuple[str, ...]:
        """Пул ключей: API_KEYS приоритетнее одиночного API_KEY."""
        return self.api_keys or ((self.api_key,) if self.api_key else ())

    def upstream_model(self, payload: dict) -> str:
        return self.model or payload.get("model") or "ai-bridge"

    def thinking_payload(self) -> dict:
        """Reasoning-режим провайдера (DeepSeek и подобные).

        Параметр добавляется только при явном enabled/disabled — иначе запрос
        нейтрален для любых OpenAI-совместимых endpoint'ов.
        """
        if self.thinking not in {"enabled", "disabled"}:
            return {}
        payload: dict = {"thinking": {"type": self.thinking}}
        if self.thinking == "enabled" and self.reasoning_effort in {"high", "max"}:
            payload["reasoning_effort"] = self.reasoning_effort
        return payload


def load_env_file(path: Path) -> int:
    """Читает KEY=VALUE из .env. Уже заданные переменные не перезаписывает."""
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return 0

    loaded = 0
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip('"').strip("'")
        loaded += 1
    return loaded
