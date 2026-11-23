#!/usr/bin/env python3
"""browser_brain.py — мозг ai-bridge, отвечающий через браузерный чат.

Claim'ит запросы у моста и гоняет вкладку браузера через aquarium
cdp-bridge (aquarium/cdp_bridge.py, TCP line-JSON :8778 → Chrome CDP):
инжектит промпт в чат (Gemini и др.), ждёт завершения генерации,
забирает текст, отвечает в /brain/respond.

Для моста это обычный brain-клиент — семантика claim/lease та же,
что у agent.py / brain_claimer.py.

Запуск:
    python3 browser_brain.py                       # демон, адаптер gemini
    python3 browser_brain.py --site mock --once    # один запрос, тест-страница
    python3 -m aibridge.browser_brain --site gemini --model gemini-web

Адаптер сайта — JSON со сниппетами JS (всё исполняется через evaluate):

    {
      "url":        "https://...",           # страница чата ("" = активная вкладка)
      "ready_js":   "...",                   # truthy — инпут готов
      "send_js":    "... PROMPT ...",        # вставить промпт и отправить;
                                             # PROMPT подставится как JSON-строка
      "extract_js": "...",                   # текущий текст последнего ответа
      "busy_js":    "...",                   # (опц.) truthy пока идёт генерация
      "reset_js":   "..."                    # (опц.) new-chat между запросами
    }

Встроенные адаптеры (`aibridge/adapters/`): gemini, deepseek, chatgpt,
generic, mock. Свой — через --adapter /path/to.json, AI_BROWSER_ADAPTER
или <site>.json в $AI_BROWSER_ADAPTER_DIR. Селекторы у чатов дрейфуют —
подгонка через `python3 aquarium/aq.py '{"cmd":"evaluate",...}'`.

Env:
    AI_BROWSER_AQ      — cdp-bridge host:port    (default 127.0.0.1:8778)
    AI_BROWSER_SITE      — имя адаптера            (default gemini)
    AI_BROWSER_BRIDGE    — base-url моста          (default http://127.0.0.1:8777)
    AI_BROWSER_MODEL     — claim-тег               (default имя адаптера)
    AI_BROWSER_CLIENT_ID — client_id в claim       (default browser-brain)
    AI_BROWSER_ADAPTER   — путь к JSON-адаптеру
    AI_BROWSER_TAB       — "reuse"|"new"           (default reuse)
    AI_BROWSER_POLL      — интервал опроса страницы, сек (default 1.5)
    AI_BROWSER_STABLE    — сколько одинаковых опросов = ответ готов (default 3)
    AI_BROWSER_TIMEOUT   — макс. ожидание ответа страницы, сек (default 240)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from .agent import http

log = logging.getLogger("browser_brain")

# ---------------------------------------------------------------------------
# Адаптеры сайтов — aibridge/adapters/<site>.json
#
# Новый чат = новый JSON-файл, код не трогаем. Приоритет загрузки:
#   --adapter path  >  $AI_BROWSER_ADAPTER_DIR/<site>.json
#   >  aibridge/adapters/<site>.json
# ---------------------------------------------------------------------------

ADAPTER_DIR = Path(__file__).resolve().parent / "adapters"


def available_adapters() -> list[str]:
    dirs = [Path(os.getenv("AI_BROWSER_ADAPTER_DIR", "") or "/nonexistent"),
            ADAPTER_DIR]
    names = {f.stem for d in dirs if d.is_dir() for f in d.glob("*.json")}
    return sorted(names)


def load_adapter(site: str, path: str | None = None) -> dict:
    if path:
        adapter = json.loads(Path(path).read_text(encoding="utf-8"))
        adapter.setdefault("name", site)
        return adapter
    extra_dir = os.getenv("AI_BROWSER_ADAPTER_DIR")
    for d in ([Path(extra_dir)] if extra_dir else []) + [ADAPTER_DIR]:
        f = d / f"{site}.json"
        if f.is_file():
            adapter = json.loads(f.read_text(encoding="utf-8"))
            adapter.setdefault("name", site)
            return adapter
    raise SystemExit(f"unknown adapter {site!r}; available: "
                     f"{available_adapters()} или --adapter /path/to.json")


@dataclass
class Config:
    aq_host: str = "127.0.0.1"
    aq_port: int = 8778
    bridge: str = "http://127.0.0.1:8777"
    client_id: str = "browser-brain"
    model: str = ""                    # claim-тег; default = adapter name
    adapter: dict = field(default_factory=dict)
    tab_policy: str = "reuse"          # reuse | new
    poll: float = 1.5                  # интервал опроса страницы
    stable_polls: int = 3              # N одинаковых подряд = генерация завершена
    answer_timeout: float = 240.0      # макс. ожидание ответа страницы
    settle: float = 2.0                # пауза после navigate/reset

    @classmethod
    def from_env(cls) -> "Config":
        aq = os.getenv("AI_BROWSER_AQ", "127.0.0.1:8778")
        host, _, port = aq.rpartition(":")
        site = os.getenv("AI_BROWSER_SITE", "gemini")
        adapter = load_adapter(site, os.getenv("AI_BROWSER_ADAPTER"))
        return cls(
            aq_host=host or "127.0.0.1",
            aq_port=int(port) if port else 8778,
            bridge=os.getenv("AI_BROWSER_BRIDGE", "http://127.0.0.1:8777").rstrip("/"),
            client_id=os.getenv("AI_BROWSER_CLIENT_ID", "browser-brain"),
            model=os.getenv("AI_BROWSER_MODEL", ""),
            adapter=adapter,
            tab_policy=os.getenv("AI_BROWSER_TAB", "reuse"),
            poll=float(os.getenv("AI_BROWSER_POLL", "1.5")),
            stable_polls=int(os.getenv("AI_BROWSER_STABLE", "3")),
            answer_timeout=float(os.getenv("AI_BROWSER_TIMEOUT", "240")),
        )


# ---------------------------------------------------------------------------
# Aquarium TCP-клиент
# ---------------------------------------------------------------------------

class Aquarium:
    """Тонкий клиент cdp-bridge TCP-моста (JSON-строка → JSON-ответ)."""

    def __init__(self, host: str, port: int, timeout: float = 30.0):
        self.addr = (host, port)
        self.timeout = timeout

    def cmd(self, **kw) -> dict:
        line = json.dumps(kw).encode() + b"\n"
        with socket.create_connection(self.addr, timeout=self.timeout) as s:
            s.sendall(line)
            buf = b""
            while not buf.endswith(b"\n"):
                chunk = s.recv(1 << 20)
                if not chunk:
                    break
                buf += chunk
        try:
            return json.loads(buf.decode())
        except json.JSONDecodeError:
            return {"ok": False, "error": f"bad aquarium reply: {buf[:200]!r}"}

    def eval(self, expression: str, context: str | None):
        """evaluate в табе; возвращает value или кидает RuntimeError."""
        kw = {"cmd": "evaluate", "expression": expression}
        if context:
            kw["context"] = context
        resp = self.cmd(**kw)
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "evaluate failed"))
        value = resp.get("value")
        if isinstance(value, str) and value.startswith("ERR:"):
            raise RuntimeError(value)
        return value


# ---------------------------------------------------------------------------
# Драйвер чат-вкладки
# ---------------------------------------------------------------------------

class ChatTab:
    """Одна вкладка = один одновременный диалог."""

    def __init__(self, aq: Aquarium, adapter: dict, cfg: Config):
        self.aq = aq
        self.adapter = adapter
        self.cfg = cfg
        self.context: str | None = None

    def open(self) -> None:
        url = self.adapter.get("url", "")
        if self.context is not None and not self._alive():
            log.warning("tab %s gone — reopening", self.context)
            self.context = None
        if self.context is None and url:
            self.context = self._find_tab(url) or self._new_tab(url)
            log.info("tab %s → %s", self.context, url)
        elif self.context is None:
            log.info("adapter has no url — driving active tab")
        self._wait_ready()

    def _alive(self) -> bool:
        """Закешированный context ещё жив?
        (таб могли закрыть руками или браузер рестартанули)."""
        try:
            self.aq.eval("1", self.context)
            return True
        except Exception:
            return False

    def _find_tab(self, url: str) -> str | None:
        """reuse: цепляемся к уже открытой вкладке сайта вместо дубля."""
        if self.cfg.tab_policy != "reuse":
            return None
        try:
            resp = self.aq.cmd(cmd="tabs")
        except OSError:
            return None
        if not resp.get("ok"):
            return None
        target = urlsplit(url)
        for tab in resp.get("tabs") or []:
            current_url = str(tab.get("url", ""))
            current = urlsplit(current_url)
            same_origin = (
                target.scheme in {"http", "https"}
                and target.scheme == current.scheme
                and target.netloc.lower() == current.netloc.lower()
            )
            if same_origin or current_url == url:
                context = str(tab.get("context") or "")
                if context:
                    return context
        return None

    def _new_tab(self, url: str) -> str:
        resp = self.aq.cmd(cmd="newtab", url=url)
        if not resp.get("ok"):
            raise RuntimeError(f"newtab failed: {resp}")
        return str(resp["context"])

    @staticmethod
    def _expr(js: str) -> str:
        """Адаптер-JS → expression: всегда async-IIFE, чтобы мост мог
        взять `return await (...)` для любой формы сниппета."""
        return f"(async () => {{ {js} }})()"

    def _wait_ready(self, timeout: float = 60.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if self.aq.eval(self._expr(self.adapter["ready_js"]), self.context):
                    return
            except RuntimeError as e:
                log.debug("ready probe: %s", e)
            time.sleep(1.5)
        raise TimeoutError("chat input not ready (page not loaded / not logged in?)")

    def reset(self) -> None:
        js = self.adapter.get("reset_js")
        if not js:
            return
        self.aq.eval(self._expr(js), self.context)
        time.sleep(self.cfg.settle)
        self._wait_ready()

    def close(self) -> None:
        if self.context is None:
            return
        try:
            self.aq.cmd(cmd="closetab", context=self.context)
        except OSError:
            log.warning("Could not close temporary tab")
        finally:
            self.context = None

    def send(self, prompt: str) -> None:
        js = self.adapter["send_js"].replace("PROMPT", json.dumps(prompt))
        res = self.aq.eval(self._expr(js), self.context)
        if isinstance(res, str) and res.startswith("ERR"):
            raise RuntimeError(res)
        log.debug("send: %s", res)

    def wait_answer(self, deadline: float) -> str:
        """Опрашиваем extract_js; ответ готов когда текст не меняется
        stable_polls подряд и нет busy-индикатора."""
        last, same = "", 0
        busy_js = self.adapter.get("busy_js")
        while time.time() < deadline:
            try:
                text = self.aq.eval(self._expr(self.adapter["extract_js"]),
                                    self.context) or ""
                busy = bool(busy_js and self.aq.eval(self._expr(busy_js),
                                                     self.context))
            except RuntimeError as e:
                log.debug("poll error: %s", e)
                time.sleep(self.cfg.poll)
                continue
            if text and text == last and not busy:
                same += 1
                if same >= self.cfg.stable_polls:
                    return text
            else:
                same = 0
            last = text
            time.sleep(self.cfg.poll)
        raise TimeoutError(f"no stable answer in {self.cfg.answer_timeout}s")


# ---------------------------------------------------------------------------
# Промпт / парсинг
# ---------------------------------------------------------------------------

def build_prompt(request: dict) -> str:
    """Сжимаем messages[] в один текст для чата."""
    parts = []
    for m in request.get("messages") or []:
        role = (m.get("role") or "user").upper()
        content = m.get("content")
        if isinstance(content, list):  # multimodal blocks → текстовая часть
            content = "\n".join(b.get("text", "") for b in content
                                if isinstance(b, dict) and b.get("type") == "text")
        if content:
            parts.append(f"[{role}]\n{content}")
    prompt = "\n\n".join(parts)
    if request.get("tools"):
        names = [t.get("function", {}).get("name", "?")
                 for t in request["tools"] if isinstance(t, dict)]
        prompt += (
            "\n\n[SYSTEM] Доступны функции: " + ", ".join(names) + ". "
            "Если нужен вызов функции — ответь ТОЛЬКО JSON без markdown: "
            '{"tool_call": {"name": "...", "arguments": {...}}}')
    return prompt


def parse_answer(text: str, request: dict) -> dict:
    """Ответ страницы → body для /brain/respond."""
    out: dict = {"content": text}
    if not request.get("tools") or not text:
        return out
    # Выковыриваем {"tool_call": ...} даже из ```json ...``` обёртки.
    chunk = text
    if "```" in chunk:
        seg = chunk.split("```")[1]
        chunk = seg[seg.find("\n"):] if "\n" in seg else seg
    start = chunk.find("{")
    if start >= 0:
        try:
            # raw_decode: игнорит текст после JSON (Gemini любит комментарии)
            obj, _ = json.JSONDecoder().raw_decode(chunk[start:])
        except json.JSONDecodeError:
            obj = None
        if isinstance(obj, dict) and isinstance(obj.get("tool_call"), dict):
            tc = obj["tool_call"]
            out["tool_calls"] = [{
                "name": tc.get("name", ""),
                "arguments": tc.get("arguments", {}),
            }]
            out["content"] = ""
    return out


# ---------------------------------------------------------------------------
# Brain loop
# ---------------------------------------------------------------------------

def claim(cfg: Config) -> dict | None:
    body = {"client_id": cfg.client_id}
    if cfg.model:
        body["model"] = cfg.model
    status, payload = http("POST", f"{cfg.bridge}/brain/claim", body)
    if status == 204:
        return None
    if status != 200:
        log.warning("claim failed: %s %s", status, payload)
        return None
    return payload


def respond(cfg: Config, req_id: str, body: dict) -> bool:
    status, payload = http("POST", f"{cfg.bridge}/brain/respond/{req_id}", body)
    if status != 200:
        log.error("respond %s → %s %s", req_id, status, payload)
        return False
    return True


def renew(cfg: Config, req_id: str, token: str) -> float | None:
    status, data = http("POST", f"{cfg.bridge}/brain/renew/{req_id}", {"lease_token": token})
    return float(data["lease_expires_at"]) if status == 200 else None


def handle_one(tab: ChatTab, cfg: Config, lease_seconds: float) -> bool:
    claim_payload = claim(cfg)
    if not claim_payload:
        return False
    req = claim_payload["request"]
    req_id, token = req["id"], claim_payload["lease_token"]
    log.info("claimed %s (model=%s)", req_id, req.get("model"))
    try:
        tab.open()
        if cfg.tab_policy == "reuse":
            tab.reset()
        expires = renew(cfg, req_id, token)
        if expires is None:
            return False
        tab.send(build_prompt(req))
        half_lease = min(lease_seconds, expires - time.time()) / 2
        renew_at = time.time() + half_lease
        deadline = time.time() + cfg.answer_timeout
        # poll'им ответ, продлевая lease на длинных генерациях
        while time.time() < deadline:
            if time.time() >= renew_at:
                expires = renew(cfg, req_id, token)
                if expires is None:
                    return False
                half_lease = min(lease_seconds, expires - time.time()) / 2
                renew_at = time.time() + half_lease
            try:
                text = tab.wait_answer(min(renew_at, deadline))
                break
            except TimeoutError:
                continue
        else:
            raise TimeoutError("answer timeout")
        body = parse_answer(text, req)
        body["lease_token"] = token
        if respond(cfg, req_id, body):
            log.info("answered %s (%d chars, tool_calls=%d)",
                     req_id, len(body.get("content") or ""),
                     len(body.get("tool_calls") or []))
            return True
        return False
    except Exception as e:
        log.error("request %s failed: %s", req_id, type(e).__name__)
        respond(cfg, req_id, {"lease_token": token,
                              "error": f"browser adapter failed: {type(e).__name__}"})
        return False
    finally:
        if cfg.tab_policy == "new":
            tab.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="brain-worker: браузерный чат (aquarium) как LLM для ai-bridge")
    p.add_argument("--site", default=None, help="адаптер: gemini|mock (default $AI_BROWSER_SITE)")
    p.add_argument("--adapter", default=None, help="путь к JSON-адаптеру")
    p.add_argument("--bridge", default=None)
    p.add_argument("--model", default=None, help="claim-тег (default: имя адаптера)")
    p.add_argument("--client-id", default=None)
    p.add_argument("--aq", default=None, help="host:port аквариума")
    p.add_argument("--tab", choices=["reuse", "new"], default=None)
    p.add_argument("--lease", type=float, default=300.0)
    p.add_argument("--interval", type=float, default=2.0,
                   help="пауза между claim-попытками")
    p.add_argument("--once", action="store_true", help="один запрос и выход")
    p.add_argument("--log-level", default="INFO")
    return p


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    cfg = Config.from_env()
    if args.site or args.adapter:
        cfg.adapter = load_adapter(args.site or "gemini", args.adapter)
    if args.aq:
        host, _, port = args.aq.rpartition(":")
        cfg.aq_host, cfg.aq_port = host, int(port)
    for attr, val in (("bridge", args.bridge), ("client_id", args.client_id),
                      ("tab_policy", args.tab)):
        if val:
            setattr(cfg, attr, val)
    cfg.bridge = cfg.bridge.rstrip("/")
    cfg.model = args.model or cfg.model or cfg.adapter.get("name", "gemini")

    aq = Aquarium(cfg.aq_host, cfg.aq_port)
    st = aq.cmd(cmd="status")
    if not st.get("ok"):
        log.error("aquarium not reachable at %s:%s — %s",
                  cfg.aq_host, cfg.aq_port, st.get("error"))
        return 2

    tab = ChatTab(aq, cfg.adapter, cfg)
    log.info("browser-brain up: adapter=%s model=%s bridge=%s tab=%s",
             cfg.adapter.get("name"), cfg.model, cfg.bridge, cfg.tab_policy)

    while True:
        # preflight: браузер/таб не готов — не claim'им, запросы ждут
        # в очереди, а не сгорают с ошибкой. Само восстановится, когда
        # браузер вернётся.
        try:
            if cfg.tab_policy == "reuse":
                tab.open()
            elif not aq.cmd(cmd="status").get("browser"):
                raise RuntimeError("browser not reachable")
        except Exception as e:
            log.warning("browser not ready: %s", e)
            if args.once:
                return 1
            time.sleep(max(args.interval, 5.0))
            continue
        try:
            did = handle_one(tab, cfg, args.lease)
        except Exception as e:  # claim-уровень (мост лежит и т.п.)
            log.error("loop error: %s", e)
            did = False
        if args.once:
            return 0 if did else 1
        if not did:
            time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
