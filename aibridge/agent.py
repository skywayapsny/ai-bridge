#!/usr/bin/env python3
"""agent.py — клиент мозга для ai-bridge (brain claim/lease protocol).

Так Devin-сессия (или любой другой агент) становится LLM-бэкендом проекта,
подключённого к мосту.

    # Разовый claim (мозг сам поллит):
    python3 agent.py claim                        → JSON: request + lease_token
    python3 agent.py claim --model devin          → только запросы с model=devin

    # Ответить (нужен lease_token из claim):
    python3 agent.py respond <id> --token <t> --content "текст"
    python3 agent.py respond <id> --token <t> --tool get_weather --args '{"city":"Msk"}'
    python3 agent.py respond <id> --token <t> --file resp.json
    python3 agent.py respond <id> --token <t> --error "can't answer"

    # Долгое думание / отказ:
    python3 agent.py renew <id> --token <t>       # +LEASE_SECONDS к аренде
    python3 agent.py release <id> --token <t>     # вернуть запрос в очередь

    # Claimer-демон: пишет claim-JSON в файл,
    # мозг читает файл и отвечает через respond:
    python3 agent.py watch --out /tmp/ai_bridge_claim.json
    python3 agent.py watch --wait 25            # long-poll: мгновенная доставка

    # Сессионный мозг (рекомендуется): SSE-подписка /brain/stream —
    # lease продлевается сам, при обрыве запросы сразу назад в очередь:
    python3 agent.py stream --model devin-local --out /tmp/brain/local.claim

    # Обзор:
    python3 agent.py pending     # unclaimed запросы (полные payload'ы)
    python3 agent.py stats       # per-client claims/answers

Stdlib only — работает в любом окружении без pip.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Iterator
from pathlib import Path

DEFAULT_BRIDGE = os.getenv("AI_BRIDGE_URL", "http://127.0.0.1:8777")
DEFAULT_CLIENT_ID = os.getenv("AI_BRIDGE_CLIENT_ID", "brain-agent")


def authorize(request: urllib.request.Request) -> None:
    key = (os.getenv("AI_BRIDGE_AGENT_KEY") or os.getenv("AI_BRIDGE_API_KEY", "")).strip()
    if key:
        request.add_unredirected_header("Authorization", f"Bearer {key}")


def http(method: str, url: str, body: dict | None = None,
         timeout: float = 30.0) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    authorize(req)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return e.code, {"error": raw.decode("utf-8", errors="replace")}
    except urllib.error.URLError as e:
        return 0, {"error": str(e.reason)}


def cmd_claim(args) -> int:
    """Claim один запрос; печатает JSON (request + lease_token) или 204."""
    body: dict = {"client_id": args.client_id}
    if args.model:
        body["model"] = args.model
    if args.id:
        body["request_id"] = args.id
    wait = getattr(args, "wait", 0) or 0
    if wait:
        body["wait"] = wait
    status, payload = http("POST", f"{args.bridge}/brain/claim", body,
                           timeout=max(30.0, wait + 10))
    if status == 204:
        print("no pending requests", file=sys.stderr)
        return 1
    if status != 200:
        print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
        return 1
    if args.out:
        _write_claim(args.out, payload)
        req = payload["request"]
        print(f"claimed {req.get('id')} -> {args.out} "
                  f"(lease until {time.strftime('%H:%M:%S', time.localtime(payload['lease_expires_at']))})")
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _build_response(args) -> dict:
    resp: dict = {"lease_token": args.token}
    if args.error is not None:
        resp["error"] = args.error
    if args.content is not None:
        resp["content"] = args.content
    if args.tool:
        try:
            tool_args = json.loads(args.args or "{}")
        except json.JSONDecodeError:
            tool_args = args.args or "{}"
        if not isinstance(tool_args, str):
            tool_args = json.dumps(tool_args, ensure_ascii=False)
        resp["tool_calls"] = [{
            "id": f"call_{uuid.uuid4().hex[:24]}",
            "name": args.tool,
            "arguments": tool_args,  # строка — канон для OpenAI-клиентов
        }]
        resp.setdefault("finish_reason", "tool_calls")
    if args.file:
        resp.update(json.loads(Path(args.file).read_text(encoding="utf-8")))
        resp["lease_token"] = args.token
    return resp


def _resolve_token(args) -> str:
    if getattr(args, "claim_file", None):
        data = json.loads(Path(args.claim_file).read_text(encoding="utf-8"))
        return data.get("lease_token", "")
    return args.token or ""


def cmd_respond(args) -> int:
    args.token = _resolve_token(args)
    resp = _build_response(args)
    status, payload = http("POST", f"{args.bridge}/brain/respond/{args.id}", resp)
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if status == 200 else 1


def cmd_renew(args) -> int:
    status, payload = http("POST", f"{args.bridge}/brain/renew/{args.id}",
                           {"lease_token": _resolve_token(args)})
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if status == 200 else 1


def cmd_release(args) -> int:
    status, payload = http("POST", f"{args.bridge}/brain/release/{args.id}",
                           {"lease_token": _resolve_token(args)})
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if status == 200 else 1


def cmd_pending(args) -> int:
    status, payload = http("GET", f"{args.bridge}/brain/pending")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if status == 200 else 1


def cmd_stats(args) -> int:
    status, payload = http("GET", f"{args.bridge}/brain/stats")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if status == 200 else 1


def _write_claim(path: str, claim: dict) -> None:
    target = Path(path)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=target.parent, prefix=".claim-", delete=False
    ) as file:
        temporary = Path(file.name)
        try:
            json.dump(claim, file, ensure_ascii=False)
            file.close()
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)


def cmd_watch(args) -> int:
    """Claimer-демон: забирает запросы и пишет claim-JSON в файл.

    Мозг читает файл, думает, отвечает через `agent.py respond`.
    --wait N включает long-poll /brain/claim (мгновенная доставка,
    без поллинга); lease при этом НЕ продлевается — для сессионного
    режима с авто-арендой используй `agent.py stream`.
    """
    print(f"watching {args.bridge} as {args.client_id} "
          f"(model={args.model or '*'}, wait={args.wait}) -> {args.out}", flush=True)
    n = 0
    while True:
        body: dict = {"client_id": args.client_id, "wait": args.wait}
        if args.model:
            body["model"] = args.model
        try:
            status, claim = http("POST", f"{args.bridge}/brain/claim",
                                 body, timeout=max(10.0, args.wait + 10))
        except Exception as e:
            print(f"claim error: {e}", flush=True)
            status, claim = 0, {}
        if status == 200 and claim.get("request"):
            n += 1
            path = args.out if n == 1 else f"{args.out}.{n}"
            _write_claim(path, claim)
            print(f"claimed {claim['request'].get('id')} -> {path}", flush=True)
        elif status not in (0, 204):
            print(f"claim http {status}: {claim}", flush=True)
        # long-poll уже ждал на сервере — спим только на ошибке/пустом
        # неблокирующем цикле, иначе busy-loop.
        if status != 200 or args.wait == 0:
            time.sleep(args.interval)


def _sse_events(resp) -> Iterator[tuple[str, str]]:
    """Разобрать SSE-поток в пары (event, data). Блокирует до события."""
    event = "message"
    data_lines: list[str] = []
    for raw in resp:
        line = raw.decode("utf-8", "replace").rstrip("\r\n")
        if line == "":
            if data_lines:
                yield event, "\n".join(data_lines)
            event, data_lines = "message", []
        elif line.startswith(":"):
            continue  # heartbeat-комментарий
        elif line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())


def cmd_stream(args) -> int:
    """Сессионный мозг: подписка на /brain/stream (SSE) → claim-файлы.

    Сервер claim'ит запросы от нашего client_id и пушит их событиями;
    lease продлевается, пока соединение живо; при обрыве невыполненное
    само возвращается в очередь. Это «нативное» подключение мозга:
    один стрим = ты онлайн как LLM.
    """
    query = {"client_id": args.client_id, "max_inflight": args.max_inflight}
    if args.model:
        query["model"] = args.model
    url = f"{args.bridge}/brain/stream?{urllib.parse.urlencode(query)}"
    n = 0
    while True:
        try:
            req = urllib.request.Request(url)
            authorize(req)
            with urllib.request.urlopen(req, timeout=args.timeout) as resp:
                print(f"stream connected {args.bridge} as {args.client_id} "
                      f"(model={args.model or '*'})", flush=True)
                for event, data in _sse_events(resp):
                    if event != "claim":
                        continue
                    try:
                        claim = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    n += 1
                    path = args.out if n == 1 else f"{args.out}.{n}"
                    _write_claim(path, claim)
                    print(f"claimed {claim['request'].get('id')} -> {path}",
                          flush=True)
        except (OSError, ValueError) as e:
            print(f"stream error: {e}", flush=True)
        time.sleep(3)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Brain client for ai-bridge (claim/lease protocol)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    p.add_argument("--bridge", default=DEFAULT_BRIDGE)
    p.add_argument("--client-id", default=DEFAULT_CLIENT_ID,
                   help="идентификатор мозга в /brain/stats")
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("claim", help="Atomically claim one pending request")
    c.add_argument("--model", help="claim only requests with this model tag")
    c.add_argument("--id", help="claim a specific request id")
    c.add_argument("--wait", type=float, default=0,
                   help="long-poll: wait up to N sec for a request")
    c.add_argument("--out", help="write claim JSON to file instead of stdout")

    r = sub.add_parser("respond", help="Respond to a claimed request")
    r.add_argument("id")
    r.add_argument("--token", help="lease_token from claim")
    r.add_argument("--claim-file", help="JSON file from 'claim --out' (reads token)")
    r.add_argument("--content")
    r.add_argument("--tool", help="tool name for a tool_call response")
    r.add_argument("--args", help="tool arguments JSON (with --tool)")
    r.add_argument("--error", help="report an error instead of an answer")
    r.add_argument("--file", help="full response JSON file (merged last)")

    n = sub.add_parser("renew", help="Extend the lease")
    n.add_argument("id")
    n.add_argument("--token")
    n.add_argument("--claim-file", help="JSON file from 'claim --out'")

    rel = sub.add_parser("release", help="Return a claimed request to the queue")
    rel.add_argument("id")
    rel.add_argument("--token")
    rel.add_argument("--claim-file", help="JSON file from 'claim --out'")

    sub.add_parser("pending", help="List unclaimed requests (full payloads)")
    sub.add_parser("stats", help="Per-client brain statistics")

    w = sub.add_parser("watch", help="Claimer daemon: claim -> JSON file")
    w.add_argument("--model", help="claim only requests with this model tag")
    w.add_argument("--out", default="/tmp/ai_bridge_claim.json")
    w.add_argument("--wait", type=float, default=0,
                   help="long-poll claim wait, sec (instant delivery; "
                        "no auto-renew — prefer `stream` for a session)")
    w.add_argument("--interval", type=float, default=0.7)

    s = sub.add_parser("stream", help="Brain session: SSE /brain/stream -> claim files")
    s.add_argument("--model", help="claim only requests with this model tag")
    s.add_argument("--out", default="/tmp/ai_bridge_claim.json")
    s.add_argument("--max-inflight", type=int, default=8,
                   help="max simultaneous claimed requests held by the stream")
    s.add_argument("--timeout", type=float, default=60,
                   help="reconnect if the stream is silent for N sec")

    return p


def main() -> int:
    args = build_parser().parse_args()
    args.bridge = args.bridge.rstrip("/")
    handler = {
        "claim": cmd_claim,
        "respond": cmd_respond,
        "renew": cmd_renew,
        "release": cmd_release,
        "pending": cmd_pending,
        "stats": cmd_stats,
        "watch": cmd_watch,
        "stream": cmd_stream,
    }[args.cmd]
    try:
        return handler(args)
    except KeyboardInterrupt:
        print("\nstopped", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
