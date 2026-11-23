#!/usr/bin/env python3
"""CLI helper для агента (Devin) — поллить queue и отправлять ответы.

Usage:
    python3 poll.py list                          # список pending запросов
    python3 poll.py show <id>                     # показать конкретный запрос
    python3 poll.py respond <id> <file>           # отправить ответ из JSON-файла
    python3 poll.py respond-text <id> "текст"     # отправить текстовый ответ
    python3 poll.py respond-tool <id> <tool> <args_json>   # ответ с tool_call
    python3 poll.py respond-text-tool <id> "текст" <tool> <args_json>
    python3 poll.py respond-error <id> "причина"  # вернуть ошибку клиенту (502)
    python3 poll.py claim <id>                    # атомарно забрать запрос
    python3 poll.py release <id>                  # снять claim
    python3 poll.py stats                         # статистика
    python3 poll.py clean                         # очистить просроченные запросы
    python3 poll.py watch [--interval 1.0]        # ждать новые запросы

Пример ответа с tool_call:
    python3 poll.py respond-tool abc123 search_listings '{"query": "iphone"}'

Пример полного ответа из файла:
    echo '{"content":"Hello"}' > /tmp/resp.json
    python3 poll.py respond abc123 /tmp/resp.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path

from . import queueio


def _check_id(req_id: str) -> str:
    if not queueio.is_valid_req_id(req_id):
        print(f"Invalid request id: {req_id!r}")
        sys.exit(2)
    return req_id


def cmd_list() -> None:
    pending = queueio.list_pending()
    claimed = []
    for f in queueio.QUEUE_DIR.glob("claimed_*.json"):
        req_id = f.stem.removeprefix("claimed_")
        claim = queueio.load_claim(req_id) or {}
        expires = float(claim.get("expires_at", 0))
        claimed.append((req_id, claim.get("client_id", "?"), expires))
    if not pending and not claimed:
        print("No pending requests.")
        return

    if pending:
        print(f"Pending requests: {len(pending)}")
        print("-" * 80)
        for p in pending:
            if "error" in p:
                print(f"  {p['id']}  [ERROR: {p['error']}]")
            else:
                print(f"  {p['id']}  msgs={p['messages_count']} tools={p['tools_count']}"
                      f"  {p['first_user_msg'][:80]}")
    if claimed:
        print(f"Claimed (leased): {len(claimed)}")
        print("-" * 80)
        for req_id, client, expires in claimed:
            state = "expired" if expires < time.time() else "live"
            print(f"  {req_id}  by={client}  lease={state}")


def cmd_show(req_id: str) -> None:
    _check_id(req_id)
    path = queueio.req_path(req_id)
    if not path.exists():
        path = queueio.claimed_path(req_id)  # claimed — тоже показываем
    if not path.exists():
        print(f"Request {req_id} not found")
        sys.exit(1)
    print(json.dumps(queueio.read_json(path), indent=2, ensure_ascii=False))


def _tool_call(name: str, args_json: str) -> dict:
    try:
        args = json.loads(args_json)
    except json.JSONDecodeError:
        args = {"_raw": args_json}
    return {"id": f"call_{uuid.uuid4().hex[:24]}", "name": name, "arguments": args}


def _write_response(req_id: str, resp_data: dict) -> None:
    _check_id(req_id)
    try:
        queueio.complete_request(req_id, resp_data, local_override=True)
    except queueio.QueueConflict as exc:
        print(f"{req_id}: {exc}", file=sys.stderr)
        sys.exit(1)
    print(f"Response written for {req_id}")


def cmd_respond(req_id: str, file_path: str) -> None:
    resp_data = json.loads(Path(file_path).read_text(encoding="utf-8"))
    _write_response(req_id, resp_data)


def cmd_respond_text(req_id: str, text: str) -> None:
    _write_response(req_id, {"content": text, "finish_reason": "stop"})


def cmd_respond_tool(req_id: str, tool_name: str, args_json: str) -> None:
    _write_response(req_id, {
        "tool_calls": [_tool_call(tool_name, args_json)],
        "finish_reason": "tool_calls",
    })


def cmd_respond_text_tool(req_id: str, text: str, tool_name: str, args_json: str) -> None:
    _write_response(req_id, {
        "content": text,
        "tool_calls": [_tool_call(tool_name, args_json)],
        "finish_reason": "tool_calls",
    })


def cmd_respond_error(req_id: str, reason: str) -> None:
    """Явно вернуть ошибку клиенту (bridge отдаст 502 вместо таймаута)."""
    _write_response(req_id, {"error": reason})


def cmd_claim(req_id: str) -> None:
    _check_id(req_id)
    if not queueio.req_path(req_id).exists():
        print(f"Request {req_id} not found")
        sys.exit(1)
    claim = queueio.claim(req_id, client_id="poll-cli")
    if claim is None:
        print(f"{req_id} is already claimed")
        sys.exit(1)
    print(f"Claimed {req_id}  lease_token={claim['lease_token']}")


def cmd_release(req_id: str) -> None:
    _check_id(req_id)
    queueio.release_claim(req_id)
    print(f"Released {req_id}")


def cmd_stats() -> None:
    for key, value in queueio.stats().items():
        print(f"{key}: {value}")


def cmd_clean() -> None:
    """Recover expired leases and remove abandoned queue files."""
    timed_out = queueio.sweep_timed_out_requests()
    orphaned = queueio.sweep_orphans()
    restored = queueio.restore_expired_claims()
    print(f"Expired requests: {timed_out}, orphan artefacts: {orphaned}, restored leases: {restored}")


def cmd_watch(interval: float) -> None:
    """Poll loop — ждать новый запрос и показать его.

    Released/requeued запрос объявляется повторно: id убирается из seen,
    как только его req-файл исчезает (claim/ответ/таймаут).
    """
    print(f"Watching {queueio.QUEUE_DIR} for new requests (Ctrl+C to stop)...")
    seen = {f.stem.removeprefix("req_")
            for f in queueio.QUEUE_DIR.glob("req_*.json")}
    try:
        while True:
            present = set()
            for f in queueio.QUEUE_DIR.glob("req_*.json"):
                req_id = f.stem.removeprefix("req_")
                present.add(req_id)
                if req_id in seen:
                    continue
                seen.add(req_id)
                if queueio.resp_path(req_id).exists():
                    continue
                print(f"\n{'=' * 80}")
                print(f"NEW REQUEST: {req_id}")
                print(f"{'=' * 80}")
                cmd_show(req_id)
                print("\nTo respond:")
                print(f"  python3 poll.py respond-text {req_id} 'your response'")
                print(f"  python3 poll.py respond-tool {req_id} tool_name '{{\"arg\": 1}}'")
            # id выпадает из seen, когда req-файл исчезает (claim/ответ/
            # таймаут) → при возврате в очередь запрос объявится снова
            seen.intersection_update(present)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="ai-bridge queue CLI for agents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="List pending requests")

    p = sub.add_parser("show", help="Show a request")
    p.add_argument("id")

    p = sub.add_parser("respond", help="Respond from a JSON file")
    p.add_argument("id")
    p.add_argument("file")

    p = sub.add_parser("respond-text", help="Respond with plain text")
    p.add_argument("id")
    p.add_argument("text")

    p = sub.add_parser("respond-tool", help="Respond with a tool_call")
    p.add_argument("id")
    p.add_argument("tool")
    p.add_argument("args_json")

    p = sub.add_parser("respond-text-tool", help="Respond with text + tool_call")
    p.add_argument("id")
    p.add_argument("text")
    p.add_argument("tool")
    p.add_argument("args_json")

    p = sub.add_parser("respond-error", help="Respond with an error (client gets 502)")
    p.add_argument("id")
    p.add_argument("reason")

    p = sub.add_parser("claim", help="Atomically claim a request")
    p.add_argument("id")

    p = sub.add_parser("release", help="Release a claim")
    p.add_argument("id")

    sub.add_parser("stats", help="Queue statistics")
    sub.add_parser("clean", help="Recover expired leases and clean abandoned requests")

    p = sub.add_parser("watch", help="Watch for new requests")
    p.add_argument("--interval", type=float, default=1.0)

    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.cmd == "list":
        cmd_list()
    elif args.cmd == "show":
        cmd_show(args.id)
    elif args.cmd == "respond":
        cmd_respond(args.id, args.file)
    elif args.cmd == "respond-text":
        cmd_respond_text(args.id, args.text)
    elif args.cmd == "respond-tool":
        cmd_respond_tool(args.id, args.tool, args.args_json)
    elif args.cmd == "respond-text-tool":
        cmd_respond_text_tool(args.id, args.text, args.tool, args.args_json)
    elif args.cmd == "respond-error":
        cmd_respond_error(args.id, args.reason)
    elif args.cmd == "claim":
        cmd_claim(args.id)
    elif args.cmd == "release":
        cmd_release(args.id)
    elif args.cmd == "stats":
        cmd_stats()
    elif args.cmd == "clean":
        cmd_clean()
    elif args.cmd == "watch":
        cmd_watch(args.interval)


if __name__ == "__main__":
    main()
