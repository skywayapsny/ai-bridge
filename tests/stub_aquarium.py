#!/usr/bin/env python3
"""stub_aquarium.py — эмулятор aquarium TCP-моста для тестов browser_brain.

Поднимает JSON-line TCP сервер, который отвечает на команды как
расширение: newtab/evaluate/fill и т.д. evaluate обрабатывает сниппеты
адаптера "mock" по простым правилам — эмулируя страницу чата, которая
стримит ответ (каждый extract-опрос возвращает всё больше текста, затем
busy снимается).

Использование: python3 stub_aquarium.py [port]  → печатает PORT, работает
до SIGTERM. Все state in-memory, один таб.
"""
import json
import socketserver
import sys
import threading
import time

ANSWER_PREFIX = "STUB-ANSWER: "
ANSWER_SECONDS = 3.0


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.context = "777"
        self.prompt = None          # последний отправленный промпт
        self.sent_at = 0.0          # когда отправили
        self.answer_len = 0         # сколько символов ответа "достримлено"
        self.reset_called = 0


STATE = State()


def js_contains(expr, *needles):
    return all(n in expr for n in needles)


def dispatch(cmd: dict) -> dict:
    action = cmd.get("cmd")
    with STATE.lock:
        if action == "status":
            return {"ok": True, "mode": "stub", "browser": True}
        if action == "newtab":
            return {"ok": True, "context": STATE.context, "url": cmd.get("url", "")}
        if action == "evaluate":
            expr = cmd.get("expression", "")
            # порядок важен: send/extract тоже содержат '#prompt'
            # send_js
            if "#send'" in expr and ".click()" in expr:
                # выковыриваем JSON-литерал промпта из подстановки адаптера
                start = expr.find('"')
                end = expr.rfind('"')
                try:
                    STATE.prompt = json.loads(expr[start:end + 1])
                except (json.JSONDecodeError, ValueError):
                    STATE.prompt = expr[start:end + 1]
                STATE.sent_at = time.time()
                STATE.answer_len = 0
                return {"ok": True, "value": "sent"}
            # reset_js
            if "forEach" in expr and ".remove()" in expr:
                STATE.reset_called += 1
                return {"ok": True, "value": "reset"}
            # busy_js — busy пока ответ недостримлен (< ~3 сек)
            if "#busy'" in expr:
                busy = (STATE.prompt is not None
                        and time.time() - STATE.sent_at < ANSWER_SECONDS)
                return {"ok": True, "value": busy}
            # extract_js — растущий текст до конца стрима
            if ".response" in expr or "innerText" in expr:
                if STATE.prompt is None:
                    return {"ok": True, "value": ""}
                full = ANSWER_PREFIX + STATE.prompt
                elapsed = time.time() - STATE.sent_at
                n = min(len(full), int(elapsed * len(full) / ANSWER_SECONDS))
                STATE.answer_len = n
                return {"ok": True, "value": full[:n]}
            # ready_js — любой прочий probe с '#prompt'
            if "#prompt'" in expr:
                return {"ok": True, "value": True}
            return {"ok": True, "value": None}
        return {"ok": True}


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        for line in self.rfile:
            try:
                cmd = json.loads(line.decode())
            except json.JSONDecodeError:
                self.wfile.write(b'{"ok":false,"error":"bad json"}\n')
                continue
            self.wfile.write((json.dumps(dispatch(cmd)) + "\n").encode())


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    srv = Server(("127.0.0.1", port), Handler)
    print(srv.server_address[1], flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
