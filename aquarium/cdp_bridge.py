#!/usr/bin/env python3
"""cdp_bridge.py — TCP line-JSON мост к Chrome/Brave через CDP.

Локальный CDP-мост для управления выделенным браузером:
  * убран scope-механизм (привязка к ядру катаны, fail-closed блокировки)
  * мультитаргет: каждый context (target_id) — отдельная CDP-сессия
  * добавлен newtab (Target.createTarget через browser-level WS)
  * evaluate умеет await/async (две формы — как у extension-версии)

Протокол — одна JSON-строка на команду, одна JSON-строка в ответ:

    {"cmd": "status"}                            — состояние моста
    {"cmd": "tabs"}                              — список вкладок
    {"cmd": "newtab", "url": "..."}              — новая вкладка → context
    {"cmd": "navigate", "url": "...", "context": "<tid>"}
    {"cmd": "evaluate", "expression": "...", "context": "<tid>"}
    {"cmd": "observe",  "context": "<tid>"}      — url/title/DOM summary
    {"cmd": "click",    "selector"|"text", "context": ...}
    {"cmd": "fill",     "selector", "value", "context": ...}
    {"cmd": "type",     "selector", "value", "clear": true, "context": ...}
    {"cmd": "press",    "key": "Enter", "context": ...}
    {"cmd": "submit",   "selector", "context": ...}
    {"cmd": "screenshot", "context": ...}        — png в shots/
    {"cmd": "cookies",  "context": ...}
    {"cmd": "events",   "last": 20}
    {"cmd": "wait",     "timeout": 2000}

Команды без context → "текущая" вкладка (первый page-target или последний
использованный context).

Запуск:
    brave --remote-debugging-port=9222 --user-data-dir=... &
    python3 cdp_bridge.py [tcp_port=8778] [cdp_port=9222]

Зависимость: websocket-client (`pip install websocket-client`).
"""
from __future__ import annotations

import base64
import json
import os
import socketserver
import sys
import threading
import time
import urllib.request

import websocket

TCP_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv("AQ_TCP_PORT", "8778"))
CDP_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.getenv("AQ_CDP_PORT", "9222"))
CDP_HOST = os.getenv("AQ_CDP_HOST", "127.0.0.1")
SHOTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "shots")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------- sessions
class Session:
    """CDP-сессия на один page-target: свой ws + pending-мапа + события."""

    def __init__(self, target_id: str, ws_url: str):
        self.target_id = target_id
        self.ws = websocket.create_connection(ws_url, timeout=10,
                                              suppress_origin=True)
        self.ws.settimeout(None)
        self.lock = threading.Lock()
        self.next_id = 0
        self.pending: dict = {}
        self.events: list = []
        self.connected_at = time.strftime("%H:%M:%S")
        threading.Thread(target=self._reader, daemon=True).start()
        self.send("Runtime.enable")
        self.send("Page.enable")
        self.send("Log.enable")

    def send(self, method: str, params: dict | None = None,
             timeout: float = 30.0) -> dict:
        with self.lock:
            self.next_id += 1
            mid = self.next_id
            slot = {"ev": threading.Event(), "resp": None}
            self.pending[mid] = slot
        self.ws.send(json.dumps({"id": mid, "method": method,
                                 "params": params or {}}))
        resp = slot["resp"] if slot["ev"].wait(timeout) else {"error": "timeout"}
        with self.lock:
            self.pending.pop(mid, None)
        return resp

    def _reader(self) -> None:
        while True:
            try:
                raw = self.ws.recv()
            except Exception:
                return
            if not raw:
                continue
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if "id" in msg:
                with self.lock:
                    slot = self.pending.get(msg["id"])
                    if slot:
                        slot["resp"] = msg
                        slot["ev"].set()
            elif "method" in msg:
                entry = {"ts": time.strftime("%H:%M:%S"),
                         "type": msg["method"],
                         "target": self.target_id}
                try:
                    if msg["method"] == "Runtime.consoleAPICalled":
                        args = msg["params"].get("args", [])
                        entry["text"] = " ".join(
                            str(a.get("value", a.get("description", "")))
                            for a in args)[:300]
                    elif msg["method"] == "Log.entryAdded":
                        e = msg["params"].get("entry", {})
                        entry["level"] = e.get("level", "")
                        entry["text"] = str(e.get("text", ""))[:300]
                except Exception:
                    pass
                with self.lock:
                    self.events.append(entry)
                    del self.events[:-300]

    def close(self) -> None:
        try:
            self.ws.close()
        except Exception:
            pass


_sessions: dict[str, Session] = {}
_current: str | None = None          # последний использованный target
_sessions_lock = threading.Lock()
_browser_ws: websocket.WebSocket | None = None


def _http_json(path: str):
    with urllib.request.urlopen(
            f"http://{CDP_HOST}:{CDP_PORT}{path}", timeout=5) as resp:
        return json.loads(resp.read().decode())


def _targets() -> list:
    try:
        return _http_json("/json")
    except Exception as e:
        log(f"cannot get targets (Chrome :{CDP_PORT} up?): {e}")
        return []


def _pages() -> list:
    return [t for t in _targets() if t.get("type") == "page"]


def _attach(target_id: str) -> Session | None:
    """WS-подключение к конкретному target_id."""
    global _current
    with _sessions_lock:
        old = _sessions.pop(target_id, None)
        if old:
            old.close()
        for t in _targets():
            if t.get("id") == target_id and t.get("type") == "page":
                try:
                    s = Session(target_id, t["webSocketDebuggerUrl"])
                except Exception as e:
                    log(f"attach {target_id}: {e}")
                    return None
                _sessions[target_id] = s
                _current = target_id
                log(f"attached {target_id} → {t.get('url', '')[:60]}")
                return s
    return None


def _session(context: str | None) -> Session | None:
    """Разрешить context → сессию. None → current или первая вкладка."""
    global _current
    with _sessions_lock:
        if context:
            s = _sessions.get(context)
            if s:
                _current = context
                return s
        elif _current and _current in _sessions:
            return _sessions[_current]
    tid = context
    if not tid:
        pages = _pages()
        if not pages:
            return None
        tid = pages[0]["id"]
    return _attach(tid)


def _browser() -> websocket.WebSocket | None:
    """Browser-level WS (Target domain)."""
    global _browser_ws
    if _browser_ws is not None:
        return _browser_ws
    try:
        url = _http_json("/json/version")["webSocketDebuggerUrl"]
        _browser_ws = websocket.create_connection(url, timeout=10,
                                                  suppress_origin=True)
        _browser_ws.settimeout(15)
    except Exception as e:
        log(f"browser ws: {e}")
        _browser_ws = None
    return _browser_ws


def _browser_send(method: str, params: dict | None = None) -> dict:
    global _browser_ws
    ws = _browser()
    if not ws:
        return {"error": "no browser ws"}
    try:
        ws.send(json.dumps({"id": 1, "method": method,
                            "params": params or {}}))
        return json.loads(ws.recv())
    except Exception as e:
        try:
            _browser_ws.close()
        except Exception:
            pass
        _browser_ws = None
        return {"error": str(e)}


def _evaluate(s: Session, expression: str):
    """Runtime.evaluate → (ok, value)."""
    r = s.send("Runtime.evaluate", {"expression": expression,
                                    "returnByValue": True,
                                    "awaitPromise": True})
    if "result" in r:
        res = r["result"]
        if res.get("exceptionDetails"):
            exc = res["exceptionDetails"]
            return False, exc.get("exception", {}).get("description",
                                                       str(exc))[:400]
        return True, res.get("result", {}).get("value")
    err = r.get("error")
    msg = err.get("message") if isinstance(err, dict) else str(err or r)[:300]
    return False, msg


def _eval_wrapped(s: Session, expr: str):
    """Две формы как у extension-версии:
    1. начинается с `return` → тело async-функции
    2. иначе → `return await (expr)`"""
    if expr.lstrip().startswith("return"):
        code = f"(async () => {{ {expr} }})()"
    else:
        code = (f"(async () => {{ try {{ return await ({expr}); }}"
                f" catch (e) {{ return 'ERR:' + String(e); }} }})()")
    return _evaluate(s, code)


DOM_SUMMARY_JS = """(() => {
  const h = document.querySelector('h1,h2,h3,[role="heading"]');
  const controls = [...document.querySelectorAll(
    'input,textarea,select,button,[role="button"],[contenteditable="true"]')]
    .filter(el => !el.disabled && el.getBoundingClientRect().width > 0)
    .slice(0, 30)
    .map(el => ({tag: el.tagName.toLowerCase(),
      type: el.type || '', text: (el.innerText || el.value || '').trim().slice(0, 80)}));
  return {url: location.href, title: document.title,
    heading: h ? h.textContent.trim().slice(0,120) : '', controls};
})()"""


# ---------------------------------------------------------------- commands
def cmd_status(cmd):
    try:
        _http_json("/json/version")
        browser = True
    except Exception:
        browser = False
    return {"ok": True, "cdp": f"{CDP_HOST}:{CDP_PORT}",
            "browser": browser,
            "sessions": list(_sessions.keys()), "current": _current,
            "tabs": len(_pages())}


def cmd_tabs(cmd):
    return {"ok": True, "tabs": [
        {"context": t["id"], "url": t.get("url", ""),
         "title": t.get("title", "")} for t in _pages()]}


def cmd_newtab(cmd):
    url = cmd.get("url", "about:blank")
    r = _browser_send("Target.createTarget", {"url": url})
    tid = r.get("result", {}).get("targetId")
    if not tid:
        return {"ok": False, "error": json.dumps(r)[:300]}
    time.sleep(0.5)                       # даём таргету появиться в /json
    s = _attach(tid)
    if not s:
        return {"ok": False, "error": f"created {tid} but attach failed"}
    return {"ok": True, "context": tid, "url": url}


def cmd_navigate(cmd):
    url = cmd.get("url", "")
    if not url:
        return {"ok": False, "error": "missing url"}
    s = _session(cmd.get("context"))
    if not s:
        return {"ok": False, "error": "no tab/session"}
    r = s.send("Page.navigate", {"url": url})
    if "result" in r:
        time.sleep(1)
        return {"ok": True}
    return {"ok": False, "error": json.dumps(r)[:300]}


def cmd_closetab(cmd):
    context = cmd.get("context")
    if context:
        with _sessions_lock:
            s = _sessions.pop(context, None)
            if s:
                s.close()
    r = _browser_send("Target.closeTarget",
                      {"targetId": context or _current})
    return {"ok": "result" in r, "result": r.get("result", r.get("error"))}


def cmd_evaluate(cmd):
    expr = cmd.get("expression", "")
    if not expr:
        return {"ok": False, "error": "missing expression"}
    s = _session(cmd.get("context"))
    if not s:
        return {"ok": False, "error": "no tab/session"}
    ok, val = _eval_wrapped(s, expr)
    return {"ok": ok, "value": val}


def cmd_observe(cmd):
    s = _session(cmd.get("context"))
    if not s:
        return {"ok": False, "error": "no tab/session"}
    ok, val = _evaluate(s, DOM_SUMMARY_JS)
    return {"ok": ok, "observation": val,
            "events": s.events[-10:]}


def cmd_click(cmd):
    sel = cmd.get("selector", "")
    text = cmd.get("text", "")
    if not sel and not text:
        return {"ok": False, "error": "missing selector or text"}
    s = _session(cmd.get("context"))
    if not s:
        return {"ok": False, "error": "no tab/session"}
    if text:
        js = (f"(() => {{ const b = [...document.querySelectorAll("
              f"'button,a,[role=button]')]"
              f".find(x => (x.textContent||'').includes({json.dumps(text)})); "
              f"if (!b) return null; b.scrollIntoView({{block:'center'}}); "
              f"const r = b.getBoundingClientRect(); "
              f"return {{x: r.x + r.width/2, y: r.y + r.height/2}}; }})()")
    else:
        js = (f"(() => {{ const e = document.querySelector({json.dumps(sel)}); "
              f"if (!e) return null; e.scrollIntoView({{block:'center'}}); "
              f"const r = e.getBoundingClientRect(); "
              f"return {{x: r.x + r.width/2, y: r.y + r.height/2}}; }})()")
    ok, pos = _evaluate(s, js)
    if not ok or not isinstance(pos, dict):
        return {"ok": False, "error": f"element not found: {sel or text}"}
    x, y = int(pos["x"]), int(pos["y"])
    s.send("Input.dispatchMouseEvent",
           {"type": "mouseMoved", "x": x, "y": y})
    s.send("Input.dispatchMouseEvent",
           {"type": "mousePressed", "x": x, "y": y,
            "button": "left", "clickCount": 1})
    s.send("Input.dispatchMouseEvent",
           {"type": "mouseReleased", "x": x, "y": y,
            "button": "left", "clickCount": 1})
    return {"ok": True, "clicked": sel or text, "at": pos}


def cmd_fill(cmd):
    sel = cmd.get("selector", "")
    if not sel:
        return {"ok": False, "error": "missing selector"}
    s = _session(cmd.get("context"))
    if not s:
        return {"ok": False, "error": "no tab/session"}
    ok, val = _evaluate(s, (
        f"(() => {{ const e = document.querySelector({json.dumps(sel)}); "
        f"if (!e) return 'not found'; e.value = {json.dumps(cmd.get('value', ''))}; "
        f"e.dispatchEvent(new Event('input', {{bubbles:true}})); "
        f"e.dispatchEvent(new Event('change', {{bubbles:true}})); "
        f"return 'filled'; }})()"))
    return {"ok": ok and val == "filled", "result": val}


_SPECIAL = {
    "Enter": ("Enter", "Enter", 13), "Return": ("Enter", "Enter", 13),
    "Tab": ("Tab", "Tab", 9), "Escape": ("Escape", "Escape", 27),
    "Backspace": ("Backspace", "Backspace", 8),
    "Delete": ("Delete", "Delete", 46),
    "ArrowUp": ("ArrowUp", "ArrowUp", 38),
    "ArrowDown": ("ArrowDown", "ArrowDown", 40),
    "ArrowLeft": ("ArrowLeft", "ArrowLeft", 37),
    "ArrowRight": ("ArrowRight", "ArrowRight", 39),
    "Home": ("Home", "Home", 36), "End": ("End", "End", 35),
    "PageUp": ("PageUp", "PageUp", 33),
    "PageDown": ("PageDown", "PageDown", 34),
    "Space": (" ", "Space", 32),
}


def _key(s: Session, info: dict, kind: str):
    s.send("Input.dispatchKeyEvent", {
        "type": kind, "key": info.get("key", ""),
        "code": info.get("code", ""),
        "windowsVirtualKeyCode": info.get("vk", 0),
        "nativeVirtualKeyCode": info.get("vk", 0),
        **({"text": info["text"]} if kind == "keyDown" and info.get("text")
           else {})})


def cmd_type(cmd):
    """Реальная клавиатура через CDP (Unicode-safe)."""
    sel = cmd.get("selector", "")
    value = cmd.get("value", "")
    if not sel or not value:
        return {"ok": False, "error": "missing selector or value"}
    s = _session(cmd.get("context"))
    if not s:
        return {"ok": False, "error": "no tab/session"}
    clear_js = ""
    if cmd.get("clear", True):
        clear_js = ("if (e instanceof HTMLInputElement || "
                    "e instanceof HTMLTextAreaElement) { e.value = ''; "
                    "e.dispatchEvent(new Event('input', {bubbles:true})); }")
    ok, res = _evaluate(s, (
        f"(() => {{ const e = document.querySelector({json.dumps(sel)}); "
        f"if (!e) return 'not found'; e.scrollIntoView({{block:'center'}}); "
        f"e.focus(); {clear_js} return 'ready'; }})()"))
    if not ok or res == "not found":
        return {"ok": False, "error": f"focus failed: {res}"}
    for ch in value:
        vk = ord(ch) if ord(ch) < 256 else 0
        _key(s, {"key": ch, "vk": vk, "text": ch}, "keyDown")
        _key(s, {"key": ch, "vk": vk}, "keyUp")
    return {"ok": True, "typed": len(value)}


def cmd_press(cmd):
    s = _session(cmd.get("context"))
    if not s:
        return {"ok": False, "error": "no tab/session"}
    keys = cmd.get("key", cmd.get("keys", ""))
    keys = keys if isinstance(keys, list) else [keys]
    if cmd.get("selector"):
        _evaluate(s, (f"(() => {{ const e = document.querySelector("
                      f"{json.dumps(cmd['selector'])}); "
                      f"if (e) e.focus(); return true; }})()"))
    for k in keys:
        k, code, vk = _SPECIAL.get(k, (k, "", ord(k) if len(k) == 1 else 0))
        _key(s, {"key": k, "code": code, "vk": vk}, "keyDown")
        _key(s, {"key": k, "code": code, "vk": vk}, "keyUp")
    return {"ok": True, "pressed": keys}


def cmd_submit(cmd):
    s = _session(cmd.get("context"))
    if not s:
        return {"ok": False, "error": "no tab/session"}
    sel = cmd.get("selector", "")
    expr = (
        f"(() => {{ const e = document.querySelector({json.dumps(sel)}); "
        f"const f = e && (e.form || e); "
        f"if (!f || f.tagName !== 'FORM') return 'form not found'; "
        f"f.requestSubmit(); return 'submitted'; }})()"
        if sel else
        "(() => { const f = document.forms[0]; if (!f) return 'form not found';"
        " f.requestSubmit(); return 'submitted'; })()")
    ok, val = _evaluate(s, expr)
    return {"ok": ok and val == "submitted", "result": val}


def cmd_screenshot(cmd):
    s = _session(cmd.get("context"))
    if not s:
        return {"ok": False, "error": "no tab/session"}
    r = s.send("Page.captureScreenshot", {"format": "png"})
    data = r.get("result", {}).get("data", "")
    if not data:
        return {"ok": False, "error": json.dumps(r)[:200]}
    os.makedirs(SHOTS_DIR, exist_ok=True)
    path = os.path.join(SHOTS_DIR, f"shot-{int(time.time())}.png")
    with open(path, "wb") as f:
        f.write(base64.b64decode(data))
    return {"ok": True, "path": path}


def cmd_cookies(cmd):
    s = _session(cmd.get("context"))
    if not s:
        return {"ok": False, "error": "no tab/session"}
    r = s.send("Network.getCookies", {})
    cookies = [{"name": c.get("name"), "value": str(c.get("value", ""))[:80],
                "domain": c.get("domain"), "secure": c.get("secure"),
                "httpOnly": c.get("httpOnly")}
               for c in r.get("result", {}).get("cookies", [])]
    return {"ok": True, "cookies": cookies, "count": len(cookies)}


def cmd_events(cmd):
    n = int(cmd.get("last", 20))
    evts = []
    for s in _sessions.values():
        evts += s.events
    evts.sort(key=lambda e: e["ts"])
    return {"ok": True, "events": evts[-n:]}


def cmd_wait(cmd):
    ms = int(cmd.get("timeout", cmd.get("ms", 1000)))
    time.sleep(ms / 1000.0)
    return {"ok": True, "waited": ms}


HANDLERS = {
    "status": cmd_status, "tabs": cmd_tabs, "newtab": cmd_newtab,
    "closetab": cmd_closetab, "navigate": cmd_navigate,
    "observe": cmd_observe, "evaluate": cmd_evaluate, "click": cmd_click,
    "fill": cmd_fill, "type": cmd_type, "press": cmd_press,
    "submit": cmd_submit, "screenshot": cmd_screenshot,
    "cookies": cmd_cookies, "events": cmd_events, "wait": cmd_wait,
}


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        for raw in self.rfile:
            line = raw.strip()
            if not line:
                continue
            try:
                cmd = json.loads(line)
            except json.JSONDecodeError as e:
                resp = {"ok": False, "error": f"parse error: {e}"}
            else:
                fn = HANDLERS.get(cmd.get("cmd", ""))
                if fn is None:
                    resp = {"ok": False, "error": f"unknown cmd: {cmd.get('cmd')}"
                            f" (available: {', '.join(HANDLERS)})"}
                else:
                    try:
                        resp = fn(cmd)
                    except Exception as e:
                        resp = {"ok": False, "error": f"{type(e).__name__}: {e}"}
            self.wfile.write(
                (json.dumps(resp, ensure_ascii=False) + "\n").encode())


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    pages = _pages()
    if not pages:
        log(f"no page targets at {CDP_HOST}:{CDP_PORT} — запусти браузер с "
            f"--remote-debugging-port={CDP_PORT}")
    with Server(("127.0.0.1", TCP_PORT), Handler) as srv:
        log(f"cdp-bridge TCP on 127.0.0.1:{TCP_PORT} "
            f"(browser :{CDP_PORT}, tabs: {len(pages)})")
        srv.serve_forever()


if __name__ == "__main__":
    main()
