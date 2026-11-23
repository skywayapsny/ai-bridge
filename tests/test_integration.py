"""Live loopback tests for the installed server, agent and worker interfaces."""

import http.client
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

import stub_aquarium


CLIENT = "test-client-key"
AGENT = "test-agent-key"


class Upstream(BaseHTTPRequestHandler):
    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path != "/v1/chat/completions" or payload["model"] != "upstream-model":
            self.send_error(400)
            return
        if self.headers.get("Authorization") != "Bearer test-upstream-key":
            self.send_error(401)
            return
        result = {
            "choices": [{"message": {"tool_calls": [{
                "id": "call_42",
                "function": {"name": "lookup", "arguments": '{"name":"demo"}'},
            }]}, "finish_reason": "tool_calls"}],
            "usage": {"prompt_tokens": 4, "completion_tokens": 5},
        }
        body = json.dumps(result).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class BridgeIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(self.enter_cleanup(tempfile.TemporaryDirectory()))
        self.queue = self.tmp / "queue"
        listener = self.enter_cleanup(socket.create_server(("127.0.0.1", 0)))
        self.port = listener.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self.env = {
            key: val for key, val in os.environ.items()
            if not key.startswith(("AI_BRIDGE_", "AI_WORKER_", "AI_BROWSER_"))
        }
        self.env.update({
            "AI_BRIDGE_QUEUE_DIR": str(self.queue),
            "AI_BRIDGE_API_KEY": CLIENT,
            "AI_BRIDGE_AGENT_KEY": AGENT,
            "AI_BRIDGE_MODEL": "debug",
            "AI_BRIDGE_TIMEOUT": "5",
            "AI_BRIDGE_POLL_INTERVAL": "0.025",
            "AI_BRIDGE_MAX_INFLIGHT": "1",
            "NO_PROXY": "127.0.0.1",
        })
        fd = listener.fileno()
        server_code = (
            "import sys, uvicorn; from aibridge.app import create_app; "
            "uvicorn.run(create_app(), fd=int(sys.argv[1]), log_level='warning')"
        )
        with (self.tmp / "server.log").open("wb") as log:
            process = subprocess.Popen(
                [sys.executable, "-c", server_code, str(fd)], pass_fds=(fd,),
                env=self.env, stdout=log, stderr=log,
            )
        self.addCleanup(self.stop_server, process)
        listener.close()
        self.wait_until(lambda: self.health_ready(process))
        self.pool = self.enter_cleanup(ThreadPoolExecutor(max_workers=1))

    def enter_cleanup(self, resource):
        value = resource.__enter__()
        self.addCleanup(resource.__exit__, None, None, None)
        return value

    def stop_server(self, process):
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

    def health_ready(self, process):
        if process.poll() is not None:
            self.fail((self.tmp / "server.log").read_text())
        try:
            return self.request("GET", "/health")[0] == 200
        except (OSError, urllib.error.URLError):
            return False

    def wait_until(self, condition):
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            if condition():
                return
            time.sleep(0.025)
        self.fail("Timed out waiting for the expected bridge state")

    def request(self, method, path, body=None, key=None):
        headers = {"Content-Type": "application/json"}
        if key:
            headers["Authorization"] = f"Bearer {key}"
        request = urllib.request.Request(
            self.url + path, method=method, headers=headers,
            data=json.dumps(body).encode() if body is not None else None,
        )
        try:
            with urllib.request.urlopen(request, timeout=8) as reply:
                return reply.status, reply.read()
        except urllib.error.HTTPError as exc:
            with exc:
                return exc.code, exc.read()

    def start_chat(self, **fields):
        body = {"messages": [{"role": "user", "content": "source of the failure"}], **fields}
        reply = self.pool.submit(self.request, "POST", "/v1/chat/completions", body, CLIENT)
        self.wait_until(lambda: list(self.queue.glob("req_*.json")))
        return reply

    def run_cli(self, module, *args, **env):
        result = subprocess.run(
            [sys.executable, "-m", module, *args], env={**self.env, **env},
            capture_output=True, text=True, timeout=8,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def test_agent_cli_roundtrip_and_permissions(self):
        self.assertEqual(self.request("GET", "/v1/models")[0], 401)
        for endpoint in ("/brain/pending", "/brain/stats", "/queue/pending", "/queue/stats"):
            self.assertEqual(self.request("GET", endpoint, key=CLIENT)[0], 401)
        self.assertEqual(self.request("GET", "/v1/models", key=AGENT)[0], 401)
        reply = self.start_chat()
        status, _ = self.request("POST", "/v1/chat/completions", {"messages": []}, CLIENT)
        self.assertEqual(status, 429)
        path = self.tmp / "claim"
        args = ("aibridge.agent", "--bridge", self.url)
        self.run_cli(*args, "claim", "--model", "debug", "--out", str(path))
        record = json.loads(path.read_text())
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        req_id = record["request"]["id"]
        self.assertEqual(self.request("POST", f"/queue/respond/{req_id}",
                                      {"content": "late fallback"}, AGENT)[0], 403)
        self.run_cli(*args, "respond", req_id, "--claim-file", str(path), "--content", "diagnosis")
        status, body = reply.result(timeout=6)
        self.assertEqual(status, 200)
        result = json.loads(body)
        self.assertEqual(result["model"], "debug")
        self.assertEqual(result["choices"][0]["message"]["content"], "diagnosis")
        self.assertEqual(list(self.queue.glob("*.json")), [])

    def test_worker_tool_call_sse(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        with server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                reply = self.start_chat(model="worker", stream=True, stream_options={"include_usage": True})
                self.run_cli(
                    "aibridge.worker", "--once", "--env-file", "/dev/null",
                    AI_WORKER_BASE_URL=f"http://127.0.0.1:{server.server_port}/v1",
                    AI_WORKER_API_KEYS="test-upstream-key",
                    AI_WORKER_MODEL="upstream-model",
                    AI_WORKER_CLAIM_MODEL="worker",
                )
                status, body = reply.result(timeout=6)
            finally:
                server.shutdown()
                thread.join()
        self.assertEqual(status, 200)
        chunks = [line.removeprefix(b"data: ") for line in body.splitlines() if line.startswith(b"data: ")]
        self.assertEqual(chunks[-1], b"[DONE]")
        delta, finish, usage = map(json.loads, chunks[:-1])
        self.assertEqual(delta["choices"][0]["delta"]["tool_calls"][0]["function"]["name"], "lookup")
        self.assertIsNone(delta["usage"])
        self.assertEqual(finish["choices"][0]["finish_reason"], "tool_calls")
        self.assertEqual(usage["usage"]["total_tokens"], 9)

    def test_browser_adapter_against_mock_aquarium(self):
        with stub_aquarium.Server(("127.0.0.1", 0), stub_aquarium.Handler) as server:
            with patch.object(stub_aquarium, "ANSWER_SECONDS", 0.15):
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    reply = self.start_chat(model="browser")
                    self.run_cli(
                        "aibridge.browser_brain", "--once", "--site", "mock",
                        "--model", "browser", "--tab", "new", "--bridge", self.url,
                        "--aq", f"127.0.0.1:{server.server_address[1]}",
                        AI_BROWSER_POLL="0.05", AI_BROWSER_STABLE="2", AI_BROWSER_TIMEOUT="2",
                    )
                    status, body = reply.result(timeout=6)
                finally:
                    server.shutdown()
                    thread.join()
        self.assertEqual(status, 200)
        content = json.loads(body)["choices"][0]["message"]["content"]
        self.assertIn("source of the failure", content)

    def test_operator_cancel_wakes_waiter(self):
        reply = self.start_chat()
        code, body = self.request("GET", "/queue/pending", key=AGENT)
        self.assertEqual(code, 200)
        req_id = json.loads(body)["pending"][0]["id"]
        self.assertEqual(self.request("DELETE", f"/queue/request/{req_id}", key=AGENT)[0], 200)
        status, _ = reply.result(timeout=2)
        self.assertEqual(status, 409)

    def test_stream_requeues_on_disconnect(self):
        reply = self.start_chat(model="session")
        client = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        try:
            client.request("GET", "/brain/stream?client_id=reader&model=session",
                           headers={"Authorization": f"Bearer {AGENT}"})
            with client.getresponse() as response:
                self.assertEqual(response.status, 200)
                while response.readline() != b"event: claim\n":
                    self.assertFalse(response.closed)
                event = json.loads(response.readline().removeprefix(b"data: "))
        finally:
            client.close()
        req_id = event["request"]["id"]
        self.wait_until(lambda: (self.queue / f"req_{req_id}.json").exists())
        code, body = self.request("POST", "/brain/claim", {"client_id": "next"}, AGENT)
        self.assertEqual(code, 200)
        new_token = json.loads(body)["lease_token"]
        self.assertNotEqual(event["lease_token"], new_token)
        for method in ("renew", "release", "respond"):
            self.assertEqual(self.request("POST", f"/brain/{method}/{req_id}",
                                          {"lease_token": event["lease_token"]}, AGENT)[0], 403)
        self.assertEqual(self.request("POST", f"/brain/respond/{req_id}",
                                      {"lease_token": new_token, "content": "done"}, AGENT)[0], 200)
        self.assertEqual(reply.result(timeout=6)[0], 200)


if __name__ == "__main__":
    unittest.main()
