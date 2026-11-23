#!/usr/bin/env python3
"""HTTP-level tests for the brain session protocol (stream/long-poll).

TestClient covers /brain/claim?wait и /v1/models; SSE-генератор —
напрямую через asyncio (TestClient.stream() буферит тело и не годится
для бесконечных потоков). Живой curl-прогон — в test_e2e.sh.

    python3 -m unittest discover tests -v
    # или: python3 tests/test_brain_stream.py
"""
import asyncio
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi.testclient import TestClient  # noqa: E402

from aibridge import brain, queueio  # noqa: E402
from aibridge.app import create_app  # noqa: E402
from aibridge.config import BridgeConfig  # noqa: E402


class QueueTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._orig_dir = queueio.QUEUE_DIR
        queueio.QUEUE_DIR = Path(self.tmp.name)

    def tearDown(self):
        queueio.QUEUE_DIR = self._orig_dir
        self.tmp.cleanup()

    def _mk_req(self, req_id: str, model: str = "devin-local") -> None:
        queueio.write_request(req_id, {
            "id": req_id, "model": model,
            "messages": [{"role": "user", "content": f"t-{req_id}"}],
            "created_at": time.time()})


class HttpTestCase(QueueTestCase):
    def setUp(self):
        super().setUp()
        self.config = BridgeConfig(
            model="test-bridge", poll_interval=0.05, lease_seconds=60,
            models=("devin-local",))
        self.client = TestClient(create_app(self.config), base_url="http://127.0.0.1")


class TestClaimWait(HttpTestCase):
    def test_long_poll_returns_when_request_arrives(self):
        result: list[int] = []

        def claimer():
            r = self.client.post("/brain/claim", json={
                "client_id": "t", "model": "devin-local", "wait": 10})
            result.append(r.status_code)

        t = threading.Thread(target=claimer, daemon=True)
        t.start()
        time.sleep(0.2)          # дать claim'у уйти в ожидание
        self._mk_req("lp1")
        t.join(timeout=5)
        self.assertEqual(result, [200])
        self.assertTrue(queueio.claim_path("lp1").exists())

    def test_long_poll_timeout_empty_queue(self):
        r = self.client.post("/brain/claim", json={
            "client_id": "t", "wait": 0.3})
        self.assertEqual(r.status_code, 204)

    def test_stream_requires_client_id(self):
        r = self.client.get("/brain/stream")
        self.assertEqual(r.status_code, 422)


class TestAccess(QueueTestCase):
    def test_client_key_also_protects_agent_when_agent_key_is_absent(self):
        client = TestClient(create_app(BridgeConfig(api_key="shared")))
        self.assertEqual(client.get("/brain/pending").status_code, 401)
        self.assertEqual(client.get("/brain/pending",
                                    headers={"Authorization": "Bearer shared"}).status_code, 200)

    def test_open_loopback_rejects_foreign_host_and_cross_site_browser(self):
        client = TestClient(create_app(BridgeConfig()), base_url="http://127.0.0.1")
        self.assertEqual(client.get("/brain/pending", headers={"Host": "foreign.example"}).status_code, 403)
        self.assertEqual(client.get("/brain/pending",
                                    headers={"Sec-Fetch-Site": "cross-site"}).status_code, 403)
        self.assertEqual(client.get("/brain/pending").status_code, 200)

    def test_non_loopback_requires_auth(self):
        with self.assertRaisesRegex(ValueError, "AI_BRIDGE_API_KEY"):
            create_app(BridgeConfig(host="0.0.0.0"))


class TestModels(HttpTestCase):
    def test_models_lists_config_and_queue_tags(self):
        self._mk_req("m1", model="devin-agent")
        r = self.client.get("/v1/models")
        self.assertEqual(r.status_code, 200)
        ids = {m["id"] for m in r.json()["data"]}
        self.assertIn("test-bridge", ids)     # config.model
        self.assertIn("devin-local", ids)     # config.models
        self.assertIn("devin-agent", ids)     # тег из очереди


class TestStreamGenerator(QueueTestCase):
    """stream_claims напрямую: ready → claim → renew → aclose-release."""

    def _config(self, lease: float = 60.0) -> BridgeConfig:
        return BridgeConfig(poll_interval=0.05, lease_seconds=lease)

    def test_ready_then_claim_event(self):
        self._mk_req("s1")

        async def run():
            gen = brain.stream_claims(self._config(), brain.BrainStats(),
                                      "u", "devin-local", 4)
            ev_ready = await gen.__anext__()
            ev_claim = await gen.__anext__()
            await gen.aclose()
            return ev_ready, ev_claim

        ev_ready, ev_claim = asyncio.run(run())
        self.assertEqual(ev_ready["event"], "ready")
        self.assertEqual(ev_ready["data"]["model"], "devin-local")
        self.assertEqual(ev_claim["event"], "claim")
        self.assertEqual(ev_claim["data"]["request"]["id"], "s1")
        self.assertTrue(ev_claim["data"]["lease_token"])
        # aclose → finally вернул запрос в очередь без lease-истечения.
        self.assertTrue(queueio.req_path("s1").exists())
        self.assertFalse(queueio.claim_path("s1").exists())

    def test_max_inflight_caps_claims(self):
        self._mk_req("c1")
        self._mk_req("c2")
        self._mk_req("c3")

        async def run():
            gen = brain.stream_claims(self._config(), brain.BrainStats(),
                                      "u", None, 2)
            events = [await gen.__anext__() for _ in range(3)]
            await gen.aclose()
            return events

        events = asyncio.run(run())
        # ready + максимум 2 claim'а — третий запрос остаётся pending.
        self.assertEqual([e["event"] for e in events],
                         ["ready", "claim", "claim"])
        # после aclose все три снова в очереди
        for rid in ("c1", "c2", "c3"):
            self.assertTrue(queueio.req_path(rid).exists(), rid)

    def test_old_stream_cannot_release_reassigned_lease(self):
        self._mk_req("reassigned")

        async def run():
            gen = brain.stream_claims(self._config(), brain.BrainStats(), "old", None, 1)
            await gen.__anext__()
            event = await gen.__anext__()
            old_token = event["data"]["lease_token"]
            queueio.release_claim("reassigned", lease_token=old_token)
            new = queueio.claim("reassigned", "new")
            await gen.aclose()
            self.assertTrue(queueio.verify_lease("reassigned", new["lease_token"]))

        asyncio.run(run())

    def test_stream_renews_outstanding_lease(self):
        self._mk_req("s3")

        async def run():
            gen = brain.stream_claims(self._config(lease=0.6),
                                      brain.BrainStats(), "u", None, 4)
            await gen.__anext__()                  # ready
            ev = await gen.__anext__()             # claim
            first_expiry = ev["data"]["lease_expires_at"]
            # Генератор pull-based: renew идёт пока поток читают.
            # Реальный SSE-клиент читает непрерывно — эмулируем фоном.
            async def drain():
                async for _ in gen:
                    pass
            consume = asyncio.ensure_future(drain())
            deadline = time.monotonic() + 2.0
            renewed = False
            while time.monotonic() < deadline:
                claim = queueio.load_claim("s3") or {}
                if claim.get("expires_at", 0) > first_expiry + 0.2:
                    renewed = True
                    break
                await asyncio.sleep(0.05)
            consume.cancel()
            try:
                await consume
            except asyncio.CancelledError:
                pass
            return renewed

        self.assertTrue(asyncio.run(run()),
                        "stream did not auto-renew the lease")


if __name__ == "__main__":
    unittest.main()
