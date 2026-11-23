#!/usr/bin/env python3
"""Unit tests for aibridge.queueio — claim/lease semantics.

Stdlib unittest, no server needed:
    python3 -m unittest discover tests -v
    # or: python3 tests/test_queueio.py
"""
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aibridge import queueio  # noqa: E402


class QueueTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._orig_dir = queueio.QUEUE_DIR
        queueio.QUEUE_DIR = Path(self.tmp.name)

    def tearDown(self):
        queueio.QUEUE_DIR = self._orig_dir
        self.tmp.cleanup()

    def mk_req(self, req_id: str, model: str = "m", **extra) -> dict:
        payload = {"id": req_id, "model": model,
                   "messages": [{"role": "user", "content": f"t-{req_id}"}],
                   "created_at": time.time(), **extra}
        queueio.write_request(req_id, payload)
        return payload


class TestClaimLease(QueueTestCase):
    def test_claim_moves_req_to_claimed(self):
        self.mk_req("a1")
        claim = queueio.claim("a1", "tester", lease_seconds=60)
        self.assertIsNotNone(claim)
        self.assertFalse(queueio.req_path("a1").exists())
        self.assertTrue(queueio.claimed_path("a1").exists())
        self.assertTrue(queueio.claim_path("a1").exists())
        self.assertIn("lease_token", claim)

    def test_double_claim_fails(self):
        self.mk_req("a2")
        self.assertIsNotNone(queueio.claim("a2", "one"))
        self.assertIsNone(queueio.claim("a2", "two"))

    def test_claim_missing_returns_none(self):
        self.assertIsNone(queueio.claim("nope", "x"))
        self.assertFalse(queueio.claim_path("nope").exists())

    def test_verify_lease(self):
        self.mk_req("a3")
        claim = queueio.claim("a3", "t")
        self.assertTrue(queueio.verify_lease("a3", claim["lease_token"]))
        self.assertFalse(queueio.verify_lease("a3", "wrong"))

    def test_release_requeues(self):
        self.mk_req("a4")
        queueio.claim("a4", "t")
        queueio.release_claim("a4")
        self.assertTrue(queueio.req_path("a4").exists())
        self.assertFalse(queueio.claimed_path("a4").exists())
        self.assertFalse(queueio.claim_path("a4").exists())

    def test_renew_extends(self):
        self.mk_req("a5")
        claim = queueio.claim("a5", "t", lease_seconds=60)
        new_exp = queueio.renew_claim("a5", 600)
        self.assertGreater(new_exp, claim["expires_at"])

    def test_expired_claim_restores(self):
        self.mk_req("a6")
        claim = queueio.claim("a6", "t", lease_seconds=-1)  # уже истёк
        self.assertIsNotNone(claim)
        restored = queueio.restore_expired_claims()
        self.assertEqual(restored, 1)
        self.assertTrue(queueio.req_path("a6").exists())
        self.assertFalse(queueio.claim_path("a6").exists())

    def test_expired_claim_with_resp_not_requeued(self):
        self.mk_req("a7")
        queueio.claim("a7", "t", lease_seconds=-1)
        queueio.write_response("a7", {"content": "x"})
        restored = queueio.restore_expired_claims()
        self.assertEqual(restored, 0)  # не возвращаем — ответ уже есть
        self.assertTrue(queueio.resp_path("a7").exists())
        self.assertFalse(queueio.claim_path("a7").exists())

    def test_worker_cleanup_cannot_drop_answer_before_read(self):
        self.mk_req("reply")
        claim = queueio.claim("reply", "worker")
        queueio.complete_request("reply", {"content": "ok"}, lease_token=claim["lease_token"])
        self.assertEqual(queueio.sweep_orphans(), 0)
        self.assertEqual(queueio.read_json(queueio.resp_path("reply"))["content"], "ok")
        self.assertIsNone(queueio.claim("reply", "another"))
        queueio.cleanup_pair("reply")
        with self.assertRaises(queueio.QueueConflict):
            queueio.complete_request("reply", {"content": "late"}, lease_token=claim["lease_token"])
        self.assertFalse(queueio.resp_path("reply").exists())

    def test_finish_claim(self):
        self.mk_req("a8")
        queueio.claim("a8", "t")
        queueio.finish_claim("a8")
        self.assertFalse(queueio.claim_path("a8").exists())
        self.assertFalse(queueio.claimed_path("a8").exists())


class TestListing(QueueTestCase):
    def test_list_pending_skips_claimed_and_answered(self):
        self.mk_req("p1")
        self.mk_req("p2")
        self.mk_req("p3")
        queueio.claim("p2", "t")
        queueio.write_response("p3", {"content": "x"})
        ids = [p["id"] for p in queueio.list_pending()]
        self.assertEqual(ids, ["p1"])

    def test_next_pending_model_filter(self):
        self.mk_req("m1", model="aaa")
        self.mk_req("m2", model="bbb")
        item = queueio.next_pending(model="bbb")
        self.assertEqual(item[0], "m2")
        self.assertEqual(queueio.next_pending(model="zzz"), None)

    def test_next_pending_oldest_first(self):
        self.mk_req("old")
        time.sleep(0.01)
        self.mk_req("new")
        self.assertEqual(queueio.next_pending()[0], "old")

    def test_corrupted_listed_as_error(self):
        (queueio.QUEUE_DIR / "req_bad.json").write_text("{oops")
        pending = queueio.list_pending()
        self.assertEqual(pending[0]["error"], "corrupted")


class TestSweep(QueueTestCase):
    def test_orphan_resp_removed(self):
        queueio.write_response("ghost", {"content": "x"})
        self.assertEqual(queueio.sweep_orphans(), 1)
        self.assertFalse(queueio.resp_path("ghost").exists())

    def test_orphan_claimed_requeued(self):
        self.mk_req("oc1")
        os.replace(queueio.req_path("oc1"), queueio.claimed_path("oc1"))
        self.assertGreaterEqual(queueio.sweep_orphans(), 1)
        self.assertTrue(queueio.req_path("oc1").exists())

    def test_recover_expired_http_requests_but_keep_legacy_requests(self):
        self.mk_req("expired", deadline_at=time.time() - 1)
        self.mk_req("legacy")
        self.assertIsNone(queueio.claim("expired", "worker"))
        self.assertEqual(queueio.sweep_timed_out_requests(), 1)
        self.assertFalse(queueio.req_path("expired").exists())
        self.assertTrue(queueio.req_path("legacy").exists())

    def test_claimed_with_live_claim_untouched(self):
        self.mk_req("oc2")
        queueio.claim("oc2", "t", lease_seconds=300)
        self.assertEqual(queueio.sweep_orphans(), 0)
        self.assertTrue(queueio.claimed_path("oc2").exists())


class TestAtomicity(QueueTestCase):
    def test_write_atomic_leaves_no_tmp(self):
        self.mk_req("at1")
        queueio.write_response("at1", {"content": "y"})
        leftovers = [f for f in queueio.QUEUE_DIR.iterdir() if f.suffix == ".tmp"]
        self.assertEqual(leftovers, [])
        self.assertEqual(queueio.read_json(queueio.resp_path("at1"))["content"], "y")

    def test_claim_has_one_winner_across_processes(self):
        self.mk_req("shared")
        code = "from aibridge import queueio; print(bool(queueio.claim('shared', 'test')))"
        config = {
            "cwd": Path(__file__).resolve().parent.parent,
            "env": {**os.environ, "AI_BRIDGE_QUEUE_DIR": self.tmp.name},
            "stdout": subprocess.PIPE,
            "text": True,
        }
        with subprocess.Popen([sys.executable, "-c", code], **config) as first:
            with subprocess.Popen([sys.executable, "-c", code], **config) as second:
                results = [process.communicate(timeout=5)[0].strip() for process in (first, second)]
        self.assertCountEqual(results, ["True", "False"])

    def test_req_id_validation(self):
        for bad in ("../etc", "a/b", "", "x" * 200, "sp ace"):
            self.assertFalse(queueio.is_valid_req_id(bad), bad)
        for good in ("abc123", "a-b_c", "1"):
            self.assertTrue(queueio.is_valid_req_id(good), good)


if __name__ == "__main__":
    unittest.main()
