"""验证 ECN 视频归属接口的连接重试和熔断。"""

import threading
import unittest
from unittest.mock import Mock, patch

import requests

import ods_ecn_video_publication_owner as job


class GatewayTest(unittest.TestCase):
    def test_transient_timeout_retries_then_returns_business_status(self):
        response = Mock(status_code=404)
        session = Mock()
        session.get.side_effect = [requests.ConnectTimeout(), response]
        with patch.object(job.time, "sleep"):
            result = job.get_with_retry(session, "http://ecn", {}, job.GatewayCircuit())
        self.assertIs(result, response)
        self.assertEqual(session.get.call_count, 2)
        self.assertEqual(session.get.call_args.kwargs["timeout"], (5, 60))

    def test_repeated_connection_failures_open_circuit(self):
        session = Mock()
        session.get.side_effect = requests.ConnectTimeout()
        circuit = job.GatewayCircuit()
        with patch.object(job.time, "sleep"):
            with self.assertRaises(requests.ConnectTimeout):
                job.get_with_retry(session, "http://ecn", {}, circuit)
            with self.assertRaisesRegex(RuntimeError, "网关连续 3 次"):
                job.get_with_retry(session, "http://ecn", {}, circuit)
        self.assertEqual(session.get.call_count, 3)
        self.assertTrue(circuit.open)

    def run_recovery_case(self, mode):
        calls = []
        writes = []
        first_circuit = None

        def fetch(_host, _key, video, circuit):
            nonlocal first_circuit
            if first_circuit is None:
                first_circuit = circuit
            calls.append(video)
            circuit.raise_if_open()
            if video == "ok":
                return {"video_id": video}
            if video == "missing":
                return None
            if mode == "ordinary":
                raise ValueError("invalid response")
            if circuit is first_circuit or mode == "persistent" or video == "later":
                for _ in range(job.CIRCUIT_FAILURE_LIMIT):
                    circuit.note(502)
                circuit.raise_if_open()
            return {"video_id": video}

        ids = ['ok', 'missing', 'failed', 'queued', 'later']
        with patch.multiple(
            job, load_ecn_config=lambda: ('unused', 'test'), ensure_table=lambda: None,
            list_missing_video_ids=lambda: ids, fetch_publication_owner=fetch,
            upsert_rows=lambda rows: writes.extend(rows), CONCURRENCY=1, BATCH_SIZE=4,
        ), patch.object(job.time, 'sleep') as sleep:
            # later 在第二批再次熔断：预算必须是整轮一次，不能按批次重置。
            if mode == 'recovered':
                ids.pop()
                job.run()
            else:
                with self.assertRaises(RuntimeError):
                    job.run()
        self.assertEqual(calls.count('ok'), 1)
        self.assertEqual(calls.count('missing'), 1)
        self.assertEqual(len(writes), len({r['video_id'] for r in writes}))
        if mode == 'ordinary':
            sleep.assert_not_called()
        else:
            sleep.assert_called_once_with(45)
            self.assertEqual(calls.count('failed'), 2)
        if mode == 'recovered':
            self.assertEqual({r['video_id'] for r in writes}, {'ok', 'failed', 'queued'})
        if mode == 'persistent':
            self.assertNotIn('later', calls)

    def test_cooldown_recovers_only_failed_items(self):
        self.run_recovery_case('recovered')

    def test_cooldown_is_bounded_for_persistent_failure(self):
        self.run_recovery_case('persistent')

    def test_cooldown_budget_is_shared_across_batches(self):
        self.run_recovery_case('later')

    def test_non_gateway_error_does_not_trigger_cooldown(self):
        self.run_recovery_case('ordinary')

    def test_cooldown_waits_for_inflight_success_and_keeps_it(self):
        started = threading.Event()
        opened = threading.Event()
        drained = threading.Event()
        calls = []
        writes = []
        original = None

        def fetch(_host, _key, video, circuit):
            nonlocal original
            if video == 'slow':
                original = circuit
                started.set()
                self.assertTrue(opened.wait(5))
                drained.set()
                return {'video_id': video}
            self.assertTrue(started.wait(5))
            calls.append(video)
            if circuit is original:
                for _ in range(job.CIRCUIT_FAILURE_LIMIT):
                    circuit.note(502)
                opened.set()
                circuit.raise_if_open()
            return {'video_id': video}

        def cooldown(seconds):
            self.assertEqual(seconds, 45)
            self.assertTrue(drained.is_set())
            self.assertEqual(writes, [{'video_id': 'slow'}])

        with patch.multiple(
            job, load_ecn_config=lambda: ('unused', 'test'), ensure_table=lambda: None,
            list_missing_video_ids=lambda: ['slow', 'failed'], fetch_publication_owner=fetch,
            upsert_rows=lambda rows: writes.extend(rows), CONCURRENCY=2,
        ), patch.object(job.time, 'sleep', side_effect=cooldown) as sleep:
            job.run()
        sleep.assert_called_once_with(45)
        self.assertEqual(calls, ['failed', 'failed'])
        self.assertEqual(writes, [{'video_id': 'slow'}, {'video_id': 'failed'}])

    def test_real_gateway_retry_path_recovers_after_502(self):
        response = Mock(status_code=502)
        session = Mock()
        session.get.return_value = response

        def sleep(seconds):
            if seconds == 45:
                session.get.return_value = Mock(status_code=404, content=b'{}')

        with patch.multiple(
            job, load_ecn_config=lambda: ('unused', 'test'), ensure_table=lambda: None,
            list_missing_video_ids=lambda: ['a', 'b', 'c'], thread_session=lambda _key: session,
            upsert_rows=Mock(), CONCURRENCY=1,
        ), patch.object(job.time, 'sleep', side_effect=sleep) as wait:
            job.run()
        self.assertEqual(session.get.call_count, 6)  # 三次 502 后三次业务性 404。
        self.assertEqual([c.args[0] for c in wait.call_args_list], [2, 45])
        session.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
