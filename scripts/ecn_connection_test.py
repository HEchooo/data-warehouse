"""用本地 HTTP/1.1 服务验证两个 ECN 任务真实复用 TCP，不访问生产或 BigQuery。"""

import importlib.util
import json
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import unittest
from unittest.mock import Mock, patch

import requests

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]


def load_job(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'jobs' / 'ods' / name / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    with patch.object(bigquery, 'Client'):
        spec.loader.exec_module(module)
    return module


class ConnectionTest(unittest.TestCase):
    def test_reuse_across_batches_and_isolate_threads(self):
        for name in ('ods_ecn_account_affiliation', 'ods_ecn_video_publication_owner'):
            with self.subTest(job=name):
                job = load_job(name)
                connections = []
                headers = []

                class Handler(BaseHTTPRequestHandler):
                    protocol_version = 'HTTP/1.1'

                    def setup(self):
                        super().setup()
                        connections.append(self.client_address)

                    def do_GET(self):
                        headers.append(self.headers.get('X-API-Key'))
                        status = (200, 404, 409)[len(headers) % 3]
                        body = json.dumps({'affiliation_label': 'test', 'task_id': 'test'}).encode()
                        self.send_response(status)
                        self.send_header('Content-Length', str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)

                    def log_message(self, *_args):
                        pass

                server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                host = f'http://127.0.0.1:{server.server_port}'
                barrier = threading.Barrier(2)
                sessions = []
                circuit = job.GatewayCircuit() if hasattr(job, 'GatewayCircuit') else None

                def batch():
                    barrier.wait(timeout=5)
                    session = job.thread_session('test-key')
                    sessions.append(session)
                    for i in range(20):
                        if circuit is None:
                            job.fetch_one(host, 'test-key', f'h{i}')
                        else:
                            job.fetch_publication_owner(host, 'test-key', str(i), circuit)
                    return session

                try:
                    with ExitStack() as cleanup, ThreadPoolExecutor(
                        max_workers=2, initializer=job.init_thread_session,
                        initargs=('test-key', cleanup, threading.Lock()),
                    ) as pool:
                        first = [f.result(timeout=15) for f in [pool.submit(batch), pool.submit(batch)]]
                        second = [f.result(timeout=15) for f in [pool.submit(batch), pool.submit(batch)]]
                    self.assertEqual(len(set(first)), 2)
                    self.assertEqual(set(first), set(second))
                    self.assertEqual(len(headers), 80)
                    self.assertEqual(set(headers), {'test-key'})
                    self.assertEqual(len(connections), 2)
                finally:
                    for session in set(sessions):
                        session.close()
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=5)

    def test_sessions_close_after_workers_on_success_and_failures(self):
        for name in ('ods_ecn_account_affiliation', 'ods_ecn_video_publication_owner'):
            for failure in (None, "write", "request"):
                with self.subTest(job=name, failure=failure):
                    job = load_job(name)
                    created = []
                    active = 0
                    lock = threading.Lock()
                    barrier = threading.Barrier(2)

                    def make_session():
                        session = Mock(headers={})
                        def close():
                            self.assertEqual(active, 0, '不能关闭仍在使用的 Session')
                        session.close.side_effect = close
                        created.append(session)
                        return session

                    def fetch(*_args):
                        nonlocal active
                        with lock:
                            active += 1
                        try:
                            barrier.wait(timeout=5)
                            if failure == 'request':
                                raise RuntimeError('request failed')
                            return {'key': 'value'}
                        finally:
                            with lock:
                                active -= 1

                    video = 'video' in name
                    patches = {
                        'load_ecn_config': lambda: ('unused', 'test-key'),
                        'ensure_table': lambda: None,
                        'list_missing_video_ids' if video else 'list_affiliate_handles': lambda: ['a', 'b'],
                        'fetch_publication_owner' if video else 'fetch_one': fetch,
                        'CONCURRENCY' if video else 'MAX_WORKERS': 2,
                        'upsert_rows': Mock(side_effect=RuntimeError('write failed') if failure == 'write' else None),
                    }
                    with patch.object(job.requests, 'Session', side_effect=make_session), patch.multiple(job, **patches):
                        if failure:
                            with self.assertRaisesRegex(RuntimeError, "write failed" if failure == "write" else "2 条失败"):
                                job.run()
                        else:
                            job.run()
                    self.assertEqual(len(created), 2)
                    for session in created:
                        session.close.assert_called_once()

    def test_account_connection_retry_is_bounded(self):
        job = load_job('ods_ecn_account_affiliation')
        for error in (requests.ConnectTimeout, requests.ReadTimeout, requests.ConnectionError):
            with self.subTest(error=error):
                session = Mock()
                session.get.side_effect = [error(), Mock(status_code=404)]
                with patch.object(job.time, 'sleep') as sleep:
                    self.assertEqual(job.get_with_retry(session, 'unused', {}).status_code, 404)
                    sleep.assert_called_once_with(2)
                session.get.side_effect = error()
                session.get.reset_mock()
                with patch.object(job.time, 'sleep') as sleep:
                    with self.assertRaises(error):
                        job.get_with_retry(session, 'unused', {})
                    self.assertEqual(session.get.call_count, 2)
                    sleep.assert_called_once_with(2)
                self.assertEqual(session.get.call_args.kwargs['timeout'], (5, 60))

    def test_gateway_responses_close_before_retry_without_final_sleep(self):
        for name, attempts in [('ods_ecn_account_affiliation', 5), ('ods_ecn_video_publication_owner', 2)]:
            with self.subTest(job=name):
                job = load_job(name)
                responses = [Mock(status_code=503) for _ in range(attempts)]
                session = Mock()
                session.get.side_effect = responses
                args = [session, 'unused', {}]
                if hasattr(job, 'GatewayCircuit'):
                    args.append(job.GatewayCircuit())
                with patch.object(job.time, 'sleep') as sleep:
                    self.assertIs(job.get_with_retry(*args), responses[-1])
                    self.assertEqual(sleep.call_count, attempts - 1)
                for response in responses[:-1]:
                    response.close.assert_called_once()
                responses[-1].close.assert_not_called()  # 最终响应交由调用者读取。

    def test_video_circuit_closes_discarded_response(self):
        job = load_job('ods_ecn_video_publication_owner')
        response = Mock(status_code=503)
        session = Mock()
        session.get.return_value = response
        with patch.object(job.time, 'sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError, '网关连续 1 次'):
                job.get_with_retry(session, 'unused', {}, job.GatewayCircuit(limit=1))
            sleep.assert_not_called()
        response.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
