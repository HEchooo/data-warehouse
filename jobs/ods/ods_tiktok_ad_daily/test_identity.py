"""按官方不分类全量列表契约验证 identity；禁止网络及 BigQuery 访问。"""

import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

import requests


spec = importlib.util.spec_from_file_location(
    "tiktok_identity_test_job", Path(__file__).with_name("ods_tiktok_ad_daily.py")
)
job = importlib.util.module_from_spec(spec)
with patch("google.cloud.bigquery.Client"):
    spec.loader.exec_module(job)


def response(ids, code=0):
    result = Mock()
    result.json.return_value = {
        "code": code,
        "message": {51010: "Internal Time out.", 40100: "Request too frequent."}.get(code, "OK"),
        "data": {
            "identity_list": [{"identity_id": str(i), "username": f"user-{i}"} for i in ids],
            "page_info": {"page": 0, "page_size": 0, "total_number": 0, "total_page": 0},
        },
    }
    return result


class IdentityTest(unittest.TestCase):
    def setUp(self):
        network = patch("socket.socket.connect", side_effect=AssertionError("offline network access"))
        network.start()
        self.addCleanup(network.stop)

    def test_all_identities_returned_without_type_or_pagination(self):
        for size in [0, 100, 102, 201]:
            with self.subTest(size=size):
                session = Mock()
                session.get.return_value = response(range(size))
                self.assertEqual(job.fetch_identities(session), {str(i): f"user-{i}" for i in range(size)})
                session.get.assert_called_once_with(
                    f"{job.TIKTOK_API_BASE}/identity/get/",
                    params={"advertiser_id": job.ADVERTISER_ID}, timeout=60,
                )

    def test_unreliable_page_info_never_triggers_another_request(self):
        for metadata in [None, {}, [],
                         {"page": 1, "page_size": 100, "total_page": 2, "total_number": 200}]:
            with self.subTest(metadata=metadata):
                reply = response(range(102))
                reply.json.return_value["data"]["page_info"] = metadata
                session = Mock()
                session.get.return_value = reply
                self.assertEqual(len(job.fetch_identities(session)), 102)
                self.assertEqual(session.get.call_count, 1)

    def test_missing_page_info_is_allowed(self):
        reply = response([1])
        del reply.json.return_value["data"]["page_info"]
        session = Mock()
        session.get.return_value = reply
        self.assertEqual(job.fetch_identities(session), {"1": "user-1"})
        self.assertEqual(session.get.call_count, 1)

    def test_duplicate_identity_raises(self):
        session = Mock()
        session.get.return_value = response([1, 1])
        with self.assertRaisesRegex(RuntimeError, "重复"):
            job.fetch_identities(session)
        self.assertEqual(session.get.call_count, 1)

    def test_missing_or_malformed_list_and_id_raise(self):
        for value in [None, {}, "", [{"username": "missing-id"}],
                      [{"identity_id": ""}], [{"identity_id": 1}], [None]]:
            with self.subTest(value=value):
                reply = response([])
                reply.json.return_value["data"]["identity_list"] = value
                session = Mock()
                session.get.return_value = reply
                with self.assertRaises(RuntimeError):
                    job.fetch_identities(session)
        reply = response([])
        del reply.json.return_value["data"]["identity_list"]
        session.get.return_value = reply
        with self.assertRaises(RuntimeError):
            job.fetch_identities(session)

    def test_invalid_payload_raises(self):
        for body in [None, [], {"code": 0, "data": None}]:
            session = Mock()
            session.get.return_value.json.return_value = body
            with self.assertRaises(RuntimeError):
                job.fetch_identities(session)

    def test_transport_timeout_propagates_without_retry(self):
        for error in [requests.ReadTimeout, requests.ConnectTimeout]:
            session = Mock()
            session.get.side_effect = error("offline timeout")
            with self.assertRaises(error):
                job.fetch_identities(session)
            self.assertEqual(session.get.call_count, 1)

    def test_business_timeout_and_rate_limit_remain_failures(self):
        for code in [51010, 40100]:
            session = Mock()
            session.get.return_value = response([], code=code)
            with self.assertRaisesRegex(RuntimeError, f"code={code}"):
                job.fetch_identities(session)
            self.assertEqual(session.get.call_count, 1)

    def test_http_rate_limit_propagates(self):
        session = Mock()
        session.get.return_value.raise_for_status.side_effect = requests.HTTPError("429")
        with self.assertRaises(requests.HTTPError):
            job.fetch_identities(session)
        session.get.return_value.json.assert_not_called()

    def test_missing_username_preserves_identity(self):
        reply = response([1])
        del reply.json.return_value["data"]["identity_list"][0]["username"]
        session = Mock()
        session.get.return_value = reply
        self.assertEqual(job.fetch_identities(session), {"1": None})

    def test_identity_failure_prevents_partition_write(self):
        for reply in [response([1, 1]), response([], code=51010), response([], code=40100)]:
            session = Mock()
            session.headers = {}
            session.get.return_value = reply
            with patch.object(job, "get_access_token", return_value="offline"), \
                 patch.object(job, "fetch_currency", return_value="USD"), \
                 patch.object(job, "fetch_report", return_value=[{"dimensions": {"ad_id": "1"}}]), \
                 patch.object(job, "fetch_ad_meta", return_value={"1": {}}), \
                 patch.object(job.requests, "Session", return_value=session), \
                 patch.object(job, "replace_partitions") as write:
                with self.assertRaises(RuntimeError):
                    job.run(job.argparse.Namespace(start_date="2026-10-09", end_date="2026-10-11"))
                write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
