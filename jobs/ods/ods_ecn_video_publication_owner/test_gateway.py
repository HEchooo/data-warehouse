"""验证 ECN 视频归属接口的连接重试和熔断。"""

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


if __name__ == "__main__":
    unittest.main()
