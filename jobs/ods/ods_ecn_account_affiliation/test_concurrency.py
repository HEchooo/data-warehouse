"""只验证并发采集的上限、分批写入和单账号失败处理。"""

import threading
import time
import unittest
from unittest.mock import patch

import ods_ecn_account_affiliation as job


class ConcurrencyTest(unittest.TestCase):
    def test_run(self):
        lock = threading.Lock()
        barrier = threading.Barrier(32)
        active = peak = completed = 0
        batches = []

        def fetch_one(_host, _key, handle):
            nonlocal active, peak, completed
            with lock:
                active += 1
                peak = max(peak, active)
            if int(handle[1:]) < 32:
                barrier.wait(timeout=5)
            time.sleep(0.01)
            with lock:
                active -= 1
                completed += 1
            if handle == "h10":
                raise RuntimeError("temporary error")
            return None if handle == "h11" else {"account_handle": handle}

        with patch.multiple(
            job,
            load_ecn_config=lambda: ("host", "key"),
            ensure_table=lambda: None,
            list_affiliate_handles=lambda: [f"h{i}" for i in range(65)],
            fetch_one=fetch_one,
            upsert_rows=lambda rows: batches.append(rows),
        ):
            with self.assertRaisesRegex(RuntimeError, "1 条失败"):
                job.run()

        self.assertEqual(peak, 32)
        self.assertEqual(completed, 65)
        self.assertEqual([len(rows) for rows in batches], [48, 15])


if __name__ == "__main__":
    unittest.main()
