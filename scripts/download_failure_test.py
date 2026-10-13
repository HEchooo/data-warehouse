"""只提取函数定义执行，避免模块初始化触发凭据、HTTP 和 BigQuery。"""
import ast
from datetime import datetime, timedelta
import gzip
from io import StringIO
from pathlib import Path
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]


def function_namespace(path, name):
    tree = ast.parse((ROOT / path).read_text())
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    namespace = dict(datetime=datetime, timedelta=timedelta, logger=Mock(), requests=Mock(),
                     url='https://example.invalid', headers={}, gzip=gzip, StringIO=StringIO,
                     PROJECT_ID='test', DECOM_DATASET='test', time=time,
                     create_temp_table=Mock(), drop_table_if_exists=Mock(),
                     drop_and_rename_table=Mock(), build_ios_changed_table=Mock(),
                     build_android_changed_table=Mock(), client=Mock(), bigquery_client=Mock())
    exec(compile(ast.Module(body=[function], type_ignores=[]), path, 'exec'), namespace)
    return namespace


class DownloadFailureTests(unittest.TestCase):
    def ios(self):
        return function_namespace('jobs/ods/ods_ios_download/ods_ios_download.py', 'ios_download')

    def test_ios_network_failure_is_not_success(self):
        ns = self.ios()
        ns['requests'].get.side_effect = TimeoutError('timeout')
        with self.assertRaisesRegex(RuntimeError, '部分日期失败'):
            ns['ios_download']('2026-10-05', '2026-10-06')
        self.assertEqual(ns['requests'].get.call_count, 2)
        ns['drop_and_rename_table'].assert_not_called()

    def test_ios_404_still_means_not_published(self):
        ns = self.ios()
        ns['requests'].get.return_value.status_code = 404
        ns['ios_download']('2026-10-05', '2026-10-05')
        ns['drop_and_rename_table'].assert_not_called()

    def test_ios_http_failure_is_not_success(self):
        ns = self.ios()
        ns['requests'].get.return_value.status_code = 503
        ns['requests'].get.return_value.raise_for_status.side_effect = RuntimeError('HTTP 503')
        with self.assertRaisesRegex(RuntimeError, '部分日期失败'):
            ns['ios_download']('2026-10-05', '2026-10-05')

    def test_ios_partial_insert_rejects_swap(self):
        ns = self.ios()
        row = {'Product Type Identifier': 1, 'Units': 1, 'Developer Proceeds': None,
               'Begin Date': '10/05/2026', 'End Date': '10/05/2026',
               'Apple Identifier': None, 'Customer Price': None}
        frame = Mock()
        frame.iterrows.return_value = [(0, row)]
        ns['pd'] = SimpleNamespace(read_csv=lambda *a, **kw: frame,
                                   isna=lambda x: x is None, notna=lambda x: x is not None)
        response = ns['requests'].get.return_value
        response.status_code = 200
        response.content = gzip.compress(b'fake')
        ns['client'].insert_rows_json.return_value = [{'error': 'invalid'}]
        with self.assertRaisesRegex(RuntimeError, '部分日期失败'):
            ns['ios_download']('2026-10-05', '2026-10-05')
        ns['drop_and_rename_table'].assert_not_called()
        ns['drop_table_if_exists'].assert_called_once()

    def test_android_empty_report_is_not_success(self):
        ns = function_namespace('jobs/ods/ods_android_download/ods_android_download.py', 'android_download')
        ns['fetch_download_data'] = Mock(return_value=Mock(empty=True))
        with self.assertRaisesRegex(RuntimeError, '没有获取到'):
            ns['android_download']('202610')
        ns['drop_and_rename_table'].assert_not_called()

    def test_android_partial_insert_rejects_swap(self):
        ns = function_namespace('jobs/ods/ods_android_download/ods_android_download.py', 'android_download')
        frame = Mock(empty=False)
        frame.iterrows.return_value = [(0, {'Date': '2026-10-05'})]
        ns['fetch_download_data'] = Mock(return_value=frame)
        ns['pd'] = SimpleNamespace(isna=lambda x: x is None, notna=lambda x: x is not None)
        ns['bigquery_client'].insert_rows_json.return_value = [{'error': 'invalid'}]
        with self.assertRaisesRegex(RuntimeError, '临时表写入失败'):
            ns['android_download']('202610')
        ns['drop_and_rename_table'].assert_not_called()
        ns['drop_table_if_exists'].assert_called_once()


if __name__ == '__main__':
    unittest.main()
