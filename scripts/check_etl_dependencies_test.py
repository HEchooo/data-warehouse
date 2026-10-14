import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from check_etl_dependencies import ROOT, check, referenced_tables
sys.path.insert(0, str(ROOT))
import etl_run


class DependencyTests(unittest.TestCase):
    def test_references_ignore_documentation_and_sql_comments(self):
        source = '''"""参考 ods_documentation"""
SOURCE_TABLE = "dws_source"
sql = f"""SELECT * FROM `{SOURCE_TABLE}`
-- JOIN decom.dim_comment
JOIN `project.decom.dim_real` USING (id)
/* ads_comment */"""
'''
        self.assertEqual(referenced_tables(source), {'dws_source', 'dim_real'})

    def test_repository_dependencies_complete(self):
        self.assertEqual(check(), [])

    def test_missing_dependency_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'env').mkdir()
            tasks = [dict(table_name='ods_input', script='source.py', depends_on=[]),
                     dict(table_name='dwd_output', script='target.py', depends_on=[])]
            (root / 'env/etl_config.json').write_text(json.dumps({'tasks': tasks}))
            (root / 'source.py').write_text('')
            (root / 'target.py').write_text('SOURCE = "ods_input"')
            self.assertEqual(len(check(root)), 1)

    def test_new_shared_ledger_reader_requires_resource_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'env').mkdir()
            task = dict(table_name='dwd_new_reader', script='reader.py', depends_on=[])
            path = root / 'env/etl_config.json'
            path.write_text(json.dumps({'tasks': [task]}))
            (root / 'reader.py').write_text('TABLE = "oss_key_process_log"')
            self.assertIn('共享资源互斥', check(root)[0])
            task['resources'] = ['table:oss_key_process_log']
            path.write_text(json.dumps({'tasks': [task]}))
            self.assertEqual(check(root), [])

    def test_every_managed_failure_blocks_all_readers(self):
        # 对每个真实任务注入失败，仅模拟任务返回码，不运行任何业务脚本。
        with patch.object(etl_run, 'PATH_MODE_OVERRIDE', 'local'):
            config = etl_run.load_config(ROOT / 'env/etl_config.json')
        tasks = config['tasks']
        names = {t['table_name'] for t in tasks}
        sources = {t['table_name']: (referenced_tables((ROOT / t['script']).read_text()) & names)
                   - {t['table_name']} for t in tasks}
        with tempfile.TemporaryDirectory() as directory:
            config.update(project_dir=Path(directory), log_dir=Path(directory) / 'logs')
            for failing in names:
                with self.subTest(failing=failing):
                    blocked = {failing}
                    while True:
                        updated = blocked | {name for name, reads in sources.items() if reads & blocked}
                        if updated == blocked:
                            break
                        blocked = updated
                    ran = set()
                    def run(config, task, log, fd):
                        name = task['table_name']
                        ran.add(name)
                        return int(name == failing)
                    with patch.object(etl_run, 'run_task', side_effect=run), patch.object(etl_run, 'send_failure_alert'), contextlib.redirect_stdout(io.StringIO()):
                        self.assertEqual(etl_run.run_etl(config, Path('test.json')), 1)
                    self.assertFalse(ran & (blocked - {failing}))
                    # TikTok 或 ECN 故障必须仍放行独立素材链路。
                    if failing in {'ods_tiktok_ad_daily', 'ods_ecn_account_affiliation', 'ods_ecn_video_publication_owner'}:
                        self.assertIn('ads_weekly_ai_fashion_asset_overview', ran)


if __name__ == '__main__':
    unittest.main()
