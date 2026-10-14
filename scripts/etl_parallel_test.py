"""本地合成任务和模拟通知；绝不执行真实 ETL 或访问外部服务。"""
import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import etl_run


class ParallelTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = dict(mode='local', project_dir=self.root, python_bin=sys.executable,
                           log_dir=self.root / 'logs', env={}, tasks=[],
                           scheduler={'terminate_grace_seconds': 1})

    def task(self, name, body='', deps=(), resources=(), timeout=None):
        (self.root / f'{name}.py').write_text('from pathlib import Path\n' + body)
        task = dict(table_name=name, depends_on=list(deps), stage='TEST', name=name,
                    script=f'{name}.py', enabled=True, resources=list(resources))
        if timeout is not None:
            task['timeout_seconds'] = timeout
        self.config['tasks'].append(task)
        return task

    def run_etl(self, notify=None):
        with patch.object(etl_run, 'send_failure_alert', side_effect=notify) as alert, contextlib.redirect_stdout(io.StringIO()):
            result = etl_run.run_etl(self.config, self.root / 'config.json')
        return result, alert

    def events(self):
        paths = list((self.root / 'logs/runs').glob('*/events.jsonl'))
        return [json.loads(line) for path in paths for line in path.read_text().splitlines()]

    def test_four_real_processes_overlap_and_never_exceed_four(self):
        for i in range(8):
            # 首四个任务必须同时运行才会通过 barrier，串行实现会超时失败。
            self.task(f't{i}', f"import time\nPath('ready{i}').touch()\n"
                      "deadline=time.monotonic()+3\n"
                      "while len(list(Path('.').glob('ready*'))) < 4:\n"
                      "    assert time.monotonic()<deadline\n    time.sleep(.01)\n"
                      "time.sleep(.08)\n")
        result, _ = self.run_etl()
        self.assertEqual(result, 0)
        active = peak = 0
        for e in self.events():
            if e['event'] == 'task_started':
                active += 1
                peak = max(peak, active)
            elif e['event'] == 'task_finished':
                active -= 1
            self.assertLessEqual(active, 4)
        self.assertEqual(peak, 4)
        self.assertEqual(active, 0)

    def test_ready_downstream_does_not_wait_for_unrelated_slow_task(self):
        self.task('slow', "import time\nd=time.monotonic()+3\nwhile not Path('child.done').exists():\n    assert time.monotonic()<d\n    time.sleep(.01)\n")
        self.task('fast')
        self.task('child', "Path('child.done').touch()\n", deps=['fast'])
        self.assertEqual(self.run_etl()[0], 0)

    def test_shared_resource_serializes_but_failure_does_not_block_peer(self):
        body = "import time\nwith Path('exclusive').open('x'): time.sleep(.08)\nPath('exclusive').unlink()\n"
        self.task('a', body + 'raise SystemExit(1)\n', resources=['shared'])
        self.task('b', body, resources=['shared'])
        self.task('child', deps=['a'])
        result, _ = self.run_etl()
        self.assertEqual(result, 1)
        end = next(e for e in self.events() if e['event'] == 'run_finished')
        self.assertEqual(end['statuses'], {'a': 'failed', 'b': 'success', 'child': 'blocked'})

    def test_alert_arrives_before_unrelated_task_finishes_and_does_not_block_timeout(self):
        self.task('bad', 'raise SystemExit(1)\n')
        self.task('child', deps=['bad'])
        self.task('slow', "import time\ntime.sleep(20)\n", timeout=1)
        observed = []
        def notify(config, statuses):
            if not observed:
                observed.append(dict(statuses))
                deadline = time.monotonic()+4
                while time.monotonic()<deadline:
                    if any(e.get('task') == 'slow' and e.get('status') == 'timeout' for e in self.events()):
                        return
                    time.sleep(.02)
                raise AssertionError('notification blocked scheduler')
        result, _ = self.run_etl(notify)
        self.assertEqual(result, 1)
        self.assertEqual(observed, [{'bad': 'failed', 'child': 'blocked'}])
        self.assertTrue(all(e['success'] for e in self.events() if e['event'] == 'alert'))

    def test_silent_timeout_quarantines_only_affected_resources_across_runs(self):
        self.task('stuck', "import time,os\nPath('pid').write_text(str(os.getpid()))\ntime.sleep(20)\n",
                  timeout=1, resources=['shared'])
        self.task('peer', "Path('peer.ran').touch()\n", resources=['shared'])
        self.task('child', "Path('child.ran').touch()\n", deps=['stuck'])
        self.task('good', "Path('good.ran').touch()\n")
        self.assertEqual(self.run_etl()[0], 1)
        with self.assertRaises(ProcessLookupError):
            os.kill(int((self.root / 'pid').read_text()), 0)
        self.assertFalse((self.root / 'peer.ran').exists())
        self.assertFalse((self.root / 'child.ran').exists())
        self.assertTrue((self.root / 'good.ran').exists())
        (self.root / 'stuck.py').write_text("raise AssertionError('must not restart')\n")
        self.assertEqual(self.run_etl()[0], 1)
        state = json.loads((self.root / '.etl_task_state.json').read_text())
        self.assertEqual(state['stuck']['status'], 'recovery_required')
        self.assertTrue(any(e.get('task') == 'stuck' and e.get('status') == 'recovery_required'
                            for e in self.events()))

    def test_large_output_without_newline_cannot_deadlock_and_logs_are_separate(self):
        for name in ['a', 'b']:
            self.task(name, f"import sys\nsys.stdout.write({name!r}*2000000)\n")
        self.assertEqual(self.run_etl()[0], 0)
        run_dir = next((self.root / 'logs/runs').iterdir())
        self.assertEqual((run_dir/'a.log').read_text(), 'a'*2000000)
        self.assertEqual((run_dir/'b.log').read_text(), 'b'*2000000)

    def test_timeout_kills_sigterm_ignoring_process(self):
        self.task('stubborn', "import signal,time,os\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\nPath('pid').write_text(str(os.getpid()))\ntime.sleep(30)\n", timeout=1)
        self.assertEqual(self.run_etl()[0], 1)
        with self.assertRaises(ProcessLookupError):
            os.kill(int((self.root/'pid').read_text()), 0)

    def test_metrics_and_environment_have_consistent_run_identity(self):
        self.task('good', "import json,os\nPath('ids').write_text(json.dumps([os.environ['ETL_RUN_ID'],os.environ['ETL_TASK_ID']]))\n")
        self.assertEqual(self.run_etl()[0], 0)
        events = self.events()
        ids = json.loads((self.root/'ids').read_text())
        self.assertEqual({e['run_id'] for e in events}, {ids[0]})
        self.assertEqual(ids[1], 'good')
        finish = next(e for e in events if e['event'] == 'run_finished')
        self.assertEqual(finish['peak_running'], 1)
        self.assertGreater(finish['duration_seconds'], 0)
        begin = next(e for e in events if e['event'] == 'task_started')
        self.assertGreaterEqual(begin['queue_seconds'], 0)
        self.assertEqual(json.loads((self.root/'.etl_task_state.json').read_text()), {})

    def test_stale_running_state_after_parent_crash_is_not_retried(self):
        self.task('old', "raise AssertionError('must not run')\n")
        self.task('child', deps=['old'])
        self.task('good')
        (self.root/'.etl_task_state.json').write_text(json.dumps({
            'old': {'run_id': 'prior-run', 'status': 'running', 'resources': ['table:old']}}))
        self.assertEqual(self.run_etl()[0], 1)
        statuses = next(e['statuses'] for e in self.events() if e['event'] == 'run_finished')
        self.assertEqual(statuses, {'old': 'recovery_required', 'child': 'blocked', 'good': 'success'})

    def test_corrupt_state_fails_closed_and_alerts(self):
        self.task('good', "Path('ran').touch()\n")
        (self.root/'.etl_task_state.json').write_text('{broken')
        result, alert = self.run_etl()
        self.assertEqual(result, 1)
        self.assertFalse((self.root/'ran').exists())
        alert.assert_called_once()

    def test_invalid_scheduler_settings_are_rejected(self):
        for options in [{'max_workers': 5}, {'max_workers': True}, {'task_timeout_seconds': 0},
                        {'terminate_grace_seconds': -1}, {'unknown': 1}, []]:
            with self.subTest(options=options), self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                etl_run.scheduler_options({'scheduler': options})

    def test_known_shared_writers_and_provider_limits(self):
        resources = lambda n: etl_run.task_resources(dict(table_name=n, script=n+'.py'))
        for a,b,key in [('dwd_event_log','dwd_makepod_event_log','table:oss_key_process_log'),
                        ('ods_ecn_account_affiliation','ods_ecn_video_publication_owner','api:ecn'),
                        ('ods_tiktok_ad_daily','ods_tiktok_gmv_max_daily','api:tiktok')]:
            self.assertIn(key, resources(a)&resources(b))

    def start_four_runner(self):
        for i in range(4):
            self.task(f'p{i}', f"import time,os\nPath('pid{i}').write_text(str(os.getpid()))\n"
                      "while not Path('release').exists(): time.sleep(.02)\n")
        runner = self.root/'runner.py'
        runner.write_text(
            f"import sys,signal\nsys.path.insert(0,{str(Path(etl_run.__file__).parent)!r})\n"
            "from pathlib import Path\nimport etl_run\n"
            "def stop(s,f): raise SystemExit(128+s)\nsignal.signal(signal.SIGTERM,stop)\n"
            f"config={self.config!r}\nraise SystemExit(etl_run.run_etl(config,Path('config.json')))\n".replace('PosixPath(', 'Path('))
        process = subprocess.Popen([sys.executable,str(runner)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(lambda: process.wait(timeout=10))
        self.addCleanup(lambda: process.terminate() if process.poll() is None else None)
        self.addCleanup(lambda: (self.root/'release').touch())
        deadline=time.monotonic()+5
        while len(list(self.root.glob('pid*'))) < 4:
            self.assertIsNone(process.poll())
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.02)
        return process

    def test_sigterm_stops_all_four_children_before_releasing_lock(self):
        process = self.start_four_runner()
        process.terminate()
        self.assertEqual(process.wait(timeout=10),143)
        for path in self.root.glob('pid*'):
            with self.assertRaises(ProcessLookupError):
                os.kill(int(path.read_text()),0)
        state=json.loads((self.root/'.etl_task_state.json').read_text())
        self.assertEqual(len(state),4)
        self.assertEqual({e['status'] for e in state.values()},{'recovery_required'})
        self.config['tasks']=[]
        self.task('independent')
        self.assertEqual(self.run_etl()[0],0)

    def test_sigkill_parent_keeps_lock_until_all_children_exit_then_quarantines(self):
        process = self.start_four_runner()
        process.kill()
        process.wait(timeout=5)
        result, alert=self.run_etl()
        self.assertEqual(result,0)
        self.assertEqual(alert.call_args.args[1],{'scheduler':'lock_skipped'})
        (self.root/'release').touch()
        deadline=time.monotonic()+5
        while True:
            result, alert=self.run_etl()
            if result == 1:
                break
            self.assertLess(time.monotonic(),deadline)
            time.sleep(.05)
        self.assertEqual(set(alert.call_args.args[1].values()),{'recovery_required'})

    def test_global_timeout_and_single_worker_override(self):
        self.config['scheduler'].update(max_workers=1,task_timeout_seconds=1)
        self.task('slow','import time\ntime.sleep(20)\n')
        self.task('good')
        self.assertEqual(self.run_etl()[0],1)
        finish=next(e for e in self.events() if e['event']=='run_finished')
        self.assertEqual(finish['peak_running'],1)
        self.assertEqual(finish['statuses'],{'slow':'timeout','good':'success'})

    def test_orphan_child_is_cleaned_and_never_reported_as_success(self):
        # 子进程延迟写文件；组长正常退出也不能让后台写任务漏过监管。
        self.task('orphan', "import subprocess,sys\nsubprocess.Popen([sys.executable,'-c',"
                  "\"import time;from pathlib import Path;time.sleep(2);Path('leaked').touch()\"])\n")
        self.assertEqual(self.run_etl()[0],1)
        time.sleep(2)
        self.assertFalse((self.root/'leaked').exists())
        end=next(e for e in self.events() if e['event']=='run_finished')
        self.assertEqual(end['statuses']['orphan'],'recovery_required')

    def test_external_signal_death_is_quarantined_across_runs(self):
        self.task('killed', 'import os,signal\nos.kill(os.getpid(),signal.SIGKILL)\n')
        self.task('child', deps=['killed'])
        self.task('good')
        self.assertEqual(self.run_etl()[0], 1)
        state = json.loads((self.root/'.etl_task_state.json').read_text())
        self.assertEqual(state['killed']['status'], 'recovery_required')
        (self.root/'killed.py').write_text("from pathlib import Path\nPath('retried').touch()\n")
        self.assertEqual(self.run_etl()[0], 1)
        self.assertFalse((self.root/'retried').exists())

    def test_notification_retries_before_long_independent_task_finishes(self):
        self.task('bad', 'raise SystemExit(1)\n')
        self.task('waiting', "import time\nd=time.monotonic()+5\n"
                  "while not Path('notified').exists():\n    assert time.monotonic()<d\n    time.sleep(.02)\n")
        calls = []
        def notify(config, statuses):
            calls.append(dict(statuses))
            if len(calls) == 1:
                raise OSError('secret')
            (self.root/'notified').touch()
        self.assertEqual(self.run_etl(notify)[0], 1)
        self.assertEqual(len(calls), 2)
        finish = next(e for e in self.events() if e['event']=='run_finished')
        self.assertEqual(finish['statuses']['waiting'], 'success')
        self.assertTrue(finish['alert_success'])

    def test_persistent_notification_failure_has_bounded_attempts(self):
        self.task('bad', 'raise SystemExit(1)\n')
        self.task('slow', 'import time\ntime.sleep(1.5)\n')
        def notify(*args):
            raise OSError('secret')
        result, alert = self.run_etl(notify)
        self.assertEqual(result, 1)
        self.assertEqual(alert.call_count, 2)

    def test_script_aliases_share_lock_even_with_different_task_names(self):
        body = "import time\nwith Path('exclusive').open('x'): time.sleep(.1)\nPath('exclusive').unlink()\n"
        self.task('a', body)
        peer = self.task('b')
        peer['script'] = './a.py'
        self.assertEqual(self.run_etl()[0], 0)
        finish = next(e for e in self.events() if e['event']=='run_finished')
        self.assertEqual(finish['peak_running'], 1)

    def test_state_commit_failure_after_task_exit_alerts_and_retains_journal(self):
        self.task('a')
        original = etl_run.save_state
        calls = []
        def save(path, state):
            calls.append(1)
            if len(calls) == 3:  # 初始记录、启动记录、完成记录。
                raise OSError('secret')
            return original(path, state)
        with patch.object(etl_run, 'save_state', side_effect=save):
            result, alert = self.run_etl()
        self.assertEqual(result, 1)
        self.assertEqual(alert.call_args.args[1]['scheduler'], 'failed')
        state = json.loads((self.root/'.etl_task_state.json').read_text())
        self.assertEqual(state['a']['status'], 'running')

    def test_initial_state_write_failure_prevents_launch_and_alerts(self):
        self.task('a', "Path('ran').touch()\n")
        with patch.object(etl_run, 'save_state', side_effect=OSError('secret')):
            result, alert = self.run_etl()
        self.assertEqual(result, 1)
        self.assertFalse((self.root/'ran').exists())
        alert.assert_called_once()

    def test_process_reap_after_kill_is_bounded(self):
        from unittest.mock import Mock
        process = Mock(pid=987654321)
        process.wait.side_effect = subprocess.TimeoutExpired('test', 1)
        with patch.object(etl_run.os, 'killpg'), patch.object(etl_run.time, 'sleep'), \
                patch.object(etl_run.time, 'monotonic', side_effect=[0, 2]), \
                self.assertRaises(etl_run.TaskUncertain):
            etl_run.stop_process_group(process, 1)
        process.wait.assert_called_once_with(timeout=1)

    def test_final_new_alert_snapshot_also_gets_one_retry(self):
        self.task('first', 'raise SystemExit(1)\n')
        self.task('second', 'import time\ntime.sleep(.2)\nraise SystemExit(1)\n')
        calls = []
        def notify(config, statuses):
            calls.append(dict(statuses))
            if len(calls) == 1:
                deadline = time.monotonic()+5
                while not any(e.get('task')=='second' and e.get('status')=='failed' for e in self.events()):
                    if time.monotonic() > deadline:
                        raise AssertionError('second failure missing')
                    time.sleep(.02)
            elif len(calls) == 2:
                raise OSError('temporary')
        self.assertEqual(self.run_etl(notify)[0], 1)
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[1], calls[2])
        self.assertEqual(set(calls[2]), {'first', 'second'})

    def test_signal_between_task_completion_and_state_commit_still_alerts(self):
        self.task('a')
        original = etl_run.save_state
        calls = []
        def save(path, state):
            calls.append(1)
            if len(calls) == 3:
                raise SystemExit(143)
            return original(path, state)
        with patch.object(etl_run, 'save_state', side_effect=save), \
                patch.object(etl_run, 'send_failure_alert') as alert, \
                contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
            etl_run.run_etl(self.config, self.root/'config.json')
        self.assertEqual(alert.call_args.args[1]['scheduler'], 'cancelled')
        state = json.loads((self.root/'.etl_task_state.json').read_text())
        self.assertEqual(state['a']['status'], 'running')


if __name__ == '__main__':
    unittest.main()
