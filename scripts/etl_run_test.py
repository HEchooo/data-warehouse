"""只运行临时目录内的模拟任务，不访问数仓和通知服务。"""
import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import etl_run


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = dict(mode='local', project_dir=self.root, python_bin=sys.executable,
                           log_dir=self.root / 'logs', env={}, tasks=[])

    def task(self, name, deps=(), code=0, enabled=True):
        (self.root / f'{name}.py').write_text(
            f"from pathlib import Path\nPath('{name}.ran').touch()\nraise SystemExit({code})\n")
        task = dict(table_name=name, depends_on=list(deps), stage='TEST', name=name,
                    script=f'{name}.py', enabled=enabled)
        self.config['tasks'].append(task)
        return task

    def run_etl(self):
        with contextlib.redirect_stdout(io.StringIO()), patch.object(etl_run, 'send_failure_alert') as alert:
            result = etl_run.run_etl(self.config, self.root / 'config.json')
        return result, alert

    def test_transitive_failure_and_independent_branch(self):
        self.task('bad', code=3)
        self.task('child', ['bad'])
        self.task('grandchild', ['child'])
        self.task('good')
        self.task('good_child', ['good'])
        result, alert = self.run_etl()
        self.assertEqual(result, 1)
        self.assertFalse((self.root / 'child.ran').exists())
        self.assertFalse((self.root / 'grandchild.ran').exists())
        self.assertTrue((self.root / 'good_child.ran').exists())
        alert.assert_called_once()
        self.assertEqual(alert.call_args.args[1]['grandchild'], 'blocked')

    def test_disabled_dependency_blocks(self):
        self.task('off', enabled=False)
        self.task('child', ['off'])
        self.assertEqual(self.run_etl()[0], 1)
        self.assertFalse((self.root / 'child.ran').exists())

    def test_missing_script_isolated(self):
        self.task('missing')
        (self.root / 'missing.py').unlink()
        self.task('good')
        self.assertEqual(self.run_etl()[0], 1)
        self.assertTrue((self.root / 'good.ran').exists())

    def test_success_no_alert_and_lock_released(self):
        self.task('good')
        for _ in range(2):
            result, alert = self.run_etl()
            self.assertEqual(result, 0)
            alert.assert_not_called()

    def test_notification_failure_preserves_failure(self):
        self.task('bad', code=1)
        with patch.object(etl_run, 'send_failure_alert', side_effect=RuntimeError('secret')), contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(etl_run.run_etl(self.config, self.root / 'config.json'), 1)
        self.assertIn('失败告警发送失败', out.getvalue())
        self.assertNotIn('secret', out.getvalue())

    def test_config_dependency_validation(self):
        self.task('a')
        self.task('b', ['a'])
        runtime = dict(project_dir=str(self.root), python=sys.executable, log_dir=str(self.root / 'logs'))
        data = dict(path_mode='local', paths={'local': runtime}, tasks=self.config['tasks'])
        path = self.root / 'config.json'
        for deps, duplicate in [(['unknown'], False), (['b'], False), ([], True)]:
            data['tasks'][0]['depends_on'] = deps
            data['tasks'][1]['table_name'] = 'a' if duplicate else 'b'
            path.write_text(json.dumps(data))
            with patch.object(etl_run, 'PATH_MODE_OVERRIDE', None), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                etl_run.load_config(path)
        data['tasks'][0]['depends_on'] = []
        data['tasks'][1]['table_name'] = 'b'
        path.write_text(json.dumps(data))
        with patch.object(etl_run, 'PATH_MODE_OVERRIDE', None):
            self.assertEqual(etl_run.load_config(path)['tasks'][1]['depends_on'], ['a'])

    def test_alert_payload_and_service_rejection(self):
        (self.root / 'env').mkdir()
        (self.root / 'env/lark_webhook.json').write_text(
            json.dumps({'local': 'https://example.invalid/webhook'}))
        statuses = {'bad': 'failed', 'child': 'blocked', 'good': 'success'}
        for body, accepted in [({'code': 0}, True), ({'StatusCode': 0}, True),
                               ({'code': 1}, False), ({}, False)]:
            response = subprocess.CompletedProcess([], 0, stdout=json.dumps(body))
            with patch.object(etl_run.subprocess, 'run', return_value=response) as send:
                if accepted:
                    etl_run.send_failure_alert(self.config, statuses)
                else:
                    with self.assertRaises(RuntimeError):
                        etl_run.send_failure_alert(self.config, statuses)
                request = json.loads(send.call_args.kwargs['input'])
                payload = request['payload']
                self.assertIn('child: blocked', payload['content']['text'])
                self.assertNotIn('good', payload['content']['text'])
                self.assertEqual(send.call_args.kwargs['timeout'], 20)
                self.assertNotIn(request['webhook'], str(send.call_args.args))
                self.assertTrue(send.call_args.kwargs['close_fds'])
                self.assertNotIn('pass_fds', send.call_args.kwargs)

    def test_launch_error_does_not_stop_other_tasks(self):
        self.task('bad')
        self.task('good')
        original = etl_run.run_task
        def run(config, task, log, fd):
            if task['table_name'] == 'bad':
                raise PermissionError('cannot execute')
            return original(config, task, log, fd)
        with patch.object(etl_run, 'run_task', side_effect=run):
            self.assertEqual(self.run_etl()[0], 1)
        self.assertTrue((self.root / 'good.ran').exists())

    def test_main_ignores_repeated_signals_during_cleanup(self):
        with patch.object(etl_run.signal, 'signal') as install, \
                patch.object(etl_run, 'load_config', return_value=self.config), \
                patch.object(etl_run, 'run_etl') as run:
            def interrupt(*args):
                handler = install.call_args_list[0].args[1]
                handler(signal.SIGTERM, None)
            run.side_effect = interrupt
            with self.assertRaises(SystemExit) as caught:
                etl_run.main()
            self.assertEqual(caught.exception.code, 143)
            self.assertEqual([call.args for call in install.call_args_list[-2:]],
                             [(signal.SIGTERM, signal.SIG_IGN), (signal.SIGINT, signal.SIG_IGN)])

    def start_slow_runner(self):
        self.task('slow')
        (self.root / 'slow.py').write_text("import time\nfrom pathlib import Path\nPath('ready').touch()\nwhile not Path('release').exists(): time.sleep(.02)\n")
        runner = self.root / 'runner.py'
        runner.write_text(
            f"import sys, signal\nsys.path.insert(0, {str(Path(etl_run.__file__).parent)!r})\n"
            "from pathlib import Path\nimport etl_run\n"
            "def stop(s,f): raise SystemExit(128+s)\nsignal.signal(signal.SIGTERM,stop)\n"
            f"config={self.config!r}\nraise SystemExit(etl_run.run_etl(config, Path('config.json')))\n".replace('PosixPath(', 'Path('))
        p = subprocess.Popen([sys.executable, str(runner)], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.addCleanup(lambda: p.stderr.close())
        self.addCleanup(lambda: p.wait(timeout=5))
        self.addCleanup(lambda: p.terminate() if p.poll() is None else None)
        self.addCleanup(lambda: (self.root / 'release').touch())
        deadline = time.monotonic()+5
        while not (self.root / 'ready').exists():
            if p.poll() is not None: self.fail(p.stderr.read().decode())
            if time.monotonic()>deadline: self.fail('child did not start')
            time.sleep(.02)
        return p

    def test_overlapping_run_skips(self):
        p = self.start_slow_runner()
        self.config['tasks'] = []
        self.task('second')
        result, alert = self.run_etl()
        self.assertEqual(result, 0)
        self.assertFalse((self.root / 'second.ran').exists())
        self.assertEqual(alert.call_args.args[1], {'scheduler': 'lock_skipped'})
        (self.root / 'release').touch()
        self.assertEqual(p.wait(timeout=5), 0)
        self.run_etl()
        self.assertTrue((self.root / 'second.ran').exists())

    def test_sigterm_releases_lock_after_child_exit(self):
        p = self.start_slow_runner()
        p.terminate()
        self.assertEqual(p.wait(timeout=5), 143)
        self.config['tasks'] = []
        self.task('second')
        self.run_etl()
        self.assertTrue((self.root / 'second.ran').exists())

    def test_sigkill_parent_child_keeps_lock(self):
        p = self.start_slow_runner()
        p.kill()
        p.wait(timeout=5)
        self.config['tasks'] = []
        self.task('second')
        self.run_etl()
        self.assertFalse((self.root / 'second.ran').exists())
        (self.root / 'release').touch()
        deadline = time.monotonic()+5
        while not (self.root / 'second.ran').exists():
            self.run_etl()
            if time.monotonic()>deadline: self.fail('lock not released')
            time.sleep(.02)


if __name__ == '__main__':
    unittest.main()
