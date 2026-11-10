#!/usr/bin/env python3
"""Workload compatibility and failure-reporting tests for the matrix runner."""
import argparse
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    'matrix', ROOT / 'example/ub_test/run_ub_test_matrix.py')
matrix = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(matrix)


class MatrixTest(unittest.TestCase):
    def setUp(self):
        self.args = argparse.Namespace(server_dir='/srv/test', client_dir='/client/test',
            cpus='96-111', device='test_device', port=10918, server_address='192.0.2.1')
        self.report = (
            'Avg-Latency: 12, 50th-Latency: 10, 90th-Latency: 20, '
            '99th-Latency: 30, 99.9th-Latency: 40, 99.99th-Latency: 50, '
            'Max-Latency: 60, Throughput: 1.23e+06MB/s, QPS: 999.8, '
            'Server CPU(avg/max): 2/3%, Client CPU(avg/max): 4/5%, '
            'Client Memory(avg/max): 300/310MB, Total Sent: 20000, '
            'Total Errors: 0, Total Requests: 20000, Error rate: 0%')

    def test_original_workload_is_preserved(self):
        self.assertEqual(matrix.QPS_VALUES, [1000, 500])
        self.assertEqual(matrix.SIZES, [1024, 4096, 8192, 102400, 204800, 1048576, 8388608])
        for mode in ('single', 'multi'):
            cmd = matrix.command(self.args, 'client', mode, 500, 8388608)
            self.assertEqual(cmd[:5], ['numactl', '-C', '96-111', '-m', '1'])
            self.assertIn('/client/test/runtime/' + mode + '/ub_test_client', cmd)
            self.assertEqual(cmd[-10:], ['--servers=192.0.2.1:10918',
                '--rpc_timeout_ms=2000', '--connect_timeout_ms=6000', '--test_seconds=20',
                '--max_retry=10', '--queue_depth=10', '--req_size=8388608',
                '--dummy_port=0', '--expected_qps=500', '--initial_tokens=0'])
            self.assertIn('--urma_use_polling=false', cmd)
            self.assertFalse(any('--urma_io_mode' in arg for arg in cmd))
            srv = matrix.command(self.args, 'server', mode, 500, 8388608)
            self.assertEqual(srv[-4:], ['--port', '10918', '--rsp_size=8388608', '--num_threads=16'])

    def test_jfs_override_changes_both_nodes_without_changing_workload(self):
        for role in ('client', 'server'):
            baseline = matrix.command(self.args, role, 'multi', 1000, 1024)
            self.assertFalse(any('--urma_io_jfs_cnt=' in arg for arg in baseline))
            for count in (48, 128):
                self.args.io_jfs_count = count
                command = matrix.command(self.args, role, 'multi', 1000, 1024)
                command.remove('--urma_io_jfs_cnt=' + str(count))
                self.assertEqual(command, baseline)
            del self.args.io_jfs_count

    def test_nonpositive_jfs_count_is_rejected(self):
        for count in ('0', '-1'):
            with self.assertRaises(argparse.ArgumentTypeError):
                matrix.positive_int(count)
        self.assertEqual(matrix.positive_int('48'), 48)

    def test_metrics_preserve_values_and_support_scientific_notation(self):
        line, metrics, totals = matrix.parse_result('startup log\n' + self.report + '\n')
        self.assertEqual(metrics['Throughput'], '1.23e+06')
        self.assertEqual(metrics['Client_Mem_max'], '310')
        self.assertEqual(metrics['99.9th-Latency'], '40')
        self.assertEqual(matrix.statistics_failures(0, line, metrics, totals), [])

    def test_nonzero_error_count_is_not_hidden_by_rounded_rate(self):
        line, metrics, totals = matrix.parse_result(self.report.replace('Total Errors: 0', 'Total Errors: 1'))
        self.assertEqual(metrics['Error_rate'], '0')
        self.assertIn('RPC errors', matrix.statistics_failures(0, line, metrics, totals))

    def test_failed_exit_and_missing_report_are_rejected(self):
        line, metrics, totals = matrix.parse_result(self.report)
        self.assertIn('client exit code', matrix.statistics_failures(2, line, metrics, totals))
        line, metrics, totals = matrix.parse_result('connection failed')
        self.assertIn('incomplete statistics', matrix.statistics_failures(0, line, metrics, totals))

    def test_remote_python_is_syntactically_valid(self):
        class CaptureRemote:
            def python(self, source, timeout=30):
                compile(source, '<remote>', 'exec')
                return {}
            def put(self, local, remote):
                pass
        remote = CaptureRemote()
        matrix.deploy(remote, '/tmp/runtime', 'unused.tar.gz')
        matrix.preflight(remote, '/tmp/runtime', '96-111', 10918, True)
        argv = matrix.command(self.args, 'client', 'multi', 1000, 1024)
        matrix.launch(remote, '/tmp/runtime', 'case', 'client', 'multi', argv)
        proc = {'pid': 12345, 'args': argv}
        matrix.process_info(remote, proc)
        matrix.socket_snapshot(remote, 10918)
        matrix.stop(remote, proc)

    def test_launch_returns_numeric_pid_instead_of_pid_file_path(self):
        class LocalRemote:
            def python(self, source, timeout=30):
                def fake_popen(argv, **kwargs):
                    spec = json.loads(argv[-1])
                    Path(spec['pid']).write_text('12345')
                    return argparse.Namespace(pid=12344)
                output = io.StringIO()
                with mock.patch('subprocess.Popen', side_effect=fake_popen):
                    with contextlib.redirect_stdout(output):
                        exec(compile(source, '<launch>', 'exec'), {})
                return json.loads(output.getvalue())
        with tempfile.TemporaryDirectory() as root:
            (Path(root) / 'logs').mkdir()
            proc = matrix.launch(LocalRemote(), root, 'case', 'server', 'multi', ['test'])
            self.assertEqual(proc['pid'], 12345)
            self.assertEqual(proc['supervisor_pid'], 12344)

    def test_failed_audit_does_not_discard_completed_statistics(self):
        class LogRemote:
            def __init__(self, report):
                self.report = report
            def read(self, path):
                if path.endswith('client.exit'):
                    return '0'
                if path.endswith('server.exit'):
                    return ''
                return 'UB_TEST_TRANSPORT sge_mode=multi tp=CTP\n' + self.report
        remote = LogRemote(self.report)
        def launched(remote, root, name, role, mode, argv):
            return {'pid': 123, 'args': argv, 'log': role + '.log', 'exit': role + '.exit'}
        with tempfile.TemporaryDirectory() as root:
            self.args.output = root
            with mock.patch.object(matrix, 'launch', side_effect=launched), \
                 mock.patch.object(matrix, 'process_info', side_effect=FileNotFoundError('process exited')), \
                 mock.patch.object(matrix, 'socket_snapshot', return_value={'urma_sockets': []}), \
                 mock.patch.object(matrix, 'stop', return_value={'stopped': True}), \
                 mock.patch.object(matrix.time, 'sleep'), \
                 contextlib.redirect_stdout(io.StringIO()):
                result = matrix.run_test(self.args, {'server': remote, 'client': remote}, 'multi', 1000, 1024)
            self.assertFalse(result['valid'])
            self.assertEqual(result['client_returncode'], 0)
            self.assertEqual(result['metrics']['QPS'], '999.8')
            self.assertEqual(result['totals']['Total Requests'], 20000)
            self.assertTrue(any('process audit' in s for s in result['failures']))

    def test_jfs_audit_rejects_missing_or_wrong_runtime_count(self):
        self.args.io_jfs_count = 48
        class LogRemote:
            def __init__(self, report, count):
                self.report, self.count = report, count
            def read(self, path):
                if path.endswith('client.exit'):
                    return '0'
                if path.endswith('server.exit'):
                    return ''
                count = '' if self.count is None else ' jfs_count=' + str(self.count)
                return 'UB_TEST_TRANSPORT sge_mode=multi tp=CTP' + count + '\n' + self.report
        def launched(remote, root, name, role, mode, argv):
            return {'pid': 123, 'args': argv, 'log': role + '.log', 'exit': role + '.exit'}
        info = {'thread_affinities': [list(range(96, 112))], 'numa_policies': {'bind:1': 1},
                'libraries': [root + '/runtime/' + suffix for root in ('/srv/test', '/client/test')
                              for suffix in ('multi/libbrpc.so', 'common/libtcmalloc.so.4')]}
        with tempfile.TemporaryDirectory() as root:
            self.args.output = root
            for count in (48, 128, None):
                remote = LogRemote(self.report, count)
                with mock.patch.object(matrix, 'launch', side_effect=launched), \
                     mock.patch.object(matrix, 'process_info', return_value=info), \
                     mock.patch.object(matrix, 'socket_snapshot', return_value={'urma_sockets': [{}]}), \
                     mock.patch.object(matrix, 'stop', return_value={'stopped': True}), \
                     mock.patch.object(matrix.time, 'sleep'), \
                     contextlib.redirect_stdout(io.StringIO()):
                    result = matrix.run_test(self.args, {'server': remote, 'client': remote}, 'multi', 1000, 1024)
                self.assertEqual(result['io_jfs_count'], 48)
                self.assertEqual(result['valid'], count == 48)
                if count != 48:
                    self.assertIn('server JFS count', result['failures'])
                    self.assertIn('client JFS count', result['failures'])

    def test_repack_does_not_truncate_an_open_runtime_file(self):
        spec = importlib.util.spec_from_file_location('package_builder', ROOT / 'example/ub_test/build_package.py')
        builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(builder)
        with tempfile.TemporaryDirectory() as root:
            source, destination = Path(root) / 'source', Path(root) / 'libbrpc.so'
            source.write_bytes(b'new library')
            source.chmod(0o755)
            destination.write_bytes(b'existing mapped library')
            with destination.open('rb') as existing:
                builder.install_file(source, destination)
                self.assertEqual(existing.read(), b'existing mapped library')
            self.assertEqual(destination.read_bytes(), b'new library')
            self.assertEqual(destination.stat().st_mode & 0o777, 0o755)


class SshCompatibilityTest(unittest.TestCase):
    def test_windows_disables_multiplexing_for_ssh_scp_and_cleanup(self):
        with mock.patch.object(matrix.sys, 'platform', 'win32'):
            remote = matrix.Remote('root@192.0.2.1', 'test_key')
        with mock.patch.object(matrix.subprocess, 'run') as run:
            run.return_value = argparse.Namespace(returncode=0, stdout='true', stderr='')
            self.assertTrue(remote.python('import json; print(json.dumps(True))'))
            remote.put('runtime.tar.gz', '/tmp/runtime.tar.gz')
            remote.close()
        self.assertEqual(run.call_count, 2)  # No unsupported ssh -O exit.
        self.assertEqual([c.args[0][0] for c in run.call_args_list], ['ssh', 'scp'])
        for call in run.call_args_list:
            argv = call.args[0]
            for option in ('ControlMaster=no', 'ControlPersist=no', 'ControlPath=none'):
                self.assertIn(option, argv)
            self.assertNotIn('ControlMaster=auto', argv)
            self.assertNotIn('-O', argv)

    def test_off_overrides_multiplexing_on_posix(self):
        with mock.patch.object(matrix.sys, 'platform', 'linux'):
            remote = matrix.Remote('root@192.0.2.1', 'test_key', 'off')
        self.assertFalse(remote.multiplexing)
        self.assertIn('ControlPath=none', remote.options)

    def test_posix_auto_preserves_reuse_and_bounds_cleanup(self):
        with mock.patch.object(matrix.sys, 'platform', 'linux'):
            remote = matrix.Remote('root@192.0.2.1', 'test_key')
        self.assertTrue(remote.multiplexing)
        self.assertIn('ControlMaster=auto', remote.options)
        with mock.patch.object(matrix.subprocess, 'run') as run:
            run.side_effect = matrix.subprocess.TimeoutExpired('ssh', 10)
            remote.close()  # Must not mask a preceding exception.
        self.assertIn('-O', run.call_args.args[0])
        self.assertEqual(run.call_args.kwargs['timeout'], 10)

    def test_ssh_error_retains_details_and_suggests_disabling_mux(self):
        remote = matrix.Remote('root@192.0.2.1', 'test_key', 'on')
        with mock.patch.object(matrix.subprocess, 'run') as run:
            run.return_value = argparse.Namespace(returncode=255, stdout='',
                stderr='getsockname failed: Not a socket\n')
            with self.assertRaisesRegex(RuntimeError, '--ssh-multiplexing off'):
                remote.python('print(1)')
        self.assertEqual(run.call_count, 1)  # Never replay a remote mutation.


if __name__ == '__main__':
    unittest.main()
