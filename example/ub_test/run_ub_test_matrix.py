#!/usr/bin/env python3
"""Run the dwh110/brpc ub_test matrix using pooled single/multi SGE builds.

Upstream: b4dff13b0de19a12dc3a1b82902a20d4b51b1bb1/run_ub_test_matrix.py.
Preserved workload: QPS 1000/500; global QD 10; 20 seconds; req_size=rsp_size
1KiB,4KiB,8KiB,100KiB,200KiB,1MiB,8MiB; CPUs 96-111; memory node 1;
RPC timeout 2000ms, connect timeout 6000ms, max_retry=10, initial_tokens=0.
Only the transport selection and orchestration are adapted. Event mode is
required by pooled URMA. Uses Python's standard library and OpenSSH, no Paramiko.

The binary archive contains runtime/{multi,single}/ub_test_{client,server},
the corresponding libbrpc.so, common/libtcmalloc.so.4, and manifest.json.
No sources are uploaded. Existing system UMDK/Protobuf dependencies are required.
Use --dry-run to inspect every command without contacting either host.
"""

import argparse
import csv
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time


UPSTREAM_COMMIT = 'b4dff13b0de19a12dc3a1b82902a20d4b51b1bb1'
QPS_VALUES = [1000, 500]
SIZES = [1024, 4096, 8192, 102400, 204800, 1048576, 8388608]
METRICS = ['Avg-Latency', '50th-Latency', '90th-Latency', '99th-Latency',
           '99.9th-Latency', '99.99th-Latency', 'Max-Latency', 'Throughput',
           'QPS', 'Server_CPU_avg', 'Server_CPU_max', 'Client_CPU_avg',
           'Client_CPU_max', 'Client_Mem_avg', 'Client_Mem_max', 'Error_rate']
CSV_HEADER = ['urma_sge_mode', 'expected_qps', 'size'] + METRICS
NUMBER = r'([0-9]+(?:\.[0-9]*)?(?:[eE][+-]?[0-9]+)?)'


def parse_result(output):
    line = next((s for s in output.splitlines() if 'Avg-Latency:' in s), '')
    result = {}
    for field in METRICS[:9]:
        m = re.search(re.escape(field) + r':\s*' + NUMBER, line)
        result[field] = m.group(1) if m else 'N/A'
    for label, prefix in [('Server CPU', 'Server_CPU'), ('Client CPU', 'Client_CPU'),
                          ('Client Memory', 'Client_Mem')]:
        m = re.search(re.escape(label) + r'\(avg/max\):\s*' + NUMBER + '/' + NUMBER, line)
        result[prefix + '_avg'] = m.group(1) if m else 'N/A'
        result[prefix + '_max'] = m.group(2) if m else 'N/A'
    m = re.search(r'Error rate:\s*' + NUMBER, line)
    result['Error_rate'] = m.group(1) if m else 'N/A'
    totals = {}
    for label in ('Total Sent', 'Total Errors', 'Total Requests'):
        m = re.search(re.escape(label) + r':\s*(\d+)', line)
        totals[label] = int(m.group(1)) if m else None
    return line, result, totals


def command(args, role, mode, qps, size):
    root = getattr(args, role + '_dir')
    binary = root + '/runtime/' + mode + '/ub_test_' + role
    cmd = ['numactl', '-C', args.cpus, '-m', '1', binary,
           '--use_urma=true', '--urma_sge_mode=' + mode,
           '--urma_device=' + args.device, '--urma_use_polling=false']
    if role == 'server':
        cmd += ['--port', str(args.port), '--rsp_size=' + str(size), '--num_threads=16']
    else:
        cmd += ['--servers=' + args.server_address + ':' + str(args.port),
                '--rpc_timeout_ms=2000', '--connect_timeout_ms=6000',
                '--test_seconds=20', '--max_retry=10', '--queue_depth=10',
                '--req_size=' + str(size), '--dummy_port=0',
                '--expected_qps=' + str(qps), '--initial_tokens=0']
    return cmd


class Remote:
    def __init__(self, host, key, ssh_multiplexing='auto'):
        self.host = host
        self.multiplexing = (sys.platform != 'win32' if ssh_multiplexing == 'auto'
                             else ssh_multiplexing == 'on')
        self.options = ['-i', str(Path(key).expanduser()), '-o', 'BatchMode=yes',
                        '-o', 'ConnectTimeout=10']
        if self.multiplexing:
            self.options += ['-o', 'ControlMaster=auto', '-o', 'ControlPersist=600',
                             '-o', 'ControlPath=/tmp/brpc-ub-matrix-%C']
        else:
            # Windows OpenSSH cannot use the Unix control socket. Explicitly
            # disable all three options, including inherited ~/.ssh/config.
            self.options += ['-o', 'ControlMaster=no', '-o', 'ControlPersist=no',
                             '-o', 'ControlPath=none']

    def python(self, source, timeout=30):
        proc = subprocess.run(['ssh'] + self.options + [self.host, 'python3 -'],
                              input=source, text=True, encoding='utf-8',
                              capture_output=True, timeout=timeout)
        if proc.returncode:
            message = self.host + ': ' + proc.stderr + proc.stdout
            if self.multiplexing and 'getsockname failed' in proc.stderr.lower():
                message += '\nSSH control socket failed; use --ssh-multiplexing off.'
            raise RuntimeError(message)
        return json.loads(proc.stdout)

    def put(self, local, remote):
        subprocess.run(['scp'] + self.options + [str(local), self.host + ':' + shlex.quote(remote)],
                       check=True, timeout=120)

    def read(self, path):
        return self.python('import json,pathlib\np=pathlib.Path(' + repr(path) +
                           ')\nprint(json.dumps(p.read_text() if p.exists() else ""))\n')

    def close(self):
        if self.multiplexing:
            try:
                subprocess.run(['ssh'] + self.options + ['-O', 'exit', self.host],
                               capture_output=True, timeout=10)
            except (OSError, subprocess.SubprocessError):
                pass  # Cleanup must not hide the original SSH/test failure.


def deploy(remote, root, archive):
    remote.python('import pathlib,json\npathlib.Path(' + repr(root) +
                  ').mkdir(parents=True,exist_ok=True)\nprint(json.dumps(True))\n')
    dest = root + '/ub-test-runtime.upload.tar.gz'
    remote.put(archive, dest)
    remote.python('''import json,pathlib,tarfile
root=pathlib.Path(ROOT).resolve()
archive=root/'ub-test-runtime.upload.tar.gz'
with tarfile.open(archive) as a:
 for member in a.getmembers():
  path=(root/member.name).resolve()
  if root not in path.parents or member.issym() or member.islnk():
   raise RuntimeError('Unsafe archive member: '+member.name)
 a.extractall(root)
archive.unlink()
print(json.dumps(True))
'''.replace('ROOT', repr(root)), timeout=60)


def preflight(remote, root, cpus, port, server):
    return remote.python('''import hashlib,json,os,pathlib,subprocess
root=pathlib.Path(ROOT);runtime=root/'runtime'
manifest=json.loads((runtime/'manifest.json').read_text())
hashes={name:hashlib.sha256((runtime/name).read_bytes()).hexdigest() for name in manifest['files']}
if hashes!=manifest['files']:raise RuntimeError('Binary manifest mismatch')
deps={}
for mode in ['multi','single']:
 env=dict(os.environ,LD_LIBRARY_PATH=str(runtime/mode)+':'+str(runtime/'common'))
 for role in ['client','server']:
  key=mode+'/ub_test_'+role
  deps[key]=subprocess.check_output(['ldd',str(runtime/key)],text=True,env=env)
  if 'not found' in deps[key]:raise RuntimeError(deps[key])
binding=subprocess.check_output(['numactl','-C',CPUS,'-m','1','--show'],text=True,stderr=subprocess.STDOUT)
if SERVER:
 listeners=subprocess.check_output(['ss','-H','-ltn'],text=True)
 if any(s.split()[3].rsplit(':',1)[-1]==str(PORT) for s in listeners.splitlines()):
  raise RuntimeError('Test port already in use; refusing to stop its owner')
(root/'logs').mkdir(exist_ok=True)
print(json.dumps({'manifest':manifest,'hashes':hashes,'dependencies':deps,'binding':binding,
 'architecture':subprocess.check_output(['uname','-m'],text=True).strip()}))
'''.replace('ROOT', repr(root)).replace('CPUS', repr(cpus))
        .replace('SERVER', repr(server)).replace('PORT', str(port)), timeout=60)


def launch(remote, root, name, role, mode, argv):
    spec = {'args': argv, 'log': root + '/logs/' + name + '-' + role + '.log',
            'pid': root + '/logs/' + name + '-' + role + '.pid',
            'exit': root + '/logs/' + name + '-' + role + '.exit',
            'library_path': root + '/runtime/' + mode + ':' + root + '/runtime/common'}
    # The supervisor is sent in the SSH command, never deployed as a source file.
    supervisor = '''import json,os,pathlib,subprocess,sys
s=json.loads(sys.argv[1]);env=dict(os.environ,LD_LIBRARY_PATH=s['library_path'])
with open(s['log'],'w') as f:
 p=subprocess.Popen(s['args'],stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,env=env)
 pathlib.Path(s['pid']).write_text(str(p.pid))
 code=p.wait()
pathlib.Path(s['exit']).write_text(str(code))
'''
    result = remote.python('''import json,pathlib,subprocess,time
s=SPEC
for key in ['pid','exit']:
 p=pathlib.Path(s[key])
 if p.exists():p.unlink()
p=subprocess.Popen(['python3','-c',SUPERVISOR,json.dumps(s)],stdin=subprocess.DEVNULL,
 stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
for _ in range(100):
 if pathlib.Path(s['pid']).exists():break
 time.sleep(.02)
print(json.dumps({**s,'pid':int(pathlib.Path(s['pid']).read_text()),'supervisor_pid':p.pid}))
'''.replace('SPEC', repr(spec)).replace('SUPERVISOR', repr(supervisor)))
    return result


def process_info(remote, proc):
    return remote.python('''import collections,json,os,pathlib,re
p=pathlib.Path('/proc')/str(PID)
threads=[]
for t in (p/'task').iterdir():
 try:threads.append(sorted(os.sched_getaffinity(int(t.name))))
 except ProcessLookupError:pass
maps=(p/'maps').read_text();numa=(p/'numa_maps').read_text()
policies=collections.Counter(s.split()[1] for s in numa.splitlines() if len(s.split())>1)
print(json.dumps({'thread_affinities':threads,'numa_policies':dict(policies),
 'libraries':sorted({s.split()[-1] for s in maps.splitlines() if 'libbrpc.so' in s or 'libtcmalloc' in s}),
 'status':(p/'status').read_text()}))
'''.replace('PID', str(proc['pid'])))


def socket_snapshot(remote, port):
    return remote.python('''import json,re,urllib.request
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
base='http://127.0.0.1:PORT'
def get(path):return opener.open(base+path+'?console=1',timeout=3).read().decode()
connections=get('/connections');sockets=[]
for line in connections.splitlines():
 m=re.search(r'(\d+)\s*$',line)
 if not m:continue
 detail=get('/sockets/'+m.group(1))
 if 'state=ESTABLISHED' in detail:
  sockets.append({'id':m.group(1),'detail':detail})
print(json.dumps({'connections':connections,'urma_sockets':sockets}))
'''.replace('PORT', str(port)))


def stop(remote, proc):
    executable = next(s for s in proc['args'] if '/ub_test_' in s)
    return remote.python('''import json,os,pathlib,signal,time
p=pathlib.Path('/proc')/str(PID)
def alive():
 try:return (p/'stat').read_text().split()[2]!='Z' and (p/'cmdline').read_bytes().split(bytes([0]))[0].decode()==EXE
 except (FileNotFoundError,ProcessLookupError):return False
if alive():
 try:os.kill(PID,signal.SIGINT)
 except ProcessLookupError:pass
 for _ in range(50):
  if not alive():break
  time.sleep(.1)
 if alive():
  try:os.kill(PID,signal.SIGKILL)
  except ProcessLookupError:pass
  for _ in range(20):
   if not alive():break
   time.sleep(.05)
print(json.dumps({'stopped':not alive()}))
'''.replace('PID', str(proc['pid'])).replace('EXE', repr(executable)))


def expected_cpus(value):
    cpus = set()
    for part in value.split(','):
        ends = part.split('-')
        cpus.update(range(int(ends[0]), int(ends[-1]) + 1))
    return cpus


def statistics_failures(returncode, line, metrics, totals):
    failures = []
    if returncode != 0:
        failures.append('client exit code')
    if not line or any(v == 'N/A' for v in metrics.values()):
        failures.append('incomplete statistics')
    # Rounded "Error rate: 0%" must not hide a small non-zero error count.
    if totals['Total Errors'] != 0:
        failures.append('RPC errors')
    if not totals['Total Requests']:
        failures.append('no completed RPCs')
    return failures


def run_test(args, remotes, mode, qps, size):
    name = '{}-q{}-s{}'.format(mode, qps, size)
    out = Path(args.output)
    result = {'name': name, 'sge_mode': mode, 'expected_qps': qps, 'size': size,
              'valid': False, 'processes': {}, 'cleanup': {}}
    logs = {}
    try:
        server = launch(remotes['server'], args.server_dir, name, 'server', mode,
                        command(args, 'server', mode, qps, size))
        result['processes']['server'] = server
        time.sleep(3)  # Preserve the original pre-client startup interval.
        server_log = remotes['server'].read(server['log'])
        marker = 'UB_TEST_TRANSPORT sge_mode=' + mode
        if remotes['server'].read(server['exit']).strip():
            raise RuntimeError('Server exited during startup: ' + server_log[-1500:])
        if marker not in server_log:
            raise RuntimeError('Server did not initialize the requested URMA build: ' + server_log[-1500:])
        client = launch(remotes['client'], args.client_dir, name, 'client', mode,
                        command(args, 'client', mode, qps, size))
        result['processes']['client'] = client
        deadline = time.monotonic() + (300 if size >= 1048576 else 120)
        time.sleep(3)
        # Auditing must not shorten a benchmark if a process has exited or a
        # diagnostics request fails. Keep its final statistics and raw logs.
        audit_failures = []
        result['process_info'] = {}
        for role, proc in result['processes'].items():
            try:
                result['process_info'][role] = process_info(remotes[role], proc)
            except Exception as error:
                audit_failures.append(role + ' process audit: ' + str(error))
        try:
            result['sockets'] = socket_snapshot(remotes['server'], args.port)
        except Exception as error:
            result['sockets'] = {'urma_sockets': []}
            audit_failures.append('socket audit: ' + str(error))
        while time.monotonic() < deadline:
            code = remotes['client'].read(client['exit'])
            if code.strip():
                result['client_returncode'] = int(code)
                break
            time.sleep(1)
        else:
            raise TimeoutError('Client exceeded the original matrix timeout')
        logs['client'] = remotes['client'].read(client['log'])
        logs['server'] = remotes['server'].read(server['log'])
        line, metrics, totals = parse_result(logs['client'])
        result.update(result_line=line, metrics=metrics, totals=totals)
        failures = statistics_failures(result['client_returncode'], line, metrics, totals) + audit_failures
        if len(result['sockets']['urma_sockets']) != 1: failures.append('expected one established URMA connection')
        for role, info in result['process_info'].items():
            if marker not in logs[role] or 'tp=CTP' not in logs[role]: failures.append(role + ' transport')
            if not info['thread_affinities'] or any(set(c) != expected_cpus(args.cpus) for c in info['thread_affinities']):
                failures.append(role + ' CPU affinity')
            if set(info['numa_policies']) != {'bind:1'}: failures.append(role + ' memory binding')
            root = getattr(args, role + '_dir')
            if root + '/runtime/' + mode + '/libbrpc.so' not in info['libraries']:
                failures.append(role + ' libbrpc')
            if root + '/runtime/common/libtcmalloc.so.4' not in info['libraries']:
                failures.append(role + ' tcmalloc')
        result['failures'] = failures
        result['valid'] = not failures
    except Exception as error:
        result['exception'] = str(error)
    finally:
        for role in ('client', 'server'):
            proc = result['processes'].get(role)
            if proc:
                try:
                    result['cleanup'][role] = stop(remotes[role], proc)
                    if not result['cleanup'][role]['stopped']:
                        result['valid'] = False
                    logs[role] = remotes[role].read(proc['log'])
                except Exception as error:
                    result['cleanup'][role] = {'stopped': False, 'error': str(error)}
                    result['valid'] = False
        for role, log in logs.items():
            (out / (name + '-' + role + '.log')).write_text(log)
        (out / (name + '.json')).write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: result.get(k) for k in ('name', 'valid', 'metrics', 'failures', 'exception')}), flush=True)
    time.sleep(2)  # Preserve the original inter-case interval.
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--server', required=True, help='SSH user@host of the server/build node')
    parser.add_argument('--client', required=True, help='SSH user@host of the client node')
    parser.add_argument('--key', default='~/.ssh/id_ed25519')
    parser.add_argument('--ssh-multiplexing', choices=['auto', 'off', 'on'], default='auto',
                        help='SSH connection reuse: auto disables it on Windows; off disables it everywhere')
    parser.add_argument('--server-address', help='RPC address; defaults to the host in --server')
    parser.add_argument('--server-dir', required=True, help='Absolute remote directory containing runtime/')
    parser.add_argument('--client-dir', required=True)
    parser.add_argument('--device', required=True)
    parser.add_argument('--port', type=int, default=10918)
    parser.add_argument('--cpus', default='96-111')
    parser.add_argument('--modes', nargs='+', choices=['multi', 'single'], default=['multi', 'single'])
    parser.add_argument('--qps', nargs='+', type=int, choices=QPS_VALUES, default=QPS_VALUES)
    parser.add_argument('--sizes', nargs='+', type=int, choices=SIZES, default=SIZES)
    parser.add_argument('--deploy', type=Path, help='Deploy only the runtime binary archive to both nodes')
    parser.add_argument('--output', default='ub_test_results')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    args.server_address = args.server_address or args.server.rsplit('@', 1)[-1]
    if not all(p.startswith('/') for p in (args.server_dir, args.client_dir)):
        parser.error('Remote runtime directories must be absolute')
    plan = [{'sge_mode': m, 'qps': q, 'size': s,
             'server': command(args, 'server', m, q, s),
             'client': command(args, 'client', m, q, s)}
            for m in args.modes for q in args.qps for s in args.sizes]
    if args.dry_run:
        print(json.dumps(plan, indent=2))
        return 0
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    if (out / 'results.json').exists():
        parser.error('Output already contains results; choose a new output directory')
    (out / 'plan.json').write_text(json.dumps({'upstream_commit': UPSTREAM_COMMIT, 'cases': plan,
        'transport_changes': 'single/multi build selection; event mode; pooled transport defaults; selected device',
        'statistics': 'Unchanged upstream ub_test calculations; CSV retains original columns except SGE mode'}, indent=2) + '\n')
    remotes = {role: Remote(getattr(args, role), args.key, args.ssh_multiplexing)
               for role in ('server', 'client')}
    results = []
    try:
        if args.deploy:
            for role, remote in remotes.items():
                deploy(remote, getattr(args, role + '_dir'), args.deploy)
        audit = {role: preflight(remote, getattr(args, role + '_dir'), args.cpus, args.port, role == 'server')
                 for role, remote in remotes.items()}
        if audit['server']['hashes'] != audit['client']['hashes']:
            raise RuntimeError('The two nodes have different runtime binaries')
        (out / 'preflight.json').write_text(json.dumps(audit, indent=2) + '\n')
        for case in plan:
            results.append(run_test(args, remotes, case['sge_mode'], case['qps'], case['size']))
            (out / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
            with (out / 'ub_test_results.csv').open('w', newline='') as stream:
                writer = csv.writer(stream)
                writer.writerow(CSV_HEADER)
                for r in results:
                    writer.writerow([r['sge_mode'], r['expected_qps'], r['size']] +
                                    [r.get('metrics', {}).get(k, 'ERROR') for k in METRICS])
        print(json.dumps({'completed': len(results), 'valid': sum(r['valid'] for r in results),
                          'output': str(out.resolve())}), flush=True)
        return 0 if all(r['valid'] for r in results) else 1
    finally:
        for remote in remotes.values(): remote.close()


if __name__ == '__main__':
    raise SystemExit(main())
