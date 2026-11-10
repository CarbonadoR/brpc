#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run isolated two-node URMA cases and retain exact commands and logs."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--server', default='h00665128@192.168.25.145')
    p.add_argument('--client', default='h00665128@192.168.25.151')
    p.add_argument('--key', default='/mnt/c/Users/hujia/.ssh/id_ed25519')
    p.add_argument('--binary-dir', required=True, help='Server-side directory containing both binaries')
    p.add_argument('--remote-root', required=True, help='Unique run directory on both nodes')
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--sizes', type=int, nargs='+', default=[1024, 8192, 102400])
    p.add_argument('--links', type=int, nargs='+', default=[1, 32])
    p.add_argument('--qd', type=int, default=10)
    p.add_argument('--qps', type=int, default=500)
    p.add_argument('--seconds', type=int, default=0)
    p.add_argument('--iterations', type=int, default=50)
    p.add_argument('--port', type=int, default=10945)
    p.add_argument('--cpus', default='288-303')
    p.add_argument('--numa', default='3')
    p.add_argument('--device', default='udmac1d1e2')
    p.add_argument('--jfs-count', type=int)
    p.add_argument('--jfr-count', type=int)
    p.add_argument('--sq-depth', type=int)
    p.add_argument('--rx-depth', type=int)
    p.add_argument('--log-chains', action='store_true')
    p.add_argument('--connection-type', default='single', choices=['single', 'pooled', 'short'])
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    def ssh(host, command):
        return ['ssh', '-i', args.key, '-o', 'IdentitiesOnly=yes', '-o',
                'BatchMode=yes', '-o', 'LogLevel=ERROR', '-o', 'ConnectTimeout=10',
                host, shlex.join(command)]

    def remote(host, code, timeout=120):
        result = subprocess.run(ssh(host, ['python3', '-c', code]),
                                capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError('Remote command failed on '+host+': '+result.stderr)
        return json.loads(result.stdout)

    root = args.remote_root
    # Copy into a unique directory; builds may replace their outputs later.
    remote(args.server, 'from pathlib import Path\nimport shutil,json\n'
           'r=Path(' + repr(root) + '); (r/"bin").mkdir(parents=True,exist_ok=False)\n'
           's=Path(' + repr(args.binary_dir) + ')\n'
           'for role in ("client","server"): shutil.copy2(s/("ub_test_"+role),r/"bin"/("ub_test_"+role))\n'
           'print(json.dumps(True))')
    binary = subprocess.run(ssh(args.server, ['cat', root + '/bin/ub_test_client']),
                            capture_output=True, check=True).stdout
    upload = ('from pathlib import Path\nimport sys\n'
              'p=Path(' + repr(root + '/bin/ub_test_client') + ')\n'
              'p.parent.mkdir(parents=True,exist_ok=False)\np.write_bytes(sys.stdin.buffer.read())\np.chmod(0o755)')
    subprocess.run(ssh(args.client, ['python3', '-c', upload]),
                   input=binary, capture_output=True, check=True, timeout=120)

    common = ['--use_urma=true', '--urma_sge_mode=multi', '--urma_use_polling=false',
              '--urma_device=' + args.device]
    for key, value in [('urma_io_jfs_cnt', args.jfs_count),
                       ('urma_io_jfr_cnt', args.jfr_count),
                       ('urma_io_sq_depth', args.sq_depth),
                       ('urma_io_rx_depth', args.rx_depth)]:
        if value is not None:
            common.append('--{}={}'.format(key, value))
    if args.log_chains:
        common.append('--vmodule=urma_resource_pool=1')
    binding = ['numactl', '-C', args.cpus, '-m', args.numa]
    summary = []
    for size in args.sizes:
        for links in args.links:
            name = 's{}_links{}_qd{}'.format(size, links, args.qd)
            server_cmd = binding + [root + '/bin/ub_test_server'] + common + [
                '--port=' + str(args.port), '--rsp_size=' + str(size), '--num_threads=16']
            address = args.server.rsplit('@', 1)[-1]
            client_cmd = binding + [root + '/bin/ub_test_client'] + common + [
                '--servers=' + address + ':' + str(args.port), '--link_num=' + str(links),
                '--use_connection_group=true', '--connection_type=' + args.connection_type,
                '--queue_depth=' + str(args.qd),
                '--expected_qps=' + str(args.qps), '--initial_tokens=0', '--req_size=' + str(size),
                '--test_seconds=' + str(args.seconds), '--test_iterations=' + str(args.iterations),
                '--dummy_port=0', '--max_retry=0', '--rpc_timeout_ms=2000',
                '--connect_timeout_ms=6000', '--max_random_delay_us=0']
            server_log = root + '/' + name + '_server.log'
            start = ('import subprocess,socket,time,json,os\nfrom pathlib import Path\n'
                     'port=' + str(args.port) + '\n'
                     's=socket.socket(); s.bind(("",port)); s.close()\n'
                     'f=open(' + repr(server_log) + ',"w")\n'
                     'p=subprocess.Popen(' + repr(server_cmd) + ',stdout=f,stderr=subprocess.STDOUT,'
                     'stdin=subprocess.DEVNULL,start_new_session=True)\n'
                     'for i in range(600):\n'
                     ' if p.poll() is not None: raise RuntimeError("server exited: "+str(p.returncode))\n'
                     ' listeners=subprocess.check_output(["ss","-H","-ltn"],text=True)\n'
                     ' if any(x.split()[3].rsplit(":",1)[-1]==str(port) for x in listeners.splitlines()): break\n'
                     ' time.sleep(0.1)\n'
                     'else:\n p.terminate(); raise RuntimeError("server readiness timeout")\n'
                     'stat=Path("/proc/"+str(p.pid)+"/stat").read_text().rsplit(")",1)[1].split()\n'
                     'print(json.dumps({"pid":p.pid,"ticks":int(stat[11])+int(stat[12]),"time":time.monotonic()}))')
            server_start = remote(args.server, start)
            pid = server_start['pid']
            try:
                client_log = root + '/' + name + '_client.log'
                result = remote(args.client, 'import subprocess,json,time,resource,os,signal\n'
                                'begin=time.monotonic()\n'
                                'log_path=' + repr(client_log) + '\n'
                                'with open(log_path,"w") as f:\n'
                                ' p=subprocess.Popen(' + repr(client_cmd) + ',stdout=f,'
                                'stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL,start_new_session=True)\n'
                                ' try:\n  rc=p.wait(timeout=' + str(args.seconds + 90) + ')\n'
                                ' except subprocess.TimeoutExpired:\n'
                                '  os.killpg(p.pid,signal.SIGTERM); rc=124\n'
                                'elapsed=time.monotonic()-begin\n'
                                'usage=resource.getrusage(resource.RUSAGE_CHILDREN)\n'
                                'cpu=100*(usage.ru_utime+usage.ru_stime)/elapsed if rc != 124 else None\n'
                                'with open(log_path) as f: log=f.read()\n'
                                'print(json.dumps({"returncode":rc,"log":log,"cpu_percent":cpu,"pid":p.pid}))',
                                timeout=args.seconds + 110)
            finally:
                server_result = remote(args.server, 'import os,signal,time,json\nfrom pathlib import Path\n'
                                     'pid=' + str(pid) + '\n'
                                     'start=' + repr(server_start) + '\n'
                                     'cpu=None\n'
                                     'try:\n'
                                     ' stat=Path("/proc/"+str(pid)+"/stat").read_text().rsplit(")",1)[1].split()\n'
                                     ' cpu=100*(int(stat[11])+int(stat[12])-start["ticks"])/os.sysconf("SC_CLK_TCK")/(time.monotonic()-start["time"])\n'
                                     ' cmd=Path("/proc/"+str(pid)+"/cmdline").read_bytes()\n'
                                     ' if ' + repr((root + '/bin/ub_test_server').encode()) + ' in cmd: '
                                     'os.killpg(pid,signal.SIGTERM)\n'
                                     'except ProcessLookupError: pass\n'
                                     'except FileNotFoundError: pass\n'
                                     'time.sleep(1)\n'
                                     'print(json.dumps({"cpu_percent":cpu,"log":Path(' + repr(server_log) + ').read_text()}))')
                (args.output / (name + '_server.log')).write_text(server_result['log'])
            (args.output / (name + '_client.log')).write_text(result['log'])
            line = next((x for x in result['log'].splitlines() if 'Avg-Latency:' in x), '')
            case = {'name': name, 'server_command': server_cmd, 'client_command': client_cmd,
                    'returncode': result['returncode'], 'client_pid': result['pid'], 'summary': line,
                    'client_cpu_percent': result['cpu_percent'],
                    'server_cpu_percent': server_result['cpu_percent']}
            summary.append(case)
            (args.output / 'results.json').write_text(json.dumps(summary, indent=2) + '\n')
            print(json.dumps(case, ensure_ascii=False), flush=True)
            if result['returncode'] or not line or 'Total Errors: 0,' not in line:
                raise RuntimeError('Case failed: ' + name)


if __name__ == '__main__':
    main()
