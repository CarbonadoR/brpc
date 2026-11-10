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

"""Alternate frozen baseline/new binaries and retain three-round comparisons."""
import argparse
import json
from pathlib import Path
import re
import statistics
import subprocess
import sys

PROFILES = {
    'low': dict(sizes=[1024, 8192, 102400], links=1, qd=1, qps=500, seconds=10),
    'common': dict(sizes=[1024, 8192, 102400], links=32, qd=10, qps=500, seconds=20),
    'contention': dict(sizes=[1024, 8192], links=32, qd=128, qps=0, seconds=20),
    'sparse': dict(sizes=[1024, 8192], links=1024, qd=64, qps=500, seconds=20),
}


def metrics(case):
    line = case['summary']
    result = {}
    for name, pattern in {
        'mean_us': r'Avg-Latency: ([0-9.]+)',
        'p50_us': r'50th-Latency: ([0-9.]+)',
        'p99_us': r'99th-Latency: ([0-9.]+)',
        'qps': r'QPS: ([0-9.]+)',
        'errors': r'Total Errors: ([0-9.]+)',
    }.items():
        result[name] = float(re.search(pattern, line).group(1))
    result['client_cpu'] = case.get('client_cpu_percent')
    result['server_cpu'] = case.get('server_cpu_percent')
    return result


def write_summary(root):
    all_cases = []
    groups = {}
    for path in sorted(root.glob('r*/*/results.json')):
        profile, version = path.parent.name.rsplit('_', 1)
        if profile not in PROFILES or version not in ['baseline', 'new']:
            continue
        cases = json.loads(path.read_text())
        if len(cases) != len(PROFILES[profile]['sizes']) or any(c['returncode'] for c in cases):
            continue
        for case in cases:
            item = dict(round=path.parent.parent.name, profile=profile,
                        version=version, case=case['name'], **metrics(case))
            all_cases.append(item)
            groups.setdefault((profile, case['name'], version), []).append(item)
    (root / 'measurements.json').write_text(json.dumps(all_cases, indent=2) + '\n')
    text = ['# JFS 基线/新版本交替对照', '',
            '纳入轮次：{}；完整场景结果{}条。异常/未完成组不纳入。'.format(
                ', '.join(sorted({r['round'] for r in all_cases})), len(all_cases)), '',
            '单位µs；每格为多轮算术平均。P99列是各轮P99的平均，不是合并样本P99。',
            '时延为ub_test结束时最近10秒窗口；QPS/错误为正式压测全阶段。',
            'CPU按单核100%计，窗口含初始化/预热；原始日志保留每轮结果。', '',
            '| 场景 | 基线均值 | 新版均值 | 差值(新版-基线) | 基线P99 | 新版P99 | 基线/新版QPS |',
            '|---|---:|---:|---:|---:|---:|---:|']
    for profile, case in sorted({(p, c) for p, c, v in groups}):
        values = []
        for version in ['baseline', 'new']:
            rows = groups.get((profile, case, version), [])
            values.append({key: statistics.mean(r[key] for r in rows)
                           for key in ['mean_us', 'p99_us', 'qps']} if rows else None)
        a, b = values
        if a and b:
            text.append('| {} / {} | {:.3f} | {:.3f} | {:+.3f} | {:.3f} | {:.3f} | {:.1f} / {:.1f} |'.format(
                profile, case, a['mean_us'], b['mean_us'], b['mean_us']-a['mean_us'],
                a['p99_us'], b['p99_us'], a['qps'], b['qps']))
    text += ['', '| 场景 | 基线客户端CPU% | 新版客户端CPU% | 基线服务端CPU% | 新版服务端CPU% |',
             '|---|---:|---:|---:|---:|']
    for profile, case in sorted({(p, c) for p, c, v in groups}):
        rows = [groups.get((profile, case, version), []) for version in ['baseline', 'new']]
        if all(rows):
            values = []
            for key in ['client_cpu', 'server_cpu']:
                for samples in rows:
                    valid = [r[key] for r in samples if r[key] is not None]
                    values.append(statistics.mean(valid) if valid else float('nan'))
            text.append('| {} / {} | {:.2f} | {:.2f} | {:.2f} | {:.2f} |'.format(profile, case, *values))
    (root / '对比汇总.md').write_text('\n'.join(text) + '\n')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--baseline-bin', required=True, help='Frozen server-side binary directory')
    p.add_argument('--new-bin', required=True, help='Frozen server-side binary directory')
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--remote-root', required=True)
    p.add_argument('--rounds', type=int, default=3)
    p.add_argument('--port', type=int, default=10945)
    p.add_argument('--resume', action='store_true', help='Skip complete groups; use a fresh remote-root for retry')
    p.add_argument('--profiles', nargs='+', choices=PROFILES, default=list(PROFILES))
    p.add_argument('--server', default='h00665128@192.168.25.145')
    p.add_argument('--client', default='h00665128@192.168.25.151')
    p.add_argument('--key', default='/mnt/c/Users/hujia/.ssh/id_ed25519')
    p.add_argument('--cpus', default='288-303')
    p.add_argument('--numa', default='3')
    p.add_argument('--device', default='udmac1d1e2')
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=args.resume)
    settings_name = 'settings.json'
    if args.resume:
        index = 1
        while (args.output/('resume_settings_{}.json'.format(index))).exists(): index += 1
        settings_name = 'resume_settings_{}.json'.format(index)
    (args.output/settings_name).write_text(json.dumps(vars(args), default=str, indent=2)+'\n')
    runner = Path(__file__).with_name('run_pair.py')
    for round_index in range(1, args.rounds + 1):
        for profile in args.profiles:
            config = PROFILES[profile]
            order = ['baseline', 'new'] if round_index % 2 else ['new', 'baseline']
            for version in order:
                group = 'r{}/{}_{}'.format(round_index, profile, version)
                output = args.output / group
                if args.resume and output.exists():
                    result_file = output/'results.json'
                    if result_file.exists():
                        cases = json.loads(result_file.read_text())
                        if len(cases) == len(config['sizes']) and all(c['returncode'] == 0 for c in cases):
                            print('SKIP '+group, flush=True)
                            continue
                    attempt = 1
                    incomplete = output.with_name(output.name+'_incomplete_{}'.format(attempt))
                    while incomplete.exists():
                        attempt += 1
                        incomplete = output.with_name(output.name+'_incomplete_{}'.format(attempt))
                    output.rename(incomplete)
                    log = output.parent/(output.name+'_runner.log')
                    if log.exists():
                        log.rename(output.parent/(incomplete.name+'_runner.log'))
                command = [sys.executable, str(runner), '--binary-dir',
                           args.baseline_bin if version == 'baseline' else args.new_bin,
                           '--remote-root', args.remote_root + '/' + group,
                           '--output', str(output), '--iterations', '0',
                           '--server', args.server, '--client', args.client, '--key', args.key,
                           '--cpus', args.cpus, '--numa', args.numa, '--device', args.device,
                           '--port', str(args.port),
                           '--sizes'] + [str(x) for x in config['sizes']]
                for key in ['links', 'qd', 'qps', 'seconds']:
                    command += ['--'+key, str(config[key])]
                print('START '+group, flush=True)
                output.parent.mkdir(parents=True, exist_ok=True)
                with (output.parent/(output.name+'_runner.log')).open('w') as log:
                    result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
                write_summary(args.output)
                if result.returncode:
                    raise RuntimeError('Failed '+group+'; inspect runner log')
                for case in json.loads((output/'results.json').read_text()):
                    print(group+' '+case['name']+' '+json.dumps(metrics(case)), flush=True)


if __name__ == '__main__':
    main()
