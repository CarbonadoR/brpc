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

"""Package Bazel ub_test binaries, libbrpc, provenance and replay scripts."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tarfile


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--label', required=True)
    parser.add_argument('--source-head', required=True)
    parser.add_argument('--build-command', required=True)
    parser.add_argument('--bazel', default='bazel', help='Actual Bazel executable used for the build')
    args = parser.parse_args()
    source = args.source.resolve()
    root = args.output.resolve() / args.label
    root.mkdir(parents=True, exist_ok=False)
    for name in ('bin', 'lib', 'provenance'):
        (root / name).mkdir()

    artifacts = {
        'bin/ub_test_client': 'example/ub_test_client',
        'bin/ub_test_server': 'example/ub_test_server',
        'lib/libbrpc.so': 'tools/jfs_validation/libbrpc.so',
    }
    for target, artifact in artifacts.items():
        dest = root / target
        shutil.copy2(source / 'bazel-bin' / artifact, dest)
        dest.chmod(dest.stat().st_mode | 0o200)
        subprocess.run(['strip', '--strip-unneeded', str(dest)], check=True)

    dependencies = {}
    for name in artifacts:
        binary = root / name
        deps = subprocess.check_output(['ldd', str(binary)], text=True)
        if 'not found' in deps:
            raise RuntimeError('Unresolved dependencies for ' + name + '\n' + deps)
        dependencies[name] = deps
        # Retain Bazel-built shared dependencies; system SDK/libc stay external.
        for lib, path in re.findall(r'^\s*(\S+)\s+=>\s+(\S+)', deps, re.MULTILINE):
            if '/.cache/bazel/' in path:
                dest = root / 'lib' / lib
                if not dest.exists():
                    shutil.copy2(path, dest)
                    dest.chmod(dest.stat().st_mode | 0o200)
                    subprocess.run(['strip', '--strip-unneeded', str(dest)], check=True)

    for role in ('server', 'client'):
        script = root / ('run_' + role + '.sh')
        script.write_text(
            '#!/usr/bin/env bash\nset -euo pipefail\n'
            'ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)\n'
            'export LD_LIBRARY_PATH="$ROOT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"\n'
            'exec numactl -C "${CPUS:-288-303}" -m "${NUMA:-3}" '
            '"$ROOT/bin/ub_test_' + role + '" --use_urma=true '
            '--urma_sge_mode=multi --urma_use_polling=false '
            '--urma_device="${UB_DEVICE:-udmac1d1e2}" "$@"\n')
        script.chmod(0o755)

    (root / 'README.md').write_text('''# JFS 性能对比二进制包

本包通过 Bazel opt、真实 liburma、multi SGE、CPU frequency 计时构建。
ARM64/aarch64；运行环境需安装匹配的 UMDK/UDMA、numactl 和 ldd 列出的系统依赖。
默认 NUMA3、CPU 288–303、udmac1d1e2；请先核对目标机器实际拓扑。

服务端（145）：

```bash
./run_server.sh --port=10944 --rsp_size=1024 --num_threads=16
```

客户端（151）：

```bash
./run_client.sh --servers=192.168.25.145:10944 --link_num=1 \\
  --queue_depth=1 --expected_qps=500 --initial_tokens=0 \\
  --req_size=1024 --test_seconds=10 --dummy_port=0 --max_retry=0 \\
  --rpc_timeout_ms=2000 --connect_timeout_ms=6000
```

8K/100K 对比时同时修改 req_size 与 rsp_size 为 8192/102400。
Avg-Latency/P99使用默认最近10秒窗口，QPS/错误覆盖完整正式阶段。
QD 为进程总并发；多链路使用 link_num，并设置 --use_connection_group=true，
避免多个 Channel 共享同一底层连接；测试前确认建立的 URMA 链路数量。
修改硬件资源配置时必须同时记录两端 urma_io_jfs_cnt、urma_io_jfr_cnt、
urma_io_sq_depth、urma_io_rx_depth、urma_buffer_count 和 urma_header_buf_count。

更换绑核/设备：CPUS=96-111 NUMA=1 UB_DEVICE=udmac0d1e2 ./run_server.sh ...

校验：sha256sum -c SHA256SUMS。

ub_test 沿用仓库默认 Bazel link 模型；是否动态依赖 libbrpc 见
provenance/dependencies.json。包同时提供匹配源码构建的 libbrpc.so，
不依靠替换该文件改变已静态链接的客户端/服务端。
JFS 的本地 EID 沿用当前源码自动选择第一个可用条目，须以启动日志确认，
本包未额外增加 eid_idx 配置。
''')
    (root / 'provenance/dependencies.json').write_text(json.dumps(dependencies, indent=2))
    lock = source / 'MODULE.bazel.lock'
    if lock.exists():
        shutil.copy2(lock, root / 'provenance/MODULE.bazel.lock')
    for name in ('BUILD.bazel', 'MODULE.bazel', '.bazelrc', '.bazelversion',
                 'tools/jfs_validation/BUILD.bazel'):
        dest = root / 'provenance/build_inputs' / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / name, dest)
    (root / 'provenance/build_command.txt').write_text(args.build_command + '\n')
    for name in ('run_pair.py', 'compare_bazel.py'):
        helper = source / 'tools/jfs_validation' / name
        if helper.exists():
            (root / 'tools').mkdir(exist_ok=True)
            shutil.copy2(helper, root / 'tools' / name)
    for document in sorted((source / 'docs/cn/urma_validation').glob('jfs_step*.md')):
        (root / 'validation').mkdir(exist_ok=True)
        shutil.copy2(document, root / 'validation' / document.name)
    metadata = {
        'label': args.label,
        'source_head': args.source_head,
        'bazel_version': subprocess.check_output([args.bazel, '--version'], text=True).strip(),
        'arch': subprocess.check_output(['uname', '-m'], text=True).strip(),
        'build_command': args.build_command,
        'link_mode': 'repository default ub_test cc_binary targets; additional cc_shared_library',
        'source_lf_normalized_sha256': {
            str(p.relative_to(source)): digest(p)
            for p in sorted((source / 'src').rglob('*')) if p.is_file()
        },
        'files': {str(p.relative_to(root)): digest(p)
                  for p in sorted(root.rglob('*')) if p.is_file()},
    }
    (root / 'manifest.json').write_text(json.dumps(metadata, indent=2) + '\n')
    (root / 'SHA256SUMS').write_text(''.join(
        digest(p) + '  ' + str(p.relative_to(root)) + '\n'
        for p in sorted(root.rglob('*')) if p.is_file() and p.name != 'SHA256SUMS'))
    archive = args.output.resolve() / (args.label + '.tar.gz')
    with tarfile.open(archive, 'w:gz') as out:
        out.add(root, arcname=args.label)
    print(json.dumps({'archive': str(archive), 'bytes': archive.stat().st_size,
                      'sha256': digest(archive)}, indent=2))


if __name__ == '__main__':
    main()
