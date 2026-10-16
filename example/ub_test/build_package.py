#!/usr/bin/env python3
"""Build the two pooled URMA SGE variants and a relocatable binary bundle.

Requires Linux, CMake, a C++ compiler, Protobuf, UMDK and gperftools.
The transport sources are used unchanged. Run on the destination architecture.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(args):
    print('+ ' + ' '.join(map(str, args)), flush=True)
    subprocess.run(list(map(str, args)), check=True)


def cache_value(build, name):
    for line in (build / 'CMakeCache.txt').read_text().splitlines():
        if line.startswith(name + ':'):
            return line.split('=', 1)[1]
    raise RuntimeError('Missing CMake cache entry: ' + name)


def install_file(source, destination, strip=False):
    # Replacing the inode avoids truncating an executable or mapped library
    # when rebuilding a package in an existing work directory.
    fd, name = tempfile.mkstemp(prefix='.' + destination.name, dir=destination.parent)
    os.close(fd)
    temporary = Path(name)
    try:
        shutil.copy2(source, temporary)
        if strip:
            run(['strip', '--strip-unneeded', temporary])
        temporary.replace(destination)
    finally:
        if temporary.exists():
            temporary.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path,
                        default=Path(__file__).resolve().parents[2])
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--prefix', default='')
    parser.add_argument('--jobs', type=int, default=24)
    parser.add_argument('--urma-include', default='/usr/include/ub/umdk/urma')
    parser.add_argument('--urma-bond-include', default='/usr/local/include/ub/umdk/urma')
    parser.add_argument('--urma-library', default='/usr/lib64/liburma.so')
    args = parser.parse_args()
    source = args.source_root.resolve()
    work = args.work_dir.resolve()
    runtime = work / 'runtime'
    (runtime / 'common').mkdir(parents=True, exist_ok=True)
    for mode in ('multi', 'single'):
        build = work / ('build-' + mode)
        run(['cmake', '-S', source, '-B', build,
             '-DCMAKE_BUILD_TYPE=Release', '-DWITH_DEBUG_SYMBOLS=OFF',
             '-DWITH_URMA=ON', '-DURMA_USE_MOCK=OFF', '-DWITH_TCMALLOC=ON',
             '-DWITH_CPU_FREQUENCY=ON', '-DBUILD_UNIT_TESTS=OFF',
             '-DBUILD_BRPC_TOOLS=OFF', '-DBUILD_SHARED_LIBS=ON',
             '-DURMA_SINGLE_SGE=' + ('ON' if mode == 'single' else 'OFF'),
             '-DCMAKE_PREFIX_PATH=' + args.prefix,
             '-DURMA_INCLUDE_PATH=' + args.urma_include,
             '-DURMA_BOND_INCLUDE_PATH=' + args.urma_bond_include,
             '-DURMA_LIB=' + args.urma_library,
             '-DCMAKE_BUILD_WITH_INSTALL_RPATH=ON',
             '-DCMAKE_INSTALL_RPATH=$ORIGIN/../common'])
        run(['cmake', '--build', build, '--target', 'brpc-shared', '-j', args.jobs])
        target = runtime / mode
        target.mkdir(exist_ok=True)
        install_file(build / 'output/lib/libbrpc.so', target / 'libbrpc.so', strip=True)
    tcmalloc = Path(cache_value(work / 'build-multi', 'TCMALLOC_LIB'))
    install_file(tcmalloc.resolve(), runtime / 'common/libtcmalloc.so.4')
    example = work / 'build-example'
    run(['cmake', '-S', source / 'example/ub_test', '-B', example,
         '-DCMAKE_BUILD_TYPE=Release', '-DLINK_SO=ON', '-DWITH_CPU_FREQUENCY=ON',
         '-DCMAKE_PREFIX_PATH=' + args.prefix,
         '-DBRPC_LIB=' + str(work / 'build-multi/output/lib/libbrpc.so'),
         '-DBRPC_INCLUDE_PATH=' + str(work / 'build-multi/output/include'),
         '-DTCMALLOC_LIB=' + str(tcmalloc),
         '-DTCMALLOC_INCLUDE_PATH=' + cache_value(work / 'build-multi', 'TCMALLOC_INCLUDE_PATH'),
         '-DURMA_INCLUDE_PATH=' + args.urma_include,
         '-DURMA_LIB=' + args.urma_library])
    run(['cmake', '--build', example, '-j', min(args.jobs, 8)])
    for mode in ('multi', 'single'):
        for role in ('client', 'server'):
            dest = runtime / mode / ('ub_test_' + role)
            install_file(example / dest.name, dest, strip=True)
    dependencies = {}
    for mode in ('multi', 'single'):
        env = dict(os.environ, LD_LIBRARY_PATH=str(runtime / mode) + ':' + str(runtime / 'common'))
        for role in ('client', 'server'):
            name = mode + '/ub_test_' + role
            dep = subprocess.check_output(['ldd', str(runtime / name)], env=env, text=True)
            if 'not found' in dep:
                raise RuntimeError(name + ': unresolved runtime dependency\n' + dep)
            dependencies[name] = dep
    (work / 'dependencies.json').write_text(json.dumps(dependencies, indent=2) + '\n')
    manifest = {
        'upstream_repository': 'dwh110/brpc',
        'upstream_commit': 'b4dff13b0de19a12dc3a1b82902a20d4b51b1bb1',
        'architecture': subprocess.check_output(['uname', '-m'], text=True).strip(),
        'with_cpu_frequency': True, 'with_tcmalloc': True, 'urma_mock': False,
        'mode_selection': 'Separate libbrpc builds, URMA_SINGLE_SGE=OFF/ON',
        'files': {str(p.relative_to(runtime)): sha256(p)
                  for p in sorted(runtime.rglob('*'))
                  if p.is_file() and p != runtime / 'manifest.json'},
        'source_sha256': {str(p.relative_to(source)): sha256(p)
                          for folder in ('src/brpc/urma', 'example/ub_test')
                          for p in sorted((source / folder).rglob('*'))
                          if p.is_file() and '__pycache__' not in p.parts},
    }
    (runtime / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    with tarfile.open(work / 'ub-test-runtime-aarch64.tar.gz', 'w:gz') as archive:
        archive.add(runtime, arcname='runtime')
    print('BINARY_PACKAGE_READY ' + str(work / 'ub-test-runtime-aarch64.tar.gz'), flush=True)


if __name__ == '__main__':
    main()
