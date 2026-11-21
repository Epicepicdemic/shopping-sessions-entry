#!/usr/bin/env python3
"""Compile using Rust in an existing Docker container, link using host Xcode.
The container must mount this repository and the output directory at their host
paths. No Rust installation on macOS is needed. Build artifacts stay outside git.
"""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--container', default='shopping-worker-arm64')
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--scalar', action='store_true')
args = parser.parse_args()
output = args.output.resolve()
output.parent.mkdir(parents=True, exist_ok=True)
source = Path(__file__).resolve().parents[1] / 'src/main.rs'
subprocess.run(['docker','exec',args.container,'rustup','target','add','aarch64-apple-darwin'], check=True)
with tempfile.TemporaryDirectory(prefix='mac-link-',dir=output.parent) as temp:
    folder = Path(temp)
    linker = folder/'capture.py'
    linker.write_text('''#!/usr/bin/python3
import json, pathlib, shutil, sys
folder=pathlib.Path(__file__).parent
arguments=[]
for index,arg in enumerate(sys.argv[1:]):
    path=pathlib.Path(arg)
    if path.is_file() and (index==0 or sys.argv[index]!='-o'):
        copy=folder/f'{index}-{path.name}'
        shutil.copyfile(path,copy)
        arg=str(copy)
    arguments.append(arg)
(folder/'args.json').write_text(json.dumps(arguments))
''')
    linker.chmod(0o755)
    rust = ['docker','exec',args.container,'rustc','--target','aarch64-apple-darwin',
        '--edition=2024','-C','opt-level=3','-C','lto=fat','-C','codegen-units=1',
        '-C','panic=abort','-C',f'linker={linker}']
    if args.scalar:
        rust += ['--cfg','feature="scalar"']
    subprocess.run([*rust,str(source),'-o',str(output)],check=True)
    subprocess.run(['xcrun','clang',*json.loads((folder/'args.json').read_text())],check=True)
print(output)
