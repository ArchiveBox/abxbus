#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_DIR}"

uv run --no-project python - "${1:-}" <<'PY'
from pathlib import Path
import json
import re
import sys

paths = {
    'pyproject.toml': Path('pyproject.toml'),
    'package.json': Path('abxbus-ts/package.json'),
    'Cargo.toml': Path('abxbus-rust/Cargo.toml'),
    'Cargo.lock': Path('abxbus-rust/Cargo.lock'),
    'version.go': Path('abxbus-go/version.go'),
}
texts = {name: path.read_text() for name, path in paths.items()}
matches = {
    'pyproject.toml': re.search(r'^version = "([^"]+)"$', texts['pyproject.toml'], re.MULTILINE),
    'Cargo.toml': re.search(r'^version = "([^"]+)"$', texts['Cargo.toml'], re.MULTILINE),
    'Cargo.lock': re.search(r'(?m)^name = "abxbus"\nversion = "([^"]+)"$', texts['Cargo.lock']),
    'version.go': re.search(r'const Version = "([^"]+)"', texts['version.go']),
}
versions = {name: match.group(1) if match else None for name, match in matches.items()}
package_json = json.loads(texts['package.json'])
versions['package.json'] = package_json.get('version')
if None in versions.values() or len(set(versions.values())) != 1:
    raise SystemExit(f'Package versions disagree or are missing: {versions}')

current = next(iter(versions.values()))
requested = sys.argv[1]
pattern = re.compile(r'\d+\.\d+\.\d+(?:rc\d+)?')
def parse(value: str) -> tuple[int, int, int, int]:
    major, minor, tail = value.split('.')
    patch, _, rc = tail.partition('rc')
    return (int(major), int(minor), int(patch), int(rc) if rc else 10_000)

if not pattern.fullmatch(current):
    raise SystemExit(f'Unsupported current version: {current}')
if requested:
    if not pattern.fullmatch(requested):
        raise SystemExit(f'Unsupported requested version: {requested}')
    version = requested
else:
    major, minor, patch = map(int, current.split('rc', 1)[0].split('.'))
    version = f'{major}.{minor}.{patch + 1}'
if version == current:
    raise SystemExit(f'Version is already {current}')
if parse(version) <= parse(current):
    raise SystemExit(f'New version {version} must be greater than {current}')

paths['pyproject.toml'].write_text(re.sub(r'^version = "[^"]+"$', f'version = "{version}"', texts['pyproject.toml'], count=1, flags=re.MULTILINE))
package_json['version'] = version
paths['package.json'].write_text(json.dumps(package_json, indent=2) + '\n')
paths['Cargo.toml'].write_text(re.sub(r'^version = "[^"]+"$', f'version = "{version}"', texts['Cargo.toml'], count=1, flags=re.MULTILINE))
paths['Cargo.lock'].write_text(re.sub(r'(?m)^(name = "abxbus"\nversion = ")[^"]+("$)', rf'\g<1>{version}\2', texts['Cargo.lock'], count=1))
paths['version.go'].write_text(re.sub(r'const Version = "[^"]+"', f'const Version = "{version}"', texts['version.go'], count=1))
print(version)
PY
