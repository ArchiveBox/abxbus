#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_DIR}"

TAG_PREFIX=""
PYPI_PACKAGE="abxbus"
NPM_PACKAGE="abxbus"

source_optional_env() {
    if [[ -f "${REPO_DIR}/.env" ]]; then
        set -a
        # shellcheck disable=SC1091
        source "${REPO_DIR}/.env"
        set +a
    fi
}

repo_slug() {
    python3 - <<'PY'
import re
import subprocess

remote = subprocess.check_output(
    ['git', 'remote', 'get-url', 'origin'],
    text=True,
).strip()

patterns = [
    r'github\.com[:/](?P<slug>[^/]+/[^/.]+)(?:\.git)?$',
    r'github\.com/(?P<slug>[^/]+/[^/.]+)(?:\.git)?$',
]

for pattern in patterns:
    match = re.search(pattern, remote)
    if match:
        print(match.group('slug'))
        raise SystemExit(0)

raise SystemExit(f'Unable to parse GitHub repo slug from remote: {remote}')
PY
}

default_branch() {
    if [[ -n "${DEFAULT_BRANCH:-}" ]]; then
        echo "${DEFAULT_BRANCH}"
        return 0
    fi
    if git symbolic-ref refs/remotes/origin/HEAD >/dev/null 2>&1; then
        git symbolic-ref refs/remotes/origin/HEAD | sed 's#^refs/remotes/origin/##'
        return 0
    fi
    git remote show origin | sed -n '/HEAD branch/s/.*: //p' | head -n 1
}

current_version() {
    python3 - <<'PY'
from pathlib import Path
import json
import re

versions = []
pyproject_text = Path('pyproject.toml').read_text()
pyproject_match = re.search(r'^version = "([^"]+)"$', pyproject_text, re.MULTILINE)
if pyproject_match:
    versions.append(pyproject_match.group(1))

package_json = json.loads(Path('abxbus-ts/package.json').read_text())
if 'version' in package_json:
    versions.append(package_json['version'])

cargo_text = Path('abxbus-rust/Cargo.toml').read_text()
cargo_match = re.search(r'^version = "([^"]+)"$', cargo_text, re.MULTILINE)
if cargo_match:
    versions.append(cargo_match.group(1))

go_version_text = Path('abxbus-go/version.go').read_text()
go_version_match = re.search(r'const Version = "([^"]+)"', go_version_text)
if go_version_match:
    versions.append(go_version_match.group(1))

def parse(version: str) -> tuple[int, int, int, int]:
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:rc(\d+))?', version)
    if not match:
        raise SystemExit(f'Unsupported version format: {version}')
    major, minor, patch, rc = match.groups()
    return (int(major), int(minor), int(patch), int(rc) if rc is not None else 10_000)

print(max(versions, key=parse))
PY
}

bump_version() {
    python3 - <<'PY'
from pathlib import Path
import json
import re

def parse(version: str) -> tuple[int, int, int, int]:
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:rc(\d+))?', version)
    if not match:
        raise SystemExit(f'Unsupported version format: {version}')
    major, minor, patch, rc = match.groups()
    return (int(major), int(minor), int(patch), int(rc) if rc is not None else 10_000)

pyproject_path = Path('pyproject.toml')
pyproject_text = pyproject_path.read_text()
pyproject_match = re.search(r'^version = "([^"]+)"$', pyproject_text, re.MULTILINE)
if not pyproject_match:
    raise SystemExit('Failed to find version in pyproject.toml')

package_path = Path('abxbus-ts/package.json')
package_json = json.loads(package_path.read_text())
if 'version' not in package_json:
    raise SystemExit('Failed to find version in abxbus-ts/package.json')

cargo_path = Path('abxbus-rust/Cargo.toml')
cargo_text = cargo_path.read_text()
cargo_match = re.search(r'^version = "([^"]+)"$', cargo_text, re.MULTILINE)
if not cargo_match:
    raise SystemExit('Failed to find version in abxbus-rust/Cargo.toml')

cargo_lock_path = Path('abxbus-rust/Cargo.lock')
cargo_lock_text = cargo_lock_path.read_text()
cargo_lock_match = re.search(r'(?m)^name = "abxbus-rust"\nversion = "([^"]+)"$', cargo_lock_text)
if not cargo_lock_match:
    raise SystemExit('Failed to find abxbus-rust version in abxbus-rust/Cargo.lock')

go_version_path = Path('abxbus-go/version.go')
go_version_text = go_version_path.read_text()
go_version_match = re.search(r'const Version = "([^"]+)"', go_version_text)
if not go_version_match:
    raise SystemExit('Failed to find version in abxbus-go/version.go')

current_version = max([
    pyproject_match.group(1),
    package_json['version'],
    cargo_match.group(1),
    cargo_lock_match.group(1),
    go_version_match.group(1),
], key=parse)
major, minor, patch, _ = parse(current_version)
next_version = f'{major}.{minor}.{patch + 1}'

pyproject_path.write_text(
    re.sub(r'^version = "[^"]+"$', f'version = "{next_version}"', pyproject_text, count=1, flags=re.MULTILINE)
)
package_json['version'] = next_version
package_path.write_text(json.dumps(package_json, indent=2) + '\n')
cargo_path.write_text(
    re.sub(r'^version = "[^"]+"$', f'version = "{next_version}"', cargo_text, count=1, flags=re.MULTILINE)
)
cargo_lock_path.write_text(
    re.sub(
        r'(?m)^(name = "abxbus-rust"\nversion = ")[^"]+(")$',
        rf'\g<1>{next_version}\2',
        cargo_lock_text,
        count=1,
    )
)
go_version_path.write_text(
    re.sub(r'const Version = "[^"]+"', f'const Version = "{next_version}"', go_version_text, count=1)
)
print(next_version)
PY
}

compare_versions() {
    python3 - "$1" "$2" <<'PY'
import re
import sys

def parse(version: str) -> tuple[int, int, int, int]:
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:rc(\d+))?', version)
    if not match:
        raise SystemExit(f'Unsupported version format: {version}')
    major, minor, patch, rc = match.groups()
    return (int(major), int(minor), int(patch), int(rc) if rc is not None else 10_000)

left, right = sys.argv[1], sys.argv[2]
if parse(left) > parse(right):
    print('gt')
elif parse(left) == parse(right):
    print('eq')
else:
    print('lt')
PY
}

latest_release_version() {
    local slug="$1"
    local raw_tags
    raw_tags="$(gh api "repos/${slug}/releases?per_page=100" --jq '.[].tag_name' || true)"
    RELEASE_TAGS="${raw_tags}" python3 - <<'PY'
import os
import re

def parse(version: str) -> tuple[int, int, int, int]:
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:rc(\d+))?', version)
    if not match:
        return (-1, -1, -1, -1)
    major, minor, patch, rc = match.groups()
    return (int(major), int(minor), int(patch), int(rc) if rc is not None else 10_000)

versions = [line.strip() for line in os.environ.get('RELEASE_TAGS', '').splitlines() if line.strip()]
if not versions:
    print('')
else:
    print(max(versions, key=parse))
PY
}

wait_for_runs() {
    local slug="$1"
    local event="$2"
    local sha="$3"
    local label="$4"
    local run_count
    local run_ids
    local attempts=0

    while :; do
        run_count="$(GH_FORCE_TTY=0 GH_PROMPT_DISABLED=1 GH_PAGER=cat gh run list --repo "${slug}" --event "${event}" --commit "${sha}" --limit 20 --json databaseId,status,conclusion,workflowName --jq 'length')"
        if [[ "${run_count}" -gt 0 ]]; then
            break
        fi
        attempts=$((attempts + 1))
        if [[ "${attempts}" -ge 30 ]]; then
            echo "Timed out waiting for ${label} workflows to start" >&2
            return 1
        fi
        sleep 10
    done

    run_ids="$(GH_FORCE_TTY=0 GH_PROMPT_DISABLED=1 GH_PAGER=cat gh run list --repo "${slug}" --event "${event}" --commit "${sha}" --limit 20 --json databaseId,status,conclusion,workflowName --jq '.[].databaseId')"
    while read -r run_id; do
        gh run watch "${run_id}" --repo "${slug}" --exit-status
    done <<<"${run_ids}"
}

wait_for_pypi() {
    local package_name="$1"
    local expected_version="$2"
    local attempts=0
    local published_version

    while :; do
        published_version="$(curl -fsSL "https://pypi.org/pypi/${package_name}/json" | jq -r '.info.version')"
        if [[ "${published_version}" == "${expected_version}" ]]; then
            return 0
        fi
        attempts=$((attempts + 1))
        if [[ "${attempts}" -ge 30 ]]; then
            echo "Timed out waiting for ${package_name}==${expected_version} on PyPI" >&2
            return 1
        fi
        sleep 10
    done
}

wait_for_npm() {
    local package_name="$1"
    local expected_version="$2"
    local attempts=0
    local published_version

    while :; do
        published_version="$(npm view "${package_name}" version --silent 2>/dev/null || true)"
        if [[ "${published_version}" == "${expected_version}" ]]; then
            return 0
        fi
        attempts=$((attempts + 1))
        if [[ "${attempts}" -ge 30 ]]; then
            echo "Timed out waiting for ${package_name}@${expected_version} on npm" >&2
            return 1
        fi
        sleep 10
    done
}

run_checks() {
    uv sync --all-extras --all-groups --no-cache --upgrade
    pnpm --dir abxbus-ts install --no-frozen-lockfile
    uv run prek run --all-files
    pnpm --dir abxbus-ts run build
    uv build
}

validate_release_state() {
    local slug="$1"
    local branch="$2"
    local current latest relation

    if [[ "$(git branch --show-current)" != "${branch}" ]]; then
        echo "Skipping release-state validation on non-default branch $(git branch --show-current)"
        return 0
    fi

    current="$(current_version)"
    latest="$(latest_release_version "${slug}")"
    if [[ -z "${latest}" ]]; then
        echo "No published releases found for ${slug}; release state is valid"
        return 0
    fi

    relation="$(compare_versions "${current}" "${latest}")"
    if [[ "${relation}" == "lt" ]]; then
        echo "Current version ${current} is behind latest published version ${latest}" >&2
        return 1
    fi

    echo "Release state is valid: local=${current} latest=${latest}"
}

create_release() {
    local slug="$1"
    local version="$2"
    if gh release view "${TAG_PREFIX}${version}" --repo "${slug}" >/dev/null 2>&1; then
        echo "GitHub release ${TAG_PREFIX}${version} already exists"
        return 0
    fi
    gh release create "${TAG_PREFIX}${version}" \
        --repo "${slug}" \
        --target "$(git rev-parse HEAD)" \
        --title "${TAG_PREFIX}${version}" \
        --generate-notes
}

publish_artifacts() {
    local version="$1"
    local pypi_token="${UV_PUBLISH_TOKEN:-${PYPI_TOKEN:-${PYPI_PAT_SECRET:-}}}"
    local npm_token="${NODE_AUTH_TOKEN:-${NPM_TOKEN:-}}"

    if curl -fsSL "https://pypi.org/pypi/${PYPI_PACKAGE}/json" | jq -e --arg version "${version}" '.releases[$version] | length > 0' >/dev/null 2>&1; then
        echo "${PYPI_PACKAGE} ${version} already published on PyPI"
    else
        if [[ -n "${pypi_token}" ]]; then
            UV_PUBLISH_TOKEN="${pypi_token}" uv publish --username=__token__ dist/*
        else
            echo "Missing PyPI credentials: set UV_PUBLISH_TOKEN or PYPI_TOKEN" >&2
            return 1
        fi
    fi

    if npm view "${NPM_PACKAGE}@${version}" version --silent >/dev/null 2>&1; then
        echo "${NPM_PACKAGE} ${version} already published on npm"
    else
        if [[ -n "${npm_token}" ]]; then
            (
                cd abxbus-ts
                npm_config_file="$(mktemp)"
                printf '//registry.npmjs.org/:_authToken=%s\n' "${npm_token}" >"${npm_config_file}"
                NODE_AUTH_TOKEN="${npm_token}" npm_config_userconfig="${npm_config_file}" pnpm publish --access public --no-git-checks
                rm -f "${npm_config_file}"
            )
        else
            echo "Missing npm credentials: set NPM_TOKEN or NODE_AUTH_TOKEN" >&2
            return 1
        fi
    fi

    wait_for_pypi "${PYPI_PACKAGE}" "${version}"
    wait_for_npm "${NPM_PACKAGE}" "${version}"
}

main() {
    local slug branch version latest relation

    source_optional_env
    slug="$(repo_slug)"
    branch="$(default_branch)"

    if [[ "${GITHUB_EVENT_NAME:-}" == "push" ]]; then
        validate_release_state "${slug}" "${branch}"
        return 0
    fi

    if [[ "$(git branch --show-current)" != "${branch}" ]]; then
        echo "Release must run from ${branch}, found $(git branch --show-current)" >&2
        return 1
    fi

    version="$(current_version)"
    latest="$(latest_release_version "${slug}")"
    if [[ -z "${latest}" ]]; then
        relation="gt"
    else
        relation="$(compare_versions "${version}" "${latest}")"
    fi

    if [[ "${relation}" == "eq" ]]; then
        version="$(bump_version)"
        run_checks

        git add -A
        git commit -m "release: ${version}"
        git push origin "${branch}"

        wait_for_runs "${slug}" push "$(git rev-parse HEAD)" "push"
    elif [[ "${relation}" == "gt" ]]; then
        if [[ -n "$(git status --short)" ]]; then
            echo "Refusing to publish existing unreleased version ${version} with a dirty worktree" >&2
            return 1
        fi
        run_checks
        wait_for_runs "${slug}" push "$(git rev-parse HEAD)" "push"
    else
        echo "Current version ${version} is behind latest GitHub release ${latest}" >&2
        return 1
    fi

    publish_artifacts "${version}"
    create_release "${slug}" "${version}"

    latest="$(latest_release_version "${slug}")"
    relation="$(compare_versions "${latest}" "${version}")"
    if [[ "${relation}" != "eq" ]]; then
        echo "GitHub release version mismatch: expected ${version}, got ${latest}" >&2
        return 1
    fi

    echo "Released ${PYPI_PACKAGE} and ${NPM_PACKAGE} ${version}"
}

main "$@"
