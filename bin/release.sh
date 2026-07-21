#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_DIR}"

TAG_PREFIX=""
PYPI_PACKAGE="abxbus"
NPM_PACKAGE="abxbus"
REQUIRED_WORKFLOWS=(
    "pre-commit-hooks.yaml|pre-commit-hooks"
    "test_py.yaml|test-py"
    "test_ts.yaml|test-ts"
    "test_go.yaml|test-go"
    "test_rust.yaml|test-rust"
)

source_optional_env() {
    if [[ -f "${REPO_DIR}/.env" ]]; then
        set -a
        # shellcheck disable=SC1091
        source "${REPO_DIR}/.env"
        set +a
    fi
}

repo_slug() {
    if [[ -n "${GITHUB_REPOSITORY:-}" ]]; then
        printf '%s\n' "${GITHUB_REPOSITORY}"
        return
    fi

    git remote get-url origin | sed -E 's#^git@github\.com:##; s#^https://github\.com/##; s#\.git$##'
}

current_version() {
    uv run --no-project python - <<'PY'
from pathlib import Path
import json
import re

versions = {
    'pyproject.toml': re.search(r'^version = "([^"]+)"$', Path('pyproject.toml').read_text(), re.MULTILINE),
    'abxbus-rust/Cargo.toml': re.search(r'^version = "([^"]+)"$', Path('abxbus-rust/Cargo.toml').read_text(), re.MULTILINE),
    'abxbus-rust/Cargo.lock': re.search(r'(?m)^name = "abxbus"\nversion = "([^"]+)"$', Path('abxbus-rust/Cargo.lock').read_text()),
    'abxbus-go/version.go': re.search(r'const Version = "([^"]+)"', Path('abxbus-go/version.go').read_text()),
}
values = {path: match.group(1) if match else None for path, match in versions.items()}
values['abxbus-ts/package.json'] = json.loads(Path('abxbus-ts/package.json').read_text()).get('version')
if None in values.values():
    raise SystemExit(f'Failed to read all package versions: {values}')
if len(set(values.values())) != 1:
    raise SystemExit(f'Package versions disagree: {values}')
print(next(iter(values.values())))
PY
}

compare_versions() {
    uv run --no-project python - "$1" "$2" <<'PY'
import re
import sys

def parse(version: str) -> tuple[int, int, int, int]:
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:rc(\d+))?', version)
    if not match:
        raise SystemExit(f'Unsupported version format: {version}')
    major, minor, patch, rc = match.groups()
    return (int(major), int(minor), int(patch), int(rc) if rc is not None else 10_000)

left, right = sys.argv[1], sys.argv[2]
print('gt' if parse(left) > parse(right) else 'eq' if parse(left) == parse(right) else 'lt')
PY
}

latest_release_version() {
    local slug="$1"
    local raw_tags
    raw_tags="$(gh api "repos/${slug}/releases?per_page=100" --jq '.[].tag_name' || true)"
    RELEASE_TAGS="${raw_tags}" uv run --no-project python - <<'PY'
import os
import re

def parse(version: str) -> tuple[int, int, int, int]:
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:rc(\d+))?', version)
    if not match:
        return (-1, -1, -1, -1)
    major, minor, patch, rc = match.groups()
    return (int(major), int(minor), int(patch), int(rc) if rc is not None else 10_000)

versions = [version for version in os.environ.get('RELEASE_TAGS', '').splitlines() if parse(version) != (-1, -1, -1, -1)]
print(max(versions, key=parse) if versions else '')
PY
}

latest_registry_version() {
    local registry="$1"
    local versions
    if [[ "${registry}" == "pypi" ]]; then
        versions="$(curl -fsSL "https://pypi.org/pypi/${PYPI_PACKAGE}/json" | jq -r '.releases | keys[]' || true)"
    else
        versions="$(npm view "${NPM_PACKAGE}" versions --json --silent 2>/dev/null | jq -r '.[]' || true)"
    fi
    RELEASE_TAGS="${versions}" uv run --no-project python - <<'PY'
import os
import re

def parse(version: str) -> tuple[int, int, int, int]:
    match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:rc(\d+))?', version)
    if not match:
        return (-1, -1, -1, -1)
    major, minor, patch, rc = match.groups()
    return (int(major), int(minor), int(patch), int(rc) if rc is not None else 10_000)

versions = [version for version in os.environ.get('RELEASE_TAGS', '').splitlines() if parse(version) != (-1, -1, -1, -1)]
print(max(versions, key=parse) if versions else '')
PY
}

require_clean_exact_checkout() {
    local release_sha="$1"
    local release_branch="$2"

    if [[ ! "${release_sha}" =~ ^[0-9a-f]{40}$ ]]; then
        echo "RELEASE_SHA must be a full 40-character commit SHA" >&2
        return 1
    fi
    if [[ "$(git rev-parse HEAD)" != "${release_sha}" ]]; then
        echo "Refusing to release: checkout HEAD does not match RELEASE_SHA ${release_sha}" >&2
        return 1
    fi
    if [[ -n "$(git status --short)" ]]; then
        echo "Refusing to release from a dirty worktree" >&2
        return 1
    fi
    git fetch --quiet --no-tags origin "+refs/heads/${release_branch}:refs/remotes/origin/${release_branch}"
    if ! git merge-base --is-ancestor "${release_sha}" "refs/remotes/origin/${release_branch}"; then
        echo "Refusing to release ${release_sha}: it is not on ${release_branch}" >&2
        return 1
    fi
}

require_successful_workflows() {
    local slug="$1"
    local release_sha="$2"
    local workflow_spec workflow_file workflow_name runs state run_id attempts final_state

    for workflow_spec in "${REQUIRED_WORKFLOWS[@]}"; do
        workflow_file="${workflow_spec%%|*}"
        workflow_name="${workflow_spec#*|}"
        attempts=0

        while :; do
            runs="$(env -u GH_FORCE_TTY GH_PROMPT_DISABLED=1 GH_PAGER=cat NO_COLOR=1 gh run list \
                --repo "${slug}" \
                --workflow "${workflow_file}" \
                --event push \
                --commit "${release_sha}" \
                --limit 10 \
                --json databaseId,workflowName,headSha,status,conclusion,event)"
            state="$(jq -r --arg name "${workflow_name}" --arg sha "${release_sha}" '
                [.[] | select(.workflowName == $name and .headSha == $sha and .event == "push")]
                | if length == 1
                  then (.[0] | [.databaseId, .status, (.conclusion // "")] | @tsv)
                  elif length == 0 then "missing"
                  else "ambiguous"
                  end
            ' <<<"${runs}")"

            case "${state}" in
                missing)
                    ;;
                ambiguous)
                    echo "Found multiple ${workflow_name} push runs for ${release_sha}; refusing an ambiguous release gate" >&2
                    return 1
                    ;;
                *)
                    IFS=$'\t' read -r run_id _ _ <<<"${state}"
                    break
                    ;;
            esac

            attempts=$((attempts + 1))
            if [[ "${attempts}" -ge 12 ]]; then
                echo "Required workflow ${workflow_name} did not start for ${release_sha} within 60 seconds" >&2
                return 1
            fi
            sleep 5
        done

        env -u GH_FORCE_TTY GH_PROMPT_DISABLED=1 GH_PAGER=cat NO_COLOR=1 \
            gh run watch "${run_id}" --repo "${slug}" --exit-status
        final_state="$(env -u GH_FORCE_TTY GH_PROMPT_DISABLED=1 GH_PAGER=cat NO_COLOR=1 \
            gh run view "${run_id}" --repo "${slug}" \
            --json workflowName,headSha,status,conclusion,event \
            --jq '[.workflowName, .headSha, .event, .status, (.conclusion // "")] | @tsv')"
        if [[ "${final_state}" != "${workflow_name}"$'\t'"${release_sha}"$'\tpush\tcompleted\tsuccess' ]]; then
            echo "Required workflow ${workflow_name} was not a successful exact-SHA push run: ${final_state}" >&2
            return 1
        fi
        echo "Required workflow passed: ${workflow_name} (${run_id})"
    done
}

wait_for_pypi() {
    local version="$1"
    local attempts=0
    until curl -fsSL "https://pypi.org/pypi/${PYPI_PACKAGE}/json" | jq -e --arg version "${version}" '.releases[$version] | length > 0' >/dev/null; do
        attempts=$((attempts + 1))
        if [[ "${attempts}" -ge 30 ]]; then
            echo "Timed out waiting for ${PYPI_PACKAGE}==${version} on PyPI" >&2
            return 1
        fi
        sleep 10
    done
}

wait_for_npm() {
    local version="$1"
    local attempts=0
    until [[ "$(npm view "${NPM_PACKAGE}@${version}" version --silent 2>/dev/null || true)" == "${version}" ]]; do
        attempts=$((attempts + 1))
        if [[ "${attempts}" -ge 30 ]]; then
            echo "Timed out waiting for ${NPM_PACKAGE}@${version} on npm" >&2
            return 1
        fi
        sleep 10
    done
}

build_artifacts() {
    local version="$1"

    rm -rf "${REPO_DIR}/dist" "${REPO_DIR}/abxbus-ts/dist"
    uv build --out-dir "${REPO_DIR}/dist"
    pnpm --dir abxbus-ts install --frozen-lockfile
    pnpm --dir abxbus-ts run build
    if ! compgen -G "${REPO_DIR}/dist/${PYPI_PACKAGE}-${version}*" >/dev/null; then
        echo "Missing build artifacts for ${PYPI_PACKAGE}==${version}" >&2
        return 1
    fi
    if [[ ! -d "${REPO_DIR}/abxbus-ts/dist" ]]; then
        echo "Missing npm build artifacts for ${NPM_PACKAGE}@${version}" >&2
        return 1
    fi
}

publish_artifacts() {
    local version="$1"

    if curl -fsSL "https://pypi.org/pypi/${PYPI_PACKAGE}/json" | jq -e --arg version "${version}" '.releases[$version] | length > 0' >/dev/null 2>&1; then
        echo "${PYPI_PACKAGE} ${version} already published on PyPI"
    else
        uv publish --trusted-publishing always "${REPO_DIR}/dist/"*
    fi

    if npm view "${NPM_PACKAGE}@${version}" version --silent >/dev/null 2>&1; then
        echo "${NPM_PACKAGE} ${version} already published on npm"
    else
        (cd abxbus-ts && npm publish --access public)
    fi

    wait_for_pypi "${version}"
    wait_for_npm "${version}"
}

create_release() {
    local slug="$1"
    local version="$2"
    local release_sha="$3"

    if gh release view "${TAG_PREFIX}${version}" --repo "${slug}" >/dev/null 2>&1; then
        echo "GitHub release ${TAG_PREFIX}${version} already exists"
        return
    fi
    gh release create "${TAG_PREFIX}${version}" \
        --repo "${slug}" \
        --target "${release_sha}" \
        --title "${TAG_PREFIX}${version}" \
        --generate-notes
}

go_module_tag_names() {
    local version="$1"
    local mod_file module_dir
    find . -name go.mod -not -path './.git/*' | sort | while read -r mod_file; do
        module_dir="${mod_file%/go.mod}"
        module_dir="${module_dir#./}"
        if [[ "${module_dir}" == "." ]]; then
            printf 'v%s\n' "${version}"
        else
            printf '%s/v%s\n' "${module_dir}" "${version}"
        fi
    done
}

create_go_module_tags() {
    local version="$1"
    local release_sha="$2"
    local tag

    while read -r tag; do
        [[ -n "${tag}" ]] || continue
        if git ls-remote --exit-code --tags origin "refs/tags/${tag}" >/dev/null 2>&1; then
            if [[ "$(git ls-remote --tags origin "refs/tags/${tag}" | cut -f1)" != "${release_sha}" ]]; then
                echo "Existing Go module tag ${tag} does not target ${release_sha}" >&2
                return 1
            fi
            echo "Go module tag ${tag} already exists on ${release_sha}"
            continue
        fi
        git tag "${tag}" "${release_sha}"
        git push origin "refs/tags/${tag}"
    done < <(go_module_tag_names "${version}")
}

main() {
    local slug release_sha release_branch version latest candidate relation released_tag registry release_target pypi_exists npm_exists github_release_exists

    source_optional_env
    slug="$(repo_slug)"
    release_sha="${RELEASE_SHA:-$(git rev-parse HEAD)}"
    release_branch="${RELEASE_BRANCH:-main}"
    require_clean_exact_checkout "${release_sha}" "${release_branch}"

    version="$(current_version)"
    latest="$(latest_release_version "${slug}")"
    for registry in pypi npm; do
        candidate="$(latest_registry_version "${registry}")"
        if [[ -n "${candidate}" && ( -z "${latest}" || "$(compare_versions "${candidate}" "${latest}")" == "gt" ) ]]; then
            latest="${candidate}"
        fi
    done
    relation="gt"
    if [[ -n "${latest}" ]]; then
        relation="$(compare_versions "${version}" "${latest}")"
    fi

    if [[ "${relation}" == "lt" ]]; then
        echo "Current version ${version} is behind latest published version ${latest}" >&2
        return 1
    fi

    pypi_exists=false
    npm_exists=false
    github_release_exists=false
    if curl -fsSL "https://pypi.org/pypi/${PYPI_PACKAGE}/json" | jq -e --arg version "${version}" '.releases[$version] | length > 0' >/dev/null 2>&1; then
        pypi_exists=true
    fi
    if npm view "${NPM_PACKAGE}@${version}" version --silent >/dev/null 2>&1; then
        npm_exists=true
    fi
    release_target="$(git ls-remote origin "refs/tags/${TAG_PREFIX}${version}" | cut -f1)"
    if gh release view "${TAG_PREFIX}${version}" --repo "${slug}" >/dev/null 2>&1; then
        github_release_exists=true
    fi
    if [[ "${relation}" == "eq" && "${pypi_exists}" == true && "${npm_exists}" == true && "${github_release_exists}" == true && -n "${release_target}" ]]; then
        echo "${PYPI_PACKAGE} ${version} is already released; nothing to publish"
        return
    fi
    if [[ "${relation}" == "eq" && ( -z "${release_target}" || "${release_target}" != "${release_sha}" ) ]]; then
        echo "Refusing to recover partial release ${version}: no release tag anchors it to ${release_sha}" >&2
        return 1
    fi

    require_successful_workflows "${slug}" "${release_sha}"
    build_artifacts "${version}"
    create_release "${slug}" "${version}" "${release_sha}"
    publish_artifacts "${version}"
    create_go_module_tags "${version}" "${release_sha}"

    released_tag="$(gh release view "${TAG_PREFIX}${version}" --repo "${slug}" --json tagName,targetCommitish --jq '[.tagName, .targetCommitish] | @tsv')"
    if [[ "${released_tag}" != "${TAG_PREFIX}${version}"$'\t'"${release_sha}" ]]; then
        echo "GitHub release does not target the tested SHA ${release_sha}: ${released_tag}" >&2
        return 1
    fi
    echo "Released ${PYPI_PACKAGE} and ${NPM_PACKAGE} ${version} from ${release_sha}"
}

main "$@"
