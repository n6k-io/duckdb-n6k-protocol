#!/usr/bin/env bash
# Tags origin/main HEAD (or USE_REF, any commit on origin/main) with duckdb/v<VERSION> and
# python/v<pyproject version> and npm/v<package.json version> where missing. Does not push. Set DRY_RUN=1 to print the plan only.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

die() {
	echo "error: $*" >&2
	exit 1
}

gh auth status > /dev/null 2>&1 || die "gh is not authenticated (run: gh auth login)"

git fetch --quiet origin main --tags
if [[ -n "${USE_REF:-}" ]]; then
	sha="$(git rev-parse --verify "$USE_REF^{commit}")" || die "USE_REF '$USE_REF' is not a commit"
	git merge-base --is-ancestor "$sha" origin/main || die "$sha is not on origin/main"
else
	[[ "$(git rev-parse --abbrev-ref HEAD)" == "main" ]] || die "not on main"
	[[ -z "$(git status --porcelain)" ]] || die "working tree is not clean"
	sha="$(git rev-parse HEAD)"
	[[ "$sha" == "$(git rev-parse origin/main)" ]] || die "HEAD ($sha) is not origin/main"
fi

run="$(gh api "repos/{owner}/{repo}/actions/workflows/MainDistributionPipeline.yml/runs?head_sha=$sha&event=push&branch=main" \
	--jq '.workflow_runs[0] | "\(.status) \(.conclusion) \(.html_url)"')"
[[ -n "$run" ]] || die "no MainDistributionPipeline push run for $sha"
read -r status conclusion url <<< "$run"
[[ "$status" == "completed" && "$conclusion" == "success" ]] \
	|| die "MainDistributionPipeline for $sha is $status/$conclusion: $url"

semver='^[0-9]+\.[0-9]+\.[0-9]+$'
duckdb_version="$(git show "$sha:VERSION" | tr -d '[:space:]')"
python_version="$(git show "$sha:packages/python/pyproject.toml" \
	| python3 -c 'import sys, tomllib; print(tomllib.load(sys.stdin.buffer)["project"]["version"])')"
npm_version="$(git show "$sha:packages/npm/package.json" \
	| python3 -c 'import sys, json; print(json.load(sys.stdin)["version"])')"
[[ "$duckdb_version" =~ $semver ]] || die "VERSION '$duckdb_version' is not x.y.z"
[[ "$python_version" =~ $semver ]] || die "pyproject version '$python_version' is not x.y.z"
[[ "$npm_version" =~ $semver ]] || die "package.json version '$npm_version' is not x.y.z"

remote_tag_sha() {
	git ls-remote origin "refs/tags/$1^{}" "refs/tags/$1" | awk 'END { print $1 }'
}

new_tags=()

# A tag elsewhere is fatal for duckdb (CI forces a VERSION bump per PR) but expected for python/npm.
plan() {
	local tag="$1" on_other_commit="$2"
	local local_sha remote_sha
	local_sha="$(git rev-parse -q --verify "refs/tags/$tag^{commit}" || true)"
	remote_sha="$(remote_tag_sha "$tag")"
	if [[ -n "$local_sha" && -n "$remote_sha" && "$local_sha" != "$remote_sha" ]]; then
		die "$tag differs locally ($local_sha) and on origin ($remote_sha)"
	fi
	local existing="${remote_sha:-$local_sha}"
	if [[ -z "$existing" ]]; then
		new_tags+=("$tag")
	elif [[ "$existing" == "$sha" ]]; then
		echo "$tag already on $sha, skipping"
	elif [[ "$on_other_commit" == "fail" ]]; then
		die "$tag already exists on $existing"
	else
		echo "$tag already exists on $existing, skipping"
	fi
}

plan "duckdb/v$duckdb_version" fail
plan "python/v$python_version" skip
plan "npm/v$npm_version" skip

if [[ ${#new_tags[@]} -eq 0 ]]; then
	echo "nothing to tag"
	exit 0
fi

echo "will tag $sha: ${new_tags[*]}"
if [[ "${DRY_RUN:-0}" == "1" ]]; then
	exit 0
fi

for tag in "${new_tags[@]}"; do
	git tag -a "$tag" -m "$tag" "$sha"
done
echo "created: ${new_tags[*]}"
echo "push with: git push origin ${new_tags[*]}"
