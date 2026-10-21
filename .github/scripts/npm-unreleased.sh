#!/usr/bin/env bash
# Usage: npm-unreleased.sh <package> <version>
# Exit 0 only if <version> is not on the npm registry.
set -uo pipefail

err="$(mktemp)"
trap 'rm -f "$err"' EXIT

if out="$(npm view "$1@$2" version 2>"$err")" && [ -n "$out" ]; then
  echo "::error::$1@$2 is already published. Bump the version."
  exit 1
elif grep -q E404 "$err"; then
  echo "$1@$2 is unreleased"
  exit 0
else
  cat "$err" >&2
  echo "::error::npm lookup failed for $1@$2"
  exit 2
fi
