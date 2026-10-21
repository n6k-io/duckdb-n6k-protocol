#!/usr/bin/env bash
# Usage: tag-unused.sh <tag>
# Exit 0 only if <tag> does not exist.
set -uo pipefail

if git rev-parse -q --verify "refs/tags/$1" >/dev/null; then
  echo "::error::tag $1 already exists. Bump the version."
  exit 1
fi
echo "tag $1 is unused"
