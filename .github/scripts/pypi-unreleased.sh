#!/usr/bin/env bash
# Usage: pypi-unreleased.sh <package> <version>
# Exit 0 only if <version> is not on PyPI.
set -uo pipefail

status="$(curl -s -o /dev/null -w '%{http_code}' "https://pypi.org/pypi/$1/$2/json")"
case "$status" in
  404)
    echo "$1==$2 is unreleased"
    exit 0
    ;;
  200)
    echo "::error::$1==$2 is already published. Bump the version."
    exit 1
    ;;
  *)
    echo "::error::PyPI lookup failed for $1==$2 (HTTP $status)"
    exit 2
    ;;
esac
