#!/usr/bin/env bash
#
# verify.sh — durable verification for the tutorial collection.
#
# For each project it runs, in order: uv sync, make ci (lint + tests), and
# every example. It prints a per-project line and a final summary, and exits
# non-zero if anything failed — so it works both locally and in CI.
#
# Documentation is centralised at the repository root; `make docs-build` there
# builds the whole site in one pass.
#
# Usage:
#   scripts/verify.sh xarray dask          # named projects
#   scripts/verify.sh --all                # every project
#   scripts/verify.sh --list               # list discoverable projects
#
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Projects sit directly at the repository root, one directory each. Anything
# without a pyproject.toml (scripts/, docs/, .git/) is skipped by the check.
discover() {
  local d
  for d in "$ROOT"/*/; do
    [ -f "$d/pyproject.toml" ] || continue
    d="${d%/}"
    echo "${d#"$ROOT"/}"
  done
}

resolve() {
  local name="$1"
  if [ -f "$ROOT/$name/pyproject.toml" ]; then
    echo "$name"
    return 0
  fi
  return 1
}

verify_project() {
  local project="$1" dir="$ROOT/$1"
  echo "==> $project"
  (cd "$dir" && uv sync --quiet && make ci && make run-all) || return 1
}

case "${1:-}" in
  --list)
    discover
    exit 0
    ;;
  --all)
    projects="$(discover)"
    ;;
  "")
    echo "usage: verify.sh [--list | --all | <project>...]" >&2
    exit 2
    ;;
  *)
    projects=""
    for name in "$@"; do
      p="$(resolve "$name")" || { echo "unknown project: $name (see --list)" >&2; exit 2; }
      projects="$projects$p"$'\n'
    done
    ;;
esac

failed=()
while IFS= read -r project; do
  [ -n "$project" ] || continue
  verify_project "$project" || failed+=("$project")
done <<< "$projects"

echo
if [ "${#failed[@]}" -gt 0 ]; then
  echo "FAILED: ${failed[*]}"
  exit 1
fi
echo "All projects verified."
