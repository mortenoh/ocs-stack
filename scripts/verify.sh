#!/usr/bin/env bash
#
# verify.sh — durable verification for the tutorial collection.
#
# For each project it runs, in order: uv sync, make ci (lint + tests + docs
# build), and every example. It prints a per-project line and a final summary,
# and exits non-zero if anything failed — so it works both locally and in CI.
#
# Usage:
#   scripts/verify.sh lang/start data/polars   # named projects (or bare: start)
#   scripts/verify.sh --all                    # every project
#   scripts/verify.sh --list                   # list discoverable projects
#
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Projects live one level inside a topic group: <group>/<project>/pyproject.toml.
discover() {
  local d
  for d in "$ROOT"/*/*/; do
    [ -f "$d/pyproject.toml" ] || continue
    d="${d%/}"
    echo "${d#"$ROOT"/}"
  done
}

# Accept either the qualified path (lang/start) or the bare name (start).
resolve() {
  local name="$1" hit
  if [ -f "$ROOT/$name/pyproject.toml" ]; then
    echo "$name"
    return 0
  fi
  hit="$(discover | awk -F/ -v n="$name" '$2 == n')"
  if [ -n "$hit" ] && [ "$(echo "$hit" | grep -c .)" -eq 1 ]; then
    echo "$hit"
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
    echo "usage: verify.sh [--list | --all | <group/name>...]" >&2
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
