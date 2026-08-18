#!/usr/bin/env bash
#
# check-links.sh — validate relative links in docs/ against the filesystem.
#
# The project pages link to example and library source with paths relative to
# the repository root, so they work on GitHub and in an editor. mkdocs cannot
# follow a link outside its docs_dir, so link validation for those is disabled
# in mkdocs.yml and done here instead.
#
# Usage: scripts/check-links.sh
#
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1

broken=0
checked=0

# mktemp rather than a $$-suffixed name: the pipeline below writes the file
# unconditionally, and a predictable path in a world-writable directory is
# something another process can get there first.
report="$(mktemp "${TMPDIR:-/tmp}/check-links.XXXXXX")"
trap 'rm -f "$report"' EXIT INT TERM

while IFS= read -r doc; do
  dir="$(dirname "$doc")"
  # Pull the target out of every inline markdown link, drop anchors and any
  # external or mail scheme, and keep the relative ones.
  grep -oE '\]\([^)]+\)' "$doc" | sed 's/](//;s/)$//' | while read -r target; do
    case "$target" in
      http://* | https://* | mailto:* | '#'*) continue ;;
    esac
    path="${target%%#*}"
    [ -n "$path" ] || continue
    if [ ! -e "$dir/$path" ]; then
      echo "BROKEN: $doc -> $target"
    fi
  done
done < <(find docs -name '*.md') > "$report" 2>/dev/null

broken="$(grep -c . "$report" 2>/dev/null)"
broken="${broken:-0}"
checked="$(find docs -name '*.md' | wc -l | tr -d ' ')"
cat "$report"

echo
if [ "$broken" -gt 0 ]; then
  echo "$broken broken link(s) across $checked documentation files."
  exit 1
fi
echo "All relative links resolve across $checked documentation files."
