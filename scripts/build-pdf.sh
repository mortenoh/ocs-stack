#!/usr/bin/env bash
#
# build-pdf.sh -- print the single-file book to PDF with headless Chrome.
#
# Chrome is used rather than weasyprint or a LaTeX engine because it needs no
# system libraries beyond a browser that is already installed, and because it
# renders exactly what the browser shows -- the print stylesheet in
# build-book.py is the only thing that differs.
#
# Usage: scripts/build-pdf.sh [input.html] [output.pdf]
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT="${1:-$ROOT/dist/climate-stack.html}"
OUTPUT="${2:-$ROOT/dist/climate-stack.pdf}"

if [ ! -f "$INPUT" ]; then
  echo "no book at $INPUT -- run: make book" >&2
  exit 1
fi

# First match wins. The macOS app bundle is checked before the PATH names
# because on macOS the CLI names are usually absent even when Chrome is there.
CHROME=""
for candidate in \
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  "/Applications/Chromium.app/Contents/MacOS/Chromium" \
  "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge" \
  "$(command -v google-chrome || true)" \
  "$(command -v chromium || true)" \
  "$(command -v chromium-browser || true)"; do
  if [ -n "$candidate" ] && [ -x "$candidate" ]; then
    CHROME="$candidate"
    break
  fi
done

if [ -z "$CHROME" ]; then
  cat >&2 <<'MSG'
No Chrome or Chromium found, so the PDF cannot be rendered here.

The book itself is already built and is the better artefact on a phone anyway:

    open dist/climate-stack.html

To get a PDF from it by hand: open that file in any browser and print to PDF.
MSG
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT")"

# Delete any previous PDF first. The wait below treats "the file exists and
# has stopped growing" as success, and a stale file from an earlier run
# satisfies that before Chrome has written a byte -- so without this, a failed
# render reports success and hands back yesterday's book.
rm -f "$OUTPUT"

# A throwaway profile keeps this out of the real one; --headless=new is the
# only mode that honours the print stylesheet and @page rules.
PROFILE="$(mktemp -d)"
CHROME_PID=""

# Chrome writes the PDF and then, on macOS, routinely fails to exit -- it sits
# there holding the terminal open forever. So it is never waited on
# unconditionally: it runs in the background, gets a bounded wait, and is
# killed either way. The trap covers the Ctrl-C and set -e paths too, so no
# invocation of this script can leave a browser process behind.
cleanup() {
  if [ -n "$CHROME_PID" ] && kill -0 "$CHROME_PID" 2>/dev/null; then
    kill "$CHROME_PID" 2>/dev/null || true
    sleep 1
    kill -9 "$CHROME_PID" 2>/dev/null || true
  fi
  # Any renderer or GPU child that outlived its parent is identified by the
  # profile path, so nothing but this script's own processes can match.
  pkill -9 -f "user-data-dir=$PROFILE" 2>/dev/null || true
  rm -rf "$PROFILE"
}
trap cleanup EXIT INT TERM

echo ">>> Rendering $(basename "$INPUT") with $(basename "$CHROME")"
"$CHROME" \
  --headless=new \
  --disable-gpu \
  --no-first-run \
  --no-pdf-header-footer \
  --no-default-browser-check \
  --disable-background-networking \
  --disable-component-update \
  --disable-extensions \
  --disable-sync \
  --user-data-dir="$PROFILE" \
  --print-to-pdf="$OUTPUT" \
  --virtual-time-budget=20000 \
  "file://$INPUT" >/dev/null 2>&1 &
CHROME_PID=$!

# Wait for the PDF to appear and stop growing, up to a hard ceiling. Chrome
# hanging after a successful write is the common case, so a complete file is
# treated as success regardless of whether the process ever exits.
DEADLINE=$((SECONDS + 180))
last_size=-1
while [ "$SECONDS" -lt "$DEADLINE" ]; do
  if ! kill -0 "$CHROME_PID" 2>/dev/null; then
    break                       # exited on its own, which is the happy path
  fi
  if [ -s "$OUTPUT" ]; then
    size="$(wc -c <"$OUTPUT" | tr -d ' ')"
    if [ "$size" = "$last_size" ]; then
      break                     # written and stable: done, whatever Chrome does
    fi
    last_size="$size"
  fi
  sleep 1
done

if [ ! -s "$OUTPUT" ]; then
  echo "Chrome produced no output at $OUTPUT within 180s" >&2
  exit 1
fi

size="$(du -h "$OUTPUT" | cut -f1 | tr -d ' ')"
echo "wrote ${OUTPUT#"$ROOT"/} -- $size"
