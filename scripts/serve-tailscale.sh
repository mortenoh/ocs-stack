#!/usr/bin/env bash
#
# serve-tailscale.sh -- serve the built site to your own devices over Tailscale.
#
# Binds a static file server to the machine's tailnet address only, so the site
# is reachable from your phone, tablet, and other machines on the tailnet and
# from nowhere else. It does not use `tailscale funnel`, which would publish
# the site to the public internet.
#
# Usage: scripts/serve-tailscale.sh [port]
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${1:-8000}"
SITE="$ROOT/site"

if [ ! -f "$SITE/index.html" ]; then
  echo "no site at $SITE -- run: make docs-build" >&2
  exit 1
fi

if ! command -v tailscale >/dev/null 2>&1; then
  echo "tailscale not found on PATH" >&2
  exit 1
fi

# The tailnet IPv4 address of this machine. `tailscale ip -4` prints it and
# fails when the node is not up, which is the check worth making.
if ! TS_IP="$(tailscale ip -4 2>/dev/null | head -1)" || [ -z "$TS_IP" ]; then
  echo "this machine is not on the tailnet (try: tailscale up)" >&2
  exit 1
fi

# MagicDNS name, for a URL that survives an address change. `tailscale status
# --self` is not portable across versions, so fall back to the IP if the name
# cannot be read.
TS_NAME="$(tailscale status --json 2>/dev/null |
  sed -n 's/.*"DNSName"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1 |
  sed 's/\.$//')"

echo
echo "Serving $SITE to the tailnet only."
echo
echo "  http://$TS_IP:$PORT"
[ -n "$TS_NAME" ] && echo "  http://$TS_NAME:$PORT"
echo
echo "Open either one on your phone. Ctrl-C to stop."
echo "Note: this machine has to stay awake and online for the site to answer."
echo

# Binding to the tailnet address rather than 0.0.0.0 is the access control:
# nothing on the local wifi or a coffee-shop network can reach it.
exec python3 -m http.server "$PORT" --bind "$TS_IP" --directory "$SITE"
