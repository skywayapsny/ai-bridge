#!/usr/bin/env bash
# start_aquarium.sh — браузер (выделенный профиль) + cdp-bridge.
#
# Профиль отдельный (aquarium/profile) — твой основной браузер не трогаем.
# Один раз: откроется окно → залогинься в Gemini → сессия сохранится.
#
#   bash aquarium/start_aquarium.sh           # всё поднять
#   AQ_TCP_PORT=8778 AQ_CDP_PORT=9222 bash aquarium/start_aquarium.sh
set -u
cd "$(dirname "$0")"
SCRIPT_DIR="$PWD"

AQ_TCP_PORT="${AQ_TCP_PORT:-8778}"
AQ_CDP_PORT="${AQ_CDP_PORT:-9222}"
PROFILE_DIR="${AQ_PROFILE:-$PWD/profile}"
BROWSER="${AQ_BROWSER:-}"

if [ -z "$BROWSER" ]; then
  for b in brave brave-browser google-chrome-stable google-chrome chromium; do
    if command -v "$b" >/dev/null 2>&1; then BROWSER="$b"; break; fi
  done
fi
[ -z "$BROWSER" ] && { echo "no chromium-family browser found"; exit 1; }

mkdir -p "$PROFILE_DIR"

if ! curl -sf -m 2 "http://127.0.0.1:${AQ_CDP_PORT}/json/version" >/dev/null; then
  echo "starting $BROWSER (profile $PROFILE_DIR, cdp :$AQ_CDP_PORT)..."
  nohup "$BROWSER" \
    --remote-debugging-port="$AQ_CDP_PORT" \
    --user-data-dir="$PROFILE_DIR" \
    --no-first-run --no-default-browser-check \
    >/tmp/aibridge-browser.log 2>&1 &
  for _ in $(seq 1 20); do
    curl -sf -m 1 "http://127.0.0.1:${AQ_CDP_PORT}/json/version" >/dev/null && break
    sleep 0.5
  done
fi

if curl -sf -m 2 "http://127.0.0.1:${AQ_CDP_PORT}/json/version" >/dev/null; then
  echo "browser cdp OK on :$AQ_CDP_PORT"
else
  echo "browser cdp FAILED — see /tmp/aibridge-browser.log"; exit 1
fi

if curl -sf -m 1 "http://127.0.0.1:${AQ_TCP_PORT}" >/dev/null 2>&1; then
  echo "cdp-bridge already on :$AQ_TCP_PORT"; exit 0
fi

echo "cdp-bridge on :$AQ_TCP_PORT"
exec python3 "$SCRIPT_DIR/cdp_bridge.py" "$AQ_TCP_PORT" "$AQ_CDP_PORT"
