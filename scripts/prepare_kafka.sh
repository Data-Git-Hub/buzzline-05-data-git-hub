#!/usr/bin/env bash
# scripts/prepare_kafka.sh - one-time prep for local Kafka (WSL, macOS, Linux)
# Idempotent: only formats if meta.properties is missing.
set -euo pipefail

# Resolve Kafka location from P2 install
KAFKA_DIR="${KAFKA_HOME:-$HOME/kafka}"
SERVER="$KAFKA_DIR/config/kraft/server.properties"
STORAGE="$KAFKA_DIR/bin/kafka-storage.sh"

# Persistent data dir (avoid /tmp). Per-user, no usernames hardcoded.
DATA_DIR="${KAFKA_DATA_DIR:-$HOME/kafka-data}"

echo "Preparing Kafka at: $KAFKA_DIR"
echo "Using data dir:     $DATA_DIR"

# Basic checks
if [ ! -x "$STORAGE" ] || [ ! -f "$SERVER" ]; then
  echo "ERROR: Kafka not found at $KAFKA_DIR (missing bin/ or config/)."
  echo "Hint: Complete P2 (download/extract) before running this."
  exit 1
fi

# Ensure data dir exists
mkdir -p "$DATA_DIR"

# Ensure server.properties has log.dirs set to DATA_DIR (portable, no sed assumptions)
# - Replace existing log.dirs=... if present, otherwise append it
tmpfile="$SERVER.tmp.$$"
awk -v d="$DATA_DIR" '
  BEGIN { found=0 }
  /^\s*log\.dirs\s*=/ { print "log.dirs=" d; found=1; next }
  { print }
  END { if (found==0) print "log.dirs=" d }
' "$SERVER" > "$tmpfile"
mv "$tmpfile" "$SERVER"

# Stop any existing local Kafka gently (portable: pgrep + kill)
# (If nothing is running, this is a no-op.)
if command -v pgrep >/dev/null 2>&1; then
  pids="$(pgrep -f 'kafka\.Kafka' || true)"
  if [ -n "$pids" ]; then
    echo "Stopping existing Kafka (PIDs: $pids)..."
    # shellcheck disable=SC2086
    kill $pids || true
    sleep 1
  fi
fi

# Format storage only if meta.properties is missing
if [ ! -f "$DATA_DIR/meta.properties" ]; then
  echo "Formatting storage (first-time only)..."
  CLUSTER_ID="$("$STORAGE" random-uuid)"
  "$STORAGE" format -t "$CLUSTER_ID" -c "$SERVER"
else
  echo "Storage already formatted; skipping."
fi

echo "Kafka is ready."
echo "Start it with:"
echo "cd ~/kafka"
echo "bin/kafka-server-start.sh config/kraft/server.properties"
