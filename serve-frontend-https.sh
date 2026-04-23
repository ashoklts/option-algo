#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERT_FILE="${ROOT_DIR}/certs/localhost+2.pem"
KEY_FILE="${ROOT_DIR}/certs/localhost+2-key.pem"
PORT="${PORT:-8001}"

if [ ! -f "$CERT_FILE" ]; then
    echo "Missing certificate: $CERT_FILE"
    echo "Create it first with mkcert inside ${ROOT_DIR}/certs"
    exit 1
fi

if [ ! -f "$KEY_FILE" ]; then
    echo "Missing key file: $KEY_FILE"
    echo "Create it first with mkcert inside ${ROOT_DIR}/certs"
    exit 1
fi

cd "$ROOT_DIR"
exec npx http-server . -p "$PORT" -S -C "$CERT_FILE" -K "$KEY_FILE"
