#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cleanup() {
  echo "\nStopping services..."
  if [[ -n "${BACKEND_PID:-}" ]] && kill -0 "$BACKEND_PID" 2>/dev/null; then
    kill "$BACKEND_PID" || true
  fi
}

trap cleanup EXIT INT TERM

pkill -f "backend/app.py|python app.py|vite --host" >/dev/null 2>&1 || true

cd "$ROOT_DIR/backend"
if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi
source .venv/bin/activate
pip install -r requirements.txt >/dev/null
python seed.py >/dev/null
python app.py &
BACKEND_PID=$!
deactivate

cd "$ROOT_DIR/frontend"
npm install >/dev/null
npm run dev -- --host --port 5173 --strictPort
