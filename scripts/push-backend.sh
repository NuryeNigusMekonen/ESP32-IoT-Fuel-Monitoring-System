#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: ./scripts/push-backend.sh \"commit message\""
  exit 1
fi

MESSAGE="$1"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

git checkout backend >/dev/null
git pull --rebase origin backend

if [[ -d backend ]]; then
  git add backend
fi
if [[ -f README.md ]]; then
  git add README.md
fi
if [[ -f start-all.sh ]]; then
  git add start-all.sh
fi
if [[ -d scripts ]]; then
  git add scripts
fi

if git diff --cached --quiet; then
  echo "No backend-related staged changes to commit."
  exit 0
fi

git commit -m "$MESSAGE"
git push origin backend

echo "✅ Backend changes pushed to origin/backend"
