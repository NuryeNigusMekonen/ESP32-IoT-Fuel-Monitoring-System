#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: ./scripts/push-frontend.sh \"commit message\""
  exit 1
fi

MESSAGE="$1"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

git checkout frontend >/dev/null
git pull --rebase origin frontend

if [[ -d frontend ]]; then
  git add frontend
fi
if [[ -d screenshoot ]]; then
  git add screenshoot
fi
if [[ -f README.md ]]; then
  git add README.md
fi
if [[ -d scripts ]]; then
  git add scripts
fi

if git diff --cached --quiet; then
  echo "No frontend-related staged changes to commit."
  exit 0
fi

git commit -m "$MESSAGE"
git push origin frontend

echo "✅ Frontend changes pushed to origin/frontend"
