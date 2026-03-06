#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: ./scripts/push-main.sh \"commit message\""
  exit 1
fi

MESSAGE="$1"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

git checkout main >/dev/null
git pull --rebase origin main

git add -A

if git diff --cached --quiet; then
  echo "No changes to commit on main."
  exit 0
fi

git commit -m "$MESSAGE"
git push origin main

echo "✅ Changes pushed to origin/main"
