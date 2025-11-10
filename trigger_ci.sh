#!/bin/bash
# ============================================
# 🧪 trigger_ci.sh — One-click CI trigger script
# ============================================

set -e  # exit immediately if a command fails

# Optional: stage only test files or all changes
git add tests/ > /dev/null 2>&1 || true

# Create a short random suffix for commit message (to avoid duplicate messages)
suffix=$(date +%H%M%S)

# Create an empty commit if nothing new to add
if git diff --cached --quiet; then
  echo "[trigger_ci] No staged changes, creating empty commit..."
  git commit --allow-empty -m "trigger CI run ($suffix)"
else
  echo "[trigger_ci] Committing staged changes..."
  git commit -m "trigger CI run ($suffix)"
fi

# Optional: push if your observer watches a remote repo
# Uncomment the next line if observer fetches origin/main
# git push origin main

echo "[trigger_ci] ✅ Commit created successfully!"
echo "[trigger_ci] Waiting for observer to detect and trigger CI..."
