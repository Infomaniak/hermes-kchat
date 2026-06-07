#!/usr/bin/env bash
#
# Install hermes-kchat as a *directory plugin* under $HERMES_HOME/plugins/kchat.
#
# Usage:
#   scripts/install.sh            # symlink (default; live-edits from the repo)
#   scripts/install.sh --copy     # copy the files instead of symlinking
#   HERMES_HOME=/path scripts/install.sh
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
SRC="$REPO_ROOT/src/hermes_kchat"
HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
DEST="$HERMES_HOME/plugins/kchat"
MODE="${1:---symlink}"

if [ ! -d "$SRC" ]; then
  echo "error: $SRC not found" >&2
  exit 1
fi

mkdir -p "$HERMES_HOME/plugins"

if [ -e "$DEST" ] || [ -L "$DEST" ]; then
  echo "Removing existing $DEST"
  rm -rf "$DEST"
fi

case "$MODE" in
  --symlink)
    ln -s "$SRC" "$DEST"
    echo "Linked  $DEST -> $SRC"
    ;;
  --copy)
    mkdir -p "$DEST"
    cp "$SRC/__init__.py" "$SRC/adapter.py" "$SRC/pusher.py" "$SRC/plugin.yaml" "$DEST/"
    echo "Copied  $SRC/{__init__,adapter,pusher}.py + plugin.yaml -> $DEST"
    ;;
  *)
    echo "error: unknown option '$MODE' (use --symlink or --copy)" >&2
    exit 1
    ;;
esac

echo
echo "Next:"
echo "  hermes plugins enable kchat"
echo "  export KCHAT_URL=https://your-org.kchat.infomaniak.com"
echo "  export KCHAT_TOKEN=your-bot-token"
echo "  # then start a hermes gateway session"
