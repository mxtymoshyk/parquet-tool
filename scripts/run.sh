#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VENV="${ROOT}/.venv"
PY="${PYTHON:-python3}"

if [ ! -d "$VENV" ]; then
    "$PY" -m venv "$VENV"
fi

source "$VENV/bin/activate"

if ! python -c "import parquet_tool" 2>/dev/null; then
    pip install --upgrade pip
    pip install -e .
fi

exec parquet-tool "$@"
