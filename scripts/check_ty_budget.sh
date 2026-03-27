#!/usr/bin/env bash
set -euo pipefail

MAX_DIAGNOSTICS="${TY_MAX_DIAGNOSTICS:-0}"

echo "Running ty with project suppressions and Phase 2 exclusions..."
OUTPUT="$(
    uv run ty check \
        --ignore unresolved-import \
        --exclude "**/tests/**" \
        --exclude "demo/**" \
        --exclude "dags/**" \
        --exclude "packages/canvod-store/src/canvod/store/grid_adapters/grid_storage.py" \
        --exclude "packages/canvod-store/src/canvod/store/store.py" \
        2>&1 || true
)"
printf "%s\n" "$OUTPUT"

FOUND="$(
    printf "%s\n" "$OUTPUT" \
        | sed -nE 's/^Found ([0-9]+) diagnostics?$/\1/p' \
        | tail -n 1
)"
if [[ -z "${FOUND}" ]]; then
    if printf "%s\n" "$OUTPUT" | rg -q "^All checks passed!$"; then
        FOUND="0"
    else
        echo "Could not parse ty diagnostics count from output."
        exit 2
    fi
fi

echo "ty diagnostics: ${FOUND} (budget: <= ${MAX_DIAGNOSTICS})"
if (( FOUND > MAX_DIAGNOSTICS )); then
    echo "Type-check budget exceeded by $((FOUND - MAX_DIAGNOSTICS)) diagnostics."
    exit 1
fi
