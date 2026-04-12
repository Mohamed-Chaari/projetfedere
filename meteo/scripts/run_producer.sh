#!/bin/bash
set -e

MODE=${1:-once}

echo "╔══════════════════════════════════════════╗"
echo "║   TUNISIA METEO PRODUCER — mode: $MODE   ║"
echo "╚══════════════════════════════════════════╝"

# Activate virtualenv if present
source venv/bin/activate 2>/dev/null || true

python -m src.producer --mode "$MODE"
