#!/bin/bash
set -e

MODE=${1:-batch}
TOPIC=${2:-""}

echo "======================================"
echo "  TUNISIA METEO CONSUMER"
echo "  Mode: $MODE"
echo "======================================"

# Activate virtualenv if present
source venv/bin/activate 2>/dev/null || true

if [ -n "$TOPIC" ]; then
    python3 -m src.consumer --mode topic --topic "$TOPIC"
else
    python3 -m src.consumer --mode "$MODE"
fi

# After batch mode: show row counts
if [ "$MODE" = "batch" ]; then
    echo ""
    echo "=== DB Row Counts ==="
    python3 -c "
from src.utils.db import get_connection
conn = get_connection()
cur = conn.cursor()
for table in ['weather_historical', 'weather_current', 'weather_forecast']:
    cur.execute(f'SELECT COUNT(*) FROM {table}')
    count = cur.fetchone()[0]
    print(f'  {table:<25} : {count:>8,} rows')
conn.close()
"
fi
