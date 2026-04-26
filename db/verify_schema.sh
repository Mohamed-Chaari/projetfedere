#!/bin/bash
# ──────────────────────────────────────────────────────────────
#  TUNISIA METEO — Schema Verification Script
#  Validates all tables, indexes, views, and constraints
# ──────────────────────────────────────────────────────────────

echo "=============================================="
echo "  TUNISIA METEO — SCHEMA VERIFICATION"
echo "=============================================="

PG="docker exec meteo-postgres psql -U meteo -d meteo_db -t -c"

echo ""
echo "=== Tables ==="
TABLES=(weather_historical weather_current weather_forecast
        weather_alerts monthly_averages temperature_peaks
        correlations annual_stats data_quality_log pipeline_runs
        tunisia_regions)

for t in "${TABLES[@]}"; do
    count=$($PG "SELECT COUNT(*) FROM $t;" 2>/dev/null | xargs)
    if [ $? -eq 0 ]; then
        echo "  ✅  $t  ($count rows)"
    else
        echo "  ❌  $t  MISSING"
    fi
done

echo ""
echo "=== TimescaleDB Hypertable ==="
$PG "SELECT hypertable_name, num_chunks
     FROM timescaledb_information.hypertables;" 2>/dev/null

echo ""
echo "=== Indexes ==="
$PG "SELECT indexname FROM pg_indexes
     WHERE schemaname='public'
     ORDER BY indexname;" 2>/dev/null | grep idx_

echo ""
echo "=== Views ==="
VIEWS=(v_national_current v_national_summary v_forecast_7days
       v_monthly_trend v_active_alerts)
for v in "${VIEWS[@]}"; do
    result=$($PG "SELECT COUNT(*) FROM $v;" 2>/dev/null | xargs)
    if [ $? -eq 0 ]; then
        echo "  ✅  $v  ($result rows)"
    else
        echo "  ❌  $v  BROKEN"
    fi
done

echo ""
echo "=== UNIQUE Constraints (consumer upsert keys) ==="
$PG "SELECT tc.table_name, kcu.column_name
     FROM information_schema.table_constraints tc
     JOIN information_schema.key_column_usage kcu
       ON tc.constraint_name = kcu.constraint_name
     WHERE tc.constraint_type = 'UNIQUE'
       AND tc.table_name IN ('weather_historical','weather_current','weather_forecast')
     ORDER BY tc.table_name, kcu.ordinal_position;" 2>/dev/null

echo ""
echo "=== Compression Policy ==="
$PG "SELECT hypertable_name, compress_after
     FROM timescaledb_information.compression_settings;" 2>/dev/null

echo ""
echo "=============================================="
echo "  Schema verification complete"
echo "=============================================="
