#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
#  Météo Tunisie — Kafka Topic Setup
#  Projet TD5 — ISIMS Sfax
#
#  Creates the 5-topic streaming topology:
#    weather-historical  (24p) — backfill archive, retained FOREVER
#    weather-current     (24p) — live conditions, 3-day window
#    weather-forecast    (24p) — 7-day predictions, 12-hour window
#    weather-dlq          (1p) — dead-letter queue, 7-day window
#    weather-alerts       (7p) — alerts by region, 24-hour window
#
#  Usage:
#    chmod +x scripts/setup_kafka.sh
#    ./scripts/setup_kafka.sh
# ──────────────────────────────────────────────────────────────

set -euo pipefail

# ── CONFIG ────────────────────────────────────────────────────
COMPOSE_FILE="docker-compose.yml"
KAFKA_CONTAINER="meteo-kafka"
BOOTSTRAP="localhost:9092"
MAX_WAIT=90       # seconds
CHECK_INTERVAL=2  # seconds

# ── COLORS ────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# ── HELPERS ───────────────────────────────────────────────────
info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

kafka_cmd() {
    docker exec "${KAFKA_CONTAINER}" "$@"
}

# ══════════════════════════════════════════════════════════════
#  STEP 1 — Start core infrastructure
# ══════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  🇹🇳  MÉTÉO TUNISIE — Infrastructure Setup${NC}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

info "Starting Zookeeper, Kafka, and PostgreSQL..."
docker-compose -f "${COMPOSE_FILE}" up -d zookeeper kafka postgres

# ══════════════════════════════════════════════════════════════
#  STEP 2 — Smart-wait for Kafka readiness
# ══════════════════════════════════════════════════════════════
echo ""
info "Waiting for Kafka to become ready (max ${MAX_WAIT}s)..."
printf "     "

elapsed=0
while [ $elapsed -lt $MAX_WAIT ]; do
    if kafka_cmd kafka-topics --list --bootstrap-server "${BOOTSTRAP}" &>/dev/null; then
        echo ""
        success "Kafka is ready! (${elapsed}s)"
        break
    fi
    printf "."
    sleep $CHECK_INTERVAL
    elapsed=$((elapsed + CHECK_INTERVAL))
done

if [ $elapsed -ge $MAX_WAIT ]; then
    echo ""
    error "Kafka did not become ready within ${MAX_WAIT}s. Check logs: docker logs ${KAFKA_CONTAINER}"
fi

# ══════════════════════════════════════════════════════════════
#  STEP 3 — Create all 5 topics
# ══════════════════════════════════════════════════════════════
echo ""
info "Creating Kafka topics..."
echo ""

# ── Topic 1: weather-historical ──────────────────────────────
# Backfill data from 2020-2024. Retained FOREVER, compacted by message_id key.
info "  [1/5] weather-historical (24 partitions, retention: FOREVER, compacted)"
kafka_cmd kafka-topics --create \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic weather-historical \
    --partitions 24 \
    --replication-factor 1 \
    --config retention.ms=-1 \
    --config compression.type=gzip \
    --config cleanup.policy=compact \
    --config min.compaction.lag.ms=3600000 \
    --config segment.ms=604800000 \
    --if-not-exists
success "  weather-historical ✓"

# ── Topic 2: weather-current ─────────────────────────────────
# Live conditions every 15 min. 3-day rolling window.
info "  [2/5] weather-current (24 partitions, retention: 3 days)"
kafka_cmd kafka-topics --create \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic weather-current \
    --partitions 24 \
    --replication-factor 1 \
    --config retention.ms=259200000 \
    --config compression.type=gzip \
    --config cleanup.policy=delete \
    --config segment.ms=86400000 \
    --if-not-exists
success "  weather-current ✓"

# ── Topic 3: weather-forecast ────────────────────────────────
# 7-day predictions refreshed every 6h. 12-hour retention (stale = useless).
info "  [3/5] weather-forecast (24 partitions, retention: 12 hours)"
kafka_cmd kafka-topics --create \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic weather-forecast \
    --partitions 24 \
    --replication-factor 1 \
    --config retention.ms=43200000 \
    --config compression.type=gzip \
    --config cleanup.policy=delete \
    --config segment.ms=3600000 \
    --if-not-exists
success "  weather-forecast ✓"

# ── Topic 4: weather-dlq ─────────────────────────────────────
# Dead-letter queue for failed messages from ALL producers. 7-day retention.
info "  [4/5] weather-dlq (1 partition, retention: 7 days)"
kafka_cmd kafka-topics --create \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic weather-dlq \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config compression.type=gzip \
    --config cleanup.policy=delete \
    --if-not-exists
success "  weather-dlq ✓"

# ── Topic 5: weather-alerts ──────────────────────────────────
# Generated alerts (heatwave, drought, storm). 7 partitions (one per region).
info "  [5/5] weather-alerts (7 partitions, retention: 24 hours)"
kafka_cmd kafka-topics --create \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic weather-alerts \
    --partitions 7 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config compression.type=gzip \
    --config cleanup.policy=delete \
    --if-not-exists
success "  weather-alerts ✓"

# ══════════════════════════════════════════════════════════════
#  STEP 4 — Verify all 5 topics exist
# ══════════════════════════════════════════════════════════════
echo ""
info "Verifying topics..."

EXPECTED_TOPICS=("weather-historical" "weather-current" "weather-forecast" "weather-dlq" "weather-alerts")
EXISTING_TOPICS=$(kafka_cmd kafka-topics --list --bootstrap-server "${BOOTSTRAP}")
MISSING=0

for topic in "${EXPECTED_TOPICS[@]}"; do
    if echo "${EXISTING_TOPICS}" | grep -qw "${topic}"; then
        success "  ${topic} — exists"
    else
        warn "  ${topic} — MISSING!"
        MISSING=$((MISSING + 1))
    fi
done

if [ $MISSING -gt 0 ]; then
    error "${MISSING} topic(s) missing! Setup incomplete."
fi

# ══════════════════════════════════════════════════════════════
#  STEP 5 — Summary banner
# ══════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}"
cat << 'BANNER'
  ╔════════════════════════════════════════════════════════════╗
  ║          TUNISIA METEO — 5-TOPIC STREAMING STACK          ║
  ╠════════════════════════════════════════════════════════════╣
  ║  weather-historical  │ 24 partitions │ retention: FOREVER  ║
  ║  weather-current     │ 24 partitions │ retention: 3 days   ║
  ║  weather-forecast    │ 24 partitions │ retention: 12 hours ║
  ║  weather-dlq         │  1 partition  │ retention: 7 days   ║
  ║  weather-alerts      │  7 partitions │ retention: 24 hours ║
  ╠════════════════════════════════════════════════════════════╣
  ║  Cities: 221  │  Governorates: 24  │  Regions: 7          ║
  ║  Historical:  2020-01-01 → 2024-12-31 (one-time backfill) ║
  ║  Current:     every 15 minutes (live)                     ║
  ║  Forecast:    7-day window, refreshed every 6 hours       ║
  ╚════════════════════════════════════════════════════════════╝
BANNER
echo -e "${NC}"

success "All infrastructure is ready! 🚀"
echo ""
info "Next steps:"
echo "    1. Run the historical backfill:  python -m src.producers.historical"
echo "    2. Start the current producer:   python -m src.producers.current"
echo "    3. Start the forecast producer:  python -m src.producers.forecast"
echo "    4. Launch Airflow:               docker-compose up -d airflow"
echo "    5. Verify health:                ./scripts/kafka_verify.sh"
echo ""
