#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
#  Météo Tunisie — Kafka Health Dashboard
#  Projet TD5 — ISIMS Sfax
#
#  Prints a live health overview:
#    • All 5 topics — partition count + retention
#    • Consumer group lag per group
#    • Latest message timestamps per topic
#    • DLQ count with RED warning if > 0
#    • Messages/hour rate for weather-current
#
#  Usage:
#    chmod +x scripts/kafka_verify.sh
#    ./scripts/kafka_verify.sh
# ──────────────────────────────────────────────────────────────

set -euo pipefail

# ── CONFIG ────────────────────────────────────────────────────
KAFKA_CONTAINER="meteo-kafka"
BOOTSTRAP="localhost:9092"

# ── COLORS ────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# ── HELPERS ───────────────────────────────────────────────────
kafka_cmd() {
    docker exec "${KAFKA_CONTAINER}" "$@" 2>/dev/null
}

divider() {
    echo -e "${DIM}────────────────────────────────────────────────────────────────${NC}"
}

section() {
    echo ""
    echo -e "${BOLD}${CYAN}  ▸ $1${NC}"
    divider
}

# ══════════════════════════════════════════════════════════════
#  CHECK — Kafka reachability
# ══════════════════════════════════════════════════════════════
if ! kafka_cmd kafka-topics --list --bootstrap-server "${BOOTSTRAP}" &>/dev/null; then
    echo -e "${RED}[ERROR]${NC} Cannot connect to Kafka at ${BOOTSTRAP}"
    echo "        Is the kafka container running?  docker-compose ps"
    exit 1
fi

# ══════════════════════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════════════════════
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S %Z')

echo ""
echo -e "${BOLD}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║         🇹🇳  MÉTÉO TUNISIE — KAFKA HEALTH DASHBOARD          ║${NC}"
echo -e "${BOLD}║         ${DIM}${TIMESTAMP}${NC}${BOLD}                     ║${NC}"
echo -e "${BOLD}╚════════════════════════════════════════════════════════════════╝${NC}"

# ══════════════════════════════════════════════════════════════
#  SECTION 1 — Topic overview
# ══════════════════════════════════════════════════════════════
section "TOPIC OVERVIEW"

TOPICS=("weather-historical" "weather-current" "weather-forecast" "weather-dlq" "weather-alerts")
EXPECTED_PARTITIONS=(24 24 24 1 7)
EXPECTED_RETENTION=("FOREVER" "3 days" "12 hours" "7 days" "24 hours")
RETENTION_MS=(-1 259200000 43200000 604800000 86400000)

printf "  ${BOLD}%-25s %-14s %-18s %-8s${NC}\n" "TOPIC" "PARTITIONS" "RETENTION" "STATUS"
divider

for i in "${!TOPICS[@]}"; do
    topic="${TOPICS[$i]}"
    
    # Get partition count
    PARTITIONS=$(kafka_cmd kafka-topics --describe \
        --bootstrap-server "${BOOTSTRAP}" \
        --topic "${topic}" 2>/dev/null \
        | grep -c "Partition:" || echo "0")
    
    # Get actual retention.ms config
    ACTUAL_RETENTION=$(kafka_cmd kafka-configs --describe \
        --bootstrap-server "${BOOTSTRAP}" \
        --entity-type topics \
        --entity-name "${topic}" 2>/dev/null \
        | grep "retention.ms" \
        | sed 's/.*retention.ms=\([^ ,]*\).*/\1/' || echo "N/A")
    
    # Determine status
    STATUS=""
    if [ "${PARTITIONS}" -eq "${EXPECTED_PARTITIONS[$i]}" ] 2>/dev/null; then
        STATUS="${GREEN}● OK${NC}"
    elif [ "${PARTITIONS}" -eq "0" ]; then
        STATUS="${RED}● MISSING${NC}"
    else
        STATUS="${YELLOW}● WARN${NC}"
    fi
    
    printf "  %-25s %-14s %-18s " "${topic}" "${PARTITIONS} partitions" "${EXPECTED_RETENTION[$i]}"
    echo -e "${STATUS}"
done

# ══════════════════════════════════════════════════════════════
#  SECTION 2 — Consumer group lag
# ══════════════════════════════════════════════════════════════
section "CONSUMER GROUP LAG"

GROUPS=$(kafka_cmd kafka-consumer-groups --list --bootstrap-server "${BOOTSTRAP}" 2>/dev/null || echo "")

if [ -z "${GROUPS}" ]; then
    echo -e "  ${DIM}No consumer groups registered yet.${NC}"
else
    printf "  ${BOLD}%-30s %-20s %-10s %-10s %-10s${NC}\n" "GROUP" "TOPIC" "LAG" "MEMBERS" "STATE"
    divider
    
    while IFS= read -r group; do
        [ -z "${group}" ] && continue
        
        # Get group details
        GROUP_DESC=$(kafka_cmd kafka-consumer-groups --describe \
            --bootstrap-server "${BOOTSTRAP}" \
            --group "${group}" 2>/dev/null || echo "")
        
        if [ -z "${GROUP_DESC}" ]; then
            continue
        fi
        
        # Parse total lag per topic within this group
        echo "${GROUP_DESC}" | tail -n +3 | while IFS= read -r line; do
            [ -z "${line}" ] && continue
            
            TOPIC_NAME=$(echo "${line}" | awk '{print $1}')
            CURRENT_OFFSET=$(echo "${line}" | awk '{print $3}')
            LOG_END=$(echo "${line}" | awk '{print $4}')
            LAG=$(echo "${line}" | awk '{print $5}')
            
            # Color code lag
            LAG_COLOR="${GREEN}"
            if [ "${LAG}" != "-" ] 2>/dev/null; then
                if [ "${LAG}" -gt 1000 ] 2>/dev/null; then
                    LAG_COLOR="${RED}"
                elif [ "${LAG}" -gt 100 ] 2>/dev/null; then
                    LAG_COLOR="${YELLOW}"
                fi
            fi
            
            printf "  %-30s %-20s " "${group}" "${TOPIC_NAME}"
            echo -e "${LAG_COLOR}${LAG}${NC}"
        done
    done <<< "${GROUPS}"
fi

# ══════════════════════════════════════════════════════════════
#  SECTION 3 — Latest message offsets per topic
# ══════════════════════════════════════════════════════════════
section "MESSAGE COUNTS (latest offsets)"

printf "  ${BOLD}%-25s %-15s %-15s${NC}\n" "TOPIC" "TOTAL OFFSET" "STATUS"
divider

for topic in "${TOPICS[@]}"; do
    # Sum offsets across all partitions
    OFFSETS=$(kafka_cmd kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list "${BOOTSTRAP}" \
        --topic "${topic}" \
        --time -1 2>/dev/null || echo "")
    
    if [ -z "${OFFSETS}" ]; then
        printf "  %-25s %-15s " "${topic}" "0"
        echo -e "${DIM}empty${NC}"
        continue
    fi
    
    TOTAL=0
    while IFS=: read -r _ _ offset; do
        if [ -n "${offset}" ] && [ "${offset}" -ge 0 ] 2>/dev/null; then
            TOTAL=$((TOTAL + offset))
        fi
    done <<< "${OFFSETS}"
    
    if [ "${TOTAL}" -gt 0 ]; then
        MSG_COLOR="${GREEN}"
        MSG_STATUS="● active"
    else
        MSG_COLOR="${DIM}"
        MSG_STATUS="empty"
    fi
    
    printf "  %-25s " "${topic}"
    echo -e "${MSG_COLOR}${TOTAL}${NC}               ${MSG_COLOR}${MSG_STATUS}${NC}"
done

# ══════════════════════════════════════════════════════════════
#  SECTION 4 — DLQ health check
# ══════════════════════════════════════════════════════════════
section "DEAD-LETTER QUEUE (DLQ) CHECK"

DLQ_OFFSETS=$(kafka_cmd kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list "${BOOTSTRAP}" \
    --topic "weather-dlq" \
    --time -1 2>/dev/null || echo "")

DLQ_COUNT=0
if [ -n "${DLQ_OFFSETS}" ]; then
    while IFS=: read -r _ _ offset; do
        if [ -n "${offset}" ] && [ "${offset}" -ge 0 ] 2>/dev/null; then
            DLQ_COUNT=$((DLQ_COUNT + offset))
        fi
    done <<< "${DLQ_OFFSETS}"
fi

if [ "${DLQ_COUNT}" -eq 0 ]; then
    echo -e "  ${GREEN}✓ DLQ is clean — 0 failed messages${NC}"
else
    echo -e "  ${RED}${BOLD}⚠  WARNING: ${DLQ_COUNT} message(s) in dead-letter queue!${NC}"
    echo -e "  ${RED}  Investigate with:${NC}"
    echo -e "  ${DIM}  docker exec ${KAFKA_CONTAINER} kafka-console-consumer \\${NC}"
    echo -e "  ${DIM}    --bootstrap-server ${BOOTSTRAP} \\${NC}"
    echo -e "  ${DIM}    --topic weather-dlq --from-beginning --max-messages 5${NC}"
fi

# ══════════════════════════════════════════════════════════════
#  SECTION 5 — Throughput estimate for weather-current
# ══════════════════════════════════════════════════════════════
section "THROUGHPUT — weather-current (estimated)"

# Get current offset
CURRENT_OFFSETS=$(kafka_cmd kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list "${BOOTSTRAP}" \
    --topic "weather-current" \
    --time -1 2>/dev/null || echo "")

CURRENT_TOTAL=0
if [ -n "${CURRENT_OFFSETS}" ]; then
    while IFS=: read -r _ _ offset; do
        if [ -n "${offset}" ] && [ "${offset}" -ge 0 ] 2>/dev/null; then
            CURRENT_TOTAL=$((CURRENT_TOTAL + offset))
        fi
    done <<< "${CURRENT_OFFSETS}"
fi

# Get offset from 1 hour ago (current time - 3600000 ms)
HOUR_AGO_MS=$(( $(date +%s) * 1000 - 3600000 ))
HOUR_AGO_OFFSETS=$(kafka_cmd kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list "${BOOTSTRAP}" \
    --topic "weather-current" \
    --time "${HOUR_AGO_MS}" 2>/dev/null || echo "")

HOUR_AGO_TOTAL=0
if [ -n "${HOUR_AGO_OFFSETS}" ]; then
    while IFS=: read -r _ _ offset; do
        if [ -n "${offset}" ] && [ "${offset}" -ge 0 ] 2>/dev/null; then
            HOUR_AGO_TOTAL=$((HOUR_AGO_TOTAL + offset))
        fi
    done <<< "${HOUR_AGO_OFFSETS}"
fi

MSGS_PER_HOUR=$((CURRENT_TOTAL - HOUR_AGO_TOTAL))

if [ "${MSGS_PER_HOUR}" -gt 0 ]; then
    echo -e "  Messages in last hour: ${GREEN}${BOLD}${MSGS_PER_HOUR}${NC}"
    echo -e "  Expected (221 cities × 4 fetches/hr): ${DIM}~884 msgs/hour${NC}"
    
    # Health assessment
    if [ "${MSGS_PER_HOUR}" -ge 800 ]; then
        echo -e "  Throughput health: ${GREEN}● HEALTHY${NC}"
    elif [ "${MSGS_PER_HOUR}" -ge 400 ]; then
        echo -e "  Throughput health: ${YELLOW}● DEGRADED${NC} (some cities may be failing)"
    else
        echo -e "  Throughput health: ${RED}● LOW${NC} (check producer logs)"
    fi
else
    echo -e "  ${DIM}No messages in the last hour (producer may not be running)${NC}"
    echo -e "  Expected: ~884 msgs/hour (221 cities × 4 fetches/hr)"
fi

# ══════════════════════════════════════════════════════════════
#  SECTION 6 — Docker services status
# ══════════════════════════════════════════════════════════════
section "DOCKER SERVICES"

printf "  ${BOLD}%-25s %-15s %-20s${NC}\n" "SERVICE" "STATUS" "PORTS"
divider

SERVICES=("meteo-zookeeper" "meteo-kafka" "meteo-postgres" "meteo-airflow")
PORTS=("2181" "9092" "5432" "8080")

for i in "${!SERVICES[@]}"; do
    svc="${SERVICES[$i]}"
    port="${PORTS[$i]}"
    
    STATUS=$(docker inspect --format='{{.State.Status}}' "${svc}" 2>/dev/null || echo "not found")
    HEALTH=$(docker inspect --format='{{.State.Health.Status}}' "${svc}" 2>/dev/null || echo "N/A")
    
    if [ "${STATUS}" = "running" ]; then
        if [ "${HEALTH}" = "healthy" ]; then
            STATUS_DISPLAY="${GREEN}● running (healthy)${NC}"
        elif [ "${HEALTH}" = "unhealthy" ]; then
            STATUS_DISPLAY="${RED}● running (unhealthy)${NC}"
        else
            STATUS_DISPLAY="${GREEN}● running${NC}"
        fi
    elif [ "${STATUS}" = "not found" ]; then
        STATUS_DISPLAY="${DIM}○ not started${NC}"
    else
        STATUS_DISPLAY="${RED}● ${STATUS}${NC}"
    fi
    
    printf "  %-25s " "${svc}"
    echo -e "${STATUS_DISPLAY}            :${port}"
done

# ══════════════════════════════════════════════════════════════
#  FOOTER
# ══════════════════════════════════════════════════════════════
echo ""
divider
echo -e "  ${DIM}Dashboard generated at ${TIMESTAMP}${NC}"
echo -e "  ${DIM}Run again: ./scripts/kafka_verify.sh${NC}"
echo ""
