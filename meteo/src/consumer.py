"""
consumer.py — Multi-mode Kafka consumer for the Météo Tunisie pipeline.

Consumes from 3 Kafka topics and upserts into PostgreSQL tables:
  weather-historical → weather_historical  (ON CONFLICT date, city)
  weather-current    → weather_current     (ON CONFLICT city — latest only)
  weather-forecast   → weather_forecast    (ON CONFLICT city, forecast_for)

Modes:
  continuous  — Run all 3 consumer groups as daemon threads (production)
  batch       — Drain all 3 topics then exit (Airflow / one-shot)
  topic       — Single topic via --topic flag (debugging)
"""

# ── Imports ──────────────────────────────────────────────────────
import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from psycopg2.extras import execute_batch

from src.utils.db import get_connection, close_connection

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

BATCH_SIZE            = 200
BATCH_TIMEOUT_SECONDS = 5.0
DB_RECONNECT_DELAY    = 5      # seconds between reconnect attempts
DB_MAX_RETRIES        = 3
STATS_LOG_INTERVAL    = 500    # log progress every N records

DLQ_TOPIC = "weather-dlq"

# ── Upsert SQL ────────────────────────────────────────────────────

HISTORICAL_SQL = """
INSERT INTO weather_historical
  (date, city, governorate, region, latitude, longitude,
   temperature, temp_max, temp_min, feels_like, humidity,
   precipitation, wind_speed, weather_code, weather_desc,
   source, ingested_at)
VALUES
  (%(date)s, %(city)s, %(governorate)s, %(region)s,
   %(latitude)s, %(longitude)s, %(temperature)s, %(temp_max)s,
   %(temp_min)s, %(feels_like)s, %(humidity)s, %(precipitation)s,
   %(wind_speed)s, %(weather_code)s, %(weather_desc)s,
   %(source)s, %(ingested_at)s)
ON CONFLICT (date, city) DO UPDATE SET
  temperature   = EXCLUDED.temperature,
  temp_max      = EXCLUDED.temp_max,
  temp_min      = EXCLUDED.temp_min,
  feels_like    = EXCLUDED.feels_like,
  humidity      = EXCLUDED.humidity,
  precipitation = EXCLUDED.precipitation,
  wind_speed    = EXCLUDED.wind_speed,
  source        = EXCLUDED.source;
"""

CURRENT_SQL = """
INSERT INTO weather_current
  (city, governorate, region, latitude, longitude,
   temperature, feels_like, humidity, precipitation,
   wind_speed, wind_gusts, pressure, weather_code, weather_desc,
   source, cycle_id, observed_at, ingested_at)
VALUES
  (%(city)s, %(governorate)s, %(region)s,
   %(latitude)s, %(longitude)s, %(temperature)s, %(feels_like)s,
   %(humidity)s, %(precipitation)s, %(wind_speed)s, %(wind_gusts)s,
   %(pressure)s, %(weather_code)s, %(weather_desc)s,
   %(source)s, %(cycle_id)s, %(date)s, %(ingested_at)s)
ON CONFLICT (city) DO UPDATE SET
  temperature   = EXCLUDED.temperature,
  feels_like    = EXCLUDED.feels_like,
  humidity      = EXCLUDED.humidity,
  precipitation = EXCLUDED.precipitation,
  wind_speed    = EXCLUDED.wind_speed,
  wind_gusts    = EXCLUDED.wind_gusts,
  pressure      = EXCLUDED.pressure,
  weather_code  = EXCLUDED.weather_code,
  weather_desc  = EXCLUDED.weather_desc,
  cycle_id      = EXCLUDED.cycle_id,
  observed_at   = EXCLUDED.observed_at,
  ingested_at   = EXCLUDED.ingested_at;
"""

FORECAST_SQL = """
INSERT INTO weather_forecast
  (city, governorate, region, latitude, longitude,
   forecast_for, forecast_day, temperature, temp_max, temp_min,
   feels_like, precipitation, precipitation_probability,
   wind_speed, wind_gusts, weather_code, weather_desc,
   source, cycle_id, ingested_at)
VALUES
  (%(city)s, %(governorate)s, %(region)s,
   %(latitude)s, %(longitude)s, %(forecast_for)s, %(forecast_day)s,
   %(temperature)s, %(temp_max)s, %(temp_min)s,
   %(feels_like)s, %(precipitation)s, %(precipitation_probability)s,
   %(wind_speed)s, %(wind_gusts)s, %(weather_code)s, %(weather_desc)s,
   %(source)s, %(cycle_id)s, %(ingested_at)s)
ON CONFLICT (city, forecast_for) DO UPDATE SET
  temperature                = EXCLUDED.temperature,
  temp_max                   = EXCLUDED.temp_max,
  temp_min                   = EXCLUDED.temp_min,
  feels_like                 = EXCLUDED.feels_like,
  precipitation              = EXCLUDED.precipitation,
  precipitation_probability  = EXCLUDED.precipitation_probability,
  wind_speed                 = EXCLUDED.wind_speed,
  wind_gusts                 = EXCLUDED.wind_gusts,
  weather_code               = EXCLUDED.weather_code,
  weather_desc               = EXCLUDED.weather_desc,
  cycle_id                   = EXCLUDED.cycle_id,
  ingested_at                = EXCLUDED.ingested_at;
"""

# Topic → (group_id, table_name, upsert_sql)
TOPIC_CONFIG = {
    "weather-historical": (
        "weather-historical-group",
        "weather_historical",
        HISTORICAL_SQL,
    ),
    "weather-current": (
        "weather-current-group",
        "weather_current",
        CURRENT_SQL,
    ),
    "weather-forecast": (
        "weather-forecast-group",
        "weather_forecast",
        FORECAST_SQL,
    ),
}


# ── Logging ───────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    """Configure console + rotating-file logging for the consumer."""
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger("MeteoConsumer")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers on re-import
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Rotating file handler (50 MB, keep 5 files)
    fh = RotatingFileHandler(
        "logs/consumer.log",
        maxBytes=50 * 1024 * 1024,
        backupCount=5,
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# ── Thread-safe DLQ Producer ──────────────────────────────────────

_dlq_producer = None
_dlq_lock = threading.Lock()


def _get_dlq_producer():
    """Lazily create a shared, thread-safe KafkaProducer for DLQ."""
    global _dlq_producer
    if _dlq_producer is None:
        with _dlq_lock:
            if _dlq_producer is None:
                _dlq_producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    acks="all",
                    retries=3,
                    compression_type="gzip",
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                )
    return _dlq_producer


def send_to_dlq(record: dict, error_reason: str, group_id: str):
    """Publish a failed record to the weather-dlq topic (thread-safe)."""
    try:
        dlq_msg = {
            "reason":    error_reason,
            "group_id":  group_id,
            "original":  record,
            "failed_at": datetime.utcnow().isoformat() + "Z",
        }
        producer = _get_dlq_producer()
        with _dlq_lock:
            producer.send(
                DLQ_TOPIC,
                key=record.get("governorate", "unknown"),
                value=dlq_msg,
            )
    except Exception as e:
        logging.getLogger("MeteoConsumer").error(
            f"[DLQ] Failed to publish to DLQ: {e}"
        )


# ── DB Reconnection ──────────────────────────────────────────────

def reconnect_db(logger, group_id: str):
    """
    Attempt to re-establish a PostgreSQL connection with retries.

    Returns:
        A new psycopg2 connection, or None if all retries failed.
    """
    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            logger.warning(
                f"[{group_id}] DB reconnect attempt {attempt}/{DB_MAX_RETRIES}"
            )
            time.sleep(DB_RECONNECT_DELAY)
            conn = get_connection()
            logger.info(f"[{group_id}] DB reconnected successfully")
            return conn
        except Exception as e:
            logger.error(f"[{group_id}] Reconnect attempt {attempt} failed: {e}")
    return None


# ── Core Consumer Loop ────────────────────────────────────────────

def consume_topic(topic: str, group_id: str, table: str,
                  upsert_sql: str, logger, batch_mode: bool = False):
    """
    Core consumption loop for a single topic → DB table.

    Args:
        topic:      Kafka topic to subscribe to.
        group_id:   Consumer group ID.
        table:      Target PostgreSQL table.
        upsert_sql: Parameterised INSERT ... ON CONFLICT SQL.
        logger:     Logger instance.
        batch_mode: If True, exit when topic is drained
                    (no new messages for 10s).
    """
    logger.info(
        f"[{group_id}] Starting consumer: {topic} → {table} "
        f"(batch_mode={batch_mode})"
    )

    # ── Create Kafka consumer ──
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=BATCH_SIZE,
        consumer_timeout_ms=10000 if batch_mode else -1,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    # ── Create DB connection ──
    conn = get_connection()

    batch = []
    last_flush = time.time()
    start_time = time.time()
    stats = {"inserted": 0, "errors": 0, "last_city": ""}

    try:
        for msg in consumer:
            record = msg.value
            batch.append(record)
            stats["last_city"] = record.get("city", "?")

            # Flush when batch full OR timeout reached
            should_flush = (
                len(batch) >= BATCH_SIZE
                or (time.time() - last_flush) >= BATCH_TIMEOUT_SECONDS
            )

            if should_flush and batch:
                conn, flushed = _flush_batch(
                    conn, consumer, batch, upsert_sql, group_id, logger
                )
                stats["inserted"] += flushed
                stats["errors"] += len(batch) - flushed if not flushed else 0
                batch.clear()
                last_flush = time.time()

                # Progress logging
                if stats["inserted"] % STATS_LOG_INTERVAL < BATCH_SIZE:
                    elapsed = time.time() - start_time
                    rate = stats["inserted"] / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"[{group_id}] Progress: {stats['inserted']:,} inserted | "
                        f"{stats['errors']} errors | {rate:.1f} rec/sec | "
                        f"last city: {stats['last_city']}"
                    )

                # If DB connection was lost and couldn't reconnect, exit thread
                if conn is None:
                    logger.critical(
                        f"[{group_id}] DB connection lost permanently — "
                        f"exiting thread"
                    )
                    return

        # End of consumer iterator (batch_mode timeout or continuous exit)
        # Flush remaining
        if batch:
            conn, flushed = _flush_batch(
                conn, consumer, batch, upsert_sql, group_id, logger
            )
            stats["inserted"] += flushed
            batch.clear()

    except Exception as e:
        logger.error(f"[{group_id}] Unexpected error: {e}", exc_info=True)
        # Flush remaining batch on crash
        if batch and conn:
            conn, flushed = _flush_batch(
                conn, consumer, batch, upsert_sql, group_id, logger
            )
            stats["inserted"] += flushed
            batch.clear()
    finally:
        consumer.close()
        close_connection(conn)

    # Final summary
    elapsed = time.time() - start_time
    minutes, seconds = divmod(int(elapsed), 60)
    logger.info(
        f"[{group_id}] FINISHED: {stats['inserted']:,} records | "
        f"{stats['errors']} errors | {minutes}m {seconds}s"
    )

    return stats


def _flush_batch(conn, consumer, batch, upsert_sql, group_id, logger):
    """
    Flush a batch of records to PostgreSQL.

    Returns:
        (conn, flushed_count): Updated connection + number of records flushed.
        If the DB fails and reconnection is exhausted, conn will be None.
    """
    try:
        with conn.cursor() as cur:
            execute_batch(cur, upsert_sql, batch, page_size=BATCH_SIZE)
        conn.commit()
        consumer.commit()
        flushed = len(batch)
        logger.debug(f"[{group_id}] Flushed {flushed} records")
        return conn, flushed

    except Exception as e:
        logger.error(f"[{group_id}] Batch insert failed: {e}")

        # Rollback the failed transaction
        try:
            conn.rollback()
        except Exception:
            pass

        # Send failed records to DLQ
        for rec in batch:
            send_to_dlq(rec, f"DB insert failed: {e}", group_id)

        # Check if connection is still alive
        try:
            conn.cursor().execute("SELECT 1")
        except Exception:
            # Connection dead — attempt reconnect
            close_connection(conn)
            conn = reconnect_db(logger, group_id)

        # DO NOT commit Kafka offset — messages will be reprocessed
        return conn, 0


# ── Mode: Continuous (threaded) ───────────────────────────────────

def run_continuous(logger):
    """Run all 3 consumer groups as daemon threads (production mode)."""
    threads = []
    for topic, (group_id, table, sql) in TOPIC_CONFIG.items():
        t = threading.Thread(
            target=consume_topic,
            args=(topic, group_id, table, sql, logger, False),
            daemon=True,
            name=f"{group_id}-thread",
        )
        threads.append(t)

    for t in threads:
        t.start()
        logger.info(f"Started thread: {t.name}")

    # Keep main thread alive, print health every 60s
    try:
        while True:
            time.sleep(60)
            logger.info("=== Consumer Health ===")
            all_alive = True
            for t in threads:
                alive = t.is_alive()
                status = "✅ alive" if alive else "❌ DEAD"
                logger.info(f"  {t.name}: {status}")
                if not alive:
                    all_alive = False
            if not all_alive:
                logger.critical(
                    "One or more consumer threads have died! "
                    "Check logs for details."
                )
    except KeyboardInterrupt:
        logger.info("Shutdown requested — waiting for threads…")


# ── Mode: Batch (drain & exit) ────────────────────────────────────

def run_batch(logger):
    """
    Drain all 3 topics then exit (Airflow-friendly).

    Runs each consumer sequentially, collects stats, prints summary table.
    """
    all_stats = {}

    for topic, (group_id, table, sql) in TOPIC_CONFIG.items():
        logger.info(f"{'='*50}")
        logger.info(f"Batch consuming: {topic}")
        logger.info(f"{'='*50}")
        stats = consume_topic(
            topic, group_id, table, sql, logger, batch_mode=True
        )
        all_stats[group_id] = stats or {"inserted": 0, "errors": 0}

    # Print summary table
    logger.info("")
    logger.info(f"{'='*65}")
    logger.info(f"  {'Group':<30} | {'Records':>8} | {'Errors':>6}")
    logger.info(f"  {'-'*30}-+-{'-'*8}-+-{'-'*6}")
    for group_id, stats in all_stats.items():
        logger.info(
            f"  {group_id:<30} | {stats['inserted']:>8,} | "
            f"{stats['errors']:>6}"
        )
    logger.info(f"{'='*65}")


# ── Mode: Single Topic ────────────────────────────────────────────

def run_single_topic(topic: str, logger, batch_mode: bool = False):
    """Run a single consumer group for debugging."""
    if topic not in TOPIC_CONFIG:
        logger.error(
            f"Unknown topic: {topic}. "
            f"Valid: {', '.join(TOPIC_CONFIG.keys())}"
        )
        sys.exit(1)

    group_id, table, sql = TOPIC_CONFIG[topic]
    consume_topic(topic, group_id, table, sql, logger, batch_mode=batch_mode)


# ── Graceful Shutdown ──────────────────────────────────────────────

def setup_shutdown(logger):
    """Register SIGINT/SIGTERM for clean shutdown."""
    def handler(sig, frame):
        logger.info("Shutdown signal received — cleaning up…")
        # Close DLQ producer if it exists
        global _dlq_producer
        if _dlq_producer:
            try:
                _dlq_producer.flush(timeout=5)
                _dlq_producer.close(timeout=5)
            except Exception:
                pass
        logger.info("Consumer shutdown complete. Goodbye.")
        sys.exit(0)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


# ── CLI Entry Point ────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Tunisia Meteo Consumer — Kafka → PostgreSQL"
    )
    parser.add_argument(
        "--mode", required=True,
        choices=["continuous", "batch", "topic"],
        help="Consumer operating mode",
    )
    parser.add_argument(
        "--topic",
        choices=["weather-historical", "weather-current", "weather-forecast"],
        help="Specific topic (required for --mode topic)",
    )
    args = parser.parse_args()

    if args.mode == "topic" and not args.topic:
        parser.error("--topic is required when --mode is 'topic'")

    logger = setup_logging()
    setup_shutdown(logger)

    logger.info(f"Starting Tunisia Meteo Consumer — mode: {args.mode}")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP} | Batch size: {BATCH_SIZE}")

    if args.mode == "continuous":
        run_continuous(logger)

    elif args.mode == "batch":
        run_batch(logger)

    elif args.mode == "topic":
        run_single_topic(args.topic, logger, batch_mode=True)


if __name__ == "__main__":
    main()
