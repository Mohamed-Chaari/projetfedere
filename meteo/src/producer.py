"""
producer.py — Tri-mode Kafka producer for 221 Tunisian cities.

Modes:
  historical  — One-time backfill: daily 2020-2024 from Open-Meteo Archive
  current     — Infinite 15-min loop: live conditions for all cities
  forecast    — Single run: 7-day forecast for all cities (Airflow-friendly)
  once        — Single current-weather cycle, then exit (testing)
  capitals    — Current mode limited to 24 governorate capitals (fast demo)
  all         — Historical backfill (background thread) + current loop

Topics:
  weather-historical  — Daily archive data (2020-2024)
  weather-current     — Live conditions every 15 min
  weather-forecast    — 7-day daily forecast
  weather-dlq         — Dead Letter Queue (failed validation)
  weather-alerts      — Threshold-based weather alerts
"""

# ── Imports ──────────────────────────────────────────────────────
import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, date
from logging.handlers import RotatingFileHandler

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

from src.utils.cities import (
    get_all_cities,
    get_governorate_capitals,
    get_cities_by_governorate,
    list_governorates,
)
from src.utils.weather_codes import get_description
from src.utils.validator import validate_message, build_dlq_message

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
OWM_API_KEY     = os.getenv("OWM_API_KEY", "")
START_DATE      = os.getenv("START_DATE", "2020-01-01")
END_DATE        = os.getenv("END_DATE",   "2024-12-31")
CYCLE_INTERVAL  = 900   # 15 minutes in seconds
FORECAST_HOURS  = 6     # forecast refresh interval

TOPICS = {
    "historical": "weather-historical",
    "current":    "weather-current",
    "forecast":   "weather-forecast",
    "dlq":        "weather-dlq",
    "alerts":     "weather-alerts",
}

ALERT_THRESHOLDS = {
    "HEATWAVE":    ("temperature",   40.0, "HIGH",   "°C",   "above"),
    "COLD_SNAP":   ("temperature",    2.0, "MEDIUM", "°C",   "below"),
    "STRONG_WIND": ("wind_gusts",    80.0, "HIGH",   "km/h", "above"),
    "HEAVY_RAIN":  ("precipitation", 20.0, "HIGH",   "mm",   "above"),
}


# ── Logging ───────────────────────────────────────────────────────

def setup_logging(mode: str) -> logging.Logger:
    """Configure console + rotating-file logging for the given mode."""
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger("MeteoProducer")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Rotating file handler (50 MB, keep 5 files)
    fh = RotatingFileHandler(
        f"logs/producer_{mode}.log",
        maxBytes=50 * 1024 * 1024,
        backupCount=5,
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# ── Kafka Producer Factory ─────────────────────────────────────────

def make_producer() -> KafkaProducer:
    """Create a production-grade KafkaProducer with gzip compression."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        retries=5,
        max_block_ms=30000,
        compression_type="gzip",
        batch_size=65536,
        linger_ms=20,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


# ── Shared Message Builder ─────────────────────────────────────────

def build_message(city: dict, weather: dict, data_type: str,
                  cycle_id: str, forecast_day: int = None) -> dict:
    """
    Build the unified message envelope for all 3 data types.

    The ``weather`` dict must have keys normalized before calling this
    (i.e. use the field names expected by the schema, not raw API keys).
    """
    forecast_for = None
    if forecast_day is not None:
        forecast_for = (date.today() + timedelta(days=forecast_day)).isoformat()

    return {
        # Identity
        "message_id": (
            f"{city['name'].lower()}-{data_type}-"
            f"{weather.get('date', forecast_for)}"
            + (f"-issued-{cycle_id}" if data_type == "forecast" else "")
        ),
        "data_type":  data_type,
        "cycle_id":   cycle_id,

        # Location
        "city":         city["name"],
        "governorate":  city["governorate"],
        "region":       city["region"],
        "latitude":     city["lat"],
        "longitude":    city["lon"],

        # Date
        "date": weather.get("date"),

        # Weather
        "temperature":   weather.get("temperature"),
        "temp_max":      weather.get("temp_max"),
        "temp_min":      weather.get("temp_min"),
        "feels_like":    weather.get("feels_like"),
        "humidity":      weather.get("humidity"),
        "precipitation": weather.get("precipitation"),
        "wind_speed":    weather.get("wind_speed"),
        "wind_gusts":    weather.get("wind_gusts"),
        "pressure":      weather.get("pressure"),
        "weather_code":  weather.get("weather_code"),
        "weather_desc":  get_description(weather.get("weather_code", -1)),

        # Forecast-only
        "forecast_day":  forecast_day,
        "forecast_for":  forecast_for,
        "precipitation_probability": weather.get("precipitation_probability"),

        # Metadata
        "source":       weather.get("source", "open-meteo"),
        "ingested_at":  datetime.utcnow().isoformat() + "Z",
    }


# ── API Fetchers ───────────────────────────────────────────────────

def fetch_with_retry(session: requests.Session, url: str,
                     params: dict, logger) -> dict | None:
    """Shared retry logic with exponential backoff for all API calls."""
    for attempt in range(3):
        try:
            resp = session.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                logger.warning("Rate limited — sleeping 60s")
                time.sleep(60)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            wait = 2 ** attempt
            logger.warning(
                f"Attempt {attempt + 1}/3 failed: {e} — retry in {wait}s"
            )
            time.sleep(wait)
    return None


def fetch_historical(session, city: dict, logger) -> list[dict]:
    """Fetch 2020-2024 daily data from Open-Meteo Archive API."""
    data = fetch_with_retry(
        session,
        "https://archive-api.open-meteo.com/v1/archive",
        {
            "latitude":   city["lat"],
            "longitude":  city["lon"],
            "start_date": START_DATE,
            "end_date":   END_DATE,
            "daily":      ",".join([
                "temperature_2m_max", "temperature_2m_min",
                "precipitation_sum", "windspeed_10m_max",
                "relativehumidity_2m_max", "apparent_temperature_max",
            ]),
            "timezone": "Africa/Tunis",
        },
        logger,
    )
    if not data:
        return []

    daily = data.get("daily", {})
    times = daily.get("time", [])
    records = []
    for i, d in enumerate(times):
        tmax = daily["temperature_2m_max"][i]
        tmin = daily["temperature_2m_min"][i]
        records.append({
            "date":         d,
            "temperature":  round((tmax + tmin) / 2, 2) if tmax and tmin else None,
            "temp_max":     tmax,
            "temp_min":     tmin,
            "feels_like":   daily.get("apparent_temperature_max",
                                      [None] * len(times))[i],
            "humidity":     daily.get("relativehumidity_2m_max",
                                      [None] * len(times))[i],
            "precipitation": daily.get("precipitation_sum",
                                       [None] * len(times))[i],
            "wind_speed":   daily.get("windspeed_10m_max",
                                      [None] * len(times))[i],
            "wind_gusts":   None,
            "pressure":     None,
            "weather_code": None,
            "source":       "open-meteo-archive",
        })
    return records


def fetch_current(session, city: dict, logger) -> dict | None:
    """Fetch live current conditions from Open-Meteo Forecast API."""
    data = fetch_with_retry(
        session,
        "https://api.open-meteo.com/v1/forecast",
        {
            "latitude":      city["lat"],
            "longitude":     city["lon"],
            "current":       ",".join([
                "temperature_2m", "relativehumidity_2m",
                "apparent_temperature", "precipitation",
                "windspeed_10m", "windgusts_10m",
                "surface_pressure", "weathercode",
            ]),
            "timezone":      "Africa/Tunis",
            "forecast_days": 1,
        },
        logger,
    )
    if not data:
        return fetch_owm_fallback(session, city, logger)

    c = data.get("current", {})
    return {
        "date":         date.today().isoformat(),
        "temperature":  c.get("temperature_2m"),
        "temp_max":     None,
        "temp_min":     None,
        "feels_like":   c.get("apparent_temperature"),
        "humidity":     c.get("relativehumidity_2m"),
        "precipitation": c.get("precipitation"),
        "wind_speed":   c.get("windspeed_10m"),
        "wind_gusts":   c.get("windgusts_10m"),
        "pressure":     c.get("surface_pressure"),
        "weather_code": c.get("weathercode"),
        "source":       "open-meteo",
    }


def fetch_owm_fallback(session, city: dict, logger) -> dict | None:
    """OpenWeatherMap fallback for current weather only."""
    if not OWM_API_KEY:
        return None
    data = fetch_with_retry(
        session,
        "https://api.openweathermap.org/data/2.5/weather",
        {
            "lat":   city["lat"],
            "lon":   city["lon"],
            "appid": OWM_API_KEY,
            "units": "metric",
        },
        logger,
    )
    if not data:
        return None

    logger.info(f"OWM fallback used for {city['name']}")
    return {
        "date":         date.today().isoformat(),
        "temperature":  data["main"]["temp"],
        "temp_max":     data["main"]["temp_max"],
        "temp_min":     data["main"]["temp_min"],
        "feels_like":   data["main"]["feels_like"],
        "humidity":     data["main"]["humidity"],
        "precipitation": data.get("rain", {}).get("1h", 0.0),
        "wind_speed":   data["wind"]["speed"] * 3.6,   # m/s → km/h
        "wind_gusts":   data["wind"].get("gust", 0) * 3.6,
        "pressure":     data["main"]["pressure"],
        "weather_code": data["weather"][0]["id"],
        "source":       "openweathermap",
    }


def fetch_forecast(session, city: dict, logger) -> list[dict]:
    """Fetch 7-day forecast from Open-Meteo Forecast API."""
    data = fetch_with_retry(
        session,
        "https://api.open-meteo.com/v1/forecast",
        {
            "latitude":      city["lat"],
            "longitude":     city["lon"],
            "daily":         ",".join([
                "temperature_2m_max", "temperature_2m_min",
                "precipitation_sum", "precipitation_probability_max",
                "windspeed_10m_max", "windgusts_10m_max",
                "apparent_temperature_max", "weathercode",
            ]),
            "timezone":      "Africa/Tunis",
            "forecast_days": 7,
        },
        logger,
    )
    if not data:
        return []

    daily = data.get("daily", {})
    records = []
    for i, d in enumerate(daily.get("time", [])):
        tmax = daily["temperature_2m_max"][i]
        tmin = daily["temperature_2m_min"][i]
        records.append({
            "date":         d,
            "forecast_day": i,
            "temperature":  round((tmax + tmin) / 2, 2) if tmax and tmin else None,
            "temp_max":     tmax,
            "temp_min":     tmin,
            "feels_like":   daily.get("apparent_temperature_max", [None] * 7)[i],
            "humidity":     None,
            "precipitation": daily.get("precipitation_sum", [None] * 7)[i],
            "precipitation_probability":
                            daily.get("precipitation_probability_max", [None] * 7)[i],
            "wind_speed":   daily.get("windspeed_10m_max", [None] * 7)[i],
            "wind_gusts":   daily.get("windgusts_10m_max", [None] * 7)[i],
            "pressure":     None,
            "weather_code": daily.get("weathercode", [None] * 7)[i],
            "source":       "open-meteo-forecast",
        })
    return records


# ── Alert Publisher ────────────────────────────────────────────────

def check_and_publish_alerts(producer, cycle_results: list[dict],
                             cycle_id: str, logger):
    """Scan current cycle results and publish alerts to weather-alerts."""
    for msg in cycle_results:
        for alert_type, (field, threshold, severity, unit, direction) in \
                ALERT_THRESHOLDS.items():
            val = msg.get(field)
            if val is None:
                continue
            triggered = (val > threshold if direction == "above"
                         else val < threshold)
            if triggered:
                alert = {
                    "alert_type":   alert_type,
                    "severity":     severity,
                    "city":         msg["city"],
                    "governorate":  msg["governorate"],
                    "region":       msg["region"],
                    "value":        val,
                    "threshold":    threshold,
                    "unit":         unit,
                    "triggered_at": cycle_id,
                    "cycle_id":     cycle_id,
                }
                producer.send(
                    TOPICS["alerts"],
                    key=msg["region"],
                    value=alert,
                )
                logger.warning(
                    f"🚨 ALERT {alert_type}: {msg['city']} "
                    f"{val}{unit} ({direction} {threshold})"
                )


# ── Publish Helper ─────────────────────────────────────────────────

def publish(producer, topic: str, msg: dict,
            partition_key: str, logger) -> bool:
    """Validate and publish a message, routing failures to the DLQ."""
    valid, reason = validate_message(msg)
    if not valid:
        dlq_msg = build_dlq_message(msg, reason)
        producer.send(TOPICS["dlq"], key=partition_key, value=dlq_msg)
        logger.warning(f"DLQ: {msg.get('city')} — {reason}")
        return False
    producer.send(topic, key=partition_key, value=msg)
    return True


# ── MODE 1: HISTORICAL ─────────────────────────────────────────────

def run_historical(producer, logger):
    """One-time backfill: 2020-2024, all 221 cities (resumable)."""
    session = requests.Session()
    progress_file = "data/historical_progress.json"

    # Load checkpoint (resumable)
    try:
        with open(progress_file) as f:
            progress = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        progress = {}

    governorates = list_governorates()
    total_sent = 0
    cycle_id = datetime.utcnow().isoformat() + "Z"

    for gov_idx, gov in enumerate(governorates):
        if progress.get(gov) == "done":
            logger.info(f"Skipping {gov} (already done)")
            continue

        cities = get_cities_by_governorate(gov)
        gov_sent = 0

        for city in cities:
            records = fetch_historical(session, city, logger)
            city_sent = 0
            for rec in records:
                msg = build_message(city, rec, "historical", cycle_id)
                if publish(producer, TOPICS["historical"],
                           msg, city["governorate"], logger):
                    city_sent += 1

            gov_sent += city_sent
            logger.info(
                f"[{gov_idx + 1}/{len(governorates)}] {gov} | "
                f"{city['name']}: {city_sent} records"
            )
            time.sleep(0.3)

        producer.flush()

        # Save checkpoint
        progress[gov] = "done"
        os.makedirs("data", exist_ok=True)
        with open(progress_file, "w") as f:
            json.dump(progress, f, indent=2)

        total_sent += gov_sent
        logger.info(f"✅ Governorate {gov} complete: {gov_sent} records")
        time.sleep(1)

    producer.flush()
    logger.info(f"🎉 Historical backfill complete: {total_sent} total records")


# ── MODE 2: CURRENT (real-time loop) ──────────────────────────────

def run_current(producer, logger, cities_fn=None):
    """Infinite 15-min loop: live conditions for all 221 cities."""
    session = requests.Session()
    cities = cities_fn() if cities_fn else get_all_cities()
    cycle_number = 0

    while True:
        cycle_start = datetime.utcnow()
        cycle_id    = cycle_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        cycle_number += 1
        cycle_results = []

        sent = failed = fallback = 0

        for city in cities:
            weather = fetch_current(session, city, logger)
            if not weather:
                failed += 1
                continue
            if weather.get("source") == "openweathermap":
                fallback += 1

            msg = build_message(city, weather, "current", cycle_id)
            if publish(producer, TOPICS["current"],
                       msg, city["governorate"], logger):
                sent += 1
                cycle_results.append(msg)
            else:
                failed += 1
            time.sleep(0.05)

        producer.flush()
        check_and_publish_alerts(producer, cycle_results, cycle_id, logger)

        elapsed = (datetime.utcnow() - cycle_start).total_seconds()
        if elapsed > 780:   # 13 minutes = breach risk
            logger.critical(
                f"Cycle took {elapsed:.0f}s — approaching 15-min interval!"
            )

        logger.info(
            f"Cycle #{cycle_number} | ✅ {sent} | ❌ {failed} "
            f"| ⚠ fallback {fallback} | ⏱ {elapsed:.1f}s"
        )

        sleep_time = max(0, CYCLE_INTERVAL - elapsed)
        logger.info(f"Next cycle in {sleep_time:.0f}s")
        time.sleep(sleep_time)


# ── MODE 3: FORECAST ──────────────────────────────────────────────

def run_forecast(producer, logger):
    """Single run: 7-day forecast for all 221 cities (called by Airflow)."""
    session = requests.Session()
    cities  = get_all_cities()
    cycle_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    total_sent = 0

    for city in cities:
        records = fetch_forecast(session, city, logger)
        for rec in records:
            msg = build_message(
                city, rec, "forecast", cycle_id,
                forecast_day=rec["forecast_day"],
            )
            if publish(producer, TOPICS["forecast"],
                       msg, city["governorate"], logger):
                total_sent += 1
        time.sleep(0.1)

    producer.flush()
    logger.info(
        f"Forecast run complete: {total_sent} messages "
        f"({len(cities)} cities × 7 days)"
    )


# ── Graceful Shutdown ──────────────────────────────────────────────

def setup_shutdown(producer, logger):
    """Register SIGINT/SIGTERM handlers for clean producer shutdown."""
    def handler(sig, frame):
        logger.info("Shutdown signal received — flushing producer...")
        producer.flush()
        producer.close()
        logger.info("Producer closed cleanly. Goodbye.")
        sys.exit(0)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


# ── CLI Entry Point ────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Tunisia Meteo Producer — tri-mode Kafka publisher"
    )
    parser.add_argument(
        "--mode", required=True,
        choices=["historical", "current", "forecast", "once", "capitals", "all"],
        help="Producer operating mode",
    )
    args = parser.parse_args()

    logger   = setup_logging(args.mode)
    producer = make_producer()
    setup_shutdown(producer, logger)

    logger.info(f"Starting Tunisia Meteo Producer — mode: {args.mode}")

    if args.mode == "historical":
        run_historical(producer, logger)

    elif args.mode == "current":
        run_current(producer, logger)

    elif args.mode == "forecast":
        run_forecast(producer, logger)

    elif args.mode == "once":
        # Single current cycle, then exit (testing)
        session = requests.Session()
        cities = get_all_cities()
        cycle_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        for city in cities:
            weather = fetch_current(session, city, logger)
            if weather:
                msg = build_message(city, weather, "current", cycle_id)
                publish(producer, TOPICS["current"],
                        msg, city["governorate"], logger)
            time.sleep(0.05)
        producer.flush()
        logger.info("Single cycle complete — exiting")

    elif args.mode == "capitals":
        # Current mode but 24 capitals only (fast demo)
        run_current(producer, logger, cities_fn=get_governorate_capitals)

    elif args.mode == "all":
        # Historical in thread + current loop in main
        import threading
        hist_thread = threading.Thread(
            target=run_historical, args=(producer, logger), daemon=True,
        )
        hist_thread.start()
        logger.info("Historical backfill running in background thread")
        time.sleep(5)   # let historical start first
        run_current(producer, logger)   # blocks forever


if __name__ == "__main__":
    main()
