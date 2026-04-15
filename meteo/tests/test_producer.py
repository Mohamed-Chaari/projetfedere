"""Tests for src/producer.py — message building, alerts, publish, and API fetch logic."""

import logging
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.producer import (
    build_message,
    check_and_publish_alerts,
    publish,
    fetch_with_retry,
    fetch_historical,
    fetch_current,
    fetch_owm_fallback,
    fetch_forecast,
    ALERT_THRESHOLDS,
    TOPICS,
)
from src.utils.weather_codes import get_description


# ── Fixtures ──────────────────────────────────────────────────

SAMPLE_CITY = {
    "name": "Tunis",
    "lat": 36.8065,
    "lon": 10.1815,
    "governorate": "Tunis",
    "region": "Nord",
}

SAMPLE_WEATHER_CURRENT = {
    "date": "2024-07-15",
    "temperature": 35.0,
    "temp_max": None,
    "temp_min": None,
    "feels_like": 38.0,
    "humidity": 45,
    "precipitation": 0.0,
    "wind_speed": 15.0,
    "wind_gusts": 25.0,
    "pressure": 1013.0,
    "weather_code": 0,
    "source": "open-meteo",
}

SAMPLE_WEATHER_HISTORICAL = {
    "date": "2022-06-01",
    "temperature": 28.5,
    "temp_max": 33.0,
    "temp_min": 24.0,
    "feels_like": 30.0,
    "humidity": 60,
    "precipitation": 2.5,
    "wind_speed": 10.0,
    "wind_gusts": None,
    "pressure": None,
    "weather_code": None,
    "source": "open-meteo-archive",
}


# ── build_message ─────────────────────────────────────────────

class TestBuildMessage:

    def test_current_message_has_all_keys(self):
        msg = build_message(SAMPLE_CITY, SAMPLE_WEATHER_CURRENT,
                            "current", "2024-07-15T12:00:00Z")
        expected_keys = {
            "message_id", "data_type", "cycle_id",
            "city", "governorate", "region", "latitude", "longitude",
            "date", "temperature", "temp_max", "temp_min", "feels_like",
            "humidity", "precipitation", "wind_speed", "wind_gusts",
            "pressure", "weather_code", "weather_desc",
            "forecast_day", "forecast_for", "precipitation_probability",
            "source", "ingested_at",
        }
        assert expected_keys.issubset(msg.keys())

    def test_current_message_city_mapping(self):
        msg = build_message(SAMPLE_CITY, SAMPLE_WEATHER_CURRENT,
                            "current", "cycle-1")
        assert msg["city"] == "Tunis"
        assert msg["governorate"] == "Tunis"
        assert msg["region"] == "Nord"
        assert msg["latitude"] == 36.8065
        assert msg["longitude"] == 10.1815

    def test_current_message_weather_values(self):
        msg = build_message(SAMPLE_CITY, SAMPLE_WEATHER_CURRENT,
                            "current", "cycle-1")
        assert msg["temperature"] == 35.0
        assert msg["humidity"] == 45
        assert msg["wind_speed"] == 15.0

    def test_data_type_set_correctly(self):
        msg = build_message(SAMPLE_CITY, SAMPLE_WEATHER_CURRENT,
                            "current", "cycle-1")
        assert msg["data_type"] == "current"

    def test_forecast_message_sets_forecast_fields(self):
        weather = {**SAMPLE_WEATHER_CURRENT, "precipitation_probability": 30}
        msg = build_message(SAMPLE_CITY, weather, "forecast",
                            "cycle-1", forecast_day=2)
        assert msg["forecast_day"] == 2
        assert msg["forecast_for"] is not None
        expected_date = (date.today() + timedelta(days=2)).isoformat()
        assert msg["forecast_for"] == expected_date

    def test_non_forecast_has_no_forecast_for(self):
        msg = build_message(SAMPLE_CITY, SAMPLE_WEATHER_CURRENT,
                            "current", "cycle-1")
        assert msg["forecast_for"] is None
        assert msg["forecast_day"] is None

    def test_message_id_contains_city_and_type(self):
        msg = build_message(SAMPLE_CITY, SAMPLE_WEATHER_CURRENT,
                            "current", "cycle-1")
        assert "tunis" in msg["message_id"]
        assert "current" in msg["message_id"]

    def test_weather_desc_populated_from_code(self):
        msg = build_message(SAMPLE_CITY, SAMPLE_WEATHER_CURRENT,
                            "current", "cycle-1")
        assert msg["weather_desc"] == get_description(0)
        assert msg["weather_desc"] == "Clear sky"

    def test_ingested_at_is_iso_utc(self):
        msg = build_message(SAMPLE_CITY, SAMPLE_WEATHER_CURRENT,
                            "current", "cycle-1")
        assert msg["ingested_at"].endswith("Z")

    def test_historical_message(self):
        msg = build_message(SAMPLE_CITY, SAMPLE_WEATHER_HISTORICAL,
                            "historical", "cycle-1")
        assert msg["data_type"] == "historical"
        assert msg["date"] == "2022-06-01"
        assert msg["source"] == "open-meteo-archive"


# ── check_and_publish_alerts ──────────────────────────────────

class TestCheckAndPublishAlerts:

    def test_heatwave_alert_triggered(self):
        producer = MagicMock()
        msg = build_message(SAMPLE_CITY,
                            {**SAMPLE_WEATHER_CURRENT, "temperature": 42.0},
                            "current", "cycle-1")
        logger = logging.getLogger("test")

        check_and_publish_alerts(producer, [msg], "cycle-1", logger)
        producer.send.assert_called()

        # Find the HEATWAVE alert call
        calls = producer.send.call_args_list
        alert_values = [c.kwargs.get("value") or c[1].get("value", c[1])
                        for c in calls]
        # The call is positional: send(topic, key=..., value=...)
        heatwave_found = any(
            call[1].get("value", {}).get("alert_type") == "HEATWAVE"
            for call in calls
        )
        assert heatwave_found, f"Expected HEATWAVE alert, got: {calls}"

    def test_no_alert_when_within_thresholds(self):
        producer = MagicMock()
        msg = build_message(SAMPLE_CITY, SAMPLE_WEATHER_CURRENT,
                            "current", "cycle-1")
        logger = logging.getLogger("test")

        check_and_publish_alerts(producer, [msg], "cycle-1", logger)
        producer.send.assert_not_called()

    def test_cold_snap_alert_triggered(self):
        producer = MagicMock()
        cold_weather = {**SAMPLE_WEATHER_CURRENT, "temperature": 1.0}
        msg = build_message(SAMPLE_CITY, cold_weather, "current", "cycle-1")
        logger = logging.getLogger("test")

        check_and_publish_alerts(producer, [msg], "cycle-1", logger)
        calls = producer.send.call_args_list
        cold_found = any(
            call[1].get("value", {}).get("alert_type") == "COLD_SNAP"
            for call in calls
        )
        assert cold_found

    def test_strong_wind_alert(self):
        producer = MagicMock()
        windy = {**SAMPLE_WEATHER_CURRENT, "wind_gusts": 90.0}
        msg = build_message(SAMPLE_CITY, windy, "current", "cycle-1")
        logger = logging.getLogger("test")

        check_and_publish_alerts(producer, [msg], "cycle-1", logger)
        calls = producer.send.call_args_list
        wind_found = any(
            call[1].get("value", {}).get("alert_type") == "STRONG_WIND"
            for call in calls
        )
        assert wind_found

    def test_heavy_rain_alert(self):
        producer = MagicMock()
        rainy = {**SAMPLE_WEATHER_CURRENT, "precipitation": 25.0}
        msg = build_message(SAMPLE_CITY, rainy, "current", "cycle-1")
        logger = logging.getLogger("test")

        check_and_publish_alerts(producer, [msg], "cycle-1", logger)
        calls = producer.send.call_args_list
        rain_found = any(
            call[1].get("value", {}).get("alert_type") == "HEAVY_RAIN"
            for call in calls
        )
        assert rain_found

    def test_none_values_do_not_trigger_alerts(self):
        producer = MagicMock()
        weather = {**SAMPLE_WEATHER_CURRENT,
                   "temperature": None, "wind_gusts": None,
                   "precipitation": None}
        msg = build_message(SAMPLE_CITY, weather, "current", "cycle-1")
        logger = logging.getLogger("test")

        check_and_publish_alerts(producer, [msg], "cycle-1", logger)
        producer.send.assert_not_called()


# ── publish ───────────────────────────────────────────────────

class TestPublish:

    @patch("src.producer.validate_message", return_value=(True, ""))
    def test_valid_message_published(self, mock_validate):
        producer = MagicMock()
        logger = logging.getLogger("test")
        msg = build_message(SAMPLE_CITY, SAMPLE_WEATHER_CURRENT,
                            "current", "cycle-1")

        result = publish(producer, TOPICS["current"], msg, "Tunis", logger)
        assert result is True
        producer.send.assert_called_once()

    @patch("src.producer.validate_message",
           return_value=(False, "temperature out of range"))
    @patch("src.producer.build_dlq_message",
           return_value={"reason": "err", "raw": {}})
    def test_invalid_message_sent_to_dlq(self, mock_dlq, mock_validate):
        producer = MagicMock()
        logger = logging.getLogger("test")
        msg = {"city": "Tunis", "temperature": 999}

        result = publish(producer, TOPICS["current"], msg, "Tunis", logger)
        assert result is False
        # Should be sent to DLQ topic, not the main topic
        producer.send.assert_called_once()
        call_args = producer.send.call_args
        assert call_args[0][0] == TOPICS["dlq"]


# ── fetch_with_retry ──────────────────────────────────────────

class TestFetchWithRetry:

    @patch("time.sleep")
    def test_successful_fetch(self, mock_sleep):
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"data": "ok"}
        response.raise_for_status = MagicMock()
        session.get.return_value = response
        logger = logging.getLogger("test")

        result = fetch_with_retry(session, "http://example.com", {}, logger)
        assert result == {"data": "ok"}

    @patch("time.sleep")
    def test_returns_none_after_3_failures(self, mock_sleep):
        session = MagicMock()
        session.get.side_effect = Exception("Network error")
        logger = logging.getLogger("test")

        result = fetch_with_retry(session, "http://example.com", {}, logger)
        assert result is None
        assert session.get.call_count == 3

    @patch("time.sleep")
    def test_rate_limit_retries(self, mock_sleep):
        session = MagicMock()
        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_ok = MagicMock()
        resp_ok.status_code = 200
        resp_ok.json.return_value = {"ok": True}
        resp_ok.raise_for_status = MagicMock()
        session.get.side_effect = [resp_429, resp_ok]
        logger = logging.getLogger("test")

        result = fetch_with_retry(session, "http://example.com", {}, logger)
        assert result == {"ok": True}


# ── fetch_historical ─────────────────────────────────────────

class TestFetchHistorical:

    @patch("src.producer.fetch_with_retry")
    def test_returns_records_from_api(self, mock_fetch):
        mock_fetch.return_value = {
            "daily": {
                "time": ["2022-01-01", "2022-01-02"],
                "temperature_2m_max": [15.0, 16.0],
                "temperature_2m_min": [8.0, 9.0],
                "apparent_temperature_max": [14.0, 15.0],
                "relativehumidity_2m_max": [70, 65],
                "precipitation_sum": [0.0, 1.2],
                "windspeed_10m_max": [20.0, 25.0],
            }
        }
        logger = logging.getLogger("test")
        session = MagicMock()

        records = fetch_historical(session, SAMPLE_CITY, logger)
        assert len(records) == 2
        assert records[0]["date"] == "2022-01-01"
        assert records[0]["temperature"] == round((15.0 + 8.0) / 2, 2)
        assert records[0]["source"] == "open-meteo-archive"

    @patch("src.producer.fetch_with_retry", return_value=None)
    def test_returns_empty_on_api_failure(self, mock_fetch):
        logger = logging.getLogger("test")
        session = MagicMock()
        records = fetch_historical(session, SAMPLE_CITY, logger)
        assert records == []


# ── fetch_current ─────────────────────────────────────────────

class TestFetchCurrent:

    @patch("src.producer.fetch_owm_fallback", return_value=None)
    @patch("src.producer.fetch_with_retry")
    def test_returns_current_weather(self, mock_fetch, mock_owm):
        mock_fetch.return_value = {
            "current": {
                "temperature_2m": 30.0,
                "relativehumidity_2m": 55,
                "apparent_temperature": 32.0,
                "precipitation": 0.0,
                "windspeed_10m": 12.0,
                "windgusts_10m": 20.0,
                "surface_pressure": 1013.0,
                "weathercode": 2,
            }
        }
        logger = logging.getLogger("test")
        session = MagicMock()

        result = fetch_current(session, SAMPLE_CITY, logger)
        assert result is not None
        assert result["temperature"] == 30.0
        assert result["source"] == "open-meteo"

    @patch("src.producer.fetch_owm_fallback", return_value=None)
    @patch("src.producer.fetch_with_retry", return_value=None)
    def test_falls_back_to_owm_on_failure(self, mock_fetch, mock_owm):
        logger = logging.getLogger("test")
        session = MagicMock()

        result = fetch_current(session, SAMPLE_CITY, logger)
        mock_owm.assert_called_once()


# ── fetch_forecast ────────────────────────────────────────────

class TestFetchForecast:

    @patch("src.producer.fetch_with_retry")
    def test_returns_7_day_forecast(self, mock_fetch):
        mock_fetch.return_value = {
            "daily": {
                "time": [f"2024-07-{15+i}" for i in range(7)],
                "temperature_2m_max": [35.0] * 7,
                "temperature_2m_min": [25.0] * 7,
                "apparent_temperature_max": [37.0] * 7,
                "precipitation_sum": [0.0] * 7,
                "precipitation_probability_max": [10] * 7,
                "windspeed_10m_max": [15.0] * 7,
                "windgusts_10m_max": [25.0] * 7,
                "weathercode": [0] * 7,
            }
        }
        logger = logging.getLogger("test")
        session = MagicMock()

        records = fetch_forecast(session, SAMPLE_CITY, logger)
        assert len(records) == 7
        assert records[0]["forecast_day"] == 0
        assert records[6]["forecast_day"] == 6
        assert records[0]["source"] == "open-meteo-forecast"

    @patch("src.producer.fetch_with_retry", return_value=None)
    def test_returns_empty_on_failure(self, mock_fetch):
        logger = logging.getLogger("test")
        session = MagicMock()
        records = fetch_forecast(session, SAMPLE_CITY, logger)
        assert records == []
