"""Tests for src/consumer.py — flush batch, reconnect, config, and mode logic."""

import logging
import time
from unittest.mock import MagicMock, patch, call

import pytest

from src.consumer import (
    _flush_batch,
    reconnect_db,
    send_to_dlq,
    TOPIC_CONFIG,
    HISTORICAL_SQL,
    CURRENT_SQL,
    FORECAST_SQL,
    BATCH_SIZE,
    DB_MAX_RETRIES,
)


# ── TOPIC_CONFIG ──────────────────────────────────────────────

class TestTopicConfig:

    def test_three_topics_configured(self):
        assert len(TOPIC_CONFIG) == 3

    def test_historical_topic_present(self):
        assert "weather-historical" in TOPIC_CONFIG

    def test_current_topic_present(self):
        assert "weather-current" in TOPIC_CONFIG

    def test_forecast_topic_present(self):
        assert "weather-forecast" in TOPIC_CONFIG

    def test_each_config_has_3_elements(self):
        for topic, config in TOPIC_CONFIG.items():
            assert len(config) == 3, f"{topic} config should have 3 elements"

    def test_group_ids_are_unique(self):
        group_ids = [cfg[0] for cfg in TOPIC_CONFIG.values()]
        assert len(group_ids) == len(set(group_ids))

    def test_table_names_are_unique(self):
        tables = [cfg[1] for cfg in TOPIC_CONFIG.values()]
        assert len(tables) == len(set(tables))


# ── SQL Statements ────────────────────────────────────────────

class TestSqlStatements:

    def test_historical_sql_has_upsert(self):
        assert "INSERT INTO weather_historical" in HISTORICAL_SQL
        assert "ON CONFLICT" in HISTORICAL_SQL

    def test_current_sql_has_upsert(self):
        assert "INSERT INTO weather_current" in CURRENT_SQL
        assert "ON CONFLICT (city)" in CURRENT_SQL

    def test_forecast_sql_has_upsert(self):
        assert "INSERT INTO weather_forecast" in FORECAST_SQL
        assert "ON CONFLICT (city, forecast_for)" in FORECAST_SQL


# ── _flush_batch ──────────────────────────────────────────────

class TestFlushBatch:

    @patch("src.consumer.execute_batch")
    def test_successful_flush(self, mock_exec_batch):
        conn = MagicMock()
        consumer = MagicMock()
        logger = logging.getLogger("test")
        batch = [{"city": "Tunis"}, {"city": "Sfax"}]

        result_conn, flushed = _flush_batch(
            conn, consumer, batch, "INSERT ...", "test-group", logger
        )
        assert result_conn is conn
        assert flushed == 2
        conn.commit.assert_called_once()
        consumer.commit.assert_called_once()

    @patch("src.consumer.send_to_dlq")
    def test_failed_flush_sends_to_dlq(self, mock_dlq):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        # Make execute_batch fail
        from psycopg2.extras import execute_batch
        cursor_ctx = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor_ctx
        cursor_ctx.side_effect = None

        # Simulate DB error by making the context manager's execute_batch raise
        conn.cursor.return_value.__enter__ = MagicMock(
            side_effect=Exception("DB error")
        )
        consumer = MagicMock()
        logger = logging.getLogger("test")
        batch = [{"city": "Tunis", "governorate": "Tunis"}]

        result_conn, flushed = _flush_batch(
            conn, consumer, batch, "INSERT ...", "test-group", logger
        )
        assert flushed == 0
        mock_dlq.assert_called_once()

    @patch("src.consumer.send_to_dlq")
    def test_flush_does_not_commit_kafka_on_failure(self, mock_dlq):
        conn = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(
            side_effect=Exception("DB error")
        )
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        consumer = MagicMock()
        logger = logging.getLogger("test")
        batch = [{"city": "Tunis", "governorate": "Tunis"}]

        _flush_batch(conn, consumer, batch, "INSERT ...", "test-group", logger)
        consumer.commit.assert_not_called()


# ── reconnect_db ──────────────────────────────────────────────

class TestReconnectDb:

    @patch("src.consumer.get_connection")
    @patch("time.sleep")
    def test_reconnect_succeeds_on_first_try(self, mock_sleep, mock_conn):
        mock_conn.return_value = MagicMock()
        logger = logging.getLogger("test")

        conn = reconnect_db(logger, "test-group")
        assert conn is not None
        assert mock_conn.call_count == 1

    @patch("src.consumer.get_connection")
    @patch("time.sleep")
    def test_reconnect_succeeds_on_retry(self, mock_sleep, mock_conn):
        mock_conn.side_effect = [Exception("fail"), MagicMock()]
        logger = logging.getLogger("test")

        conn = reconnect_db(logger, "test-group")
        assert conn is not None
        assert mock_conn.call_count == 2

    @patch("src.consumer.get_connection")
    @patch("time.sleep")
    def test_reconnect_returns_none_after_max_retries(self, mock_sleep, mock_conn):
        mock_conn.side_effect = Exception("always fail")
        logger = logging.getLogger("test")

        conn = reconnect_db(logger, "test-group")
        assert conn is None
        assert mock_conn.call_count == DB_MAX_RETRIES


# ── Constants ─────────────────────────────────────────────────

class TestConstants:

    def test_batch_size_positive(self):
        assert BATCH_SIZE > 0

    def test_max_retries_positive(self):
        assert DB_MAX_RETRIES > 0
