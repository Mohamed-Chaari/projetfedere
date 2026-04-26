"""Tests for src/utils/db.py — Supabase-only DB config and utilities."""

import os
import logging
from unittest.mock import MagicMock, patch

import pytest

from src.utils.db import (
    DB_CONFIG,
    close_connection,
    _validate_config,
)


# ── DB_CONFIG ─────────────────────────────────────────────────

class TestDBConfig:

    def test_has_required_keys(self):
        required = {"host", "port", "dbname", "user", "password"}
        assert required.issubset(DB_CONFIG.keys())

    def test_port_is_int(self):
        assert isinstance(DB_CONFIG["port"], int)

    def test_port_is_valid(self):
        assert 1 <= DB_CONFIG["port"] <= 65535


# ── _validate_config ──────────────────────────────────────────

class TestValidateConfig:

    def test_valid_config_passes(self):
        """With test env vars set by conftest, validation should pass."""
        _validate_config()  # Should not raise

    @patch.dict(os.environ, {"DB_HOST": ""}, clear=False)
    def test_missing_host_raises(self):
        """Should raise EnvironmentError if DB_HOST is empty."""
        from src.utils import db
        original = db.DB_CONFIG["host"]
        db.DB_CONFIG["host"] = ""
        try:
            with pytest.raises(EnvironmentError, match="DB_HOST"):
                _validate_config()
        finally:
            db.DB_CONFIG["host"] = original

    @patch.dict(os.environ, {"DB_PASSWORD": ""}, clear=False)
    def test_missing_password_raises(self):
        """Should raise EnvironmentError if DB_PASSWORD is empty."""
        from src.utils import db
        original = db.DB_CONFIG["password"]
        db.DB_CONFIG["password"] = ""
        try:
            with pytest.raises(EnvironmentError, match="DB_PASSWORD"):
                _validate_config()
        finally:
            db.DB_CONFIG["password"] = original


# ── close_connection ──────────────────────────────────────────

class TestCloseConnection:

    def test_close_with_valid_connection(self):
        conn = MagicMock()
        close_connection(conn)
        conn.close.assert_called_once()

    def test_close_with_none_does_not_crash(self):
        close_connection(None)  # Should not raise

    def test_close_with_exception_does_not_crash(self):
        conn = MagicMock()
        conn.close.side_effect = Exception("already closed")
        close_connection(conn)  # Should not raise
