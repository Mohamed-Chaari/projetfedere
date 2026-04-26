"""Tests for src/utils/validator.py — message validation & DLQ builder."""

import pytest
from src.utils.validator import validate_message, build_dlq_message, TUNISIA_RANGES


# ── Fixtures ──────────────────────────────────────────────────

def _valid_msg(**overrides) -> dict:
    """Return a minimal valid weather message, with optional overrides."""
    base = {
        "city": "Tunis",
        "governorate": "Tunis",
        "region": "Nord",
        "date": "2024-07-15",
        "temperature": 30.0,
        "humidity": 55.0,
        "data_type": "current",
        "precipitation": 0.0,
        "wind_speed": 12.0,
        "pressure": 1013.0,
    }
    base.update(overrides)
    return base


# ── validate_message ─────────────────────────────────────────

class TestValidateMessage:

    def test_valid_message_passes(self):
        valid, reason = validate_message(_valid_msg())
        assert valid is True
        assert reason == ""

    # ── Required field checks ─────────────────────────────────

    @pytest.mark.parametrize("field", [
        "city", "governorate", "region", "date",
        "temperature", "data_type",
    ])
    def test_missing_required_field_fails(self, field):
        msg = _valid_msg()
        del msg[field]
        valid, reason = validate_message(msg)
        assert valid is False
        assert field in reason

    @pytest.mark.parametrize("field", [
        "city", "governorate", "region", "date", "data_type",
    ])
    def test_empty_string_required_field_fails(self, field):
        """Non-numeric required fields should fail when set to empty string."""
        msg = _valid_msg(**{field: ""})
        valid, reason = validate_message(msg)
        assert valid is False

    # ── Range checks ──────────────────────────────────────────

    def test_temperature_below_range_fails(self):
        msg = _valid_msg(temperature=-10.0)
        valid, reason = validate_message(msg)
        assert valid is False
        assert "temperature" in reason

    def test_temperature_above_range_fails(self):
        msg = _valid_msg(temperature=60.0)
        valid, reason = validate_message(msg)
        assert valid is False
        assert "temperature" in reason

    def test_temperature_at_lower_bound_passes(self):
        lo, _ = TUNISIA_RANGES["temperature"]
        valid, _ = validate_message(_valid_msg(temperature=lo))
        assert valid is True

    def test_temperature_at_upper_bound_passes(self):
        _, hi = TUNISIA_RANGES["temperature"]
        valid, _ = validate_message(_valid_msg(temperature=hi))
        assert valid is True

    def test_humidity_out_of_range(self):
        valid, reason = validate_message(_valid_msg(humidity=105))
        assert valid is False
        assert "humidity" in reason

    def test_negative_precipitation_fails(self):
        valid, reason = validate_message(_valid_msg(precipitation=-1))
        assert valid is False
        assert "precipitation" in reason

    def test_wind_speed_out_of_range(self):
        valid, reason = validate_message(_valid_msg(wind_speed=200))
        assert valid is False
        assert "wind_speed" in reason

    def test_pressure_out_of_range_low(self):
        valid, reason = validate_message(_valid_msg(pressure=800))
        assert valid is False
        assert "pressure" in reason

    def test_pressure_out_of_range_high(self):
        valid, reason = validate_message(_valid_msg(pressure=1100))
        assert valid is False

    def test_forecast_day_out_of_range(self):
        valid, reason = validate_message(_valid_msg(forecast_day=7))
        assert valid is False
        assert "forecast_day" in reason

    # ── Nullable fields skip range check ──────────────────────

    def test_none_values_skip_range_check(self):
        msg = _valid_msg(precipitation=None, wind_speed=None, pressure=None)
        valid, _ = validate_message(msg)
        assert valid is True


# ── build_dlq_message ────────────────────────────────────────

class TestBuildDlqMessage:

    def test_dlq_contains_reason(self):
        original = _valid_msg()
        dlq = build_dlq_message(original, "test failure")
        assert dlq["reason"] == "test failure"

    def test_dlq_contains_original(self):
        original = _valid_msg()
        dlq = build_dlq_message(original, "err")
        assert dlq["raw"] == original

    def test_dlq_contains_data_type(self):
        dlq = build_dlq_message(_valid_msg(data_type="forecast"), "err")
        assert dlq["data_type"] == "forecast"

    def test_dlq_contains_city(self):
        dlq = build_dlq_message(_valid_msg(city="Sfax"), "err")
        assert dlq["city"] == "Sfax"

    def test_dlq_has_timestamp(self):
        dlq = build_dlq_message(_valid_msg(), "err")
        assert "failed_at" in dlq
        assert dlq["failed_at"].endswith("Z")

    def test_dlq_missing_data_type_defaults_unknown(self):
        dlq = build_dlq_message({"city": "X"}, "err")
        assert dlq["data_type"] == "unknown"

    def test_dlq_missing_city_defaults_unknown(self):
        dlq = build_dlq_message({}, "err")
        assert dlq["city"] == "unknown"
