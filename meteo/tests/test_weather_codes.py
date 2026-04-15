"""Tests for src/utils/weather_codes.py — WMO code → description mapping."""

from src.utils.weather_codes import get_description, WMO_CODES


class TestGetDescription:
    """get_description() should return the correct string for known codes."""

    def test_clear_sky(self):
        assert get_description(0) == "Clear sky"

    def test_partly_cloudy(self):
        assert get_description(2) == "Partly cloudy"

    def test_heavy_rain(self):
        assert get_description(65) == "Heavy rain"

    def test_thunderstorm(self):
        assert get_description(95) == "Thunderstorm"

    def test_thunderstorm_heavy_hail(self):
        assert get_description(99) == "Thunderstorm + heavy hail"

    def test_unknown_code_returns_fallback(self):
        result = get_description(999)
        assert "Unknown" in result
        assert "999" in result

    def test_negative_code_returns_fallback(self):
        result = get_description(-1)
        assert "Unknown" in result

    def test_all_known_codes_return_non_empty_string(self):
        for code in WMO_CODES:
            desc = get_description(code)
            assert isinstance(desc, str)
            assert len(desc) > 0
