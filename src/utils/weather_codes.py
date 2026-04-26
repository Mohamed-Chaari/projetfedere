"""
weather_codes.py — WMO Weather interpretation codes (WW) mapping.

Reference: https://www.nodc.noaa.gov/archive/arc0021/0002199/1.1/data/0-data/
           WMO code table 4677 (simplified to daily codes used by Open-Meteo).
"""

# ──────────────────────────────────────────────────────────────
#  WMO WEATHER CODE → HUMAN DESCRIPTION
# ──────────────────────────────────────────────────────────────

WMO_CODES = {
    0:  "Clear sky",
    1:  "Mainly clear",
    2:  "Partly cloudy",
    3:  "Overcast",
    45: "Foggy",
    48: "Icy fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    71: "Slight snow",
    73: "Moderate snow",
    75: "Heavy snow",
    77: "Snow grains",
    80: "Slight showers",
    81: "Moderate showers",
    82: "Violent showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm + hail",
    99: "Thunderstorm + heavy hail",
}


def get_description(code: int) -> str:
    """Return a human-readable description for a WMO weather code."""
    return WMO_CODES.get(code, f"Unknown (code {code})")
