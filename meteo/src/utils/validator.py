"""
validator.py — Message validation for Tunisian weather data.

Ensures all published messages are within physically plausible ranges
for Tunisia, and routes invalid messages to the Dead Letter Queue (DLQ).
"""

from datetime import datetime


# ──────────────────────────────────────────────────────────────
#  TUNISIA-SPECIFIC VALID RANGES
# ──────────────────────────────────────────────────────────────

TUNISIA_RANGES = {
    "temperature":   (-5,   55),
    "humidity":      (0,   100),
    "precipitation": (0,   200),
    "wind_speed":    (0,   150),
    "pressure":      (950, 1050),
    "forecast_day":  (0,     6),
}


def validate_message(msg: dict) -> tuple[bool, str]:
    """
    Validate a weather message against Tunisia-specific constraints.

    Returns:
        (True, '')           if the message is valid.
        (False, 'reason')    if the message fails validation.
    """
    # Range checks
    for field, (lo, hi) in TUNISIA_RANGES.items():
        val = msg.get(field)
        if val is None:
            continue  # nullable fields skip range check
        if not (lo <= val <= hi):
            return False, f"{field} out of range: {val} (expected {lo}-{hi})"

    # Required field checks
    required = ["city", "governorate", "region", "date",
                "temperature", "humidity", "data_type"]
    for f in required:
        if not msg.get(f):
            return False, f"missing required field: {f}"

    return True, ""


def build_dlq_message(original: dict, reason: str) -> dict:
    """
    Wrap a failed message with metadata for the Dead Letter Queue.

    Args:
        original: The message that failed validation.
        reason:   Human-readable explanation of why it failed.

    Returns:
        A DLQ envelope dict ready for publishing.
    """
    return {
        "reason":    reason,
        "data_type": original.get("data_type", "unknown"),
        "city":      original.get("city", "unknown"),
        "raw":       original,
        "failed_at": datetime.utcnow().isoformat() + "Z",
    }
