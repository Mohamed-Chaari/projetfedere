"""
=============================================================================
validator.py — Message Validation for Tunisian Weather Data
=============================================================================

This module acts as the gatekeeper for our data pipeline. 

Measurements from public APIs can sometimes be glitched (e.g., a broken sensor 
reporting 900°C). This file ensures that all published messages are physically 
plausible specifically for Tunisia's climate.

If a message fails these checks, it gets routed to a "Dead Letter Queue" (DLQ)
so we can inspect it later without crashing the main data ingestion.
"""

from datetime import datetime, timezone


# ──────────────────────────────────────────────────────────────
#  TUNISIA-SPECIFIC VALID RANGES 
# ──────────────────────────────────────────────────────────────
# We define hard limits based on Tunisia's geography and climate history.
TUNISIA_RANGES = {
    "temperature":   (-5,   55),   # From snowy Ain Draham to high summer in the south
    "humidity":      (0,   100),   # 0% to 100% relative humidity
    "precipitation": (0,   200),   # Max realistic daily rainfall (mm)
    "wind_speed":    (0,   150),   # Up to violent storm winds (km/h)
    "pressure":      (850, 1050),  # Allowed down to 850hPa for high altitudes (Thala, Kesra)
    "forecast_day":  (0,     6),   # We only process up to 6 days into the future
}


def validate_message(msg: dict) -> tuple[bool, str]:
    """
    Validate a weather message against Tunisia-specific constraints.

    Returns:
        (True, '')           if the message is valid.
        (False, 'reason')    if the message fails validation.
    """
    # Step 1: Ensure all numerical readings fall within our defined realistic bounds
    for field, (lo, hi) in TUNISIA_RANGES.items():
        val = msg.get(field)
        
        # If the metric wasn't provided, we skip it (some APIs lack certain fields)
        if val is None:
            continue  
            
        # The sensor reading broke physics or climate norms!
        if not (lo <= val <= hi):
            return False, f"{field} out of range: {val} (expected {lo}-{hi})"

    # Step 2: Ensure the message has all the fundamental 'metadata' required to be useful
    required = ["city", "governorate", "region", "date",
                "temperature", "humidity", "data_type"]
    for f in required:
        if not msg.get(f):
            return False, f"missing required field: {f}"

    # If the message survived those checks, it is clean and ready for the database!
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
        "failed_at": datetime.now(timezone.utc).isoformat() + "Z",
    }
