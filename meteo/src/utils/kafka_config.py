"""Kafka connection config — Aiven SSL only (cloud-native).

All connections require SSL with mutual TLS authentication.
Required environment variables:
  KAFKA_BOOTSTRAP           — Aiven broker address (host:port)
  KAFKA_SECURITY_PROTOCOL   — Must be 'SSL'
  KAFKA_CA_FILE             — Path to Aiven CA certificate (ca.pem)
  KAFKA_CERT_FILE           — Path to client certificate  (service.cert)
  KAFKA_KEY_FILE            — Path to client private key   (service.key)
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


# ──────────────────────────────────────────────────────────────
#  PROJECT ROOT DETECTION
# ──────────────────────────────────────────────────────────────

def _detect_project_root() -> Path:
    """Find project root from env or by walking up for common markers."""
    configured = os.getenv("PROJECT_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()

    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "requirements.txt").exists() or (parent / "docker-compose.yml").exists():
            return parent
    return current.parents[2]


PROJECT_ROOT = _detect_project_root()


# ──────────────────────────────────────────────────────────────
#  CERTIFICATE PATH RESOLUTION
# ──────────────────────────────────────────────────────────────

def _resolve_cert_path(env_var: str) -> Path:
    """Resolve a certificate path from the given env var.

    Raises:
        EnvironmentError: If the env var is not set.
        FileNotFoundError: If the resolved file does not exist.
    """
    raw = os.getenv(env_var)
    if not raw:
        raise EnvironmentError(
            f"Missing required environment variable: {env_var}. "
            f"Aiven SSL requires all certificate paths to be configured."
        )

    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path

    if not path.exists():
        raise FileNotFoundError(
            f"Certificate file not found: {path}  (from {env_var}={raw}). "
            f"Download the Aiven SSL certificates and update your .env."
        )
    return path


# ──────────────────────────────────────────────────────────────
#  PUBLIC API
# ──────────────────────────────────────────────────────────────

def get_kafka_connection_config() -> tuple[str, dict]:
    """Return (bootstrap_servers, ssl_config) for kafka-python clients.

    Enforces Aiven SSL — no plaintext, no SASL, no local fallback.

    Raises:
        EnvironmentError: If any required variable is missing.
        FileNotFoundError: If any certificate file is missing.
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP")
    if not bootstrap:
        raise EnvironmentError(
            "Missing required environment variable: KAFKA_BOOTSTRAP. "
            "Set it to your Aiven Kafka broker address (host:port)."
        )

    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL")
    if protocol != "SSL":
        raise EnvironmentError(
            f"KAFKA_SECURITY_PROTOCOL must be 'SSL' for Aiven, got '{protocol}'. "
            f"Only Aiven SSL connections are supported."
        )

    ca_path   = _resolve_cert_path("KAFKA_CA_FILE")
    cert_path = _resolve_cert_path("KAFKA_CERT_FILE")
    key_path  = _resolve_cert_path("KAFKA_KEY_FILE")

    config = {
        "security_protocol": "SSL",
        "ssl_cafile":   str(ca_path),
        "ssl_certfile": str(cert_path),
        "ssl_keyfile":  str(key_path),
    }

    return bootstrap, config
