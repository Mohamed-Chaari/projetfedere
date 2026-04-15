"""Shared Kafka connection config with optional SSL auto-detection."""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_KAFKA_BOOTSTRAP = (
    "kafka-projetfedere-projetfedere.l.aivencloud.com:21849"
)


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


def _resolve_cert_path(env_var: str, default_filename: str) -> Path:
    """Resolve cert/key path from env var or project root default."""
    configured = os.getenv(env_var)
    if configured:
        path = Path(configured).expanduser()
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        return path
    return PROJECT_ROOT / default_filename


def get_kafka_connection_config() -> tuple[str, dict]:
    """
    Return bootstrap + optional SSL config for kafka-python clients.

    SSL is enabled automatically when a CA file is present.
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", DEFAULT_KAFKA_BOOTSTRAP)

    ca_path = _resolve_cert_path("KAFKA_SSL_CA_FILE", "ca.pem")
    cert_path = _resolve_cert_path("KAFKA_SSL_CERT_FILE", "service.cert")
    key_path = _resolve_cert_path("KAFKA_SSL_KEY_FILE", "service.key")

    config: dict = {}
    if ca_path.exists():
        if not cert_path.exists() or not key_path.exists():
            raise FileNotFoundError(
                "SSL auto-detected via ca.pem, but service.cert and/or service.key "
                "were not found. Set KAFKA_SSL_CERT_FILE and KAFKA_SSL_KEY_FILE or "
                "provide files in project root."
            )
        config["security_protocol"] = "SSL"
        config["ssl_cafile"] = str(ca_path)
        config["ssl_certfile"] = str(cert_path)
        config["ssl_keyfile"] = str(key_path)

    return bootstrap, config
