"""Tests for src/utils/kafka_config.py — Aiven SSL-only configuration."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.utils.kafka_config import (
    _detect_project_root,
    _resolve_cert_path,
)


class TestDetectProjectRoot:

    def test_returns_path_object(self):
        root = _detect_project_root()
        assert isinstance(root, Path)

    @patch.dict(os.environ, {"PROJECT_ROOT": "/tmp/test"})
    def test_respects_env_override(self):
        root = _detect_project_root()
        assert str(root).endswith("test")


class TestResolveCertPath:

    def test_missing_env_var_raises(self):
        """Should raise EnvironmentError if env var is not set."""
        with pytest.raises(EnvironmentError, match="Missing required"):
            _resolve_cert_path("NONEXISTENT_CERT_VAR_12345")

    @patch.dict(os.environ, {"TEST_CERT": "/nonexistent/path/cert.pem"})
    def test_missing_file_raises(self):
        """Should raise FileNotFoundError if cert file doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Certificate file not found"):
            _resolve_cert_path("TEST_CERT")


class TestGetKafkaConnectionConfig:

    @patch.dict(os.environ, {
        "KAFKA_BOOTSTRAP": "",
    }, clear=False)
    def test_missing_bootstrap_raises(self):
        """Should raise EnvironmentError if KAFKA_BOOTSTRAP is not set."""
        # Need to re-import to trigger the check
        from src.utils.kafka_config import get_kafka_connection_config
        with pytest.raises(EnvironmentError, match="KAFKA_BOOTSTRAP"):
            get_kafka_connection_config()

    @patch.dict(os.environ, {
        "KAFKA_BOOTSTRAP": "broker:9092",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "KAFKA_CA_FILE": "ca.pem",
        "KAFKA_CERT_FILE": "service.cert",
        "KAFKA_KEY_FILE": "service.key",
    }, clear=False)
    def test_non_ssl_protocol_raises(self):
        """Should reject anything other than SSL."""
        from src.utils.kafka_config import get_kafka_connection_config
        with pytest.raises(EnvironmentError, match="must be 'SSL'"):
            get_kafka_connection_config()

    @patch.dict(os.environ, {
        "KAFKA_BOOTSTRAP": "broker:9092",
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_CA_FILE": "ca.pem",
        "KAFKA_CERT_FILE": "service.cert",
        "KAFKA_KEY_FILE": "service.key",
    }, clear=False)
    def test_sasl_ssl_protocol_rejected(self):
        """SASL_SSL (Upstash-style) must be rejected — Aiven SSL only."""
        from src.utils.kafka_config import get_kafka_connection_config
        with pytest.raises(EnvironmentError, match="must be 'SSL'"):
            get_kafka_connection_config()

    @patch("src.utils.kafka_config._resolve_cert_path")
    @patch.dict(os.environ, {
        "KAFKA_BOOTSTRAP": "kafka-aiven.example.com:21849",
        "KAFKA_SECURITY_PROTOCOL": "SSL",
    }, clear=False)
    def test_valid_config_returns_ssl(self, mock_resolve):
        """With valid env + certs, should return SSL config dict."""
        mock_resolve.return_value = Path("/certs/file.pem")

        from src.utils.kafka_config import get_kafka_connection_config
        bootstrap, config = get_kafka_connection_config()

        assert bootstrap == "kafka-aiven.example.com:21849"
        assert config["security_protocol"] == "SSL"
        assert "ssl_cafile" in config
        assert "ssl_certfile" in config
        assert "ssl_keyfile" in config
