"""Pytest configuration — set up cloud env vars for test collection.

Both producer.py and consumer.py call get_kafka_connection_config()
at module level, so we must have valid env vars before any imports.
This conftest runs before test collection and sets test-safe values.
"""

import os
import sys

# ── Set cloud env vars BEFORE any src imports ────────────────
# These enable module-level calls in producer.py / consumer.py
# to succeed during test collection. Actual Kafka/DB connections
# are mocked in individual tests.

_TEST_ENV = {
    # Aiven Kafka
    "KAFKA_BOOTSTRAP": "test-broker:9092",
    "KAFKA_SECURITY_PROTOCOL": "SSL",
    "KAFKA_CA_FILE": __file__,       # use this file as a "cert" stand-in
    "KAFKA_CERT_FILE": __file__,
    "KAFKA_KEY_FILE": __file__,
    # Supabase PostgreSQL
    "DB_HOST": "test-db.supabase.co",
    "DB_PORT": "5432",
    "DB_NAME": "test_db",
    "DB_USER": "test_user",
    "DB_PASSWORD": "test_password",
}

for key, value in _TEST_ENV.items():
    os.environ.setdefault(key, value)
