"""
db.py — Shared database utilities for the Météo Tunisie pipeline.

Used by:
  • src/consumer.py   — batch upserts from Kafka
  • dags/*.py         — Airflow analysis tasks (via get_engine / get_connection)

Provides connection pooling, SQLAlchemy engine, and pipeline run logging.
"""

import logging
import os
from datetime import datetime

import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch  # noqa: F401 — re-exported for consumers
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

logger = logging.getLogger("MeteoConsumer.db")

# ──────────────────────────────────────────────────────────────
#  DATABASE CONFIGURATION
# ──────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "meteo_db"),
    "user":     os.getenv("DB_USER",     "meteo"),
    "password": os.getenv("DB_PASSWORD", "meteo123"),
}

# Singleton connection pool (created on first call)
_pool = None

# Singleton SQLAlchemy engine
_engine = None


# ──────────────────────────────────────────────────────────────
#  1. SINGLE CONNECTION
# ──────────────────────────────────────────────────────────────

def get_connection():
    """
    Return a single psycopg2 connection from DB_CONFIG.

    Raises:
        RuntimeError: If the connection cannot be established.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        raise RuntimeError(
            f"Failed to connect to PostgreSQL at "
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} — {e}"
        ) from e


# ──────────────────────────────────────────────────────────────
#  2. CONNECTION POOL (singleton)
# ──────────────────────────────────────────────────────────────

def get_pool(minconn=2, maxconn=10):
    """
    Return a SimpleConnectionPool (singleton — created once, reused).

    Args:
        minconn: Minimum connections to keep open.
        maxconn: Maximum connections allowed.

    Returns:
        psycopg2.pool.SimpleConnectionPool
    """
    global _pool
    if _pool is None or _pool.closed:
        try:
            _pool = pool.SimpleConnectionPool(
                minconn, maxconn, **DB_CONFIG
            )
            logger.info(
                f"Connection pool created: min={minconn}, max={maxconn}"
            )
        except psycopg2.Error as e:
            raise RuntimeError(
                f"Failed to create connection pool: {e}"
            ) from e
    return _pool


# ──────────────────────────────────────────────────────────────
#  3. SQLALCHEMY ENGINE (for pandas / Airflow DAGs)
# ──────────────────────────────────────────────────────────────

def get_engine():
    """
    Return a SQLAlchemy engine for pandas read_sql() usage.

    Uses the singleton pattern — engine created once, reused.
    """
    global _engine
    if _engine is None:
        conn_str = (
            f"postgresql+psycopg2://"
            f"{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        )
        _engine = create_engine(conn_str, pool_size=5, max_overflow=10)
        logger.info("SQLAlchemy engine created")
    return _engine


# ──────────────────────────────────────────────────────────────
#  4. PIPELINE RUN LOGGING
# ──────────────────────────────────────────────────────────────

def log_pipeline_run(dag_name, task_name, started_at,
                     rows_processed, status, error_message=None):
    """
    Insert a row into the pipeline_runs table for observability.

    This is safe — wraps in try/except and never raises, so it
    cannot break the calling DAG or consumer.

    Args:
        dag_name:       Name of the DAG or consumer group.
        task_name:      Specific task within the DAG.
        started_at:     datetime when the task started.
        rows_processed: Number of rows processed.
        status:         'success' | 'failed' | 'partial'.
        error_message:  Optional error details.
    """
    try:
        finished_at = datetime.utcnow()
        duration_seconds = (finished_at - started_at).total_seconds()

        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs
                    (dag_name, task_name, started_at, finished_at,
                     duration_seconds, rows_processed, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (dag_name, task_name, started_at, finished_at,
                 duration_seconds, rows_processed, status, error_message),
            )
        conn.commit()
        close_connection(conn)
        logger.info(
            f"Pipeline run logged: {dag_name}/{task_name} — "
            f"{status} ({rows_processed} rows, {duration_seconds:.1f}s)"
        )
    except Exception as e:
        logger.warning(f"Failed to log pipeline run: {e}")


# ──────────────────────────────────────────────────────────────
#  5. SAFE CONNECTION CLOSE
# ──────────────────────────────────────────────────────────────

def close_connection(conn):
    """Safely close a psycopg2 connection, catching any exception."""
    if conn is None:
        return
    try:
        conn.close()
    except Exception as e:
        logger.warning(f"Error closing connection: {e}")
