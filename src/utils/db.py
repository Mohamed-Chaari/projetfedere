"""
=============================================================================
db.py — Shared Database Utilities for the Météo Tunisie Pipeline
=============================================================================

This module is responsible for managing all outgoing connections to our 
Supabase PostgreSQL cloud database. 

It handles everything from creating single connections, to managing 
connection pools for heavy batch ingestion, to providing SQLAlchemy engines 
for analytical Airflow DAGs.

Important Architecture Note:
---------------------------
We connect exclusively to Supabase using their "Session Pooler" endpoint
(aws-1-eu-central-1.pooler.supabase.com) rather than the direct database URL.
This acts as an IPv4 proxy, which bypasses the IPv6 limitations on free-tier 
Supabase clusters and handles connection drops gracefully.

Required Environment Variables:
------------------------------
  DB_HOST      — Supabase pooler host 
  DB_PORT      — Port (Default: 5432 for session pooling)
  DB_NAME      — Database name (postgres)
  DB_USER      — Database user (postgres.[project_ref])
  DB_PASSWORD  — Database password
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
#  DATABASE CONFIGURATION — Supabase only, no local fallbacks
# ──────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME", "postgres"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
}

# Singleton connection pool (created on first call)
_pool = None

# Singleton SQLAlchemy engine
_engine = None


def _validate_config():
    """Validate that required Supabase env vars are set.

    Raises:
        EnvironmentError: If DB_HOST or DB_PASSWORD is missing.
    """
    if not DB_CONFIG["host"]:
        raise EnvironmentError(
            "Missing required environment variable: DB_HOST — "
            "Supabase host (e.g. db.xxxxx.supabase.co). "
            "Configure your .env with Supabase credentials."
        )
    if not DB_CONFIG["password"]:
        raise EnvironmentError(
            "Missing required environment variable: DB_PASSWORD — "
            "Supabase database password. "
            "Configure your .env with Supabase credentials."
        )


# ──────────────────────────────────────────────────────────────
#  1. SINGLE CONNECTION
# ──────────────────────────────────────────────────────────────

def get_connection():
    """
    Return a single psycopg2 connection to Supabase.

    Raises:
        EnvironmentError: If required env vars are missing.
        RuntimeError: If the connection cannot be established.
    """
    _validate_config()
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        raise RuntimeError(
            f"Failed to connect to Supabase PostgreSQL at "
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
    _validate_config()
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
    _validate_config()
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
