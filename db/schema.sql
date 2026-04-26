-- ============================================================
--  MÉTÉO TUNISIE — Complete Database Schema
--  TimescaleDB on PostgreSQL 15
--  Auto-runs via /docker-entrypoint-initdb.d/01_schema.sql
-- ============================================================

-- ============================================================
-- EXTENSIONS
-- ============================================================

-- ============================================================
-- TABLE 1: weather_historical
-- Raw daily data from Open-Meteo Archive API (2020-2024)
-- One row per (date, city) — 221 cities x 1826 days = ~403,546 rows
-- ============================================================
CREATE TABLE IF NOT EXISTS weather_historical (
    id              BIGSERIAL,
    date            DATE          NOT NULL,
    city            VARCHAR(100)  NOT NULL,
    governorate     VARCHAR(100)  NOT NULL,
    region          VARCHAR(50)   NOT NULL,
    latitude        NUMERIC(8,5)  NOT NULL,
    longitude       NUMERIC(8,5)  NOT NULL,
    temperature     NUMERIC(5,2),
    temp_max        NUMERIC(5,2),
    temp_min        NUMERIC(5,2),
    feels_like      NUMERIC(5,2),
    humidity        NUMERIC(5,2)  CHECK (humidity    BETWEEN 0 AND 100),
    precipitation   NUMERIC(6,2)  CHECK (precipitation >= 0),
    wind_speed      NUMERIC(5,2)  CHECK (wind_speed  >= 0),
    weather_code    SMALLINT,
    weather_desc    VARCHAR(100),
    source          VARCHAR(30)   NOT NULL DEFAULT 'open-meteo-archive',
    ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, date),
    UNIQUE (date, city)
);

-- Convert to TimescaleDB hypertable (partition by date, 3-month chunks)
SELECT create_hypertable(
    'weather_historical', 'date',
    chunk_time_interval => INTERVAL '3 months',
    if_not_exists => TRUE
);

-- ============================================================
-- TABLE 2: weather_current
-- Live conditions — ONE ROW PER CITY, always overwritten
-- 221 rows total (latest reading per city)
-- ============================================================
CREATE TABLE IF NOT EXISTS weather_current (
    id              BIGSERIAL     PRIMARY KEY,
    city            VARCHAR(100)  NOT NULL,
    governorate     VARCHAR(100)  NOT NULL,
    region          VARCHAR(50)   NOT NULL,
    latitude        NUMERIC(8,5)  NOT NULL,
    longitude       NUMERIC(8,5)  NOT NULL,
    temperature     NUMERIC(5,2),
    feels_like      NUMERIC(5,2),
    humidity        NUMERIC(5,2)  CHECK (humidity    BETWEEN 0 AND 100),
    precipitation   NUMERIC(6,2)  CHECK (precipitation >= 0),
    wind_speed      NUMERIC(5,2)  CHECK (wind_speed  >= 0),
    wind_gusts      NUMERIC(5,2),
    pressure        NUMERIC(7,2)  CHECK (pressure    BETWEEN 850 AND 1100),
    weather_code    SMALLINT,
    weather_desc    VARCHAR(100),
    source          VARCHAR(30)   NOT NULL DEFAULT 'open-meteo',
    cycle_id        VARCHAR(30),
    observed_at     TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (city)
);

-- ============================================================
-- TABLE 3: weather_forecast
-- 7-day predictions — ONE ROW PER (city, forecast_for)
-- 221 cities x 7 days = 1,547 rows, refreshed every 6h
-- ============================================================
CREATE TABLE IF NOT EXISTS weather_forecast (
    id                         BIGSERIAL     PRIMARY KEY,
    city                       VARCHAR(100)  NOT NULL,
    governorate                VARCHAR(100)  NOT NULL,
    region                     VARCHAR(50)   NOT NULL,
    latitude                   NUMERIC(8,5)  NOT NULL,
    longitude                  NUMERIC(8,5)  NOT NULL,
    forecast_for               DATE          NOT NULL,
    forecast_day               SMALLINT      CHECK (forecast_day BETWEEN 0 AND 6),
    temperature                NUMERIC(5,2),
    temp_max                   NUMERIC(5,2),
    temp_min                   NUMERIC(5,2),
    feels_like                 NUMERIC(5,2),
    precipitation              NUMERIC(6,2)  CHECK (precipitation >= 0),
    precipitation_probability  SMALLINT      CHECK (precipitation_probability BETWEEN 0 AND 100),
    wind_speed                 NUMERIC(5,2)  CHECK (wind_speed >= 0),
    wind_gusts                 NUMERIC(5,2),
    weather_code               SMALLINT,
    weather_desc               VARCHAR(100),
    source                     VARCHAR(30)   NOT NULL DEFAULT 'open-meteo-forecast',
    cycle_id                   VARCHAR(30),
    ingested_at                TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (city, forecast_for)
);

-- ============================================================
-- TABLE 4: weather_alerts
-- Real-time alerts generated after each current cycle
-- ============================================================
CREATE TABLE IF NOT EXISTS weather_alerts (
    id            BIGSERIAL     PRIMARY KEY,
    alert_type    VARCHAR(30)   NOT NULL,
    severity      VARCHAR(20)   NOT NULL CHECK (severity IN ('LOW','MEDIUM','HIGH','CRITICAL')),
    city          VARCHAR(100)  NOT NULL,
    governorate   VARCHAR(100)  NOT NULL,
    region        VARCHAR(50)   NOT NULL,
    value         NUMERIC(7,2)  NOT NULL,
    threshold     NUMERIC(7,2)  NOT NULL,
    unit          VARCHAR(10)   NOT NULL,
    triggered_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    cycle_id      VARCHAR(30)
);

-- ============================================================
-- TABLE 5: monthly_averages
-- Computed by Airflow DAG monthly_analysis
-- One row per (year, month, city) — fully recomputed each run
-- ============================================================
CREATE TABLE IF NOT EXISTS monthly_averages (
    id            BIGSERIAL     PRIMARY KEY,
    year          SMALLINT      NOT NULL,
    month         SMALLINT      NOT NULL CHECK (month BETWEEN 1 AND 12),
    city          VARCHAR(100)  NOT NULL,
    governorate   VARCHAR(100)  NOT NULL,
    region        VARCHAR(50)   NOT NULL,
    avg_temp      NUMERIC(5,2),
    max_temp      NUMERIC(5,2),
    min_temp      NUMERIC(5,2),
    avg_humidity  NUMERIC(5,2),
    avg_precip    NUMERIC(6,2),
    avg_wind      NUMERIC(5,2),
    record_count  SMALLINT,
    computed_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (year, month, city)
);

-- ============================================================
-- TABLE 6: temperature_peaks
-- Detected by Airflow DAG peak_detection (mean + 2*std threshold)
-- ============================================================
CREATE TABLE IF NOT EXISTS temperature_peaks (
    id               BIGSERIAL     PRIMARY KEY,
    date             DATE          NOT NULL,
    city             VARCHAR(100)  NOT NULL,
    governorate      VARCHAR(100)  NOT NULL,
    region           VARCHAR(50)   NOT NULL,
    temperature      NUMERIC(5,2)  NOT NULL,
    mean_temp        NUMERIC(5,2)  NOT NULL,
    std_temp         NUMERIC(5,2)  NOT NULL,
    threshold        NUMERIC(5,2)  NOT NULL,
    z_score          NUMERIC(6,3)  NOT NULL,
    severity         VARCHAR(20)   NOT NULL CHECK (severity IN ('moderate','high','extreme')),
    detection_method VARCHAR(30)   NOT NULL DEFAULT 'global',
    detected_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (date, city)
);

-- ============================================================
-- TABLE 7: correlations
-- Pearson matrix computed by Airflow DAG correlation_job
-- 6 unique pairs x optional seasonality
-- ============================================================
CREATE TABLE IF NOT EXISTS correlations (
    id              BIGSERIAL     PRIMARY KEY,
    variable_a      VARCHAR(30)   NOT NULL,
    variable_b      VARCHAR(30)   NOT NULL,
    pearson_r       NUMERIC(7,4)  NOT NULL,
    p_value         NUMERIC(10,8),
    interpretation  VARCHAR(50),
    is_significant  BOOLEAN       NOT NULL DEFAULT FALSE,
    season          VARCHAR(10)   DEFAULT 'annual',
    computed_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (variable_a, variable_b, season)
);

-- ============================================================
-- TABLE 8: annual_stats
-- Yearly aggregates computed by Airflow DAG annual_stats
-- ============================================================
CREATE TABLE IF NOT EXISTS annual_stats (
    id                  BIGSERIAL     PRIMARY KEY,
    year                SMALLINT      NOT NULL,
    city                VARCHAR(100)  NOT NULL,
    governorate         VARCHAR(100)  NOT NULL,
    region              VARCHAR(50)   NOT NULL,
    avg_temp            NUMERIC(5,2),
    max_temp            NUMERIC(5,2),
    min_temp            NUMERIC(5,2),
    total_precip        NUMERIC(8,2),
    avg_wind            NUMERIC(5,2),
    avg_humidity        NUMERIC(5,2),
    hot_days_count      SMALLINT,
    cold_days_count     SMALLINT,
    rainy_days_count    SMALLINT,
    dry_days_count      SMALLINT,
    data_completeness   NUMERIC(5,2),
    temp_trend_per_year NUMERIC(7,4),
    computed_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (year, city)
);

-- ============================================================
-- TABLE 9: data_quality_log
-- Daily quality report from Airflow data_quality_dag
-- ============================================================
CREATE TABLE IF NOT EXISTS data_quality_log (
    id                   BIGSERIAL   PRIMARY KEY,
    check_date           DATE        NOT NULL,
    layer                VARCHAR(20) NOT NULL CHECK (layer IN ('historical','current','forecast')),
    total_rows           INT,
    missing_values_count INT         DEFAULT 0,
    out_of_range_count   INT         DEFAULT 0,
    duplicate_count      INT         DEFAULT 0,
    quality_score        NUMERIC(5,2) CHECK (quality_score BETWEEN 0 AND 100),
    issues_detail        TEXT,
    checked_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- TABLE 10: pipeline_runs
-- Observability — every DAG run logged here via db.log_pipeline_run()
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              BIGSERIAL     PRIMARY KEY,
    dag_name        VARCHAR(100)  NOT NULL,
    task_name       VARCHAR(100),
    started_at      TIMESTAMPTZ   NOT NULL,
    finished_at     TIMESTAMPTZ,
    duration_seconds INT,
    rows_processed  INT           DEFAULT 0,
    status          VARCHAR(20)   NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running','success','failed','skipped')),
    error_message   TEXT
);

-- ============================================================
-- INDEXES — optimised for the query patterns in FastAPI + Airflow
-- ============================================================

-- weather_historical (most queried table)
CREATE INDEX IF NOT EXISTS idx_hist_city
    ON weather_historical (city);
CREATE INDEX IF NOT EXISTS idx_hist_governorate
    ON weather_historical (governorate);
CREATE INDEX IF NOT EXISTS idx_hist_region
    ON weather_historical (region);
CREATE INDEX IF NOT EXISTS idx_hist_year
    ON weather_historical (EXTRACT(YEAR  FROM date));
CREATE INDEX IF NOT EXISTS idx_hist_month
    ON weather_historical (EXTRACT(MONTH FROM date));
CREATE INDEX IF NOT EXISTS idx_hist_city_date
    ON weather_historical (city, date DESC);

-- weather_current
CREATE INDEX IF NOT EXISTS idx_curr_governorate
    ON weather_current (governorate);
CREATE INDEX IF NOT EXISTS idx_curr_region
    ON weather_current (region);
CREATE INDEX IF NOT EXISTS idx_curr_ingested
    ON weather_current (ingested_at DESC);

-- weather_forecast
CREATE INDEX IF NOT EXISTS idx_fcst_city_date
    ON weather_forecast (city, forecast_for);
CREATE INDEX IF NOT EXISTS idx_fcst_forecast_day
    ON weather_forecast (forecast_day);

-- weather_alerts
CREATE INDEX IF NOT EXISTS idx_alert_type
    ON weather_alerts (alert_type);
CREATE INDEX IF NOT EXISTS idx_alert_city
    ON weather_alerts (city);
CREATE INDEX IF NOT EXISTS idx_alert_triggered
    ON weather_alerts (triggered_at DESC);

-- analysis results
CREATE INDEX IF NOT EXISTS idx_monthly_city_year
    ON monthly_averages (city, year, month);
CREATE INDEX IF NOT EXISTS idx_monthly_region
    ON monthly_averages (region, year);
CREATE INDEX IF NOT EXISTS idx_peaks_city_date
    ON temperature_peaks (city, date DESC);
CREATE INDEX IF NOT EXISTS idx_peaks_severity
    ON temperature_peaks (severity);
CREATE INDEX IF NOT EXISTS idx_annual_city_year
    ON annual_stats (city, year);

-- ============================================================
-- VIEWS — pre-joined for FastAPI endpoints (zero computation at query time)
-- ============================================================

-- v_national_current: latest conditions for all 221 cities
-- Used by: GET /api/dashboard/summary
CREATE OR REPLACE VIEW v_national_current AS
SELECT
    city, governorate, region, latitude, longitude,
    temperature, feels_like, humidity, precipitation,
    wind_speed, wind_gusts, pressure,
    weather_code, weather_desc, observed_at, cycle_id
FROM weather_current
ORDER BY governorate, city;

-- v_national_summary: min/max/avg across all cities (one row)
-- Used by: GET /api/dashboard/summary (KPIs)
CREATE OR REPLACE VIEW v_national_summary AS
SELECT
    COUNT(*)                             AS cities_reporting,
    ROUND(AVG(temperature)::numeric, 1)  AS avg_temp_national,
    MAX(temperature)                     AS max_temp,
    MIN(temperature)                     AS min_temp,
    city_max.city                        AS hottest_city,
    city_min.city                        AS coldest_city,
    ROUND(AVG(humidity)::numeric, 1)     AS avg_humidity,
    ROUND(SUM(precipitation)::numeric,1) AS total_precip,
    MAX(observed_at)                     AS last_updated
FROM weather_current
CROSS JOIN LATERAL (
    SELECT city FROM weather_current
    ORDER BY temperature DESC NULLS LAST LIMIT 1
) city_max
CROSS JOIN LATERAL (
    SELECT city FROM weather_current
    ORDER BY temperature ASC  NULLS LAST LIMIT 1
) city_min;

-- v_forecast_7days: today's 7-day national forecast (all cities)
-- Used by: GET /api/forecast/{city}
CREATE OR REPLACE VIEW v_forecast_7days AS
SELECT
    city, governorate, region,
    forecast_for, forecast_day,
    temperature, temp_max, temp_min, feels_like,
    precipitation, precipitation_probability,
    wind_speed, wind_gusts,
    weather_code, weather_desc, cycle_id
FROM weather_forecast
WHERE forecast_for >= CURRENT_DATE
ORDER BY city, forecast_day;

-- v_monthly_trend: national monthly averages across all cities
-- Used by: GET /api/analysis/monthly
CREATE OR REPLACE VIEW v_monthly_trend AS
SELECT
    year, month,
    governorate, region,
    ROUND(AVG(avg_temp)::numeric,  2)  AS avg_temp,
    ROUND(AVG(avg_humidity)::numeric, 2) AS avg_humidity,
    ROUND(AVG(avg_precip)::numeric, 2) AS avg_precip,
    ROUND(AVG(avg_wind)::numeric,   2) AS avg_wind,
    SUM(record_count)                  AS total_records
FROM monthly_averages
GROUP BY year, month, governorate, region
ORDER BY year, month, governorate;

-- v_active_alerts: alerts from last 24 hours
-- Used by: GET /api/alerts/active
CREATE OR REPLACE VIEW v_active_alerts AS
SELECT
    alert_type, severity, city, governorate, region,
    value, threshold, unit, triggered_at, cycle_id
FROM weather_alerts
WHERE triggered_at >= NOW() - INTERVAL '24 hours'
ORDER BY triggered_at DESC;

-- ============================================================
-- TIMESCALEDB: enable compression on weather_historical
-- (chunks older than 6 months are compressed automatically)
-- ============================================================
ALTER TABLE weather_historical SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'date DESC',
    timescaledb.compress_segmentby = 'governorate'
);

SELECT add_compression_policy(
    'weather_historical',
    INTERVAL '6 months',
    if_not_exists => TRUE
);

-- ============================================================
-- SEED DATA: Tunisia regions reference (for validation)
-- ============================================================
CREATE TABLE IF NOT EXISTS tunisia_regions (
    region      VARCHAR(50) PRIMARY KEY,
    description VARCHAR(100)
);

INSERT INTO tunisia_regions (region, description) VALUES
    ('Nord',         'Nord de la Tunisie'),
    ('Nord-Est',     'Nord-Est (Cap Bon, Nabeul)'),
    ('Nord-Ouest',   'Nord-Ouest (Beja, Jendouba)'),
    ('Centre',       'Centre (Sousse, Monastir, Mahdia)'),
    ('Centre-Ouest', 'Centre-Ouest (Kairouan, Kasserine, Sidi Bouzid)'),
    ('Sud-Est',      'Sud-Est (Gabes, Medenine, Tataouine)'),
    ('Sud-Ouest',    'Sud-Ouest (Gafsa, Tozeur, Kebili)')
ON CONFLICT (region) DO NOTHING;
