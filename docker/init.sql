-- =============================================================================
-- RPM System - TimescaleDB Schema Initialization
-- =============================================================================
-- This script runs automatically when the TimescaleDB container starts for the
-- first time. It creates hypertables optimized for time-series ECG data.
-- =============================================================================

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- =============================================================================
-- Table: raw_ecg_readings
-- Stores individual raw ECG data points received from sensors via WebSocket.
-- Each row represents a single (timestamp, value) pair from the vitals array.
-- =============================================================================
CREATE TABLE IF NOT EXISTS raw_ecg_readings (
    time            TIMESTAMPTZ         NOT NULL,
    sensor_id       VARCHAR(50)         NOT NULL,
    ecg_value       DOUBLE PRECISION    NOT NULL
);

SELECT create_hypertable('raw_ecg_readings', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_raw_sensor_time ON raw_ecg_readings (sensor_id, time DESC);

-- =============================================================================
-- Table: processed_ecg_metrics
-- Stores processed ECG results produced by the Processing Engine after
-- NeuroKit2 analysis. One row per processing batch (per sensor submission).
-- =============================================================================
CREATE TABLE IF NOT EXISTS processed_ecg_metrics (
    time            TIMESTAMPTZ         NOT NULL,
    sensor_id       VARCHAR(50)         NOT NULL,
    heart_rate      DOUBLE PRECISION    NOT NULL DEFAULT 0.0,
    signal_quality  DOUBLE PRECISION    NOT NULL DEFAULT 0.0,
    ecg_clean       DOUBLE PRECISION[]
);

SELECT create_hypertable('processed_ecg_metrics', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_processed_sensor_time ON processed_ecg_metrics (sensor_id, time DESC);
