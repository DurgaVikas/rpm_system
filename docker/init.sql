CREATE TABLE raw_ecg_readings (
    time TIMESTAMPTZ NOT NULL,
    patient_id VARCHAR(50) NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    reading_value DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('raw_ecg_readings', 'time');

CREATE TABLE processed_ecg_metrics (
    time TIMESTAMPTZ NOT NULL,
    patient_id VARCHAR(50) NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    heart_rate DOUBLE PRECISION,
    arrhythmia_detected BOOLEAN,
    anomaly_score DOUBLE PRECISION,
    signal_quality DOUBLE PRECISION
);

SELECT create_hypertable('processed_ecg_metrics', 'time');
