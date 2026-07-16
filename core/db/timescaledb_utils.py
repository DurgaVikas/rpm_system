"""
TimescaleDB Utilities for RPM System.

Provides reusable database connection management and insert/query functions
for both raw ECG readings and processed ECG metrics. This module is designed
to be imported by any service that needs to interact with TimescaleDB
(e.g., db_consumer, processing_engine, api).

Usage:
    from core.db.timescaledb_utils import TimescaleDBClient

    db = TimescaleDBClient()
    db.insert_raw_ecg_batch(sensor_id, vitals)
    db.insert_processed_ecg(sensor_id, heart_rate, signal_quality, ecg_clean)
    results = db.get_analytics_summary(sensor_id)
    db.close()
"""

import os
import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any

from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool, extras

# Load variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


class TimescaleDBClient:
    """Reusable TimescaleDB client with connection pooling for the RPM System."""

    def __init__(
        self,
        host: str = None,
        port: str = None,
        user: str = None,
        password: str = None,
        dbname: str = None,
        min_connections: int = 1,
        max_connections: int = 5,
    ):
        self.host = host or os.getenv("DB_HOST", "localhost")
        self.port = port or os.getenv("DB_PORT", "5432")
        self.user = user or os.getenv("DB_USER", "user")
        self.password = password or os.getenv("DB_PASSWORD", "password")
        self.dbname = dbname or os.getenv("DB_NAME", "rpm_db")

        self._pool = pool.SimpleConnectionPool(
            minconn=min_connections,
            maxconn=max_connections,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
        )
        logger.info(f"TimescaleDB connection pool created ({self.host}:{self.port}/{self.dbname})")

    # =========================================================================
    # Connection Management
    # =========================================================================

    def get_connection(self):
        """Get a connection from the pool."""
        return self._pool.getconn()

    def release_connection(self, conn):
        """Return a connection to the pool."""
        self._pool.putconn(conn)

    def close(self):
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()
            logger.info("TimescaleDB connection pool closed")

    # =========================================================================
    # Raw ECG Inserts
    # =========================================================================

    def insert_raw_ecg_batch(self, sensor_id: str, vitals: List[Dict]) -> int:
        """
        Batch insert raw ECG data points into the raw_ecg_readings hypertable.

        Args:
            sensor_id: The sensor/device identifier (e.g., "11:89:9A:A2:7D:5B")
            vitals: List of dicts with keys 'e' (ecg value) and 't' (epoch ms timestamp)
                    as received from the WebSocket payload.

        Returns:
            Number of rows inserted.
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            # Build rows, skipping null ECG values
            rows = []
            for v in vitals:
                ecg_value = v.get("e")
                if ecg_value is None:
                    continue
                timestamp_ms = v.get("t")
                if timestamp_ms:
                    ts = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                else:
                    ts = datetime.now(tz=timezone.utc)
                rows.append((ts, sensor_id, float(ecg_value)))

            if not rows:
                return 0

            extras.execute_values(
                cursor,
                "INSERT INTO raw_ecg_readings (time, sensor_id, ecg_value) VALUES %s",
                rows,
                page_size=500,
            )
            conn.commit()
            inserted = len(rows)
            logger.debug(f"Inserted {inserted} raw ECG readings for sensor {sensor_id}")
            return inserted
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting raw ECG batch for sensor {sensor_id}: {e}")
            raise
        finally:
            cursor.close()
            self.release_connection(conn)

    # =========================================================================
    # Processed ECG Inserts
    # =========================================================================

    def insert_processed_ecg(
        self,
        sensor_id: str,
        heart_rate: float,
        signal_quality: float,
        ecg_clean: List[float],
        timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Insert a processed ECG metrics row into the processed_ecg_metrics hypertable.

        Args:
            sensor_id: The sensor/device identifier.
            heart_rate: Average heart rate from NeuroKit2 processing.
            signal_quality: Average signal quality score.
            ecg_clean: List of cleaned ECG signal values.
            timestamp: Optional timestamp; defaults to current UTC time.
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            ts = timestamp or datetime.now(tz=timezone.utc)

            cursor.execute(
                """INSERT INTO processed_ecg_metrics
                   (time, sensor_id, heart_rate, signal_quality, ecg_clean)
                   VALUES (%s, %s, %s, %s, %s)""",
                (ts, sensor_id, heart_rate, signal_quality, ecg_clean),
            )
            conn.commit()
            logger.debug(f"Inserted processed ECG metrics for sensor {sensor_id} (HR={heart_rate})")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting processed ECG for sensor {sensor_id}: {e}")
            raise
        finally:
            cursor.close()
            self.release_connection(conn)

    # =========================================================================
    # Query Functions (used by REST API analytics)
    # =========================================================================

    def get_analytics_summary(
        self,
        sensor_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Get aggregated analytics for a given sensor over a time range.

        Returns:
            Dict with keys: avg_heart_rate, min_heart_rate, max_heart_rate,
            avg_signal_quality, total_readings, time_range.
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            query = """
                SELECT
                    COUNT(*)            AS total_readings,
                    AVG(heart_rate)     AS avg_heart_rate,
                    MIN(heart_rate)     AS min_heart_rate,
                    MAX(heart_rate)     AS max_heart_rate,
                    AVG(signal_quality) AS avg_signal_quality,
                    MIN(time)           AS earliest,
                    MAX(time)           AS latest
                FROM processed_ecg_metrics
                WHERE sensor_id = %s
            """
            params = [sensor_id]

            if start_time:
                query += " AND time >= %s"
                params.append(start_time)
            if end_time:
                query += " AND time <= %s"
                params.append(end_time)

            cursor.execute(query, params)
            row = cursor.fetchone()

            if row and row[0] > 0:
                return {
                    "sensor_id": sensor_id,
                    "total_readings": row[0],
                    "avg_heart_rate": round(float(row[1]), 2) if row[1] else 0.0,
                    "min_heart_rate": round(float(row[2]), 2) if row[2] else 0.0,
                    "max_heart_rate": round(float(row[3]), 2) if row[3] else 0.0,
                    "avg_signal_quality": round(float(row[4]), 4) if row[4] else 0.0,
                    "time_range": {
                        "earliest": row[5].isoformat() if row[5] else None,
                        "latest": row[6].isoformat() if row[6] else None,
                    },
                }
            return {
                "sensor_id": sensor_id,
                "total_readings": 0,
                "message": "No data found for this sensor.",
            }
        except Exception as e:
            logger.error(f"Error querying analytics for sensor {sensor_id}: {e}")
            raise
        finally:
            cursor.close()
            self.release_connection(conn)

    def get_anomalies(
        self,
        sensor_id: str,
        hr_low: float = 60.0,
        hr_high: float = 100.0,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get heart rate anomalies (readings outside the normal range) for a sensor.

        Args:
            sensor_id: The sensor identifier.
            hr_low: Lower threshold for normal heart rate (default 60 bpm).
            hr_high: Upper threshold for normal heart rate (default 100 bpm).

        Returns:
            List of anomaly records with time, heart_rate, and signal_quality.
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            query = """
                SELECT time, heart_rate, signal_quality
                FROM processed_ecg_metrics
                WHERE sensor_id = %s
                  AND (heart_rate < %s OR heart_rate > %s)
            """
            params = [sensor_id, hr_low, hr_high]

            if start_time:
                query += " AND time >= %s"
                params.append(start_time)
            if end_time:
                query += " AND time <= %s"
                params.append(end_time)

            query += " ORDER BY time DESC LIMIT 100"

            cursor.execute(query, params)
            rows = cursor.fetchall()

            return [
                {
                    "time": row[0].isoformat(),
                    "heart_rate": round(float(row[1]), 2),
                    "signal_quality": round(float(row[2]), 4) if row[2] else 0.0,
                    "anomaly_type": "bradycardia" if row[1] < hr_low else "tachycardia",
                }
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Error querying anomalies for sensor {sensor_id}: {e}")
            raise
        finally:
            cursor.close()
            self.release_connection(conn)

    def get_raw_ecg_timeseries(
        self,
        sensor_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 5000,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve raw ECG time-series data for a sensor within a time range.

        Returns:
            List of dicts with 'time' and 'ecg_value'.
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            query = """
                SELECT time, ecg_value
                FROM raw_ecg_readings
                WHERE sensor_id = %s
            """
            params = [sensor_id]

            if start_time:
                query += " AND time >= %s"
                params.append(start_time)
            if end_time:
                query += " AND time <= %s"
                params.append(end_time)

            query += " ORDER BY time DESC LIMIT %s"
            params.append(limit)

            cursor.execute(query, params)
            rows = cursor.fetchall()

            return [
                {"time": row[0].isoformat(), "ecg_value": float(row[1])}
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Error querying raw ECG for sensor {sensor_id}: {e}")
            raise
        finally:
            cursor.close()
            self.release_connection(conn)
