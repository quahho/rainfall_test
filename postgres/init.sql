CREATE DATABASE airflow_db;     
CREATE DATABASE metabase_db;    
CREATE DATABASE weather_db;     

\connect weather_db

ALTER DATABASE weather_db SET timezone = 'Asia/Singapore';

CREATE TABLE rainfall_readings (
    station_id VARCHAR(4) NOT NULL,
    value INTEGER NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    extraction_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (station_id, timestamp)
);

CREATE TABLE IF NOT EXISTS stations (
    id VARCHAR(4) PRIMARY KEY,
    name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    extraction_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE hourly_rainfall_readings (
    station_id VARCHAR(4) NOT NULL,
    value INTEGER NOT NULL,
    timestamp_hour TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (station_id, timestamp_hour)
);

CREATE MATERIALIZED VIEW hist_monthly_avg_rainfall AS
WITH total_monthly_rainfall AS (
    SELECT
        station_id,
        EXTRACT(YEAR FROM timestamp_hour) AS year,
        EXTRACT(MONTH FROM timestamp_hour) AS month,
        SUM(value) AS total_value
    FROM 
        hourly_rainfall_readings
    WHERE 
        timestamp_hour < DATE_TRUNC('month', NOW())  -- Only work on completed months
    GROUP BY 
		station_id,
        year,
        month
),
avg_monthly_rainfall AS (
    SELECT
        station_id,
        month,
        SUM(total_value) / COUNT(year) AS avg_value,
        NOW() AS updated_at
    FROM 
        total_monthly_rainfall
	GROUP BY 
		station_id,
        month
)
SELECT * FROM avg_monthly_rainfall;