-- Target Database Configuration
-- CREATE DATABASE thermopro;
-- \connect thermopro

-- Core Extensions
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 1. Locations Lookup Table
CREATE TABLE public.locations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- 2. Hardware Sensors Tracking Table
CREATE TABLE public.sensors (
    id SERIAL PRIMARY KEY,
    ble_id VARCHAR(50) NOT NULL UNIQUE,
    first_seen TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Temporal Assignments Junction Table (Handles room changes over time)
CREATE TABLE public.sensor_assignments (
    id SERIAL PRIMARY KEY,
    sensor_id INT NOT NULL REFERENCES public.sensors(id) ON DELETE CASCADE,
    location_id INT NOT NULL REFERENCES public.locations(id) ON DELETE CASCADE,
    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    removed_at TIMESTAMP WITH TIME ZONE DEFAULT NULL
);

-- 4. Time-Series Measures Table (TimescaleDB Core)
CREATE TABLE public.measures (
    time TIMESTAMP WITH TIME ZONE NOT NULL,
    sensor_id INT NOT NULL REFERENCES public.sensors(id) ON DELETE CASCADE,
    temperature NUMERIC(4, 2) NOT NULL,
    humidity_raw INT NOT NULL,
    battery_raw INT DEFAULT NULL,
    PRIMARY KEY (time, sensor_id)
);

-- 5. Convert to TimescaleDB Hypertable
SELECT create_hypertable('public.measures', 'time', chunk_time_interval => INTERVAL '90 days');

-- 6. Performance Optimization Indexes
CREATE INDEX idx_measures_sensor_time ON public.measures (sensor_id, time DESC);
CREATE INDEX idx_assignments_lookup ON public.sensor_assignments (sensor_id, assigned_at, removed_at);
