-- ====================================================================
-- OPTIONAL ANALYTICS VIEWS FOR TEMPRO COLLECTOR
-- Provides advanced environmental metrics (Absolute Humidity & Dew Point)
-- ====================================================================

CREATE OR REPLACE VIEW public.vw_environmental_metrics AS
 SELECT 
    m.time,
    l.name AS location_name,
    m.temperature,
    m.humidity_raw,
    -- 1. Absolute Humidity Calculation (g/m³) using Tetens formula
    (((((6.112)::double precision * exp((((17.67)::double precision * (m.temperature)::double precision) / ((m.temperature)::double precision + (243.5)::double precision)))) * (m.humidity_raw)::double precision) * (2.1674)::double precision) / ((273.15)::double precision + (m.temperature)::double precision)) AS absolute_humidity,
    
    -- 2. Dew Point Calculation (°C) using Magnus-Tetens approximation
    (((237.7)::double precision * ((((17.27)::double precision * (m.temperature)::double precision) / ((237.7)::double precision + (m.temperature)::double precision)) + (ln(((m.humidity_raw)::numeric / 100.0)))::double precision)) / ((17.27)::double precision - ((((17.27)::double precision * (m.temperature)::double precision) / ((237.7)::double precision + (m.temperature)::double precision)) + (ln(((m.humidity_raw)::numeric / 100.0)))::double precision))) AS dew_point
 FROM public.measures m
 -- Temporal join to ensure measurements map to the correct room at that specific time
 JOIN public.sensor_assignments sa 
   ON m.sensor_id = sa.sensor_id 
  AND m.time >= sa.assigned_at 
  AND (sa.removed_at IS NULL OR m.time <= sa.removed_at)
 JOIN public.locations l 
   ON sa.location_id = l.id;
