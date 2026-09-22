CREATE OR REPLACE VIEW public.v_measures_enhanced AS
WITH base_data AS (
    SELECT 
        m."time",
        m.sensor_id,
        s.ble_id,
        l.name AS location_name,
        m.temperature,
        m.humidity_raw AS humidity,
        m.battery_raw AS battery,
        6.112::double precision * exp(17.67::double precision * m.temperature / (m.temperature + 243.5::double precision)) AS es,
        m.humidity_raw::double precision / 100.0::double precision AS rh_ratio,
        -- Inertie thermique : Moyenne glissante de la température extérieure sur 12h
        ext.temp_ext_smoothed AS temp_ext
    FROM measures m
    JOIN sensors s ON m.sensor_id = s.id
    JOIN sensor_assignments sa ON s.id = sa.sensor_id AND m."time" >= sa.assigned_at AND (sa.removed_at IS NULL OR m."time" <= sa.removed_at)
    JOIN locations l ON sa.location_id = l.id
    LEFT JOIN LATERAL (
        SELECT avg(m_ext.temperature) AS temp_ext_smoothed
        FROM measures m_ext
        JOIN sensor_assignments sa_ext ON m_ext.sensor_id = sa_ext.sensor_id AND m_ext."time" >= sa_ext.assigned_at AND (sa_ext.removed_at IS NULL OR m_ext."time" <= sa_ext.removed_at)
        JOIN locations l_ext ON sa_ext.location_id = l_ext.id
        WHERE l_ext.name = 'Ernage' 
          AND m_ext."time" <= m."time"
          AND m_ext."time" >= m."time" - interval '12 hours'
    ) ext ON l.name != 'Ernage'
), 
intermediate_metrics AS (
    SELECT 
        "time", sensor_id, ble_id, location_name, temperature, humidity, battery, es, rh_ratio,
        ln(rh_ratio) + 17.67::double precision * temperature / (243.5::double precision + temperature) AS gamma,
        temp_ext,
        temp_ext + 0.3::double precision * (temperature - temp_ext) AS t_paroi
    FROM base_data
), 
health_metrics AS (
    SELECT 
        "time", sensor_id, ble_id, location_name, temperature, humidity, battery, es, rh_ratio, gamma, temp_ext, t_paroi,
        6.112::double precision * exp(17.67::double precision * t_paroi / (t_paroi + 243.5::double precision)) AS es_paroi
    FROM intermediate_metrics
),
calculated_metrics AS (
    SELECT 
        "time", sensor_id, ble_id, location_name, temperature, humidity, battery, es, rh_ratio, gamma, temp_ext, t_paroi, es_paroi,
        (rh_ratio * (es / es_paroi) * 100.0) AS rh_paroi
    FROM health_metrics
)
SELECT 
    "time",
    sensor_id,
    ble_id,
    location_name,
    temperature,
    humidity,
    battery,
    round((243.5::double precision * gamma / (17.67::double precision - gamma))::numeric, 2) AS dew_point,
    round((temperature + (5.0 / 9.0)::double precision * (es * rh_ratio - 10.0::double precision))::numeric, 1) AS humidex,
    round((es * rh_ratio * 216.7::double precision / (temperature + 273.15::double precision))::numeric, 2) AS absolute_humidity,
    round(t_paroi::numeric, 2) AS t_paroi,
    round(rh_paroi::numeric, 1) AS rh_paroi,
    -- Score amorti avec 100 et 0 très rares
    CASE
        WHEN temp_ext IS NULL OR location_name = 'Ernage' THEN NULL
        ELSE GREATEST(0, LEAST(100, round(
            (CASE
                WHEN rh_paroi >= 95.0 THEN 0.0
                WHEN rh_paroi >= 80.0 THEN 20.0 - (rh_paroi - 80.0) * 1.333
                WHEN rh_paroi >= 65.0 THEN 70.0 - (rh_paroi - 65.0) * 3.333
                WHEN rh_paroi >= 45.0 THEN 100.0 - (rh_paroi - 45.0) * 1.500
                ELSE 100.0
            END) 
            - GREATEST(0.0, (16.0 - temperature) * 2.0)
        )))::integer
    END AS health_score
FROM calculated_metrics;