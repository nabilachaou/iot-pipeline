

-- =====================================================
-- 0. Détermination de l'heure cible (dernière heure complète)
-- =====================================================
-- Toutes les requêtes ci-dessous utilisent la même fenêtre
-- [hour_start, hour_start + 1h[, calculée une seule fois via CTE
-- pour garantir la cohérence entre toutes les tables.

-- =====================================================
-- 1. dim_date : s'assurer que la date cible existe
-- =====================================================

INSERT INTO dim_date (date_key, full_date, year, month, day, day_of_week, week_of_year, month_name, is_weekend)
SELECT
    (EXTRACT(YEAR FROM d)::INTEGER * 10000
        + EXTRACT(MONTH FROM d)::INTEGER * 100
        + EXTRACT(DAY FROM d)::INTEGER)                    AS date_key,
    d::DATE                                                 AS full_date,
    EXTRACT(YEAR FROM d)::INTEGER                           AS year,
    EXTRACT(MONTH FROM d)::INTEGER                          AS month,
    EXTRACT(DAY FROM d)::INTEGER                            AS day,
    EXTRACT(ISODOW FROM d)::INTEGER                         AS day_of_week,
    EXTRACT(WEEK FROM d)::INTEGER                            AS week_of_year,
    TO_CHAR(d, 'TMMonth')                                    AS month_name,
    EXTRACT(ISODOW FROM d) IN (6, 7)                         AS is_weekend
FROM (SELECT date_trunc('hour', now() - interval '1 hour')::DATE AS d) AS x
ON CONFLICT (date_key) DO NOTHING;


-- =====================================================
-- 2. dim_equipment : s'assurer que tous les équipements
--    présents dans la fenêtre cible existent déjà
-- =====================================================

INSERT INTO dim_equipment (equipment_id, equipment_type)
SELECT DISTINCT equipment_id, 'ELEVATOR'
FROM elevator_data_clean
WHERE event_timestamp >= date_trunc('hour', now() - interval '1 hour')
  AND event_timestamp <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
ON CONFLICT (equipment_id) DO NOTHING;

INSERT INTO dim_equipment (equipment_id, equipment_type)
SELECT DISTINCT equipment_id, 'RMG'
FROM rmg_data_clean
WHERE event_timestamp >= date_trunc('hour', now() - interval '1 hour')
  AND event_timestamp <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
ON CONFLICT (equipment_id) DO NOTHING;

INSERT INTO dim_equipment (equipment_id, equipment_type)
SELECT DISTINCT equipment_id, 'RTG'
FROM rtg_data_clean
WHERE event_timestamp >= date_trunc('hour', now() - interval '1 hour')
  AND event_timestamp <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
ON CONFLICT (equipment_id) DO NOTHING;

INSERT INTO dim_equipment (equipment_id, equipment_type)
SELECT DISTINCT equipment_id, 'STS'
FROM sts_data_clean
WHERE event_timestamp >= date_trunc('hour', now() - interval '1 hour')
  AND event_timestamp <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
ON CONFLICT (equipment_id) DO NOTHING;

INSERT INTO dim_equipment (equipment_id, equipment_type)
SELECT DISTINCT equipment_id, 'STRADDLE'
FROM straddle_data_clean
WHERE event_timestamp >= date_trunc('hour', now() - interval '1 hour')
  AND event_timestamp <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
ON CONFLICT (equipment_id) DO NOTHING;

INSERT INTO dim_equipment (equipment_id, equipment_type)
SELECT DISTINCT equipment_id, 'TRACTOR'
FROM tractor_data_clean
WHERE event_timestamp >= date_trunc('hour', now() - interval '1 hour')
  AND event_timestamp <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
ON CONFLICT (equipment_id) DO NOTHING;


-- =====================================================
-- 3. FAITS -- agrégation horaire par équipement
-- =====================================================

-- ---------- ELEVATOR ----------
INSERT INTO fact_elevator_hourly (
    equipment_key, date_key, hour_key, measurement_count, alarm_count,
    avg_speed, min_speed, max_speed,
    avg_load_value, min_load_value, max_load_value,
    avg_temperature, min_temperature, max_temperature,
    avg_energy, min_energy, max_energy,
    max_engine_hours
)
SELECT
    e.equipment_key,
    (EXTRACT(YEAR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 10000
        + EXTRACT(MONTH FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 100
        + EXTRACT(DAY FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER)   AS date_key,
    EXTRACT(HOUR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER          AS hour_key,
    COUNT(*)                                        AS measurement_count,
    COUNT(*) FILTER (WHERE c.alarm <> 'NONE')        AS alarm_count,
    AVG(c.speed), MIN(c.speed), MAX(c.speed),
    AVG(c.load_value), MIN(c.load_value), MAX(c.load_value),
    AVG(c.temperature), MIN(c.temperature), MAX(c.temperature),
    AVG(c.energy), MIN(c.energy), MAX(c.energy),
    MAX(c.engine_hours)
FROM elevator_data_clean c
JOIN dim_equipment e ON e.equipment_id = c.equipment_id
WHERE c.event_timestamp >= date_trunc('hour', now() - interval '1 hour')
  AND c.event_timestamp <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
GROUP BY e.equipment_key
ON CONFLICT (equipment_key, date_key, hour_key) DO UPDATE SET
    measurement_count = EXCLUDED.measurement_count,
    alarm_count = EXCLUDED.alarm_count,
    avg_speed = EXCLUDED.avg_speed, min_speed = EXCLUDED.min_speed, max_speed = EXCLUDED.max_speed,
    avg_load_value = EXCLUDED.avg_load_value, min_load_value = EXCLUDED.min_load_value, max_load_value = EXCLUDED.max_load_value,
    avg_temperature = EXCLUDED.avg_temperature, min_temperature = EXCLUDED.min_temperature, max_temperature = EXCLUDED.max_temperature,
    avg_energy = EXCLUDED.avg_energy, min_energy = EXCLUDED.min_energy, max_energy = EXCLUDED.max_energy,
    max_engine_hours = EXCLUDED.max_engine_hours,
    processed_at = now();


-- ---------- RMG ----------
INSERT INTO fact_rmg_hourly (
    equipment_key, date_key, hour_key, measurement_count, alarm_count,
    avg_rail_position, min_rail_position, max_rail_position,
    avg_trolley_position, min_trolley_position, max_trolley_position,
    avg_hoist_height, min_hoist_height, max_hoist_height,
    avg_travelling_speed, min_travelling_speed, max_travelling_speed,
    avg_load_weight, min_load_weight, max_load_weight,
    avg_motor_current, min_motor_current, max_motor_current,
    avg_vibration, min_vibration, max_vibration,
    avg_temperature, min_temperature, max_temperature,
    avg_energy, min_energy, max_energy,
    max_engine_hours
)
SELECT
    e.equipment_key,
    (EXTRACT(YEAR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 10000
        + EXTRACT(MONTH FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 100
        + EXTRACT(DAY FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER)   AS date_key,
    EXTRACT(HOUR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER          AS hour_key,
    COUNT(*),
    COUNT(*) FILTER (WHERE c.alarm <> 'NONE'),
    AVG(c.rail_position), MIN(c.rail_position), MAX(c.rail_position),
    AVG(c.trolley_position), MIN(c.trolley_position), MAX(c.trolley_position),
    AVG(c.hoist_height), MIN(c.hoist_height), MAX(c.hoist_height),
    AVG(c.travelling_speed), MIN(c.travelling_speed), MAX(c.travelling_speed),
    AVG(c.load_weight), MIN(c.load_weight), MAX(c.load_weight),
    AVG(c.motor_current), MIN(c.motor_current), MAX(c.motor_current),
    AVG(c.vibration), MIN(c.vibration), MAX(c.vibration),
    AVG(c.temperature), MIN(c.temperature), MAX(c.temperature),
    AVG(c.energy), MIN(c.energy), MAX(c.energy),
    MAX(c.engine_hours)
FROM rmg_data_clean c
JOIN dim_equipment e ON e.equipment_id = c.equipment_id
WHERE c.event_timestamp >= date_trunc('hour', now() - interval '1 hour')
  AND c.event_timestamp <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
GROUP BY e.equipment_key
ON CONFLICT (equipment_key, date_key, hour_key) DO UPDATE SET
    measurement_count = EXCLUDED.measurement_count,
    alarm_count = EXCLUDED.alarm_count,
    avg_rail_position = EXCLUDED.avg_rail_position, min_rail_position = EXCLUDED.min_rail_position, max_rail_position = EXCLUDED.max_rail_position,
    avg_trolley_position = EXCLUDED.avg_trolley_position, min_trolley_position = EXCLUDED.min_trolley_position, max_trolley_position = EXCLUDED.max_trolley_position,
    avg_hoist_height = EXCLUDED.avg_hoist_height, min_hoist_height = EXCLUDED.min_hoist_height, max_hoist_height = EXCLUDED.max_hoist_height,
    avg_travelling_speed = EXCLUDED.avg_travelling_speed, min_travelling_speed = EXCLUDED.min_travelling_speed, max_travelling_speed = EXCLUDED.max_travelling_speed,
    avg_load_weight = EXCLUDED.avg_load_weight, min_load_weight = EXCLUDED.min_load_weight, max_load_weight = EXCLUDED.max_load_weight,
    avg_motor_current = EXCLUDED.avg_motor_current, min_motor_current = EXCLUDED.min_motor_current, max_motor_current = EXCLUDED.max_motor_current,
    avg_vibration = EXCLUDED.avg_vibration, min_vibration = EXCLUDED.min_vibration, max_vibration = EXCLUDED.max_vibration,
    avg_temperature = EXCLUDED.avg_temperature, min_temperature = EXCLUDED.min_temperature, max_temperature = EXCLUDED.max_temperature,
    avg_energy = EXCLUDED.avg_energy, min_energy = EXCLUDED.min_energy, max_energy = EXCLUDED.max_energy,
    max_engine_hours = EXCLUDED.max_engine_hours,
    processed_at = now();


-- ---------- RTG ----------
INSERT INTO fact_rtg_hourly (
    equipment_key, date_key, hour_key, measurement_count, alarm_count,
    avg_trolley_position, min_trolley_position, max_trolley_position,
    avg_hoist_height, min_hoist_height, max_hoist_height,
    avg_gantry_speed, min_gantry_speed, max_gantry_speed,
    avg_load_weight, min_load_weight, max_load_weight,
    avg_hydraulic_pressure, min_hydraulic_pressure, max_hydraulic_pressure,
    avg_vibration, min_vibration, max_vibration,
    avg_temperature, min_temperature, max_temperature,
    avg_energy, min_energy, max_energy,
    max_engine_hours
)
SELECT
    e.equipment_key,
    (EXTRACT(YEAR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 10000
        + EXTRACT(MONTH FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 100
        + EXTRACT(DAY FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER)   AS date_key,
    EXTRACT(HOUR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER          AS hour_key,
    COUNT(*),
    COUNT(*) FILTER (WHERE c.alarm <> 'NONE'),
    AVG(c.trolley_position), MIN(c.trolley_position), MAX(c.trolley_position),
    AVG(c.hoist_height), MIN(c.hoist_height), MAX(c.hoist_height),
    AVG(c.gantry_speed), MIN(c.gantry_speed), MAX(c.gantry_speed),
    AVG(c.load_weight), MIN(c.load_weight), MAX(c.load_weight),
    AVG(c.hydraulic_pressure), MIN(c.hydraulic_pressure), MAX(c.hydraulic_pressure),
    AVG(c.vibration), MIN(c.vibration), MAX(c.vibration),
    AVG(c.temperature), MIN(c.temperature), MAX(c.temperature),
    AVG(c.energy), MIN(c.energy), MAX(c.energy),
    MAX(c.engine_hours)
FROM rtg_data_clean c
JOIN dim_equipment e ON e.equipment_id = c.equipment_id
WHERE c.event_timestamp >= date_trunc('hour', now() - interval '1 hour')
  AND c.event_timestamp <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
GROUP BY e.equipment_key
ON CONFLICT (equipment_key, date_key, hour_key) DO UPDATE SET
    measurement_count = EXCLUDED.measurement_count,
    alarm_count = EXCLUDED.alarm_count,
    avg_trolley_position = EXCLUDED.avg_trolley_position, min_trolley_position = EXCLUDED.min_trolley_position, max_trolley_position = EXCLUDED.max_trolley_position,
    avg_hoist_height = EXCLUDED.avg_hoist_height, min_hoist_height = EXCLUDED.min_hoist_height, max_hoist_height = EXCLUDED.max_hoist_height,
    avg_gantry_speed = EXCLUDED.avg_gantry_speed, min_gantry_speed = EXCLUDED.min_gantry_speed, max_gantry_speed = EXCLUDED.max_gantry_speed,
    avg_load_weight = EXCLUDED.avg_load_weight, min_load_weight = EXCLUDED.min_load_weight, max_load_weight = EXCLUDED.max_load_weight,
    avg_hydraulic_pressure = EXCLUDED.avg_hydraulic_pressure, min_hydraulic_pressure = EXCLUDED.min_hydraulic_pressure, max_hydraulic_pressure = EXCLUDED.max_hydraulic_pressure,
    avg_vibration = EXCLUDED.avg_vibration, min_vibration = EXCLUDED.min_vibration, max_vibration = EXCLUDED.max_vibration,
    avg_temperature = EXCLUDED.avg_temperature, min_temperature = EXCLUDED.min_temperature, max_temperature = EXCLUDED.max_temperature,
    avg_energy = EXCLUDED.avg_energy, min_energy = EXCLUDED.min_energy, max_energy = EXCLUDED.max_energy,
    max_engine_hours = EXCLUDED.max_engine_hours,
    processed_at = now();


-- ---------- STS (pas de champ "alarm" : on utilise status = 'FAULT') ----------
INSERT INTO fact_sts_hourly (
    equipment_key, date_key, hour_key, measurement_count, fault_count,
    avg_motor_temperature, min_motor_temperature, max_motor_temperature,
    avg_vibration, min_vibration, max_vibration,
    avg_container_load_kg, min_container_load_kg, max_container_load_kg,
    avg_trolley_position_m, min_trolley_position_m, max_trolley_position_m,
    avg_spreader_height_m, min_spreader_height_m, max_spreader_height_m,
    avg_movement_speed_m_s, min_movement_speed_m_s, max_movement_speed_m_s,
    avg_energy_consumption_kw, min_energy_consumption_kw, max_energy_consumption_kw,
    avg_hydraulic_oil_level, min_hydraulic_oil_level, max_hydraulic_oil_level
)
SELECT
    e.equipment_key,
    (EXTRACT(YEAR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 10000
        + EXTRACT(MONTH FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 100
        + EXTRACT(DAY FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER)   AS date_key,
    EXTRACT(HOUR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER          AS hour_key,
    COUNT(*),
    COUNT(*) FILTER (WHERE c.status = 'FAULT'),
    AVG(c.motor_temperature), MIN(c.motor_temperature), MAX(c.motor_temperature),
    AVG(c.vibration), MIN(c.vibration), MAX(c.vibration),
    AVG(c.container_load_kg), MIN(c.container_load_kg), MAX(c.container_load_kg),
    AVG(c.trolley_position_m), MIN(c.trolley_position_m), MAX(c.trolley_position_m),
    AVG(c.spreader_height_m), MIN(c.spreader_height_m), MAX(c.spreader_height_m),
    AVG(c.movement_speed_m_s), MIN(c.movement_speed_m_s), MAX(c.movement_speed_m_s),
    AVG(c.energy_consumption_kw), MIN(c.energy_consumption_kw), MAX(c.energy_consumption_kw),
    AVG(c.hydraulic_oil_level), MIN(c.hydraulic_oil_level), MAX(c.hydraulic_oil_level)
FROM sts_data_clean c
JOIN dim_equipment e ON e.equipment_id = c.equipment_id
WHERE c.event_timestamp >= date_trunc('hour', now() - interval '1 hour')
  AND c.event_timestamp <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
GROUP BY e.equipment_key
ON CONFLICT (equipment_key, date_key, hour_key) DO UPDATE SET
    measurement_count = EXCLUDED.measurement_count,
    fault_count = EXCLUDED.fault_count,
    avg_motor_temperature = EXCLUDED.avg_motor_temperature, min_motor_temperature = EXCLUDED.min_motor_temperature, max_motor_temperature = EXCLUDED.max_motor_temperature,
    avg_vibration = EXCLUDED.avg_vibration, min_vibration = EXCLUDED.min_vibration, max_vibration = EXCLUDED.max_vibration,
    avg_container_load_kg = EXCLUDED.avg_container_load_kg, min_container_load_kg = EXCLUDED.min_container_load_kg, max_container_load_kg = EXCLUDED.max_container_load_kg,
    avg_trolley_position_m = EXCLUDED.avg_trolley_position_m, min_trolley_position_m = EXCLUDED.min_trolley_position_m, max_trolley_position_m = EXCLUDED.max_trolley_position_m,
    avg_spreader_height_m = EXCLUDED.avg_spreader_height_m, min_spreader_height_m = EXCLUDED.min_spreader_height_m, max_spreader_height_m = EXCLUDED.max_spreader_height_m,
    avg_movement_speed_m_s = EXCLUDED.avg_movement_speed_m_s, min_movement_speed_m_s = EXCLUDED.min_movement_speed_m_s, max_movement_speed_m_s = EXCLUDED.max_movement_speed_m_s,
    avg_energy_consumption_kw = EXCLUDED.avg_energy_consumption_kw, min_energy_consumption_kw = EXCLUDED.min_energy_consumption_kw, max_energy_consumption_kw = EXCLUDED.max_energy_consumption_kw,
    avg_hydraulic_oil_level = EXCLUDED.avg_hydraulic_oil_level, min_hydraulic_oil_level = EXCLUDED.min_hydraulic_oil_level, max_hydraulic_oil_level = EXCLUDED.max_hydraulic_oil_level,
    processed_at = now();


-- ---------- STRADDLE ----------
INSERT INTO fact_straddle_hourly (
    equipment_key, date_key, hour_key, measurement_count, alarm_count,
    avg_position_x, min_position_x, max_position_x,
    avg_position_y, min_position_y, max_position_y,
    avg_speed, min_speed, max_speed,
    avg_container_load, min_container_load, max_container_load,
    avg_height, min_height, max_height,
    avg_temperature, min_temperature, max_temperature,
    avg_energy, min_energy, max_energy,
    max_engine_hours
)
SELECT
    e.equipment_key,
    (EXTRACT(YEAR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 10000
        + EXTRACT(MONTH FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 100
        + EXTRACT(DAY FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER)   AS date_key,
    EXTRACT(HOUR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER          AS hour_key,
    COUNT(*),
    COUNT(*) FILTER (WHERE c.alarm <> 'NONE'),
    AVG(c.position_x), MIN(c.position_x), MAX(c.position_x),
    AVG(c.position_y), MIN(c.position_y), MAX(c.position_y),
    AVG(c.speed), MIN(c.speed), MAX(c.speed),
    AVG(c.container_load), MIN(c.container_load), MAX(c.container_load),
    AVG(c.height), MIN(c.height), MAX(c.height),
    AVG(c.temperature), MIN(c.temperature), MAX(c.temperature),
    AVG(c.energy), MIN(c.energy), MAX(c.energy),
    MAX(c.engine_hours)
FROM straddle_data_clean c
JOIN dim_equipment e ON e.equipment_id = c.equipment_id
WHERE c.event_timestamp >= date_trunc('hour', now() - interval '1 hour')
  AND c.event_timestamp <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
GROUP BY e.equipment_key
ON CONFLICT (equipment_key, date_key, hour_key) DO UPDATE SET
    measurement_count = EXCLUDED.measurement_count,
    alarm_count = EXCLUDED.alarm_count,
    avg_position_x = EXCLUDED.avg_position_x, min_position_x = EXCLUDED.min_position_x, max_position_x = EXCLUDED.max_position_x,
    avg_position_y = EXCLUDED.avg_position_y, min_position_y = EXCLUDED.min_position_y, max_position_y = EXCLUDED.max_position_y,
    avg_speed = EXCLUDED.avg_speed, min_speed = EXCLUDED.min_speed, max_speed = EXCLUDED.max_speed,
    avg_container_load = EXCLUDED.avg_container_load, min_container_load = EXCLUDED.min_container_load, max_container_load = EXCLUDED.max_container_load,
    avg_height = EXCLUDED.avg_height, min_height = EXCLUDED.min_height, max_height = EXCLUDED.max_height,
    avg_temperature = EXCLUDED.avg_temperature, min_temperature = EXCLUDED.min_temperature, max_temperature = EXCLUDED.max_temperature,
    avg_energy = EXCLUDED.avg_energy, min_energy = EXCLUDED.min_energy, max_energy = EXCLUDED.max_energy,
    max_engine_hours = EXCLUDED.max_engine_hours,
    processed_at = now();


-- ---------- TRACTOR ----------
INSERT INTO fact_tractor_hourly (
    equipment_key, date_key, hour_key, measurement_count, alarm_count,
    avg_position_x, min_position_x, max_position_x,
    avg_position_y, min_position_y, max_position_y,
    avg_speed, min_speed, max_speed,
    avg_fuel_level, min_fuel_level, max_fuel_level,
    avg_temperature, min_temperature, max_temperature,
    avg_energy, min_energy, max_energy,
    max_engine_hours
)
SELECT
    e.equipment_key,
    (EXTRACT(YEAR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 10000
        + EXTRACT(MONTH FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 100
        + EXTRACT(DAY FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER)   AS date_key,
    EXTRACT(HOUR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER          AS hour_key,
    COUNT(*),
    COUNT(*) FILTER (WHERE c.alarm <> 'NONE'),
    AVG(c.position_x), MIN(c.position_x), MAX(c.position_x),
    AVG(c.position_y), MIN(c.position_y), MAX(c.position_y),
    AVG(c.speed), MIN(c.speed), MAX(c.speed),
    AVG(c.fuel_level), MIN(c.fuel_level), MAX(c.fuel_level),
    AVG(c.temperature), MIN(c.temperature), MAX(c.temperature),
    AVG(c.energy), MIN(c.energy), MAX(c.energy),
    MAX(c.engine_hours)
FROM tractor_data_clean c
JOIN dim_equipment e ON e.equipment_id = c.equipment_id
WHERE c.event_timestamp >= date_trunc('hour', now() - interval '1 hour')
  AND c.event_timestamp <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
GROUP BY e.equipment_key
ON CONFLICT (equipment_key, date_key, hour_key) DO UPDATE SET
    measurement_count = EXCLUDED.measurement_count,
    alarm_count = EXCLUDED.alarm_count,
    avg_position_x = EXCLUDED.avg_position_x, min_position_x = EXCLUDED.min_position_x, max_position_x = EXCLUDED.max_position_x,
    avg_position_y = EXCLUDED.avg_position_y, min_position_y = EXCLUDED.min_position_y, max_position_y = EXCLUDED.max_position_y,
    avg_speed = EXCLUDED.avg_speed, min_speed = EXCLUDED.min_speed, max_speed = EXCLUDED.max_speed,
    avg_fuel_level = EXCLUDED.avg_fuel_level, min_fuel_level = EXCLUDED.min_fuel_level, max_fuel_level = EXCLUDED.max_fuel_level,
    avg_temperature = EXCLUDED.avg_temperature, min_temperature = EXCLUDED.min_temperature, max_temperature = EXCLUDED.max_temperature,
    avg_energy = EXCLUDED.avg_energy, min_energy = EXCLUDED.min_energy, max_energy = EXCLUDED.max_energy,
    max_engine_hours = EXCLUDED.max_engine_hours,
    processed_at = now();


-- =====================================================
-- 4. FAIT complémentaire : rejets Spark, agrégés par heure
-- =====================================================

INSERT INTO fact_rejected_hourly (date_key, hour_key, topic, reject_reason, rejected_count)
SELECT
    (EXTRACT(YEAR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 10000
        + EXTRACT(MONTH FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER * 100
        + EXTRACT(DAY FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER)   AS date_key,
    EXTRACT(HOUR FROM date_trunc('hour', now() - interval '1 hour'))::INTEGER          AS hour_key,
    r.topic,
    r.reject_reason,
    COUNT(*) AS rejected_count
FROM rejected_data r
WHERE r.rejected_at >= date_trunc('hour', now() - interval '1 hour')
  AND r.rejected_at <  date_trunc('hour', now() - interval '1 hour') + interval '1 hour'
GROUP BY r.topic, r.reject_reason
ON CONFLICT (date_key, hour_key, topic, reject_reason) DO UPDATE SET
    rejected_count = EXCLUDED.rejected_count,
    processed_at = now();
