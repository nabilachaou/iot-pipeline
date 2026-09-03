
-- =====================================================
-- DIMENSIONS
-- =====================================================

-- Un équipement par ligne, réutilisé par toutes les tables de faits.
CREATE TABLE IF NOT EXISTS dim_equipment (
    equipment_key   BIGSERIAL PRIMARY KEY,
    equipment_id    VARCHAR(50) NOT NULL UNIQUE,
    equipment_type  VARCHAR(20) NOT NULL,
    created_at      TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dim_equipment_type ON dim_equipment (equipment_type);


-- Une date par ligne. date_key au format YYYYMMDD (entier), pratique
-- comme clé de jointure et pour les filtres/tri sans recalcul.
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,      -- ex: 20260821
    full_date       DATE NOT NULL UNIQUE,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,          -- 1-12
    day             INTEGER NOT NULL,          -- 1-31
    day_of_week     INTEGER NOT NULL,          -- 1 (lundi) - 7 (dimanche)
    week_of_year    INTEGER NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);


-- Dimension "heure de la journée" : 24 lignes fixes, 0 à 23.
-- Séparée de dim_date pour permettre des analyses par tranche
-- horaire indépendamment de la date (ex: "quelle heure de la journée
-- concentre le plus d'alarmes, tous jours confondus").
CREATE TABLE IF NOT EXISTS dim_hour (
    hour_key    INTEGER PRIMARY KEY,   -- 0 à 23
    hour_label  VARCHAR(5) NOT NULL    -- ex: "14h"
);

INSERT INTO dim_hour (hour_key, hour_label)
SELECT h, h || 'h'
FROM generate_series(0, 23) AS h
ON CONFLICT (hour_key) DO NOTHING;


-- =====================================================
-- FAITS -- une table par type d'équipement
-- Grain : (equipment_key, date_key, hour_key)
-- Mesures critiques alignées sur CRITICAL_MEASURE_FIELDS
-- (cf. spark_streaming_job.py) pour chaque type.
-- =====================================================

CREATE TABLE IF NOT EXISTS fact_elevator_hourly (
    fact_key            BIGSERIAL PRIMARY KEY,
    equipment_key        BIGINT NOT NULL REFERENCES dim_equipment (equipment_key),
    date_key             INTEGER NOT NULL REFERENCES dim_date (date_key),
    hour_key             INTEGER NOT NULL REFERENCES dim_hour (hour_key),

    measurement_count    INTEGER NOT NULL DEFAULT 0,
    alarm_count           INTEGER NOT NULL DEFAULT 0,

    avg_speed DOUBLE PRECISION, min_speed DOUBLE PRECISION, max_speed DOUBLE PRECISION,
    avg_load_value DOUBLE PRECISION, min_load_value DOUBLE PRECISION, max_load_value DOUBLE PRECISION,
    avg_temperature DOUBLE PRECISION, min_temperature DOUBLE PRECISION, max_temperature DOUBLE PRECISION,
    avg_energy DOUBLE PRECISION, min_energy DOUBLE PRECISION, max_energy DOUBLE PRECISION,
    max_engine_hours INTEGER,   -- compteur cumulatif : max plutôt que avg/min

    processed_at         TIMESTAMP DEFAULT now(),

    UNIQUE (equipment_key, date_key, hour_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_elevator_date_hour ON fact_elevator_hourly (date_key, hour_key);


CREATE TABLE IF NOT EXISTS fact_rmg_hourly (
    fact_key            BIGSERIAL PRIMARY KEY,
    equipment_key        BIGINT NOT NULL REFERENCES dim_equipment (equipment_key),
    date_key             INTEGER NOT NULL REFERENCES dim_date (date_key),
    hour_key             INTEGER NOT NULL REFERENCES dim_hour (hour_key),

    measurement_count    INTEGER NOT NULL DEFAULT 0,
    alarm_count           INTEGER NOT NULL DEFAULT 0,

    avg_rail_position DOUBLE PRECISION, min_rail_position DOUBLE PRECISION, max_rail_position DOUBLE PRECISION,
    avg_trolley_position DOUBLE PRECISION, min_trolley_position DOUBLE PRECISION, max_trolley_position DOUBLE PRECISION,
    avg_hoist_height DOUBLE PRECISION, min_hoist_height DOUBLE PRECISION, max_hoist_height DOUBLE PRECISION,
    avg_travelling_speed DOUBLE PRECISION, min_travelling_speed DOUBLE PRECISION, max_travelling_speed DOUBLE PRECISION,
    avg_load_weight DOUBLE PRECISION, min_load_weight DOUBLE PRECISION, max_load_weight DOUBLE PRECISION,
    avg_motor_current DOUBLE PRECISION, min_motor_current DOUBLE PRECISION, max_motor_current DOUBLE PRECISION,
    avg_vibration DOUBLE PRECISION, min_vibration DOUBLE PRECISION, max_vibration DOUBLE PRECISION,
    avg_temperature DOUBLE PRECISION, min_temperature DOUBLE PRECISION, max_temperature DOUBLE PRECISION,
    avg_energy DOUBLE PRECISION, min_energy DOUBLE PRECISION, max_energy DOUBLE PRECISION,
    max_engine_hours INTEGER,

    processed_at         TIMESTAMP DEFAULT now(),

    UNIQUE (equipment_key, date_key, hour_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_rmg_date_hour ON fact_rmg_hourly (date_key, hour_key);


CREATE TABLE IF NOT EXISTS fact_rtg_hourly (
    fact_key            BIGSERIAL PRIMARY KEY,
    equipment_key        BIGINT NOT NULL REFERENCES dim_equipment (equipment_key),
    date_key             INTEGER NOT NULL REFERENCES dim_date (date_key),
    hour_key             INTEGER NOT NULL REFERENCES dim_hour (hour_key),

    measurement_count    INTEGER NOT NULL DEFAULT 0,
    alarm_count           INTEGER NOT NULL DEFAULT 0,

    avg_trolley_position DOUBLE PRECISION, min_trolley_position DOUBLE PRECISION, max_trolley_position DOUBLE PRECISION,
    avg_hoist_height DOUBLE PRECISION, min_hoist_height DOUBLE PRECISION, max_hoist_height DOUBLE PRECISION,
    avg_gantry_speed DOUBLE PRECISION, min_gantry_speed DOUBLE PRECISION, max_gantry_speed DOUBLE PRECISION,
    avg_load_weight DOUBLE PRECISION, min_load_weight DOUBLE PRECISION, max_load_weight DOUBLE PRECISION,
    avg_hydraulic_pressure DOUBLE PRECISION, min_hydraulic_pressure DOUBLE PRECISION, max_hydraulic_pressure DOUBLE PRECISION,
    avg_vibration DOUBLE PRECISION, min_vibration DOUBLE PRECISION, max_vibration DOUBLE PRECISION,
    avg_temperature DOUBLE PRECISION, min_temperature DOUBLE PRECISION, max_temperature DOUBLE PRECISION,
    avg_energy DOUBLE PRECISION, min_energy DOUBLE PRECISION, max_energy DOUBLE PRECISION,
    max_engine_hours INTEGER,

    processed_at         TIMESTAMP DEFAULT now(),

    UNIQUE (equipment_key, date_key, hour_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_rtg_date_hour ON fact_rtg_hourly (date_key, hour_key);


CREATE TABLE IF NOT EXISTS fact_sts_hourly (
    fact_key                BIGSERIAL PRIMARY KEY,
    equipment_key            BIGINT NOT NULL REFERENCES dim_equipment (equipment_key),
    date_key                 INTEGER NOT NULL REFERENCES dim_date (date_key),
    hour_key                 INTEGER NOT NULL REFERENCES dim_hour (hour_key),

    measurement_count        INTEGER NOT NULL DEFAULT 0,
    fault_count               INTEGER NOT NULL DEFAULT 0,  -- status = 'FAULT' (pas de champ "alarm" pour STS)

    avg_motor_temperature DOUBLE PRECISION, min_motor_temperature DOUBLE PRECISION, max_motor_temperature DOUBLE PRECISION,
    avg_vibration DOUBLE PRECISION, min_vibration DOUBLE PRECISION, max_vibration DOUBLE PRECISION,
    avg_container_load_kg DOUBLE PRECISION, min_container_load_kg DOUBLE PRECISION, max_container_load_kg DOUBLE PRECISION,
    avg_trolley_position_m DOUBLE PRECISION, min_trolley_position_m DOUBLE PRECISION, max_trolley_position_m DOUBLE PRECISION,
    avg_spreader_height_m DOUBLE PRECISION, min_spreader_height_m DOUBLE PRECISION, max_spreader_height_m DOUBLE PRECISION,
    avg_movement_speed_m_s DOUBLE PRECISION, min_movement_speed_m_s DOUBLE PRECISION, max_movement_speed_m_s DOUBLE PRECISION,
    avg_energy_consumption_kw DOUBLE PRECISION, min_energy_consumption_kw DOUBLE PRECISION, max_energy_consumption_kw DOUBLE PRECISION,
    avg_hydraulic_oil_level DOUBLE PRECISION, min_hydraulic_oil_level DOUBLE PRECISION, max_hydraulic_oil_level DOUBLE PRECISION,

    processed_at             TIMESTAMP DEFAULT now(),

    UNIQUE (equipment_key, date_key, hour_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_sts_date_hour ON fact_sts_hourly (date_key, hour_key);


CREATE TABLE IF NOT EXISTS fact_straddle_hourly (
    fact_key            BIGSERIAL PRIMARY KEY,
    equipment_key        BIGINT NOT NULL REFERENCES dim_equipment (equipment_key),
    date_key             INTEGER NOT NULL REFERENCES dim_date (date_key),
    hour_key             INTEGER NOT NULL REFERENCES dim_hour (hour_key),

    measurement_count    INTEGER NOT NULL DEFAULT 0,
    alarm_count           INTEGER NOT NULL DEFAULT 0,

    avg_position_x DOUBLE PRECISION, min_position_x DOUBLE PRECISION, max_position_x DOUBLE PRECISION,
    avg_position_y DOUBLE PRECISION, min_position_y DOUBLE PRECISION, max_position_y DOUBLE PRECISION,
    avg_speed DOUBLE PRECISION, min_speed DOUBLE PRECISION, max_speed DOUBLE PRECISION,
    avg_container_load DOUBLE PRECISION, min_container_load DOUBLE PRECISION, max_container_load DOUBLE PRECISION,
    avg_height DOUBLE PRECISION, min_height DOUBLE PRECISION, max_height DOUBLE PRECISION,
    avg_temperature DOUBLE PRECISION, min_temperature DOUBLE PRECISION, max_temperature DOUBLE PRECISION,
    avg_energy DOUBLE PRECISION, min_energy DOUBLE PRECISION, max_energy DOUBLE PRECISION,
    max_engine_hours INTEGER,

    processed_at         TIMESTAMP DEFAULT now(),

    UNIQUE (equipment_key, date_key, hour_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_straddle_date_hour ON fact_straddle_hourly (date_key, hour_key);


CREATE TABLE IF NOT EXISTS fact_tractor_hourly (
    fact_key            BIGSERIAL PRIMARY KEY,
    equipment_key        BIGINT NOT NULL REFERENCES dim_equipment (equipment_key),
    date_key             INTEGER NOT NULL REFERENCES dim_date (date_key),
    hour_key             INTEGER NOT NULL REFERENCES dim_hour (hour_key),

    measurement_count    INTEGER NOT NULL DEFAULT 0,
    alarm_count           INTEGER NOT NULL DEFAULT 0,

    avg_position_x DOUBLE PRECISION, min_position_x DOUBLE PRECISION, max_position_x DOUBLE PRECISION,
    avg_position_y DOUBLE PRECISION, min_position_y DOUBLE PRECISION, max_position_y DOUBLE PRECISION,
    avg_speed DOUBLE PRECISION, min_speed DOUBLE PRECISION, max_speed DOUBLE PRECISION,
    avg_fuel_level DOUBLE PRECISION, min_fuel_level DOUBLE PRECISION, max_fuel_level DOUBLE PRECISION,
    avg_temperature DOUBLE PRECISION, min_temperature DOUBLE PRECISION, max_temperature DOUBLE PRECISION,
    avg_energy DOUBLE PRECISION, min_energy DOUBLE PRECISION, max_energy DOUBLE PRECISION,
    max_engine_hours INTEGER,

    processed_at         TIMESTAMP DEFAULT now(),

    UNIQUE (equipment_key, date_key, hour_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_tractor_date_hour ON fact_tractor_hourly (date_key, hour_key);


-- =====================================================
-- FAIT complémentaire : suivi qualité de données
-- Agrégat horaire des rejets Spark (cf. table opérationnelle
-- rejected_data dans init.sql), pour suivre l'évolution du taux
-- de rejet dans le temps sans requêter la table brute à chaque fois.
-- =====================================================

CREATE TABLE IF NOT EXISTS fact_rejected_hourly (
    fact_key        BIGSERIAL PRIMARY KEY,
    date_key        INTEGER NOT NULL REFERENCES dim_date (date_key),
    hour_key        INTEGER NOT NULL REFERENCES dim_hour (hour_key),
    topic           VARCHAR(50) NOT NULL,
    reject_reason   VARCHAR(50) NOT NULL,
    rejected_count  INTEGER NOT NULL DEFAULT 0,

    processed_at    TIMESTAMP DEFAULT now(),

    UNIQUE (date_key, hour_key, topic, reject_reason)
);

CREATE INDEX IF NOT EXISTS idx_fact_rejected_date_hour ON fact_rejected_hourly (date_key, hour_key);
CREATE INDEX IF NOT EXISTS idx_fact_rejected_topic ON fact_rejected_hourly (topic, reject_reason);
