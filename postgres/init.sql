

CREATE TABLE IF NOT EXISTS elevator_data_clean (
    id                  BIGSERIAL PRIMARY KEY,
    equipment_id        VARCHAR(50) NOT NULL,
    event_timestamp     TIMESTAMP NOT NULL,
    type                VARCHAR(20),
    state               VARCHAR(20),
    position            VARCHAR(20),
    speed               DOUBLE PRECISION,
    load_value          DOUBLE PRECISION,
    direction           VARCHAR(20),
    temperature         DOUBLE PRECISION,
    energy              DOUBLE PRECISION,
    engine_hours        DOUBLE PRECISION,
    alarm               VARCHAR(30),
    processed_at        TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rmg_data_clean (
    id                  BIGSERIAL PRIMARY KEY,
    equipment_id        VARCHAR(50) NOT NULL,
    event_timestamp     TIMESTAMP NOT NULL,
    type                VARCHAR(20),
    state               VARCHAR(20),
    rail_position       DOUBLE PRECISION,
    trolley_position    DOUBLE PRECISION,
    hoist_height        DOUBLE PRECISION,
    travelling_speed    DOUBLE PRECISION,
    load_weight         DOUBLE PRECISION,
    motor_current       DOUBLE PRECISION,
    vibration           DOUBLE PRECISION,
    temperature         DOUBLE PRECISION,
    energy              DOUBLE PRECISION,
    engine_hours        DOUBLE PRECISION,
    alarm               VARCHAR(30),
    processed_at        TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rtg_data_clean (
    id                  BIGSERIAL PRIMARY KEY,
    equipment_id        VARCHAR(50) NOT NULL,
    event_timestamp     TIMESTAMP NOT NULL,
    type                VARCHAR(20),
    state               VARCHAR(20),
    trolley_position    DOUBLE PRECISION,
    hoist_height        DOUBLE PRECISION,
    gantry_speed        DOUBLE PRECISION,
    load_weight         DOUBLE PRECISION,
    hydraulic_pressure  DOUBLE PRECISION,
    vibration           DOUBLE PRECISION,
    temperature         DOUBLE PRECISION,
    energy              DOUBLE PRECISION,
    engine_hours        DOUBLE PRECISION,
    alarm               VARCHAR(30),
    processed_at        TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sts_data_clean (
    id                      BIGSERIAL PRIMARY KEY,
    equipment_id            VARCHAR(50) NOT NULL,
    event_timestamp         TIMESTAMP NOT NULL,
    type                    VARCHAR(20),
    state                   VARCHAR(20),
    motor_temperature       DOUBLE PRECISION,
    vibration               DOUBLE PRECISION,
    container_load_kg       DOUBLE PRECISION,
    trolley_position_m      DOUBLE PRECISION,
    spreader_height_m       DOUBLE PRECISION,
    movement_speed_m_s      DOUBLE PRECISION,
    energy_consumption_kw   DOUBLE PRECISION,
    hydraulic_oil_level     DOUBLE PRECISION,
    status                  VARCHAR(30),
    processed_at            TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS straddle_data_clean (
    id                  BIGSERIAL PRIMARY KEY,
    equipment_id        VARCHAR(50) NOT NULL,
    event_timestamp     TIMESTAMP NOT NULL,
    type                VARCHAR(20),
    state               VARCHAR(20),
    position_x          DOUBLE PRECISION,
    position_y          DOUBLE PRECISION,
    speed               DOUBLE PRECISION,
    container_load      DOUBLE PRECISION,
    height              DOUBLE PRECISION,
    operation_mode      VARCHAR(20),
    temperature         DOUBLE PRECISION,
    energy              DOUBLE PRECISION,
    engine_hours        DOUBLE PRECISION,
    alarm               VARCHAR(30),
    processed_at        TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tractor_data_clean (
    id                  BIGSERIAL PRIMARY KEY,
    equipment_id        VARCHAR(50) NOT NULL,
    event_timestamp     TIMESTAMP NOT NULL,
    type                VARCHAR(20),
    state               VARCHAR(20),
    position_x          DOUBLE PRECISION,
    position_y          DOUBLE PRECISION,
    speed               DOUBLE PRECISION,
    fuel_level          DOUBLE PRECISION,
    container_attached  BOOLEAN,
    operation_mode      VARCHAR(20),
    temperature         DOUBLE PRECISION,
    energy              DOUBLE PRECISION,
    engine_hours        DOUBLE PRECISION,
    alarm               VARCHAR(30),
    processed_at        TIMESTAMP DEFAULT now()
);

-- Table unique recevant toutes les lignes rejetées par Spark, tous
-- topics confondus, pour audit/debug. La ligne d'origine est
-- conservée en JSON (raw_payload) pour permettre le diagnostic sans
-- avoir à connaître le schéma exact du topic source.
CREATE TABLE IF NOT EXISTS rejected_data (
    id              BIGSERIAL PRIMARY KEY,
    topic           VARCHAR(50),
    equipment_id    VARCHAR(50),
    raw_payload     TEXT,
    reject_reason   VARCHAR(50),
    rejected_at     TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rejected_topic_reason ON rejected_data (topic, reject_reason);
CREATE INDEX IF NOT EXISTS idx_rejected_equipment ON rejected_data (equipment_id, rejected_at DESC);

-- Table d'état utilisée par Spark pour détecter les capteurs bloqués
-- (valeur strictement identique sur plusieurs mesures consécutives).
-- Une seule ligne par équipement, mise à jour à chaque micro-batch.
CREATE TABLE IF NOT EXISTS sensor_monitoring (
    equipment_id    VARCHAR(50) PRIMARY KEY,
    last_value      DOUBLE PRECISION,
    repeat_count    INTEGER DEFAULT 1,
    updated_at      TIMESTAMP DEFAULT now()
);

-- Index utiles pour les requêtes analytiques les plus courantes
-- (historique par équipement, trié par date).
CREATE INDEX IF NOT EXISTS idx_elevator_equipment_ts ON elevator_data_clean (equipment_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_rmg_equipment_ts ON rmg_data_clean (equipment_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_rtg_equipment_ts ON rtg_data_clean (equipment_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sts_equipment_ts ON sts_data_clean (equipment_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_straddle_equipment_ts ON straddle_data_clean (equipment_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_tractor_equipment_ts ON tractor_data_clean (equipment_id, event_timestamp DESC);
