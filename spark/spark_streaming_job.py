
import os

import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    upper,
    trim,
    when,
    lit,
    to_json,
    struct,
    concat,
    concat_ws,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
)


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "iot_realtime")
POSTGRES_USER = os.getenv("POSTGRES_USER", "iot_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "iot_password")

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

CHECKPOINT_BASE_DIR = "/tmp/spark-checkpoints"


# ============================================================
# PARAMETRES DE DETECTION
# ============================================================

STUCK_THRESHOLD_COUNT = int(os.getenv("STUCK_THRESHOLD_COUNT", "5"))
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000"))


# ============================================================
# TYPES D'EQUIPEMENTS
# ============================================================

VALID_EQUIPMENT_TYPES = [
    "elevator_data",
    "rmg_data",
    "rtg_data",
    "sts_data",
    "straddle_data",
    "tractor_data",
]

EQUIPMENT_TYPE = os.getenv("EQUIPMENT_TYPE")

if EQUIPMENT_TYPE is None:
    raise SystemExit(
        "ERREUR : EQUIPMENT_TYPE est obligatoire.\n"
        f"Valeurs possibles : {VALID_EQUIPMENT_TYPES}"
    )

if EQUIPMENT_TYPE not in VALID_EQUIPMENT_TYPES:
    raise SystemExit(
        f"ERREUR : EQUIPMENT_TYPE='{EQUIPMENT_TYPE}' invalide.\n"
        f"Valeurs possibles : {VALID_EQUIPMENT_TYPES}"
    )


# ============================================================
# SCHEMAS JSON
# ============================================================

def _common_fields():
    return [
        StructField("equipment_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("state", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("energy", DoubleType(), True),
        StructField("engine_hours", DoubleType(), True),
        StructField("alarm", StringType(), True),
    ]


SCHEMAS = {

    "elevator_data": StructType(_common_fields() + [
        StructField("position", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("load", DoubleType(), True),
        StructField("direction", StringType(), True),
    ]),

    "rmg_data": StructType(_common_fields() + [
        StructField("rail_position", DoubleType(), True),
        StructField("trolley_position", DoubleType(), True),
        StructField("hoist_height", DoubleType(), True),
        StructField("travelling_speed", DoubleType(), True),
        StructField("load_weight", DoubleType(), True),
        StructField("motor_current", DoubleType(), True),
        StructField("vibration", DoubleType(), True),
    ]),

    "rtg_data": StructType(_common_fields() + [
        StructField("trolley_position", DoubleType(), True),
        StructField("hoist_height", DoubleType(), True),
        StructField("gantry_speed", DoubleType(), True),
        StructField("load_weight", DoubleType(), True),
        StructField("hydraulic_pressure", DoubleType(), True),
        StructField("vibration", DoubleType(), True),
    ]),

    "sts_data": StructType([
        StructField("equipment_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("state", StringType(), True),
        StructField("motor_temperature", DoubleType(), True),
        StructField("vibration", DoubleType(), True),
        StructField("container_load_kg", DoubleType(), True),
        StructField("trolley_position_m", DoubleType(), True),
        StructField("spreader_height_m", DoubleType(), True),
        StructField("movement_speed_m_s", DoubleType(), True),
        StructField("energy_consumption_kw", DoubleType(), True),
        StructField("hydraulic_oil_level", DoubleType(), True),
        StructField("status", StringType(), True),
    ]),

    "straddle_data": StructType(_common_fields() + [
        StructField("position_x", DoubleType(), True),
        StructField("position_y", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("container_load", DoubleType(), True),
        StructField("height", DoubleType(), True),
        StructField("operation_mode", StringType(), True),
    ]),

    "tractor_data": StructType(_common_fields() + [
        StructField("position_x", DoubleType(), True),
        StructField("position_y", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("fuel_level", DoubleType(), True),
        StructField("container_attached", BooleanType(), True),
        StructField("operation_mode", StringType(), True),
    ]),
}


# ============================================================
# TABLES POSTGRESQL
# ============================================================

TARGET_TABLES = {
    "elevator_data": "elevator_data_clean",
    "rmg_data": "rmg_data_clean",
    "rtg_data": "rtg_data_clean",
    "sts_data": "sts_data_clean",
    "straddle_data": "straddle_data_clean",
    "tractor_data": "tractor_data_clean",
}

REJECTED_TABLE = "rejected_data"

# Table de monitoring identifiée par (topic, equipment_id), pas
# uniquement equipment_id, pour éviter les collisions entre types.
SENSOR_MONITORING_TABLE = "sensor_monitoring_v2"


# ============================================================
# MESURES CRITIQUES
# ============================================================

CRITICAL_MEASURE_FIELDS = {

    "elevator_data": ["speed", "load", "temperature", "energy"],

    "rmg_data": [
        "rail_position", "trolley_position", "hoist_height",
        "travelling_speed", "load_weight", "motor_current",
        "vibration", "temperature", "energy",
    ],

    "rtg_data": [
        "trolley_position", "hoist_height", "gantry_speed",
        "load_weight", "hydraulic_pressure", "vibration",
        "temperature", "energy",
    ],

    "sts_data": [
        "motor_temperature", "vibration", "container_load_kg",
        "trolley_position_m", "spreader_height_m",
        "movement_speed_m_s", "energy_consumption_kw",
        "hydraulic_oil_level",
    ],

    "straddle_data": [
        "position_x", "position_y", "speed", "container_load",
        "height", "temperature", "energy",
    ],

    "tractor_data": [
        "position_x", "position_y", "speed", "fuel_level",
        "temperature", "energy",
    ],
}




VALUE_RANGES = {

    "elevator_data": {
        "speed": (0, 5),               # ELARGI (était 0-3)
        "load": (0, 2000),
        "temperature": (-20, 80),
        "energy": (0, 100),            # ELARGI (était 0-50)
        "engine_hours": (0, 100000),
    },

    "rmg_data": {
        "rail_position": (0, 1000),
        "trolley_position": (0, 100),
        "hoist_height": (0, 50),
        "travelling_speed": (0, 10),
        "load_weight": (0, 65000),
        "motor_current": (0, 500),
        "vibration": (0, 20),
        "temperature": (-20, 80),
        "energy": (0, 100),            # ELARGI (était 0-50)
        "engine_hours": (0, 100000),
    },

    "rtg_data": {
        "trolley_position": (0, 100),
        "hoist_height": (0, 50),
        "gantry_speed": (0, 10),
        "load_weight": (0, 65000),
        "hydraulic_pressure": (0, 500),
        "vibration": (0, 20),
        "temperature": (-20, 80),
        "energy": (0, 100),            # ELARGI (était 0-50)
        "engine_hours": (0, 100000),
    },

    "sts_data": {
        "motor_temperature": (-20, 100),
        "vibration": (0, 20),
        "container_load_kg": (0, 65000),
        "trolley_position_m": (0, 100),
        "spreader_height_m": (0, 60),
        "movement_speed_m_s": (0, 10),
        "energy_consumption_kw": (0, 1000),   # ELARGI (était 0-200)
        "hydraulic_oil_level": (0, 100),
    },

    "straddle_data": {
        "position_x": (0, 1000),
        "position_y": (0, 500),
        "speed": (0, 40),
        "container_load": (0, 65000),
        "height": (0, 20),
        "temperature": (-20, 80),
        "energy": (0, 100),            # ELARGI (était 0-50)
        "engine_hours": (0, 100000),
    },

    "tractor_data": {
        "position_x": (0, 1000),
        "position_y": (0, 500),
        "speed": (0, 50),
        "fuel_level": (0, 100),
        "temperature": (-20, 80),
        "energy": (0, 100),            # ELARGI (était 0-50)
        "engine_hours": (0, 100000),
    },
}


# ============================================================
# NORMALISATION DES CHAMPS TEXTE
# ============================================================

STRING_FIELDS_TO_NORMALIZE = {
    "elevator_data": ["state", "alarm", "direction"],
    "rmg_data": ["state", "alarm"],
    "rtg_data": ["state", "alarm"],
    "sts_data": ["state", "status"],
    "straddle_data": ["state", "alarm", "operation_mode"],
    "tractor_data": ["state", "alarm", "operation_mode"],
}


# ============================================================
# CHAMP SURVEILLE POUR CAPTEUR BLOQUE
# ============================================================

STUCK_SENSOR_FIELD = {
    "elevator_data": "speed",
    "rmg_data": "travelling_speed",
    "rtg_data": "gantry_speed",
    "sts_data": "movement_speed_m_s",
    "straddle_data": "speed",
    "tractor_data": "speed",
}


# ============================================================
# CONNEXION POSTGRESQL
# ============================================================

def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


# ============================================================
# INITIALISATION DE LA TABLE SENSOR MONITORING
# ============================================================

def initialize_sensor_monitoring_table():
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SENSOR_MONITORING_TABLE} (
                topic VARCHAR(100) NOT NULL,
                equipment_id VARCHAR(255) NOT NULL,
                last_value DOUBLE PRECISION,
                repeat_count INTEGER NOT NULL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (topic, equipment_id)
            )
        """)
        conn.commit()
        cur.close()
    finally:
        conn.close()


# ============================================================
# CONSTRUCTION REJECT_REASON
# ============================================================

def build_reject_reason_column(df, topic):
    """
    Construit reject_reason (NULL si la ligne est valide).
    "mesure_critique_manquante" et "valeur_hors_limites" incluent
    explicitement le ou les noms de champs en cause, ex :
        "mesure_critique_manquante:rail_position,vibration"
        "valeur_hors_limites:temperature"
    """

    critical_fields = CRITICAL_MEASURE_FIELDS.get(topic, [])
    ranges = VALUE_RANGES.get(topic, {})

    missing_fields_col = None
    if critical_fields:
        missing_fields_col = concat_ws(
            ",",
            *[when(col(f).isNull(), lit(f)) for f in critical_fields]
        )

    out_of_range_fields_col = None
    if ranges:
        out_of_range_fields_col = concat_ws(
            ",",
            *[
                when((col(f) < lit(min_val)) | (col(f) > lit(max_val)), lit(f))
                for f, (min_val, max_val) in ranges.items()
            ]
        )

    reason = (
        when(
            col("equipment_id").isNull() | (trim(col("equipment_id")) == ""),
            "identifiant_manquant"
        )
        .when(
            col("timestamp").isNull() | (trim(col("timestamp")) == ""),
            "timestamp_manquant"
        )
        .when(col("event_timestamp_ts").isNull(), "timestamp_invalide")
    )

    if missing_fields_col is not None:
        reason = reason.when(
            missing_fields_col != "",
            concat(lit("mesure_critique_manquante:"), missing_fields_col)
        )

    if out_of_range_fields_col is not None:
        reason = reason.when(
            out_of_range_fields_col != "",
            concat(lit("valeur_hors_limites:"), out_of_range_fields_col)
        )

    reason = reason.otherwise(None)

    return df.withColumn("reject_reason", reason)


# ============================================================
# DETECTION CAPTEUR BLOQUE
# ============================================================

def detect_stuck_equipment(rows, monitor_field, topic):
    

    stuck_row_keys = set()

    if not rows:
        return stuck_row_keys

    equipment_ids = list({
        row.get("equipment_id") for row in rows if row.get("equipment_id") is not None
    })

    if not equipment_ids:
        return stuck_row_keys

    conn = get_pg_connection()

    try:
        cur = conn.cursor()

        cur.execute(
            f"""
            SELECT topic, equipment_id, last_value, repeat_count
            FROM {SENSOR_MONITORING_TABLE}
            WHERE topic = %s AND equipment_id = ANY(%s)
            """,
            (topic, equipment_ids),
        )

        state = {}
        for db_topic, equipment_id, last_value, repeat_count in cur.fetchall():
            state[equipment_id] = (last_value, repeat_count)

        for row in rows:
            equipment_id = row.get("equipment_id")
            value = row.get(monitor_field)
            event_ts = row.get("event_timestamp")

            if equipment_id is None or value is None:
                continue

            previous_value, previous_count = state.get(equipment_id, (None, 0))

            if previous_value is not None and abs(float(previous_value) - float(value)) < 1e-9:
                new_count = previous_count + 1
            else:
                new_count = 1

            state[equipment_id] = (float(value), new_count)

            # Seuil atteint -- SAUF si la valeur répétée est 0
            # (équipement légitimement à l'arrêt, cf. docstring).
            if new_count >= STUCK_THRESHOLD_COUNT and abs(float(value)) > 1e-9:
                stuck_row_keys.add((equipment_id, event_ts))

        upsert_rows = []
        for equipment_id, (last_value, repeat_count) in state.items():
            if last_value is None:
                continue
            upsert_rows.append((topic, equipment_id, last_value, repeat_count))

        if upsert_rows:
            execute_values(
                cur,
                f"""
                INSERT INTO {SENSOR_MONITORING_TABLE}
                (topic, equipment_id, last_value, repeat_count, updated_at)
                VALUES %s
                ON CONFLICT (topic, equipment_id) DO UPDATE SET
                    last_value = EXCLUDED.last_value,
                    repeat_count = EXCLUDED.repeat_count,
                    updated_at = CURRENT_TIMESTAMP
                """,
                upsert_rows,
                template="(%s, %s, %s, %s, CURRENT_TIMESTAMP)",
            )

        conn.commit()
        return stuck_row_keys

    except Exception:
        conn.rollback()
        raise

    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


# ============================================================
# CONSTRUCTION DU STREAM
# ============================================================

def build_stream(spark, topic):

    schema = SCHEMAS[topic]
    table = TARGET_TABLES[topic]

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )

    for field in STRING_FIELDS_TO_NORMALIZE.get(topic, []):
        if field in parsed_df.columns:
            parsed_df = parsed_df.withColumn(field, upper(trim(col(field))))

    cleaned_df = parsed_df.withColumn("event_timestamp_ts", to_timestamp(col("timestamp")))

    cleaned_df = build_reject_reason_column(cleaned_df, topic)

    cleaned_df = (
        cleaned_df
        .drop("timestamp")
        .withColumnRenamed("event_timestamp_ts", "event_timestamp")
    )

    if "load" in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumnRenamed("load", "load_value")

    return cleaned_df, table


# ============================================================
# ECRITURE POSTGRESQL
# ============================================================

def write_to_postgres(df, table, mode="append"):
    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
        .mode(mode)
        .save()
    )


# ============================================================
# FOREACH BATCH
# ============================================================

def make_write_batch_fn(topic, table):

    monitor_field = STUCK_SENSOR_FIELD.get(topic)

    def write_batch(batch_df, batch_id):

        if batch_df.rdd.isEmpty():
            print(f"[batch {batch_id}] {topic} : batch vide")
            return

        batch_df = batch_df.cache()
        candidate_valid_df = None

        try:
            already_rejected_df = batch_df.filter(col("reject_reason").isNotNull())

            candidate_valid_df = (
                batch_df
                .filter(col("reject_reason").isNull())
                .dropDuplicates(["equipment_id", "event_timestamp"])
                .cache()
            )

            stuck_row_keys = set()

            if monitor_field and not candidate_valid_df.rdd.isEmpty():
                sorted_rows_df = candidate_valid_df.orderBy("equipment_id", "event_timestamp")
                rows_as_dicts = [row.asDict() for row in sorted_rows_df.collect()]
                stuck_row_keys = detect_stuck_equipment(rows_as_dicts, monitor_field, topic)

            if stuck_row_keys:
                stuck_keys_schema = StructType([
                    StructField("equipment_id", StringType(), False),
                    StructField("event_timestamp", TimestampType(), False),
                ])

                stuck_keys_df = batch_df.sparkSession.createDataFrame(
                    list(stuck_row_keys), schema=stuck_keys_schema
                )

                stuck_df = (
                    candidate_valid_df.join(
                        stuck_keys_df, ["equipment_id", "event_timestamp"], "inner"
                    ).withColumn("reject_reason", lit("capteur_bloque"))
                )

                final_valid_df = candidate_valid_df.join(
                    stuck_keys_df, ["equipment_id", "event_timestamp"], "left_anti"
                )
            else:
                stuck_df = None
                final_valid_df = candidate_valid_df

            valid_count = final_valid_df.count()

            if valid_count > 0:
                valid_export_df = final_valid_df.drop("reject_reason")
                write_to_postgres(valid_export_df, table)

            rejected_parts = [already_rejected_df]
            if stuck_df is not None:
                rejected_parts.append(stuck_df)

            rejected_df = rejected_parts[0]
            for part in rejected_parts[1:]:
                rejected_df = rejected_df.unionByName(part, allowMissingColumns=True)

            rejected_count = rejected_df.count()

            if rejected_count > 0:
                data_columns = [c for c in rejected_df.columns if c != "reject_reason"]

                rejected_export_df = (
                    rejected_df
                    .withColumn("topic", lit(topic))
                    .withColumn("raw_payload", to_json(struct(*data_columns)))
                    .select("topic", "equipment_id", "raw_payload", "reject_reason")
                )

                write_to_postgres(rejected_export_df, REJECTED_TABLE)

            print(
                f"[batch {batch_id}] {topic} -> {table} | "
                f"valides={valid_count} | rejets={rejected_count} | "
                f"capteur_bloque={len(stuck_row_keys)}"
            )

        except Exception as exc:
            print(f"[batch {batch_id}] ERREUR pour {topic}: {exc}")
            raise

        finally:
            if candidate_valid_df is not None:
                candidate_valid_df.unpersist()
            batch_df.unpersist()

    return write_batch


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("DÉMARRAGE SPARK STRUCTURED STREAMING")
    print("=" * 70)
    print(f"EQUIPMENT_TYPE          : {EQUIPMENT_TYPE}")
    print(f"KAFKA_BOOTSTRAP_SERVERS : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"POSTGRES_HOST           : {POSTGRES_HOST}")
    print(f"MAX_OFFSETS_PER_TRIGGER : {MAX_OFFSETS_PER_TRIGGER}")
    print(f"STUCK_THRESHOLD_COUNT   : {STUCK_THRESHOLD_COUNT}")
    print("=" * 70)

    spark = SparkSession.builder.appName(f"IoTStreamingCleaning-{EQUIPMENT_TYPE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    initialize_sensor_monitoring_table()

    cleaned_df, table = build_stream(spark, EQUIPMENT_TYPE)

    checkpoint_location = f"{CHECKPOINT_BASE_DIR}/{EQUIPMENT_TYPE}"

    query = (
        cleaned_df.writeStream
        .foreachBatch(make_write_batch_fn(EQUIPMENT_TYPE, table))
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .start()
    )

    print(f"Streaming démarré : {EQUIPMENT_TYPE} -> {table}")
    print(f"Checkpoint : {checkpoint_location}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
