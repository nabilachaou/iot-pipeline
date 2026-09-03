
import logging
import os
import time

from cassandra.cluster import Cluster, NoHostAvailable, DriverException
from datetime import datetime



logger = logging.getLogger("cassandra_client")


class CassandraClient:
    """
    Client Cassandra pour le pipeline IoT.
    """

    # Liste des tables gérées, utilisée pour la préparation des requêtes
    TABLES = [
        "elevator_data",
        "rmg_data",
        "rtg_data",
        "sts_data",
        "straddle_data",
        "tractor_data",
    ]

    
    CONNECT_MAX_RETRIES = int(os.getenv("CASSANDRA_CONNECT_MAX_RETRIES", 10))
    CONNECT_BACKOFF_SECONDS = float(os.getenv("CASSANDRA_CONNECT_BACKOFF_SECONDS", 3))

    def __init__(
        self,
        host=None,
        keyspace="iot_pipeline"
    ):

       
        host = host or os.getenv("CASSANDRA_HOST", "127.0.0.1")

    
        self._host = host

        
        self.keyspace = keyspace

        self.cluster = Cluster([host])
        self.session = self._connect_with_retry()

        self.session.default_timeout = 60
        
        self.session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }}
        """)

        self.session.set_keyspace(keyspace)

        
        self.create_tables()

        
        self._prepared_inserts = {}

        logger.info("Connexion Cassandra réussie.")

    # -----------------------------------------------------
    # Connexion avec retry 
    # -----------------------------------------------------

    def _connect_with_retry(self):
        """
        CORRIGÉ : quand cluster.connect() échoue, le driver Cassandra
        arrête (shutdown) l'objet Cluster en interne. Réutiliser ce
        même objet pour une nouvelle tentative levait alors
        DriverException("Cluster is already shut down") -- une
        exception NON catchée par le `except NoHostAvailable`
        d'origine, qui remontait donc telle quelle et faisait planter
        tout le process au lieu de retenter proprement.

        Fix : on capture aussi DriverException, ET on recrée un
        Cluster([...]) tout neuf avant chaque nouvelle tentative,
        au lieu de rappeler .connect() sur l'ancien objet mort.
        """

        attempt = 0
        backoff = self.CONNECT_BACKOFF_SECONDS

        while True:

            attempt += 1

            try:
                return self.cluster.connect()

            except (NoHostAvailable, DriverException) as e:

                if attempt >= self.CONNECT_MAX_RETRIES:
                    logger.error(
                        f"Impossible de se connecter à Cassandra après "
                        f"{attempt} tentatives, abandon : {e}"
                    )
                    raise

                logger.warning(
                    f"Cassandra pas encore disponible (tentative "
                    f"{attempt}/{self.CONNECT_MAX_RETRIES}), "
                    f"nouvelle tentative dans {backoff:.0f}s..."
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)  # plafonné à 30s entre tentatives

                # Un Cluster dont connect() a échoué est auto-shutdown
                # par le driver -> on ne peut pas rappeler .connect()
                # dessus (DriverException "already shut down"). On en
                # recrée donc un neuf pour la prochaine tentative.
                self.cluster = Cluster([self._host])

    # =====================================================
    # Création des tables
    # =====================================================

    def create_tables(self):

        
        compaction_clause = """
            AND compaction = {
                'class': 'TimeWindowCompactionStrategy',
                'compaction_window_unit': 'DAYS',
                'compaction_window_size': 1
            }
        """

        self.session.execute(f"""
        CREATE TABLE IF NOT EXISTS elevator_data (

            equipment_id text,
            day_bucket text,
            timestamp timestamp,

            type text,
            state text,

            position text,
            speed text,
            load text,
            direction text,

            temperature text,
            energy text,
            engine_hours text,

            alarm text,

            PRIMARY KEY ((equipment_id, day_bucket), timestamp)

        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        {compaction_clause};
        """)

        self.session.execute(f"""
        CREATE TABLE IF NOT EXISTS rmg_data (

            equipment_id text,
            day_bucket text,
            timestamp timestamp,

            type text,
            state text,

            rail_position text,
            trolley_position text,
            hoist_height text,
            travelling_speed text,

            load_weight text,
            motor_current text,
            vibration text,

            temperature text,
            energy text,
            engine_hours text,

            alarm text,

            PRIMARY KEY ((equipment_id, day_bucket), timestamp)

        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        {compaction_clause};
        """)

        self.session.execute(f"""
        CREATE TABLE IF NOT EXISTS rtg_data (

            equipment_id text,
            day_bucket text,
            timestamp timestamp,

            type text,
            state text,

            trolley_position text,
            hoist_height text,
            gantry_speed text,
            load_weight text,

            hydraulic_pressure text,
            vibration text,

            temperature text,
            energy text,
            engine_hours text,

            alarm text,

            PRIMARY KEY ((equipment_id, day_bucket), timestamp)

        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        {compaction_clause};
        """)

        self.session.execute(f"""
        CREATE TABLE IF NOT EXISTS sts_data (

            equipment_id text,
            day_bucket text,
            timestamp timestamp,

            type text,
            state text,

            motor_temperature text,
            vibration text,
            container_load_kg text,

            trolley_position_m text,
            spreader_height_m text,
            movement_speed_m_s text,

            energy_consumption_kw text,
            hydraulic_oil_level text,

            status text,

            PRIMARY KEY ((equipment_id, day_bucket), timestamp)

        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        {compaction_clause};
        """)

        self.session.execute(f"""
        CREATE TABLE IF NOT EXISTS straddle_data (

            equipment_id text,
            day_bucket text,
            timestamp timestamp,

            type text,
            state text,

            position_x text,
            position_y text,

            speed text,
            container_load text,
            height text,

            operation_mode text,

            temperature text,
            energy text,
            engine_hours text,

            alarm text,

            PRIMARY KEY ((equipment_id, day_bucket), timestamp)

        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        {compaction_clause};
        """)

        self.session.execute(f"""
        CREATE TABLE IF NOT EXISTS tractor_data (

            equipment_id text,
            day_bucket text,
            timestamp timestamp,

            type text,
            state text,

            position_x text,
            position_y text,

            speed text,
            fuel_level text,

            container_attached text,
            operation_mode text,

            temperature text,
            energy text,
            engine_hours text,

            alarm text,

            PRIMARY KEY ((equipment_id, day_bucket), timestamp)

        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        {compaction_clause};
        """)

        logger.info("Tables Cassandra créées (schéma avec day_bucket + TWCS).")

    # =====================================================
    # Conversion des valeurs
    # =====================================================

    def clean_value(self, value):

        if value is None:
            return None

        if isinstance(value, bool):
            return str(value)

        return str(value)

    def compute_day_bucket(self, timestamp):
        """
        Calcule le bucket journalier (partie de la clé de partition)
        à partir d'un objet datetime.
        Format : 'YYYY-MM-DD'
        """
        return timestamp.strftime("%Y-%m-%d")

    # =====================================================
    # Insertion générique
    # =====================================================

    def _get_prepared_insert(self, table, columns):
        """
        Retourne un PreparedStatement pour une table et un jeu de
        colonnes donné. Les statements sont mis en cache par
        (table, colonnes) pour éviter de re-préparer inutilement,
        mais attention : si les fichiers Excel n'ont pas toujours
        exactement les mêmes colonnes remplies, le cache peut grossir.
        Pour un usage simple avec des colonnes stables, une seule
        entrée par table suffit dans la pratique.
        """
        cache_key = (table, tuple(columns))

        if cache_key in self._prepared_inserts:
            return self._prepared_inserts[cache_key]

        placeholders = ", ".join(["?"] * len(columns))

        query = f"""
        INSERT INTO {self.keyspace}.{table} ({", ".join(columns)})
        VALUES ({placeholders})
        """

        prepared = self.session.prepare(query)
        self._prepared_inserts[cache_key] = prepared

        return prepared

    def insert_equipment(
        self,
        table,
        data
    ):

        # Calcul du timestamp et du day_bucket si absents.
        ts = data.get("timestamp")

        if ts is None:
            ts = datetime.now()
            data["timestamp"] = ts

        if not data.get("day_bucket"):
            data["day_bucket"] = self.compute_day_bucket(ts)

        columns = []
        values = []

        for key, value in data.items():

            # Ignore les colonnes vides des fichiers Excel
            if key is None:
                continue

            # Ignore les valeurs None
            if value is None:
                continue

            columns.append(key)

            # Le timestamp reste au format timestamp Cassandra
            if key == "timestamp":
                values.append(value)
            else:
                values.append(self.clean_value(value))

        prepared = self._get_prepared_insert(table, columns)

        self.session.execute(prepared, values)

        # logger.debug plutôt que print() : à haut débit (dizaines/
        # centaines de messages par seconde), un print() par message
        # inonde stdout et ajoute une I/O synchrone par insertion.
        # debug est silencieux par défaut (niveau INFO configuré par
        # l'application appelante) ; passer le logger en DEBUG pour
        # le réactiver au besoin.
        logger.debug(
            f"Donnée insérée : {data.get('equipment_id')} "
            f"(bucket={data.get('day_bucket')})"
        )

    # =====================================================
    # Méthodes d'insertion
    # =====================================================

    def insert_elevator(
        self,
        data
    ):

        self.insert_equipment(
            "elevator_data",
            data
        )

    def insert_rmg(
        self,
        data
    ):

        self.insert_equipment(
            "rmg_data",
            data
        )

    def insert_rtg(
        self,
        data
    ):

        self.insert_equipment(
            "rtg_data",
            data
        )

    def insert_sts(
        self,
        data
    ):

        self.insert_equipment(
            "sts_data",
            data
        )

    def insert_straddle(
        self,
        data
    ):

        self.insert_equipment(
            "straddle_data",
            data
        )

    def insert_tractor(
        self,
        data
    ):

        self.insert_equipment(
            "tractor_data",
            data
        )

    # =====================================================
    # Lecture (exemple : historique d'un équipement sur N jours)
    # =====================================================

    def get_equipment_history(self, table, equipment_id, day_buckets):
        """
        Récupère l'historique d'un équipement sur une liste de
        day_bucket donnée (ex : les 7 derniers jours).

        Comme day_bucket fait partie de la clé de partition, on ne
        peut pas faire "tout l'historique" en une seule requête :
        il faut interroger bucket par bucket (ou utiliser une requête
        IN sur day_bucket, ce qui revient à interroger plusieurs
        partitions en une seule requête réseau).

        Exemple d'appel :
            buckets = ["2026-07-25", "2026-07-26", "2026-07-27"]
            rows = client.get_equipment_history(
                "tractor_data", "TRACTOR_01", buckets
            )
        """

        query = f"""
        SELECT * FROM {self.keyspace}.{table}
        WHERE equipment_id = %s AND day_bucket IN %s
        """

        result = self.session.execute(
            query,
            (equipment_id, tuple(day_buckets))
        )

        return list(result)

    # =====================================================
    # Fermeture
    # =====================================================

    def close(self):

        self.cluster.shutdown()

        logger.info("Connexion Cassandra fermée.")


# =====================================================
# Test
# =====================================================

if __name__ == "__main__":

    # Ce script configure lui-même le logging global lorsqu'il est
    # lancé en autonome (usage standalone). En import depuis
    # kafka_consumer.py, c'est ce dernier qui configure le logging
    # (voir note v5 en tête de fichier) : ce module n'impose plus
    # rien globalement.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [cassandra_client] %(message)s",
        force=True,
    )

    client = CassandraClient()

    test = {

        "timestamp": datetime.now(),

        "equipment_id": "TRACTOR_TEST",

        "type": "TRACTOR",

        "state": "WAITING",

        "position_x": 125.8,

        "position_y": 36.4,

        "speed": 0,

        "fuel_level": 90.5,

        "container_attached": False,

        "operation_mode": "IDLE",

        "temperature": 32.1,

        "energy": 0.62,

        "engine_hours": 1500,

        "alarm": "NONE"

    }

    client.insert_tractor(test)

    # Lecture du bucket du jour pour vérifier l'insertion
    today_bucket = client.compute_day_bucket(datetime.now())

    history = client.get_equipment_history(
        "tractor_data",
        "TRACTOR_TEST",
        [today_bucket]
    )

    print(f"Lignes trouvées pour aujourd'hui : {len(history)}")

    client.close()
