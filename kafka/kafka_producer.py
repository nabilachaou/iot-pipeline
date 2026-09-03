import argparse
import json
import logging
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError
from cassandra.cluster import Cluster, NoHostAvailable, DriverException


# =====================================================
# Configuration
# =====================================================

KAFKA_BOOTSTRAP_SERVERS = [
    os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
]


KAFKA_API_VERSION = tuple(
    int(p) for p in os.getenv("KAFKA_API_VERSION", "2.8.1").split(".")
)


TOPICS = [
    "elevator_data",
    "rmg_data",
    "rtg_data",
    "sts_data",
    "straddle_data",
    "tractor_data",
]


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "iot_pipeline")


CASSANDRA_CONNECT_MAX_RETRIES = int(os.getenv("CASSANDRA_CONNECT_MAX_RETRIES", 10))
CASSANDRA_CONNECT_BACKOFF_SECONDS = float(os.getenv("CASSANDRA_CONNECT_BACKOFF_SECONDS", 3))

# Colonnes techniques à ne jamais renvoyer telles quelles vers Kafka.
COLUMNS_TO_DROP = {"day_bucket"}

# Intervalle entre deux cycles de rejeu, en secondes.
SEND_INTERVAL_SECONDS = float(os.getenv("SEND_INTERVAL_SECONDS", 15))

# Nombre de messages envoyés par équipement à chaque cycle.
# Le nombre d'équipements distincts (tous topics confondus) est
# fixe (~13 dans ce jeu de données) : réduire SEND_INTERVAL_SECONDS
# seul ne permet donc pas d'augmenter le débit au-delà de ce que
# permet le temps de flush() Kafka, qui domine largement le temps
# de cycle. MESSAGES_PER_EQUIPMENT découple le débit voulu du
# nombre fixe d'équipements en répétant l'envoi (avec une mesure
# réelle choisie aléatoirement à chaque fois) plusieurs fois par
# équipement et par cycle.
MESSAGES_PER_EQUIPMENT = int(os.getenv("MESSAGES_PER_EQUIPMENT", 1))

# Réglages de débit du producteur Kafka.
LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", 20))
BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", 65536))         # 64 Ko
COMPRESSION_TYPE = os.getenv("KAFKA_COMPRESSION_TYPE", "lz4")  # None, "gzip", "snappy", "lz4"


# =====================================================
# Logging
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("kafka_producer")


# =====================================================
# Connexion Cassandra 
# =====================================================

def connect_cassandra_with_retry():
    """
    Connexion à Cassandra avec retry (même logique que
    CassandraClient._connect_with_retry dans cassandra_client.py) :
    un Cluster dont connect() a échoué est auto-shutdown par le
    driver, donc on en recrée un neuf avant chaque nouvelle tentative.
    """

    attempt = 0
    backoff = CASSANDRA_CONNECT_BACKOFF_SECONDS
    cluster = Cluster([CASSANDRA_HOST])

    while True:

        attempt += 1

        try:
            session = cluster.connect()
            session.set_keyspace(CASSANDRA_KEYSPACE)
            return session

        except (NoHostAvailable, DriverException) as e:

            if attempt >= CASSANDRA_CONNECT_MAX_RETRIES:
                logger.error(
                    f"Impossible de se connecter à Cassandra après "
                    f"{attempt} tentatives, abandon : {e}"
                )
                raise

            logger.warning(
                f"Cassandra pas encore disponible (tentative "
                f"{attempt}/{CASSANDRA_CONNECT_MAX_RETRIES}), "
                f"nouvelle tentative dans {backoff:.0f}s..."
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

            cluster = Cluster([CASSANDRA_HOST])


# =====================================================
# Conversion de type (text Cassandra -> type JSON adapté à Spark)
# =====================================================

def _infer_and_cast(value):
 

    if value is None:
        return None

    if not isinstance(value, str):
        # Déjà un type natif côté driver (ex: timestamp -> datetime) :
        # rien à convertir.
        return value

    lowered = value.strip().lower()

    if lowered in ("true", "false"):
        return lowered == "true"

    try:
        return int(value)
    except (TypeError, ValueError):
        pass

    try:
        return float(value)
    except (TypeError, ValueError):
        pass

    return value


# =====================================================
# Chargement des données réelles depuis Cassandra
# =====================================================

def load_all_replay_data(session):
    

    replay_data = {}

    for topic in TOPICS:

        try:
            rows = session.execute(f"SELECT * FROM {CASSANDRA_KEYSPACE}.{topic}")
        except Exception as e:
            logger.warning(f"Impossible de lire la table {topic} : {e} -- ce topic ne sera pas alimenté.")
            replay_data[topic] = {}
            continue

        grouped = {}
        count = 0

        for row in rows:

            row_dict = row._asdict()
            equipment_id = row_dict.get("equipment_id")

            if equipment_id is None:
                continue

            cleaned = {
                key: _infer_and_cast(value)
                for key, value in row_dict.items()
                if key not in COLUMNS_TO_DROP
            }

            grouped.setdefault(equipment_id, []).append(cleaned)
            count += 1

        replay_data[topic] = grouped

        logger.info(
            f"{topic} : {count} lignes réelles chargées depuis Cassandra "
            f"({len(grouped)} équipements distincts)"
        )

    return replay_data


# =====================================================
# Producteur
# =====================================================

class IoTKafkaProducer:

    def __init__(self):

        # Connexion Cassandra en lecture seule, et chargement unique
        # des données réelles en mémoire, avant de démarrer le
        # producteur Kafka lui-même.
        self.cassandra_session = connect_cassandra_with_retry()
        self.replay_data = load_all_replay_data(self.cassandra_session)

        
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            linger_ms=LINGER_MS,
            batch_size=BATCH_SIZE,
            compression_type=COMPRESSION_TYPE,
            api_version=KAFKA_API_VERSION,
        )

        logger.info(
            f"Producteur démarré (linger_ms={LINGER_MS}, batch_size={BATCH_SIZE}, "
            f"compression={COMPRESSION_TYPE}, acks=all, "
            f"messages_per_equipment={MESSAGES_PER_EQUIPMENT})"
        )

    # -----------------------------------------------------
    # Construction d'un message à partir des données réelles
    # -----------------------------------------------------

    def build_message(self, topic, equipment_id):
    

        rows = self.replay_data[topic][equipment_id]

        # dict(...) pour copier la ligne et ne jamais modifier
        # l'historique original conservé en mémoire.
        data = dict(random.choice(rows))

        data["equipment_id"] = equipment_id
        data["timestamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")

        return data

    # -----------------------------------------------------
    # Callbacks (envoi asynchrone)
    # -----------------------------------------------------

    def _on_send_error(self, topic, equipment_id, exc):
        logger.error(f"Échec d'envoi sur {topic} ({equipment_id}) : {exc}")

    def send(self, topic, data, blocking=False):
        

        try:
            future = self.producer.send(
                topic,
                key=data.get("equipment_id"),
                value=data,
            )

            if blocking:
                future.get(timeout=10)
                logger.info(f"Envoyé sur {topic} : {data['equipment_id']} @ {data['timestamp']}")
            else:
                future.add_errback(
                    lambda exc: self._on_send_error(topic, data.get("equipment_id"), exc)
                )

        except KafkaError as e:
            logger.error(f"Échec d'envoi sur {topic} : {e}")

    def run_simulation(self):
        

        logger.info("Démarrage du rejeu de données réelles Cassandra -> Kafka. Ctrl+C pour arrêter.")

        try:
            while True:

                total_sent = 0

                for topic in TOPICS:

                    equipment_ids = list(self.replay_data.get(topic, {}).keys())

                    if not equipment_ids:
                        continue

                    for equipment_id in equipment_ids:
                        # MESSAGES_PER_EQUIPMENT permet d'augmenter le
                        # débit sans dépendre du nombre fixe
                        # d'équipements ni d'un flush() plus fréquent :
                        # on envoie plusieurs mesures réelles (choisies
                        # aléatoirement) pour le même équipement à
                        # chaque cycle.
                        for _ in range(MESSAGES_PER_EQUIPMENT):
                            data = self.build_message(topic, equipment_id)
                            self.send(topic, data, blocking=False)
                            total_sent += 1

                self.producer.flush(timeout=10)
                logger.info(f"Cycle envoyé ({total_sent} messages)")

                time.sleep(SEND_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("Arrêt demandé par l'utilisateur.")

        finally:
            self.close()

    def send_once(self, equipment_type):
        """
        Envoie une seule mesure réelle de test sur le topic
        correspondant au type d'équipement donné. Reste bloquant
        (confirmation immédiate utile pour un test ponctuel).
        """

        equipment_ids = list(self.replay_data.get(equipment_type, {}).keys())

        if not equipment_ids:
            logger.error(
                f"Aucune donnée réelle disponible pour {equipment_type} dans Cassandra. "
                f"Valeurs possibles : {TOPICS}"
            )
            return

        equipment_id = equipment_ids[0]
        data = self.build_message(equipment_type, equipment_id)

        self.send(equipment_type, data, blocking=True)
        self.producer.flush()
        self.close()

    def close(self):
        self.producer.flush()
        self.producer.close()
        logger.info("Producteur Kafka fermé.")


# =====================================================
# Point d'entrée
# =====================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Producteur Kafka IoT (rejeu de données réelles depuis Cassandra)")

    parser.add_argument(
        "--once",
        action="store_true",
        help="Envoie une seule mesure réelle de test au lieu de lancer le rejeu continu",
    )

    parser.add_argument(
        "--type",
        type=str,
        default="tractor_data",
        help="Type d'équipement pour l'envoi ponctuel (--once). "
             "Valeurs : elevator_data, rmg_data, rtg_data, sts_data, "
             "straddle_data, tractor_data",
    )

    args = parser.parse_args()

    producer = IoTKafkaProducer()

    if args.once:
        producer.send_once(args.type)
    else:
        producer.run_simulation()
