# Dockerfile pour le job Spark Structured Streaming (Kafka -> Postgres)
#
# Basé sur l'image OFFICIELLE Apache Spark (maintenue par l'ASF),
# pas sur bitnami/spark : Bitnami a fermé l'accès public à ses images
# suite au rachat par Broadcom, le tag bitnami/spark:3.5 n'existe donc
# plus. apache/spark est l'alternative officielle et pérenne.
#
# Le tag doit correspondre exactement à celui utilisé pour les
# spark-master-*/spark-worker-* dans docker-compose.yml : un mismatch
# de version entre driver et executors casse la sérialisation.
FROM apache/spark:3.5.4-python3
USER root
WORKDIR /app

# psycopg2-binary : nécessaire pour la connexion directe à PostgreSQL
# depuis le driver Spark (détection de capteur bloqué, état conservé
# dans la table sensor_monitoring, désormais en requêtes par lot).
RUN pip3 install --no-cache-dir psycopg2-binary

COPY spark/spark_streaming_job.py .
COPY spark/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Le driver JDBC PostgreSQL et le connecteur Kafka sont récupérés par
# spark-submit via --packages (cf. entrypoint.sh), donc pas besoin de
# les installer ici. Cela nécessite un accès internet au démarrage du
# conteneur (téléchargement Maven), acceptable en dev/test. Pour un
# déploiement sans accès internet, pré-télécharger les .jar et les
# monter dans /opt/spark/jars/.
#
# DEPUIS LE PASSAGE À 6 CLUSTERS SPARK SÉPARÉS (un master/worker dédié
# par type d'équipement, voir docker-compose.yml) : l'adresse du
# master n'est plus fixe (spark://spark-master:7077 pour tout le
# monde), chaque conteneur doit se connecter à SON PROPRE master
# (spark-master-elevator, spark-master-rmg, etc., injecté via la
# variable d'environnement SPARK_MASTER). Un CMD exec-form classique
# ne permettant pas de substitution de variables d'environnement,
# on passe par entrypoint.sh qui construit la commande spark-submit
# dynamiquement à partir de $SPARK_MASTER (et $SPARK_CORES_MAX,
# remonté à 2 par défaut : chaque worker dédié a maintenant 2 cœurs
# rien que pour son type, plus besoin de le plafonner à 1 comme
# lorsque le worker était partagé entre 6 pipelines).
ENTRYPOINT ["/app/entrypoint.sh"]

