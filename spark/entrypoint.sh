#!/bin/bash
# entrypoint.sh
#
# Construit dynamiquement la commande spark-submit à partir des
# variables d'environnement injectées par docker-compose.yml.
#
# NÉCESSAIRE depuis le passage à 6 clusters Spark séparés (un par
# type d'équipement) : chaque conteneur spark-app-* doit se connecter
# à SON PROPRE spark-master (spark-master-elevator, spark-master-rmg,
# etc.), donc l'adresse du master ne peut plus être une valeur fixe
# dans un CMD exec-form (qui ne fait pas de substitution de variables
# d'environnement) — il faut passer par un script shell comme celui-ci.
set -e

SPARK_MASTER="${SPARK_MASTER:?Variable SPARK_MASTER obligatoire (ex: spark://spark-master-elevator:7077)}"

# Avec un cluster dédié par type (2 cœurs alloués rien que pour ce
# type), plus besoin de plafonner à 1 cœur comme quand le worker
# était partagé entre les 6 pipelines. Par défaut on prend tout ce
# que le worker dédié propose (2), ajustable via SPARK_CORES_MAX si
# besoin de laisser de la marge.
SPARK_CORES_MAX="${SPARK_CORES_MAX:-2}"

exec /opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER}" \
    --conf "spark.cores.max=${SPARK_CORES_MAX}" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.7.3 \
    /app/spark_streaming_job.py
