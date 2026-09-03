# Pipeline de données IoT temps réel pour les équipements industriels

Projet réalisé dans le cadre d'un stage au sein de **Tanger Med Engineering (TME)**, département Conseil & Digitalisation — Filière Génie Big Data et Intelligence Artificielle (ENSA Tétouan), Année universitaire 2025-2026.

> **Auteur :** Nabila Chaou
> **Encadrante entreprise :** Mlle. Loubna Boukayoua
> **Encadrant école :** M. Imad Sassi

## 📌 Description

Ce projet consiste à concevoir et développer un **pipeline de données IoT temps réel** capable de collecter, transporter, traiter, valider et stocker les données issues de capteurs installés sur des équipements industriels d'un terminal portuaire, puis de les exposer via un tableau de bord interactif.

L'architecture repose sur une chaîne complète de Data Engineering combinant stockage NoSQL, streaming distribué, traitement en continu, base relationnelle, entrepôt de données et visualisation web, le tout conteneurisé avec Docker.

## 🏗️ Architecture globale

```
Capteurs IoT → Apache Cassandra → Kafka Producer → Apache Kafka
   → Spark Structured Streaming → PostgreSQL → Data Warehouse
   → API Django REST → Dashboard Web
```

| Étape | Rôle |
|---|---|
| **Capteurs IoT** | Génèrent en continu les mesures des équipements (position, vitesse, charge, température, consommation, statut...) |
| **Apache Cassandra** | Stockage initial des mesures brutes, organisé par type d'équipement |
| **Kafka Producer** | Lit les données depuis Cassandra, les structure en JSON et les rejoue en flux continu |
| **Apache Kafka** | Transport et diffusion des flux vers 6 topics dédiés (mode KRaft, sans ZooKeeper) |
| **Apache Spark Structured Streaming** | Validation, nettoyage, normalisation et détection d'anomalies (6 clusters indépendants, un par équipement) |
| **PostgreSQL** | Stockage des données valides et des données rejetées (`rejected_data`) |
| **DWH-ETL** | Agrégation horaire périodique des données propres |
| **Django + Django REST Framework** | API et tableau de bord de supervision (données temps réel, agrégats, qualité des données) |

## ⚙️ Équipements industriels pris en charge

- **Ship-to-Shore Crane (STS)** — grue de quai
- **Rail Mounted Gantry (RMG)** — portique sur rails
- **Rubber Tyred Gantry (RTG)** — portique sur pneus
- **Elevator** — ascenseur industriel
- **Straddle Carrier** — véhicule enjambeur
- **Terminal Tractor** — tracteur portuaire

Chaque équipement dispose de son propre topic Kafka, de son propre cluster Spark (Master + Worker + application) et de ses propres tables PostgreSQL (`*_data_clean`).

## 🧰 Technologies utilisées

- **Apache Cassandra** 4.1 — stockage initial des mesures IoT
- **Apache Kafka** 3.8.0 (mode KRaft) — transport des flux
- **Apache Spark Structured Streaming** — traitement en temps réel
- **PostgreSQL** 16 — stockage relationnel final
- **Docker / Docker Compose** — conteneurisation et orchestration
- **Django + Django REST Framework** — API et dashboard de visualisation

## 📂 Organisation des topics Kafka

| Topic | Équipement |
|---|---|
| `elevator_data` | Elevator |
| `rmg_data` | Rail Mounted Gantry |
| `rtg_data` | Rubber Tyred Gantry |
| `sts_data` | Ship-to-Shore Crane |
| `straddle_data` | Straddle Carrier |
| `tractor_data` | Terminal Tractor |

Chaque topic est créé avec **6 partitions** et un facteur de réplication de **1** (environnement local mono-broker).

## 🗄️ Tables PostgreSQL

**Données propres**
```
elevator_data_clean
rmg_data_clean
rtg_data_clean
sts_data_clean
straddle_data_clean
tractor_data_clean
```

**Données rejetées**
```
rejected_data
```

**Agrégats (Data Warehouse)**
```
fact_*_hourly
fact_rejected_hourly
```

## ✅ Contrôles qualité appliqués par Spark

- Vérification de la structure et des champs obligatoires
- Contrôle des types de données
- Validation des horodatages
- Contrôle des plages de valeurs des mesures
- Cohérence des états de fonctionnement (`RUNNING`, `IDLE`, `WAITING`, `MAINTENANCE`)
- Détection de capteurs potentiellement bloqués (valeur inchangée sur 5 observations consécutives)

Les données non conformes ne sont **jamais supprimées** : elles sont conservées dans `rejected_data` pour analyse ultérieure.

## 🚀 Démarrage rapide

### Prérequis

- Docker
- Docker Compose

### Lancement de l'infrastructure

```bash
docker-compose up -d
```

Cette commande démarre l'ensemble des services : Cassandra, Kafka, le producteur Kafka, les 6 clusters Spark, PostgreSQL, le service ETL du Data Warehouse et le dashboard Django.

### Réseau Docker

Tous les services communiquent via le réseau `iot_network`, en utilisant directement les noms de conteneurs (`kafka:29092`, `postgres`, `cassandra`).

### Persistance des données

Les volumes suivants garantissent la conservation des données entre les redémarrages :
```
cassandra_data
kafka_data
postgres_data
```

### Vérifier les topics Kafka

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Accéder au dashboard

Une fois les services démarrés, l'interface Django est accessible via le navigateur (port exposé par le service `django-dashboard`).

## 📊 Résultats de l'expérimentation

| Table | Nombre de lignes |
|---|---:|
| `elevator_data_clean` | 54 594 |
| `rmg_data_clean` | 7 231 |
| `rtg_data_clean` | 6 722 |
| `sts_data_clean` | 28 207 |
| `straddle_data_clean` | 35 681 |
| `tractor_data_clean` | 54 104 |
| **Total données valides** | **186 539** |
| `rejected_data` | 172 326 |
| **Total général** | **358 865** |

- **Taux de rejet observé :** ≈ 48,02 %
- **Débit de traitement mesuré :** ≈ 104 messages/seconde

## ⚠️ Limites connues

- Environnement Docker local, ne reproduisant pas toutes les contraintes d'une infrastructure industrielle réelle (pertes réseau, interruptions capteurs, variations de débit importantes)
- Kafka déployé avec un seul broker et un facteur de réplication de 1
- Ressources limitées, résultats non directement extrapolables à grande échelle

## 🔭 Perspectives d'évolution

- Déploiement sur infrastructure distribuée (plusieurs brokers Kafka, réplication, haute disponibilité)
- Tests de montée en charge
- Mise en place d'outils de supervision
- Intégration de Machine Learning pour la détection d'anomalies et la maintenance prédictive

## 📚 Références

- [Documentation Apache Kafka](https://kafka.apache.org/documentation/)
- [Documentation Apache Spark](https://spark.apache.org/docs/latest/)
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Documentation Apache Cassandra](https://cassandra.apache.org/doc/latest/)
- [Documentation PostgreSQL](https://www.postgresql.org/docs/)
- [Documentation Docker](https://docs.docker.com/)
- [Documentation Django](https://docs.djangoproject.com/)
- [Documentation Django REST Framework](https://www.django-rest-framework.org/)
- [Tanger Med Engineering](https://www.tme.ma/fr/tanger-med-engineering/a-propos/)

## 📝 Licence

Projet académique réalisé dans le cadre d'un stage de fin d'année — usage éducatif.
