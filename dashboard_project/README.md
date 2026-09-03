# Dashboard Django -- Visualisation du pipeline IoT

Application de visualisation en **lecture seule** connectée directement à la
base PostgreSQL `iot_realtime` déjà alimentée par le pipeline
(Cassandra -> Kafka -> Spark Structured Streaming -> PostgreSQL).

Aucune table du pipeline n'est créée ni modifiée par Django : tous les
modèles dans `monitoring/models.py` sont déclarés `managed = False`.

## Structure

```
dashboard_project/
├── manage.py
├── requirements.txt
├── Dockerfile
├── docker-compose.snippet.yml      <- extrait à coller dans le compose principal
├── dashboard/                       <- configuration du projet Django
│   ├── settings.py
│   ├── urls.py
│   └── wsgi.py
└── monitoring/                      <- application de visualisation
    ├── models.py                    <- mapping des tables existantes (managed=False)
    ├── registry.py                  <- table de correspondance équipement -> modèles
    ├── serializers.py
    ├── views.py                     <- endpoints API + vue HTML
    ├── urls.py
    └── templates/monitoring/dashboard.html
```

## Lancer en local (hors Docker)

```bash
cd dashboard_project
python -m venv venv
source venv/bin/activate          # ou venv\Scripts\activate sous Windows
pip install -r requirements.txt

export POSTGRES_HOST=localhost    # le Postgres du docker-compose expose déjà le port 5432
export POSTGRES_DB=iot_realtime
export POSTGRES_USER=iot_user
export POSTGRES_PASSWORD=iot_password

python manage.py migrate          # crée uniquement les tables internes Django (aucun impact sur le pipeline)
python manage.py runserver 0.0.0.0:8000
```

Puis ouvrir http://localhost:8000/

## Lancer via Docker Compose

1. Copier le contenu de `docker-compose.snippet.yml` dans le
   `docker-compose.yml` principal du projet (au même niveau que les autres
   services).
2. Placer le dossier `dashboard_project/` à la racine du projet (au même
   niveau que `kafka/`, `spark/`, `postgres/`).
3. Démarrer :

```bash
docker compose up -d --build django-dashboard
```

Le dashboard sera accessible sur http://localhost:8000/, indépendamment de
Kafka et Spark (seule la disponibilité de PostgreSQL est requise).

## Endpoints API disponibles

| Endpoint | Description |
|---|---|
| `GET /api/<equipment_type>/equipment-ids/` | Liste des identifiants d'équipements distincts |
| `GET /api/<equipment_type>/latest/?equipment_id=...&limit=100` | Dernières mesures brutes (`_clean`) |
| `GET /api/<equipment_type>/hourly/?equipment_id=...&hours=48` | Agrégats horaires (entrepôt `fact_*_hourly`) |
| `GET /api/rejected/summary/?hours=24` | Compteurs de rejets par topic/motif |
| `GET /api/rejected/recent/?limit=50` | Dernières lignes rejetées brutes (avec `raw_payload`) |

`<equipment_type>` accepte : `elevator`, `rmg`, `rtg`, `sts`, `straddle`, `tractor`.

## Points d'attention

- **Ne jamais lancer `python manage.py makemigrations monitoring`** : les
  modèles étant `managed = False`, cela n'aurait aucun effet utile et
  risquerait de créer de la confusion. Seule `python manage.py migrate`
  (sans argument d'app) est nécessaire, pour les tables internes Django.
- Le champ `max_engine_hours` des modèles `Fact*Hourly` est mappé en
  `FloatField` (cohérent avec `DOUBLE PRECISION`) : si votre
  `dwh_schema.sql` déclare encore ce champ en `INTEGER`, corrigez-le pour
  éviter une perte de précision sur les heures moteur fractionnaires
  (cf. correctif v3 de `spark_streaming_job.py` / `kafka_producer.py`).
- Le dashboard interroge PostgreSQL en HTTP polling toutes les 5 secondes
  (pas de WebSocket). Suffisant ici car le producteur Kafka envoie déjà à
  intervalle régulier (`SEND_INTERVAL_SECONDS`). Pour un vrai temps réel
  poussé au client, envisager Django Channels + WebSockets dans une
  itération future.
