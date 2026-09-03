"""
Job d'initialisation Cassandra.

Ce script se contente d'instancier CassandraClient, ce qui déclenche
la création du keyspace + des tables (toutes en `IF NOT EXISTS`, donc
sans danger si elles existent déjà). Il est fait pour être lancé une
seule fois au démarrage du stack Docker, via le service
`cassandra-init` dans docker-compose.yml.
"""

from cassandra_client import CassandraClient

if __name__ == "__main__":

    client = CassandraClient()
    client.close()

    print("Schéma Cassandra initialisé (keyspace + tables).")
