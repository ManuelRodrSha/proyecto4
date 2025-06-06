from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

# 1. Cargar clientId y secret desde el JSON
with open("recsys_db-token.json", "r") as f:
    token_data = json.load(f)

client_id = token_data["clientId"]
client_secret = token_data["secret"]

# 2. Ruta a la carpeta descomprimida del secure connect bundle
SECURE_BUNDLE_PATH = 'secure-connect-recsys-db.zip'

# 3. Configuración del bundle y autenticación
cloud_config = {
    'secure_connect_bundle': SECURE_BUNDLE_PATH
}

auth_provider = PlainTextAuthProvider(client_id, client_secret)

# 4. Conectar a Cassandra Astra
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

print("✅ Conexión a Cassandra Astra establecida")

# (Opcional) Ver keyspaces disponibles
rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces;")
print("📦 Keyspaces:")
for row in rows:
    print("-", row.keyspace_name)


# D:/Users/Rafa Velasco/0 Formación/5 Máster IABD/proyecto4/recsys_project
