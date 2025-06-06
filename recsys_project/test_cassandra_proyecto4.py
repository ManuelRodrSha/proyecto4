from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import json

print("🔐 Cargando credenciales...")

# Cargar credenciales desde tu archivo JSON
with open("proyecto_4-token.json", "r") as f:
    token = json.load(f)

# Configuración del bundle de Astra
cloud_config = {
    'secure_connect_bundle': 'secure-connect-proyecto-4.zip'
}

# Autenticación
auth_provider = PlainTextAuthProvider(token["clientId"], token["secret"])

print("🔌 Conectando a Cassandra Astra...")

# Conexión a la base de datos
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

print("✅ Conectado")

print("📦 Seleccionando keyspace 'series'...")

# Seleccionar keyspace
session.set_keyspace('series')

print("📄 Ejecutando consulta...")

# Leer datos de la tabla "series"
query = "SELECT show_id, name, overview, vote_average FROM stream LIMIT 10"
rows = list(session.execute(query, timeout=60))  # Almacena los resultados en una lista

# 5. Mostrar resultados
print("📄 Primeras series:")
for row in rows[:10]:
    print(f"{row.show_id} - {row.name} | ⭐ {row.vote_average}")

# Convertir a DataFrame correctamente
print("📊 Convirtiendo a DataFrame...")
df = pd.DataFrame([row._asdict() for row in rows])

if df.empty:
    print("⚠️ La tabla 'series' está vacía o no se ha leído correctamente.")
else:
    print("✅ Datos obtenidos:")
    print(f"🔢 Número de filas en el DataFrame: {len(df)}")
    print(df.head(10))

