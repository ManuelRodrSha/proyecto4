from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from datetime import datetime, timezone


# 🔐 Cargar credenciales desde JSON
with open("recsys_db-token.json", "r") as f:
    token_data = json.load(f)

client_id = token_data["clientId"]
client_secret = token_data["secret"]

SECURE_BUNDLE_PATH = "secure-connect-recsys-db.zip"

cloud_config = {
    'secure_connect_bundle': SECURE_BUNDLE_PATH
}

auth_provider = PlainTextAuthProvider(client_id, client_secret)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# ✅ Crear Keyspace y tablas si no existen
session.set_keyspace("recsys")

session.execute("""
CREATE TABLE IF NOT EXISTS user_feedback (
    user_id TEXT,
    show_id INT,
    action TEXT,
    timestamp TIMESTAMP,
    PRIMARY KEY (user_id, timestamp)
);
""")

# 🛰️ Conectar al topic de Kafka
consumer = KafkaConsumer(
    'user_feedback',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='feedback_group'
)

print("🟢 Esperando feedback de Kafka...")

for message in consumer:
    feedback_data = message.value
    print(f"📥 Recibido: {feedback_data}")

    # 🗃️ Insertar en Cassandra
    query = """
        INSERT INTO user_feedback (user_id, show_id, action, timestamp)
        VALUES (%s, %s, %s, %s)
    """
    if isinstance(feedback_data, list):
        for item in feedback_data:
            session.execute(query, (
                item['user_id'],
                int(item['show_id']),
                item['action'],
                datetime.now(timezone.utc)
            ))
    else:
        session.execute(query, (
            feedback_data['user_id'],
            int(feedback_data['show_id']),
            feedback_data['action'],
            datetime.now(timezone.utc)
        ))


    print("✅ Guardado en Cassandra.")
