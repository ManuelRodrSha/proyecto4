import os, time, json
from datetime import datetime, timezone
from kafka import KafkaConsumer, errors
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

# Cassandra Astra
ASTRA_BUNDLE = 'secure-connect-proyecto-4.zip'
ASTRA_TOKEN  = 'proyecto_4-token.json'
KEYSPACE     = 'series'

with open(ASTRA_TOKEN) as f:
    creds = json.load(f)
auth = PlainTextAuthProvider(creds['clientId'], creds['secret'])
cluster = Cluster(cloud={'secure_connect_bundle': ASTRA_BUNDLE}, auth_provider=auth)
session = cluster.connect(KEYSPACE)
session.execute("""
CREATE TABLE IF NOT EXISTS user_feedback (
    user_id TEXT, show_id TEXT, action TEXT, timestamp TIMESTAMP,
    PRIMARY KEY (user_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
""")


def make_consumer(topic, delay=5):
    bootstrap = os.environ.get('KAFKA_BOOTSTRAP_SERVERS','kafka:9092').split(',')
    while True:
        try:
            group = f'feedback_group_{int(time.time())}'
            c = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=group
            )
            print(f"✅ KafkaConsumer conectado al grupo '{group}' y bootstrap {bootstrap}", flush=True)
            return c
        except errors.NoBrokersAvailable as e:
            print(f"⚠️ Kafka no disponible en {bootstrap}. Reintentando en {delay}s…", flush=True)
            time.sleep(delay)


consumer = make_consumer('user_feedback')
print("🟢 Esperando mensajes de feedback…", flush=True)

insert_cql = """
INSERT INTO user_feedback (user_id, show_id, action, timestamp)
VALUES (%s, %s, %s, %s)
"""

try:
    for msg in consumer:
        data = msg.value
        print("📥 Recibido:", data, flush=True)
        # siempre lista de entradas
        entries = data if isinstance(data, list) else [data]
        for entry in entries:
            ts = datetime.now(timezone.utc)
            session.execute(insert_cql, (
                entry['user_id'],
                str(entry['show_id']),
                entry['action'],
                ts
            ))
            print(f"💾 Guardado en Cassandra: {entry['user_id']} | {entry['show_id']} | {entry['action']} @ {ts.isoformat()}", flush=True)
except Exception as e:
    print(f"❌ Error inesperado en el consumer: {e}", flush=True)
    raise


