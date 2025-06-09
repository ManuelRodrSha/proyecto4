# verify_all_feedback.py
import json
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

ASTRA_BUNDLE = 'secure-connect-proyecto-4.zip'
ASTRA_TOKEN  = 'proyecto_4-token.json'
KEYSPACE     = 'series'
TABLE        = 'user_feedback'

with open(ASTRA_TOKEN) as f:
    creds = json.load(f)
auth = PlainTextAuthProvider(creds['clientId'], creds['secret'])
cluster = Cluster(cloud={'secure_connect_bundle': ASTRA_BUNDLE}, auth_provider=auth)
session = cluster.connect(KEYSPACE)

# Leer todo (cuidado con volumen de datos; usar sólo para debug)
rows = session.execute(f"SELECT user_id, show_id, action, timestamp FROM {TABLE} ALLOW FILTERING")

df = pd.DataFrame([r._asdict() for r in rows])
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp', ascending=False).head(10)

print("Últimos 10 feedback de todos los usuarios:")
print(df.to_string(index=False))
