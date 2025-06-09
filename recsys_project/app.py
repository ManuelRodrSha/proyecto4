import os
import json
import numpy as np
import pandas as pd
from flask import Flask, request, render_template, redirect, url_for
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from kafka import KafkaProducer, errors
import webbrowser
import threading
from datetime import datetime, timezone
import itertools

# --- Flask app ---
app = Flask(__name__)

# --- Kafka Producer --- Inicializa el producer con fallback
# --- Kafka Producer --- Inicializa el producer con fallback
KAFKA_TOPIC = 'user_feedback'
bootstrap = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
try:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"✅ KafkaProducer conectado a {bootstrap}")
except Exception as e:
    print(f"⚠️ Kafka no disponible en {bootstrap}: feedback no se enviará — {e}")
    producer = None


# --- Load embedding model ---
model = SentenceTransformer('all-MiniLM-L6-v2')

# --- Cassandra connection setup ---
ASTRA_BUNDLE = 'secure-connect-proyecto-4.zip'
ASTRA_TOKEN = 'proyecto_4-token.json'

with open(ASTRA_TOKEN) as f:
    creds = json.load(f)
auth_provider = PlainTextAuthProvider(creds['clientId'], creds['secret'])
cloud_config = {'secure_connect_bundle': ASTRA_BUNDLE}
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('series')


# --- Load DataFrame from Cassandra ---
# Usamos un caché para evitar recargar el DataFrame en cada petición
_df_cache = None

def load_df_from_cassandra():
    cols = ['show_id', 'name', 'overview', 'embeddings_con_genero', 'vote_average']
    cql = f"SELECT {', '.join(cols)} FROM stream"
    stmt = SimpleStatement(cql, fetch_size=1000)
    print("🗄️ Primera petición: cargando datos de Cassandra…")
    rows = session.execute(stmt)
    records = [row._asdict() for row in rows]
    df = pd.DataFrame(records)
    df['embeddings_con_genero'] = df['embeddings_con_genero'].apply(lambda x: np.array(x))
    print(f"✅ Datos cargados: {len(df)} filas en memoria.")
    return df

def get_df():
    global _df_cache
    if _df_cache is None:
        _df_cache = load_df_from_cassandra()
    return _df_cache


# --- Routes ---
@app.route('/', methods=['GET', 'POST'])
def index():
    recommendations = []
    user_id = ""
    feedback_sent = request.args.get('feedback_sent') == 'true'

    if request.method == 'POST':
        user_input = request.form['user_input']
        num_recommendations = int(request.form['num_recommendations'])
        user_id = request.form['user_id']

        # Cargamos el DataFrame sólo la primera vez
        df = get_df()

        # Embedding del texto del usuario
        input_embedding = model.encode([user_input])[0].reshape(1, -1)

        # Calcular similitudes
        matrix = np.vstack(df['embeddings_con_genero'].values)
        similarities = cosine_similarity(input_embedding, matrix)[0]
        top_indices = similarities.argsort()[::-1][:num_recommendations]

        for idx in top_indices:
            recommendations.append({
                "show_id": df.iloc[idx]['show_id'],
                "name": df.iloc[idx]['name'],
                "score": round(df.iloc[idx]['vote_average'], 2),
                "overview": df.iloc[idx]['overview']
            })

        return render_template('index.html', recommendations=recommendations, user_id=user_id, feedback_sent=False)

    # GET
    return render_template('index.html', recommendations=[], user_id=user_id, feedback_sent=feedback_sent)


@app.route('/feedback', methods=['POST'])
def feedback():
    user_id = request.form.get('user_id', '')
    feedback_data = []

    print("▶️ Entrando en feedback()", flush=True)
    print("➡ Formulario recibido:", request.form, flush=True)

    # Para cada radio con name="feedback_<show_id>"
    for key, val in request.form.items():
        if key.startswith('feedback_'):
            show_id = key.split('_', 1)[1]
            action  = val  # 'like' o 'dislike'
            feedback_data.append({
                'user_id':  user_id,
                'show_id':  show_id,
                'action':   action,
                'timestamp': datetime.now(timezone.utc).isoformat()
            })

    if not feedback_data:
        print("⚠️ No se recibió ningún feedback válido.", flush=True)
    elif producer:
        for item in feedback_data:
            try:
                future = producer.send('user_feedback', item)
                meta   = future.get(timeout=10)
                print(f"✅ Enviado a Kafka: topic={meta.topic} partition={meta.partition} offset={meta.offset}", flush=True)
            except Exception as e:
                print(f"❌ Error enviando a Kafka: {e}", flush=True)
        producer.flush()
    else:
        print("⚠️ Producer no disponible. Feedback omitido.", flush=True)

    return redirect(url_for('index', user_id=user_id, feedback_sent='true'))


# --- Auto-open browser ---
def open_browser():
    webbrowser.open_new('http://localhost:5000')

if __name__ == '__main__':
    # espera un segundo a que arranque todo y luego abre navegador (local)
    threading.Timer(1.25, open_browser).start()
    # bind a 0.0.0.0 para que esté accesible fuera del contenedor
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
    
