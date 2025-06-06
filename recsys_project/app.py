import os
import json
import numpy as np
import pandas as pd
from flask import Flask, request, render_template, redirect, url_for
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import webbrowser
import threading
from kafka import KafkaProducer
import datetime

# --- Flask app ---
app = Flask(__name__)

# --- Kafka Producer ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Cargar modelo de embeddings ---
model = SentenceTransformer('all-MiniLM-L6-v2')

# --- Configurar conexión con Cassandra ---
ASTRA_DB_BUNDLE = "secure-connect-proyecto-4.zip"
TOKEN_JSON = "proyecto_4-token.json"

with open(TOKEN_JSON, "r") as f:
    token_data = json.load(f)

client_id = token_data["clientId"]
client_secret = token_data["secret"]

cloud_config = {
    'secure_connect_bundle': ASTRA_DB_BUNDLE
}
auth_provider = PlainTextAuthProvider(client_id, client_secret)

cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect("series")

# --- Consultar datos y cargar DataFrame ---
query = "SELECT show_id, name, overview, genre_name, embeddings_con_genero, vote_count, vote_average FROM stream"
rows = session.execute(query)

# Convertir a DataFrame
records = []
for row in rows:
    records.append({
        "show_id": row.show_id,
        "name": row.name,
        "overview": row.overview,
        "genre_name": row.genre_name,
        "embeddings_con_genero": row.embeddings_con_genero,
        "vote_count": row.vote_count,
        "vote_average": row.vote_average
    })

df = pd.DataFrame(records)

# Asegurarse de que los embeddings están en formato numpy array
df['embeddings_con_genero'] = df['embeddings_con_genero'].apply(lambda x: np.array(x))


# --- Ruta principal ---
@app.route('/', methods=['GET', 'POST'])
def index():
    recommendations = []
    user_id = ""
    feedback_sent = request.args.get('feedback_sent') == 'true'

    if request.method == 'POST':
        user_input = request.form['user_input']
        num_recommendations = int(request.form['num_recommendations'])
        user_id = request.form['user_id']

        input_embedding = model.encode([user_input])[0].reshape(1, -1)

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

    return render_template('index.html', recommendations=recommendations, user_id=user_id, feedback_sent=feedback_sent)


# --- Ruta para feedback ---
@app.route('/feedback', methods=['POST'])
def feedback():
    print("➡ Formulario recibido:", request.form)
    feedback_data = []
    user_id = request.form['user_id']
    
    for key, value in request.form.items():
        if key.startswith("feedback_"):
            show_id = key.split("_")[1]
            feedback_data.append({
                "user_id": user_id,
                "show_id": show_id,
                "action": value,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })

    for item in feedback_data:
        print(f"[Feedback] Usuario: {item['user_id']} | Show ID: {item['show_id']} | Acción: {item['action']}")
        producer.send("user_feedback", value=item)

    return redirect(url_for('index', feedback_sent='true'))


# --- Abrir navegador automáticamente ---
def open_browser():
    webbrowser.open_new("http://localhost:5000")


# --- Ejecutar app ---
if __name__ == '__main__':
    threading.Timer(1.25, open_browser).start()
    app.run(debug=True)

