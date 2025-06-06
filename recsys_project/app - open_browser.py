from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
import ast
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Cargar modelo de embeddings
model = SentenceTransformer('all-MiniLM-L6-v2')

# Cargar dataset con embeddings
df = pd.read_csv("dataset_con_embbedings_pruebas.csv")
df['embeddings_con_genero'] = df['embeddings_con_genero'].apply(lambda x: np.array(ast.literal_eval(x)))

# Inicializa el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

KAFKA_TOPIC = 'user_feedback'


@app.route('/', methods=['GET', 'POST'])
def index():
    recommendations = []
    user_id = ""
    feedback_sent = request.args.get('feedback_sent') == 'true'

    if request.method == 'POST':
        user_input = request.form['user_input']
        num_recommendations = int(request.form['num_recommendations'])
        user_id = request.form['user_id']

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
    
        # En POST, renderizamos sin mostrar "Gracias por tu feedback"
        return render_template('index.html', recommendations=recommendations, user_id=user_id, feedback_sent=False)   
    
    # En GET, cuando se redirige desde /feedback con ?feedback_sent=true
    return render_template('index.html', recommendations=[], user_id=user_id, feedback_sent=feedback_sent)


@app.route('/feedback', methods=['POST'])
def feedback():
    print("✅ Feedback recibido")
    print("➡ Formulario recibido:", request.form)
    user_id = request.form['user_id']
    feedback_data = []

    for key in request.form:
        if key.startswith('feedback_'):
            show_id = key.split('_')[1]
            action = request.form[key]
            feedback_data.append({
                'user_id': user_id,
                'show_id': show_id,
                'action': action
            })

    # Mostrar feedbacks recibidos
    for item in feedback_data:
        print(f"[Feedback] Usuario: {item['user_id']} | Show ID: {item['show_id']} | Acción: {item['action']}")
        # Enviar a Kafka
        producer.send(KAFKA_TOPIC, item)
    
    # Redirigir con un parámetro en la URL
    return redirect(url_for('index', feedback_sent='true'))
    

def open_browser():
    webbrowser.open_new("http://localhost:5000")

# Iniciar la aplicación Flask y abrir el navegador automáticamente    
if __name__ == '__main__':
    threading.Timer(1.0, open_browser).start()  # Espera 1 segundo antes de abrir
    app.run(debug=True)

# if __name__ == '__main__':
#    app.run(host='0.0.0.0', port=5000, debug=True)