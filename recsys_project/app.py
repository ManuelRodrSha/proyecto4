from flask import Flask, render_template, request
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
import ast

app = Flask(__name__)

# Cargar modelo de embedding (solo se usa para el input del usuario)
model = SentenceTransformer('all-MiniLM-L6-v2')

# Cargar dataset
df = pd.read_csv("dataset_con_embbedings_pruebas.csv")

# Convertir la columna de embeddings de string a lista de floats
df['embeddings_con_genero'] = df['embeddings_con_genero'].apply(lambda x: np.array(ast.literal_eval(x)))

@app.route('/', methods=['GET', 'POST'])
def index():
    recommendations = []

    if request.method == 'POST':
        user_input = request.form['user_input']
        num_recommendations = int(request.form['num_recommendations'])

        # Generar embedding del texto introducido por el usuario
        input_embedding = model.encode([user_input])[0].reshape(1, -1)

        # Calcular similitudes con los embeddings existentes
        embedding_matrix = np.vstack(df['embeddings_con_genero'].values)
        similarities = cosine_similarity(input_embedding, embedding_matrix)[0]

        # Obtener los índices de las N series más similares
        top_indices = similarities.argsort()[::-1][:num_recommendations]

        # Construir las recomendaciones
        for idx in top_indices:
            recommendations.append({
                "name": df.iloc[idx]['name'],
                "score": round(df.iloc[idx]['vote_average'], 2),
                "overview": df.iloc[idx]['overview']
            })

    return render_template('index.html', recommendations=recommendations)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
