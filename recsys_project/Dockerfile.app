# Dockerfile.app
FROM python:3.11-slim

# instala curl para el healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Sólo las dependencias que necesita Flask + recomendador
COPY requirements_app.txt .
RUN pip install --no-cache-dir -r requirements_app.txt

# Copiamos el código
COPY . /app

EXPOSE 5000
CMD ["python", "app.py"]
