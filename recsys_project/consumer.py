from kafka import KafkaConsumer
import json

# Configuración
TOPIC = 'user_feedback'
BOOTSTRAP_SERVERS = 'localhost:9092'

# Inicializa el consumidor
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # leer desde el principio
    enable_auto_commit=True,
    group_id='feedback_group'  # consumer group ID
)

print(f"✅ Escuchando el topic '{TOPIC}'... (Ctrl+C para salir)")

# Escucha indefinidamente
try:
    for message in consumer:
        data = message.value
        print("📥 Feedback recibido:")
        print(f"👤 Usuario: {data['user_id']} | 🎬 Show ID: {data['show_id']} | 👍 Acción: {data['action']}")
except KeyboardInterrupt:
    print("\n🛑 Interrumpido por el usuario.")
finally:
    consumer.close()
