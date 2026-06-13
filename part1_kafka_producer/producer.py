import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Configuración del productor
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generar_evento():
    return {
        "event_time": datetime.utcnow().isoformat(),
        "payment_id": f"PAY-{random.randint(1000, 9999)}",
        "customer_id": f"CUST-{random.randint(1, 100)}",
        "card_id": f"CARD-{random.randint(1, 50)}",
        "merchant_id": f"MERCH-{random.randint(1, 20)}",
        "device_id": f"DEV-{random.randint(1, 10)}",
        "ip": f"192.168.1.{random.randint(1, 255)}",
        "country": random.choice(["ES", "US", "FR", "DE"]),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "currency": "EUR",
        "status": random.choice(["approved", "approved", "approved", "declined"]), # Más aprobados que rechazados
        "mcc": random.choice([5411, 5812, 5311])
    }

# Bucle principal de generación
print("🚀 Generador de eventos iniciado. Publicando en Kafka...")
try:
    while True:
        evento = generar_evento()
        producer.send('pagos_topic', value=evento)
        print(f"Enviado: {evento['payment_id']} - {evento['status']}")
        time.sleep(1) # Un evento por segundo
except KeyboardInterrupt:
    print("Generador detenido.")