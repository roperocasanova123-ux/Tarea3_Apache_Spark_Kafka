"""
Generador de eventos de ventas para Kafka.
Ejecutar desde la carpeta raíz:
    python codigo/productor_kafka.py
Requiere: pip install kafka-python
Kafka debe estar iniciado y el topic 'ventas-tiempo-real' creado.
"""
import json, random, time
from datetime import datetime
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "ventas-tiempo-real"
PRODUCTOS = [
    ("Teclado", 85000), ("Mouse", 45000), ("Monitor", 650000),
    ("Audifonos", 120000), ("Memoria_USB", 35000), ("Webcam", 160000)
]
CIUDADES = ["Cucuta", "Bucaramanga", "Medellin", "Bogota", "Pamplona"]

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8")
)
try:
    contador = 1
    print(f"Enviando eventos al topic '{TOPIC}'. Ctrl+C para detener.")
    while True:
        producto, precio = random.choice(PRODUCTOS)
        evento = {
            "venta_id": f"RT-{contador:05d}",
            "fecha_hora": datetime.now().isoformat(timespec="seconds"),
            "producto": producto,
            "cantidad": random.randint(1, 4),
            "precio_unitario": precio,
            "ciudad": random.choice(CIUDADES)
        }
        producer.send(TOPIC, key=evento["venta_id"], value=evento)
        producer.flush()
        print("Enviado:", evento)
        contador += 1
        time.sleep(2)
except KeyboardInterrupt:
    print("\nGenerador detenido.")
finally:
    producer.close()
