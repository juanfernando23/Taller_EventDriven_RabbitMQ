import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import pika

HOST = os.getenv("RABBITMQ_HOST", "localhost")
PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
USER = os.getenv("RABBITMQ_USER", "guest")
PASSWORD = os.getenv("RABBITMQ_PASS", "guest")
INTERVALO = float(os.getenv("INTERVALO_SEGUNDOS", "3"))
NOMBRE = os.getenv("NOMBRE_SERVICIO", "prod-pedidos")

EXCHANGE = "eventos.negocio"
ROUTING_KEY = "pedido.creado"


def conectar():
    credenciales = pika.PlainCredentials(USER, PASSWORD)
    parametros = pika.ConnectionParameters(host=HOST, port=PORT, credentials=credenciales)
    while True:
        try:
            return pika.BlockingConnection(parametros)
        except pika.exceptions.AMQPConnectionError:
            print(f"[{NOMBRE}] broker no disponible, reintentando en 2 s", flush=True)
            time.sleep(2)


def construir_evento(contador):
    return {
        "id": str(uuid.uuid4()),
        "evento": ROUTING_KEY,
        "origen": NOMBRE,
        "fecha": datetime.now(timezone.utc).isoformat(),
        "datos": {
            "pedido_id": contador,
            "cliente_id": random.randint(1, 100),
            "total": round(random.uniform(10, 500), 2),
        },
    }


def main():
    conexion = conectar()
    canal = conexion.channel()
    canal.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    print(f"[{NOMBRE}] conectado a {HOST}:{PORT}, publicando '{ROUTING_KEY}' cada {INTERVALO} s", flush=True)

    contador = 1
    while True:
        evento = construir_evento(contador)
        canal.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY,
            body=json.dumps(evento),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
        print(f"[{NOMBRE}] publicado {ROUTING_KEY} #{contador}", flush=True)
        contador += 1
        time.sleep(INTERVALO)


if __name__ == "__main__":
    main()
