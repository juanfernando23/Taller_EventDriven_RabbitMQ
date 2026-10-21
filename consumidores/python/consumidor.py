import json
import os
import time

import pika

HOST = os.getenv("RABBITMQ_HOST", "localhost")
PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
USER = os.getenv("RABBITMQ_USER", "guest")
PASSWORD = os.getenv("RABBITMQ_PASS", "guest")
NOMBRE = os.getenv("NOMBRE_SERVICIO", "cons-inventario")

EXCHANGE = "eventos.negocio"
COLA = "cola.inventario"
BINDINGS = ["pedido.creado"]


def conectar():
    credenciales = pika.PlainCredentials(USER, PASSWORD)
    parametros = pika.ConnectionParameters(host=HOST, port=PORT, credentials=credenciales)
    while True:
        try:
            return pika.BlockingConnection(parametros)
        except pika.exceptions.AMQPConnectionError:
            print(f"[{NOMBRE}] broker no disponible, reintentando en 2 s", flush=True)
            time.sleep(2)


def al_recibir(canal, metodo, propiedades, cuerpo):
    evento = json.loads(cuerpo)
    print(f"[{NOMBRE}] recibido {metodo.routing_key} | id={evento['id']} | datos={evento['datos']}", flush=True)
    canal.basic_ack(delivery_tag=metodo.delivery_tag)


def main():
    conexion = conectar()
    canal = conexion.channel()
    canal.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    canal.queue_declare(queue=COLA, durable=True)
    for patron in BINDINGS:
        canal.queue_bind(queue=COLA, exchange=EXCHANGE, routing_key=patron)
    canal.basic_qos(prefetch_count=1)
    canal.basic_consume(queue=COLA, on_message_callback=al_recibir)
    print(f"[{NOMBRE}] esperando eventos en {COLA} con {', '.join(BINDINGS)}", flush=True)
    canal.start_consuming()


if __name__ == "__main__":
    main()
