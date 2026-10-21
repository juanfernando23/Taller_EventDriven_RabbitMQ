const amqp = require("amqplib");

const HOST = process.env.RABBITMQ_HOST || "localhost";
const PORT = Number(process.env.RABBITMQ_PORT || 5672);
const USER = process.env.RABBITMQ_USER || "guest";
const PASS = process.env.RABBITMQ_PASS || "guest";
const NOMBRE = process.env.NOMBRE_SERVICIO || "cons-facturacion";

const EXCHANGE = "eventos.negocio";
const COLA = "cola.facturacion";
const BINDINGS = ["pedido.creado", "pago.aprobado"];

const esperar = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function conectar() {
  const url = `amqp://${USER}:${PASS}@${HOST}:${PORT}`;
  for (;;) {
    try {
      return await amqp.connect(url);
    } catch (err) {
      console.log(`[${NOMBRE}] broker no disponible (${err.message}), reintentando en 2 s`);
      await esperar(2000);
    }
  }
}

async function main() {
  const conexion = await conectar();
  const canal = await conexion.createChannel();
  await canal.assertExchange(EXCHANGE, "topic", { durable: true });
  await canal.assertQueue(COLA, { durable: true });
  for (const patron of BINDINGS) {
    await canal.bindQueue(COLA, EXCHANGE, patron);
  }
  await canal.prefetch(1);

  console.log(`[${NOMBRE}] esperando eventos en ${COLA} con ${BINDINGS.join(", ")}`);
  await canal.consume(COLA, (msg) => {
    if (msg === null) return;
    const evento = JSON.parse(msg.content.toString());
    console.log(`[${NOMBRE}] recibido ${msg.fields.routingKey} | id=${evento.id} | datos=${JSON.stringify(evento.datos)}`);
    canal.ack(msg);
  });
}

main().catch((err) => {
  console.error(`[${NOMBRE}] error fatal: ${err.message}`);
  process.exit(1);
});
