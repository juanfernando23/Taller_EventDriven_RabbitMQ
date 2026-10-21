const amqp = require("amqplib");
const { randomUUID } = require("crypto");

const HOST = process.env.RABBITMQ_HOST || "localhost";
const PORT = Number(process.env.RABBITMQ_PORT || 5672);
const USER = process.env.RABBITMQ_USER || "guest";
const PASS = process.env.RABBITMQ_PASS || "guest";
const INTERVALO_MS = Number(process.env.INTERVALO_SEGUNDOS || 3) * 1000;
const NOMBRE = process.env.NOMBRE_SERVICIO || "prod-pagos";

const EXCHANGE = "eventos.negocio";
const ROUTING_KEY = "pago.aprobado";

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
  console.log(`[${NOMBRE}] conectado a ${HOST}:${PORT}, publicando '${ROUTING_KEY}' cada ${INTERVALO_MS / 1000} s`);

  let contador = 1;
  setInterval(() => {
    const evento = {
      id: randomUUID(),
      evento: ROUTING_KEY,
      origen: NOMBRE,
      fecha: new Date().toISOString(),
      datos: {
        pago_id: contador,
        pedido_id: Math.floor(Math.random() * 100) + 1,
        monto: Number((Math.random() * 490 + 10).toFixed(2)),
      },
    };
    canal.publish(EXCHANGE, ROUTING_KEY, Buffer.from(JSON.stringify(evento)), {
      contentType: "application/json",
      persistent: true,
    });
    console.log(`[${NOMBRE}] publicado ${ROUTING_KEY} #${contador}`);
    contador++;
  }, INTERVALO_MS);
}

main().catch((err) => {
  console.error(`[${NOMBRE}] error fatal: ${err.message}`);
  process.exit(1);
});
