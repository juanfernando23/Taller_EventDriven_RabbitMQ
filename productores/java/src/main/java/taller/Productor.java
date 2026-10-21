package taller;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class Productor {

    private static final String EXCHANGE = "eventos.negocio";
    private static final String ROUTING_KEY = "usuario.registrado";

    public static void main(String[] args) throws Exception {
        String host = env("RABBITMQ_HOST", "localhost");
        int port = Integer.parseInt(env("RABBITMQ_PORT", "5672"));
        String user = env("RABBITMQ_USER", "guest");
        String pass = env("RABBITMQ_PASS", "guest");
        String nombre = env("NOMBRE_SERVICIO", "prod-usuarios");
        long intervaloMs = (long) (Double.parseDouble(env("INTERVALO_SEGUNDOS", "3")) * 1000);

        ConnectionFactory fabrica = new ConnectionFactory();
        fabrica.setHost(host);
        fabrica.setPort(port);
        fabrica.setUsername(user);
        fabrica.setPassword(pass);

        AMQP.BasicProperties propiedades = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2)
                .build();

        try (Connection conexion = conectar(fabrica, nombre);
             Channel canal = conexion.createChannel()) {

            canal.exchangeDeclare(EXCHANGE, "topic", true);
            System.out.printf("[%s] conectado a %s:%d, publicando '%s' cada %d ms%n",
                    nombre, host, port, ROUTING_KEY, intervaloMs);

            for (int contador = 1; ; contador++) {
                String evento = String.format(
                        "{\"id\":\"%s\",\"evento\":\"%s\",\"origen\":\"%s\",\"fecha\":\"%s\","
                                + "\"datos\":{\"usuario_id\":%d,\"email\":\"usuario%d@ejemplo.com\"}}",
                        UUID.randomUUID(), ROUTING_KEY, nombre, Instant.now(), contador, contador);

                canal.basicPublish(EXCHANGE, ROUTING_KEY, propiedades, evento.getBytes(StandardCharsets.UTF_8));
                System.out.printf("[%s] publicado %s #%d%n", nombre, ROUTING_KEY, contador);
                Thread.sleep(intervaloMs);
            }
        }
    }

    private static Connection conectar(ConnectionFactory fabrica, String nombre) throws InterruptedException {
        while (true) {
            try {
                return fabrica.newConnection();
            } catch (IOException | TimeoutException e) {
                System.out.printf("[%s] broker no disponible (%s), reintentando en 2 s%n", nombre, e.getMessage());
                Thread.sleep(2000);
            }
        }
    }

    private static String env(String clave, String porDefecto) {
        String valor = System.getenv(clave);
        return valor == null || valor.isEmpty() ? porDefecto : valor;
    }
}
