package taller;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Consumidor {

    private static final String EXCHANGE = "eventos.negocio";
    private static final String COLA = "cola.notificaciones";
    private static final String[] BINDINGS = {"pago.aprobado", "usuario.registrado"};

    public static void main(String[] args) throws Exception {
        String host = env("RABBITMQ_HOST", "localhost");
        int port = Integer.parseInt(env("RABBITMQ_PORT", "5672"));
        String user = env("RABBITMQ_USER", "guest");
        String pass = env("RABBITMQ_PASS", "guest");
        String nombre = env("NOMBRE_SERVICIO", "cons-notificaciones");

        ConnectionFactory fabrica = new ConnectionFactory();
        fabrica.setHost(host);
        fabrica.setPort(port);
        fabrica.setUsername(user);
        fabrica.setPassword(pass);

        Connection conexion = conectar(fabrica, nombre);
        conexion.addShutdownListener(causa -> {
            System.out.printf("[%s] conexión cerrada por el broker: %s%n", nombre, causa.getMessage());
            System.exit(1);
        });

        Channel canal = conexion.createChannel();
        canal.exchangeDeclare(EXCHANGE, "topic", true);
        canal.queueDeclare(COLA, true, false, false, null);
        for (String patron : BINDINGS) {
            canal.queueBind(COLA, EXCHANGE, patron);
        }
        canal.basicQos(1);

        DeliverCallback alRecibir = (consumerTag, entrega) -> {
            String cuerpo = new String(entrega.getBody(), StandardCharsets.UTF_8);
            System.out.printf("[%s] recibido %s | %s%n", nombre, entrega.getEnvelope().getRoutingKey(), cuerpo);
            canal.basicAck(entrega.getEnvelope().getDeliveryTag(), false);
        };
        canal.basicConsume(COLA, false, alRecibir, (consumerTag, causa) -> { });
        System.out.printf("[%s] esperando eventos en %s con %s%n", nombre, COLA, String.join(", ", BINDINGS));

        Thread.currentThread().join();
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
