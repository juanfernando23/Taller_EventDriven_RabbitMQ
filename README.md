# Taller: arquitectura event-driven multilenguaje con RabbitMQ y Docker

Cuatro productores y cuatro consumidores en Python, Node.js, Go y Java, comunicados por un broker RabbitMQ mediante un exchange `topic`. Cada servicio corre en su propio contenedor.

## Topología

- **Exchange:** `eventos.negocio` (tipo `topic`, durable)
- **Productores:** publican cada 3 segundos con routing keys `pedido.creado`, `pago.aprobado`, `inventario.actualizado` y `usuario.registrado`.
- **Consumidores:**

| Consumidor | Lenguaje | Cola | Bindings |
|---|---|---|---|
| cons-facturacion | Node.js | `cola.facturacion` | `pedido.creado`, `pago.aprobado` |
| cons-inventario | Python | `cola.inventario` | `pedido.creado` |
| cons-notificaciones | Java | `cola.notificaciones` | `pago.aprobado`, `usuario.registrado` |
| cons-auditoria | Go | `cola.auditoria` | `#` |

## Estructura

```
productores/     4 productores (python, nodejs, go, java)
consumidores/    4 consumidores (python, nodejs, go, java)
docker/          docker-compose.yml por escenario y nodo
evidencias/      logs y capturas de ejecución
```

## Variables de entorno

Todos los servicios leen:

| Variable | Valor por defecto | Uso |
|---|---|---|
| `RABBITMQ_HOST` | `localhost` (en compose: `rabbitmq`) | Host del broker |
| `RABBITMQ_PORT` | `5672` | Puerto AMQP |
| `RABBITMQ_USER` | `guest` (en compose: `taller`) | Usuario |
| `RABBITMQ_PASS` | `guest` (en compose: `taller`) | Contraseña |
| `NOMBRE_SERVICIO` | según servicio | Nombre que aparece en los logs |
| `INTERVALO_SEGUNDOS` | `3` | Solo productores: segundos entre eventos |

Los servicios reintentan la conexión cada 2 segundos hasta que el broker responda.

## Ejecución: escenario 1 (centralizado)

Requiere Docker Desktop en ejecución.

```bash
cd docker/escenario-1
docker compose up -d --build
docker compose logs -f
```

Interfaz de administración de RabbitMQ: <http://localhost:15672> (usuario `taller`, contraseña `taller`).

Para detener y eliminar los contenedores:

```bash
docker compose down
```

## Escenarios 2 y 3 (distribuidos)

Cada nodo tiene su propio `docker-compose.yml`:

| Escenario | Nodo | Contenido | Archivo |
|---|---|---|---|
| 2 | 1 | Productores y consumidores | `docker/escenario-2/nodo-1/docker-compose.yml` |
| 2 | 2 | Broker | `docker/escenario-2/nodo-2/docker-compose.yml` |
| 3 | 1 | Grupo A: prod-pedidos, prod-pagos, cons-facturacion, cons-inventario | `docker/escenario-3/nodo-1/docker-compose.yml` |
| 3 | 2 | Grupo B: prod-inventario, prod-usuarios, cons-notificaciones, cons-auditoria | `docker/escenario-3/nodo-2/docker-compose.yml` |
| 3 | 3 | Broker | `docker/escenario-3/nodo-3/docker-compose.yml` |

En los nodos cliente, `RABBITMQ_HOST` es obligatorio y debe ser la IP del nodo del broker. El nombre `rabbitmq` solo resuelve dentro de un mismo nodo.

```bash
# Nodo del broker
cd docker/escenario-2/nodo-2        # o docker/escenario-3/nodo-3
docker compose up -d

# Cada nodo cliente
cd docker/escenario-2/nodo-1        # o docker/escenario-3/nodo-1 y nodo-2
RABBITMQ_HOST=<IP-del-broker> docker compose up -d --build
```
