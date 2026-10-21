package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchange   = "eventos.negocio"
	routingKey = "inventario.actualizado"
)

type evento struct {
	ID     string         `json:"id"`
	Evento string         `json:"evento"`
	Origen string         `json:"origen"`
	Fecha  string         `json:"fecha"`
	Datos  map[string]any `json:"datos"`
}

func valorPorDefecto(clave, porDefecto string) string {
	if valor := os.Getenv(clave); valor != "" {
		return valor
	}
	return porDefecto
}

func conectar(url, nombre string) *amqp.Connection {
	for {
		conexion, err := amqp.Dial(url)
		if err == nil {
			return conexion
		}
		log.Printf("[%s] broker no disponible (%v), reintentando en 2 s", nombre, err)
		time.Sleep(2 * time.Second)
	}
}

func main() {
	host := valorPorDefecto("RABBITMQ_HOST", "localhost")
	port := valorPorDefecto("RABBITMQ_PORT", "5672")
	user := valorPorDefecto("RABBITMQ_USER", "guest")
	pass := valorPorDefecto("RABBITMQ_PASS", "guest")
	nombre := valorPorDefecto("NOMBRE_SERVICIO", "prod-inventario")

	intervalo, err := strconv.ParseFloat(valorPorDefecto("INTERVALO_SEGUNDOS", "3"), 64)
	if err != nil {
		log.Fatalf("INTERVALO_SEGUNDOS inválido: %v", err)
	}

	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, pass, host, port)
	conexion := conectar(url, nombre)
	defer conexion.Close()

	canal, err := conexion.Channel()
	if err != nil {
		log.Fatalf("abrir canal: %v", err)
	}
	defer canal.Close()

	if err := canal.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		log.Fatalf("declarar exchange: %v", err)
	}
	log.Printf("[%s] conectado a %s:%s, publicando '%s' cada %v s", nombre, host, port, routingKey, intervalo)

	ticker := time.NewTicker(time.Duration(intervalo * float64(time.Second)))
	defer ticker.Stop()

	contador := 1
	for range ticker.C {
		ev := evento{
			ID:     fmt.Sprintf("%s-%d-%d", nombre, contador, time.Now().UnixNano()),
			Evento: routingKey,
			Origen: nombre,
			Fecha:  time.Now().UTC().Format(time.RFC3339),
			Datos: map[string]any{
				"producto": fmt.Sprintf("SKU-%03d", rand.Intn(20)+1),
				"stock":    rand.Intn(101),
			},
		}
		cuerpo, err := json.Marshal(ev)
		if err != nil {
			log.Fatalf("serializar evento: %v", err)
		}

		err = canal.PublishWithContext(context.Background(), exchange, routingKey, false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         cuerpo,
		})
		if err != nil {
			log.Fatalf("publicar evento: %v", err)
		}
		log.Printf("[%s] publicado %s #%d", nombre, routingKey, contador)
		contador++
	}
}
