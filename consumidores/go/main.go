package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchange = "eventos.negocio"
	cola     = "cola.auditoria"
	patron   = "#"
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
	nombre := valorPorDefecto("NOMBRE_SERVICIO", "cons-auditoria")

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
	if _, err := canal.QueueDeclare(cola, true, false, false, false, nil); err != nil {
		log.Fatalf("declarar cola: %v", err)
	}
	if err := canal.QueueBind(cola, patron, exchange, false, nil); err != nil {
		log.Fatalf("enlazar cola: %v", err)
	}
	if err := canal.Qos(1, 0, false); err != nil {
		log.Fatalf("configurar prefetch: %v", err)
	}

	entregas, err := canal.Consume(cola, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consumir cola: %v", err)
	}
	log.Printf("[%s] esperando eventos en %s con '%s'", nombre, cola, patron)

	for entrega := range entregas {
		var ev evento
		if err := json.Unmarshal(entrega.Body, &ev); err != nil {
			log.Printf("[%s] mensaje inválido: %v", nombre, err)
			entrega.Nack(false, false)
			continue
		}
		log.Printf("[%s] recibido %s | id=%s | datos=%v", nombre, entrega.RoutingKey, ev.ID, ev.Datos)
		entrega.Ack(false)
	}

	log.Fatalf("[%s] canal cerrado por el broker", nombre)
}
