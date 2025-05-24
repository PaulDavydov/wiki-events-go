package main

import (
	"context"
	"github.com/r3labs/sse/v2"
	"github.com/segmentio/kafka-go"
	"log"
	"pauldavydov/producer/config"
)

type application struct{}

func main() {
	app := &application{}

	pConfig := config.ProducerConfig{
		URL:       "https://stream.wikimedia.org/v2/stream/recentchange",
		Topic:     "wiki-events",
		Partition: 5,
		Server:    "localhost:9092",
		Group:     "wikiEvents",
	}

	w, err := app.createKafkaWriter(pConfig.Server, pConfig.Topic)
	if err != nil {
		log.Fatal("Your writer failed to start...")
	}
	defer w.Close()

	client := app.startSSE(pConfig.URL)

	client.SubscribeRaw(func(msg *sse.Event) {
		err = w.WriteMessages(context.Background(), kafka.Message{Key: []byte(pConfig.Group), Value: []byte(msg.Data)})
		log.Printf("Logger info: %#v ||| Message: %s", w.Topic, msg.Data)
	})
}
