package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"pauldavydov/consumer/config"
)

func main() {
	cConfig := config.ConsumerConfig{
		Topic:  "wiki-events",
		Server: "localhost:9092",
		Group:  "wikiEvents",
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cConfig.Server},
		Topic:   cConfig.Topic,
		GroupID: cConfig.Group,
	})

	defer func() {
		if err := r.Close(); err != nil {
			log.Fatalf("failed to close consumer: %v", err)
		}
	}()
	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("failed to read message: %v", err)
		}
		fmt.Printf("Received message at offset %d: %s = %s\n", msg.Offset, string(msg.Key), string(msg.Value))
	}
}
