package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"pauldavydov/consumer/config"
)

type application struct{}

type document struct {
	Schema string
}

func main() {
	cConfig := config.ConsumerConfig{
		Topic:  "wiki-events",
		Server: "localhost:9092",
		Group:  "wikiEvents",
	}
	//dependency injection hack
	app := &application{}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cConfig.Server},
		Topic:   cConfig.Topic,
		GroupID: cConfig.Group,
	})

	// start opensearch connection
	client, err := app.startOpenSearch()
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
	log.Println("Connection to OpenSearch done")

	defer func() {
		if err := r.Close(); err != nil {
			log.Fatalf("failed to close consumer: %v", err)
		}
	}()

	for {
		msg, err := r.ReadMessage(context.Background())
		fmt.Println("Check")
		if err != nil {
			log.Fatalf("failed to read message: %v", err)
		}
		
		d := document{
			string(msg.Value),
		}

		insertResp, err := app.insertOpenSearchIndex(d, *client)
		if err != nil {
			log.Fatalf("Failed to push to opensearch: %v", err)
		}
		log.Printf("Created recrod in %s\n ID: %s\n", insertResp.Index, insertResp.ID)
		log.Printf("Received message at offset %d: %s = %s\n", cConfig.Topic, string(msg.Key), string(msg.Value))
	}
}
