package main

import (
	"context"
	"github.com/r3labs/sse/v2"
	"github.com/segmentio/kafka-go"
	"log"
)

const (
	url         = "https://stream.wikimedia.org/v2/stream/recentchange"
	topic       = "wiki-events"
	parition    = 5
	kafkaServer = "localhost:9092"
	groudId     = "wikiEvents"
)

type application struct{}

func main() {
	app := &application{}
	w, err := app.createKafkaWriter(topic, kafkaServer)
	if err != nil {
		log.Fatal("Your writer failed to start...")
	}
	defer w.Close()

	client := app.startSSE(url)

	client.SubscribeRaw(func(msg *sse.Event) {
		err = w.WriteMessages(context.Background(), kafka.Message{Key: []byte(groudId), Value: []byte(msg.Data)})
		log.Printf("Logger info: %#v ||| Message: %s", w.Logger, msg.Data)
	})
}
