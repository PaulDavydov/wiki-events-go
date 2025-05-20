package main

import (
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
)

func logf(msg string, a ...interface{}) {
	fmt.Printf(msg, a...)
	fmt.Println()
}

func (app *application) createKafkaWriter(kafkaServer, topic string) (*kafka.Writer, error) {
	w := &kafka.Writer{
		Addr:        kafka.TCP(kafkaServer),
		Topic:       topic,
		Async:       true,
		Balancer:    &kafka.Hash{},
		Logger:      kafka.LoggerFunc(logf),
		ErrorLogger: kafka.LoggerFunc(logf),
	}

	if w == nil {
		return w, errors.New("Writer did not start")
	}

	return w, nil
}
