package kafka

import (
	"github.com/segmentio/kafka-go"
)

const (
	url         = "https://stream.wikimedia.org/v2/stream/recentchange"
	topic       = "wiki-events"
	parition    = 5
	kafkaServer = "localhost:9092"
)

func connectKafka() {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaServer),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		Async:                  true,
	}

	defer w.Close()

}
