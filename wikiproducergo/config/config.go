package config

type ProducerConfig struct {
	URL       string
	Topic     string
	Partition int
	Server    string
	Group     string
}
