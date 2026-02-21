package config

import "github.com/caarlos0/env/v11"

type Config struct {
	HTTPPort     string   `env:"HTTP_PORT"     envDefault:"8080"`
	KafkaBrokers []string `env:"KAFKA_BROKERS" envDefault:"localhost:9092" envSeparator:","`
	OTELEndpoint string   `env:"OTEL_ENDPOINT" envDefault:""`
}

func Load() (Config, error) {
	return env.ParseAs[Config]()
}
