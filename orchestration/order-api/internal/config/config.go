package config

import (
	"time"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	HTTPPort         string        `env:"HTTP_PORT"         envDefault:"8080"`
	KafkaBrokers     []string      `env:"KAFKA_BROKERS"     envDefault:"localhost:9092" envSeparator:","`
	DeduplicationTTL time.Duration `env:"DEDUPLICATION_TTL" envDefault:"24h"`
}

func Load() (Config, error) {
	return env.ParseAs[Config]()
}
