package config

import (
	"time"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	HTTPPort         string        `env:"HTTP_PORT"          envDefault:"8080"`
	KafkaBrokers     []string      `env:"KAFKA_BROKERS"      envDefault:"localhost:9092" envSeparator:","`
	SuccessRate      float64       `env:"SUCCESS_RATE"       envDefault:"0.8"`
	ChaosMode        bool          `env:"CHAOS_MODE"         envDefault:"false"`
	OTELEndpoint     string        `env:"OTEL_ENDPOINT"      envDefault:""`
	DeduplicationTTL time.Duration `env:"DEDUPLICATION_TTL"  envDefault:"24h"`
}

func Load() (Config, error) {
	return env.ParseAs[Config]()
}
