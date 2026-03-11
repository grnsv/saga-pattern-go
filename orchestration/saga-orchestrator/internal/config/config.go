package config

import (
	"time"

	"github.com/caarlos0/env/v11"
)

// Config holds all environment-based configuration for the saga-orchestrator.
type Config struct {
	HTTPPort             string        `env:"HTTP_PORT"               envDefault:"8081"`
	KafkaBrokers         []string      `env:"KAFKA_BROKERS"           envDefault:"localhost:9092" envSeparator:","`
	DatabaseURL          string        `env:"DATABASE_URL"            envDefault:"postgres://saga:saga@localhost:5432/saga?sslmode=disable"`
	StepTimeout          time.Duration `env:"STEP_TIMEOUT"            envDefault:"5s"`
	MaxRetries           int           `env:"MAX_RETRIES"             envDefault:"3"`
	TimeoutCheckInterval time.Duration `env:"TIMEOUT_CHECK_INTERVAL"  envDefault:"1s"`
}

// Load parses configuration from environment variables.
func Load() (Config, error) {
	return env.ParseAs[Config]()
}
