package config

import "github.com/caarlos0/env/v11"

type Config struct {
	HTTPPort string `env:"HTTP_PORT" envDefault:"8080"`
}

func Load() (Config, error) {
	return env.ParseAs[Config]()
}
