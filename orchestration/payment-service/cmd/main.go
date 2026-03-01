package main

import (
	"log/slog"
	"os"

	"github.com/grnsv/saga-pattern-go/orchestration/payment-service/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	slog.Info("starting payment-service", "port", cfg.HTTPPort, "brokers", cfg.KafkaBrokers)
}
