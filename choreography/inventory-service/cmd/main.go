package main

import (
	"log/slog"
	"os"

	_ "github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/events"
	_ "github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/model"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	slog.Info("starting inventory-service")
}
