package main

import (
	"log/slog"
	"os"

	_ "github.com/grnsv/saga-pattern-go/choreography/order-service/internal/events"
	_ "github.com/grnsv/saga-pattern-go/choreography/order-service/internal/model"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	slog.Info("starting order-service")
}
