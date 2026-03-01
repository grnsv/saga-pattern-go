package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/grnsv/saga-pattern-go/orchestration/inventory-service/internal/config"
	"github.com/grnsv/saga-pattern-go/orchestration/inventory-service/internal/handler"
	"github.com/grnsv/saga-pattern-go/orchestration/inventory-service/internal/kafka"
	"github.com/grnsv/saga-pattern-go/orchestration/inventory-service/internal/store"
)

const (
	inventoryCommandsTopic = "inventory-commands"
	consumerGroupID        = "inventory-service"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	inventoryStore := store.NewInMemoryInventoryStore()
	producer := kafka.NewProducer(cfg.KafkaBrokers)
	cmdHandler := handler.NewCommandHandler(inventoryStore, producer, cfg.SuccessRate)
	consumer := kafka.NewConsumer(cfg.KafkaBrokers, inventoryCommandsTopic, consumerGroupID, cmdHandler.Handle)

	mux := http.NewServeMux()
	handler.RegisterProbeRoutes(mux)

	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		slog.InfoContext(ctx, "starting kafka consumer", "topic", inventoryCommandsTopic)
		if err := consumer.Start(ctx); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "consumer error", "error", err)
			stop()
		}
	})

	go func() {
		slog.InfoContext(ctx, "starting inventory-service", "port", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.ErrorContext(ctx, "http server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.InfoContext(ctx, "shutting down inventory-service")

	wg.Wait()

	if err := consumer.Close(); err != nil {
		slog.Error("consumer close error", "error", err)
	}
	if err := producer.Close(); err != nil {
		slog.Error("producer close error", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.ErrorContext(shutdownCtx, "http server shutdown error", "error", err)
	}

	slog.InfoContext(ctx, "inventory-service stopped")
}
