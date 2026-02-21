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

	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/config"
	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/handler"
	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/kafka"
	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/store"
	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/telemetry"
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
	shutdownTracing, err := telemetry.Setup(ctx, "inventory-service", cfg.OTELEndpoint)
	if err != nil {
		stop()
		slog.Error("failed to setup tracing", "error", err)
		os.Exit(1)
	}
	defer stop()

	inventoryStore := store.NewInMemoryInventoryStore()
	producer := kafka.NewProducer(cfg.KafkaBrokers)

	paymentReservedHandler := handler.NewPaymentReservedHandler(inventoryStore, producer, cfg.SuccessRate)

	httpHandler := handler.NewHTTPHandler()
	mux := http.NewServeMux()
	httpHandler.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}

	paymentEventsConsumer := kafka.NewConsumer(
		cfg.KafkaBrokers,
		"payment-events",
		"inventory-service",
		map[events.EventType]kafka.EventHandler{
			events.PaymentReserved: paymentReservedHandler.Handle,
		},
	)

	var wg sync.WaitGroup

	wg.Go(func() {
		slog.Info("starting payment-events consumer")
		if err := paymentEventsConsumer.Start(ctx); err != nil && ctx.Err() == nil {
			slog.Error("payment-events consumer error", "error", err)
		}
	})

	go func() {
		slog.Info("starting inventory-service", "port", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down inventory-service")

	if err := paymentEventsConsumer.Close(); err != nil {
		slog.Error("failed to close payment-events consumer", "error", err)
	}

	wg.Wait()

	if err := producer.Close(); err != nil {
		slog.Error("failed to close producer", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http server shutdown error", "error", err)
	}

	tracingCtx, tracingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer tracingCancel()
	if err := shutdownTracing(tracingCtx); err != nil {
		slog.Error("tracing shutdown error", "error", err)
	}

	slog.Info("inventory-service stopped")
}
