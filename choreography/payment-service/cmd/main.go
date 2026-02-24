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

	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/config"
	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/handler"
	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/kafka"
	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/store"
	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/telemetry"
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
	shutdownTracing, err := telemetry.Setup(ctx, "payment-service", cfg.OTELEndpoint)
	if err != nil {
		slog.ErrorContext(ctx, "failed to setup tracing", "error", err)
		stop()
		os.Exit(1)
	}
	defer stop()

	paymentStore := store.NewInMemoryPaymentStore()
	producer := kafka.NewProducer(cfg.KafkaBrokers)

	orderCreatedHandler := handler.NewOrderCreatedHandler(paymentStore, producer, cfg.SuccessRate)
	inventoryFailedHandler := handler.NewInventoryFailedHandler(paymentStore, producer)

	httpHandler := handler.NewHTTPHandler()
	mux := http.NewServeMux()
	httpHandler.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}

	orderEventsConsumer := kafka.NewConsumer(
		cfg.KafkaBrokers,
		"order-events",
		"payment-service",
		map[events.EventType]kafka.EventHandler{
			events.OrderCreated: orderCreatedHandler.Handle,
		},
	)

	inventoryEventsConsumer := kafka.NewConsumer(
		cfg.KafkaBrokers,
		"inventory-events",
		"payment-service",
		map[events.EventType]kafka.EventHandler{
			events.InventoryFailed: inventoryFailedHandler.Handle,
		},
	)

	var wg sync.WaitGroup

	wg.Go(func() {
		slog.InfoContext(ctx, "starting order-events consumer")
		if err := orderEventsConsumer.Start(ctx); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "order-events consumer error", "error", err)
		}
	})

	wg.Go(func() {
		slog.InfoContext(ctx, "starting inventory-events consumer")
		if err := inventoryEventsConsumer.Start(ctx); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "inventory-events consumer error", "error", err)
		}
	})

	go func() {
		slog.InfoContext(ctx, "starting payment-service", "port", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.ErrorContext(ctx, "http server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.InfoContext(ctx, "shutting down payment-service")

	if err := orderEventsConsumer.Close(); err != nil {
		slog.ErrorContext(ctx, "failed to close order-events consumer", "error", err)
	}
	if err := inventoryEventsConsumer.Close(); err != nil {
		slog.ErrorContext(ctx, "failed to close inventory-events consumer", "error", err)
	}

	wg.Wait()

	if err := producer.Close(); err != nil {
		slog.ErrorContext(ctx, "failed to close producer", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.ErrorContext(shutdownCtx, "http server shutdown error", "error", err)
	}

	tracingCtx, tracingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer tracingCancel()
	if err := shutdownTracing(tracingCtx); err != nil {
		slog.ErrorContext(tracingCtx, "tracing shutdown error", "error", err)
	}

	slog.InfoContext(ctx, "payment-service stopped")
}
