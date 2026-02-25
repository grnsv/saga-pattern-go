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

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/config"
	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/handler"
	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/idempotency"
	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/kafka"
	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/store"
	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/telemetry"
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
	shutdownTracing, err := telemetry.Setup(ctx, "order-service", cfg.OTELEndpoint)
	if err != nil {
		slog.ErrorContext(ctx, "failed to setup tracing", "error", err)
		stop()
		os.Exit(1)
	}
	defer stop()

	orderStore := store.NewInMemoryOrderStore()
	producer := kafka.NewProducer(cfg.KafkaBrokers)
	dedup := idempotency.NewDeduplicator(cfg.DeduplicationTTL)

	httpHandler := handler.NewHTTPHandler(orderStore, producer)
	sagaEventHandler := handler.NewSagaEventHandler(orderStore)

	mux := http.NewServeMux()
	httpHandler.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: otelhttp.NewHandler(mux, "order-service"),
	}

	paymentEventsConsumer := kafka.NewConsumer(
		cfg.KafkaBrokers,
		"payment-events",
		"order-service",
		map[events.EventType]kafka.EventHandler{
			events.PaymentFailed:     dedup.Wrap(sagaEventHandler.HandlePaymentFailed),
			events.PaymentRolledBack: dedup.Wrap(sagaEventHandler.HandlePaymentRolledBack),
		},
	)

	inventoryEventsConsumer := kafka.NewConsumer(
		cfg.KafkaBrokers,
		"inventory-events",
		"order-service",
		map[events.EventType]kafka.EventHandler{
			events.InventoryReserved: dedup.Wrap(sagaEventHandler.HandleInventoryReserved),
		},
	)

	var wg sync.WaitGroup

	wg.Go(func() {
		slog.InfoContext(ctx, "starting payment-events consumer")
		if err := paymentEventsConsumer.Start(ctx); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "payment-events consumer error", "error", err)
		}
	})

	wg.Go(func() {
		slog.InfoContext(ctx, "starting inventory-events consumer")
		if err := inventoryEventsConsumer.Start(ctx); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "inventory-events consumer error", "error", err)
		}
	})

	go func() {
		slog.InfoContext(ctx, "starting order-service", "port", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.ErrorContext(ctx, "http server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.InfoContext(ctx, "shutting down order-service")

	if err := paymentEventsConsumer.Close(); err != nil {
		slog.ErrorContext(ctx, "failed to close payment-events consumer", "error", err)
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

	slog.InfoContext(ctx, "order-service stopped")
}
