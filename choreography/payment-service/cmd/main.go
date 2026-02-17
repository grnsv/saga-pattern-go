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
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	wg.Go(func() {
		slog.Info("starting order-events consumer")
		if err := orderEventsConsumer.Start(ctx); err != nil && ctx.Err() == nil {
			slog.Error("order-events consumer error", "error", err)
		}
	})

	wg.Go(func() {
		slog.Info("starting inventory-events consumer")
		if err := inventoryEventsConsumer.Start(ctx); err != nil && ctx.Err() == nil {
			slog.Error("inventory-events consumer error", "error", err)
		}
	})

	go func() {
		slog.Info("starting payment-service", "port", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down payment-service")

	if err := orderEventsConsumer.Close(); err != nil {
		slog.Error("failed to close order-events consumer", "error", err)
	}
	if err := inventoryEventsConsumer.Close(); err != nil {
		slog.Error("failed to close inventory-events consumer", "error", err)
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

	slog.Info("payment-service stopped")
}
