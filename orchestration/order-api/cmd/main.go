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

	"github.com/grnsv/saga-pattern-go/orchestration/order-api/internal/config"
	"github.com/grnsv/saga-pattern-go/orchestration/order-api/internal/handler"
	"github.com/grnsv/saga-pattern-go/orchestration/order-api/internal/idempotency"
	"github.com/grnsv/saga-pattern-go/orchestration/order-api/internal/kafka"
	"github.com/grnsv/saga-pattern-go/orchestration/order-api/internal/store"
)

const (
	topicSagaEvents = "saga-events"
	consumerGroupID = "order-api"
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

	orderStore := store.NewInMemoryOrderStore()
	producer := kafka.NewProducer(cfg.KafkaBrokers)

	dedup := idempotency.NewDeduplicator(cfg.DeduplicationTTL)
	sagaEventHandler := handler.NewSagaEventHandler(orderStore)
	sagaEvtConsumer := kafka.NewConsumer(cfg.KafkaBrokers, topicSagaEvents, consumerGroupID, dedup.Wrap(sagaEventHandler.Handle))

	httpHandler := handler.NewHTTPHandler(orderStore, producer)
	mux := http.NewServeMux()
	httpHandler.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		slog.InfoContext(ctx, "starting saga-events consumer")
		if err := sagaEvtConsumer.Start(ctx); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "consumer error", "error", err)
			stop()
		}
	})

	go func() {
		slog.InfoContext(ctx, "starting order-api", "port", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.ErrorContext(ctx, "http server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.InfoContext(ctx, "shutting down order-api")

	wg.Wait()

	if err := sagaEvtConsumer.Close(); err != nil {
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

	slog.InfoContext(ctx, "order-api stopped")
}
