package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/config"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/handler"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/kafka"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/saga"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/store"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/migrations"
)

const (
	topicSagaCommands    = "saga-commands"
	topicPaymentEvents   = "payment-events"
	topicInventoryEvents = "inventory-events"
	consumerGroupID      = "saga-orchestrator"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// --- Migrations and DB init (before context, so os.Exit is safe) ---
	if err := runMigrations(cfg.DatabaseURL); err != nil {
		slog.Error("failed to run migrations", "error", err)
		os.Exit(1)
	}

	sagaStore, err := store.NewPostgresSagaStore(context.Background(), cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- Kafka ---
	producer := kafka.NewProducer(cfg.KafkaBrokers)
	orchestrator := saga.NewOrchestrator(sagaStore, producer, cfg.StepTimeout)
	kafkaHandler := handler.NewKafkaHandler(orchestrator)

	sagaCmdConsumer := kafka.NewConsumer(cfg.KafkaBrokers, topicSagaCommands, consumerGroupID, kafkaHandler.HandleCommand)
	paymentEvtConsumer := kafka.NewConsumer(cfg.KafkaBrokers, topicPaymentEvents, consumerGroupID, kafkaHandler.HandleEvent)
	inventoryEvtConsumer := kafka.NewConsumer(cfg.KafkaBrokers, topicInventoryEvents, consumerGroupID, kafkaHandler.HandleEvent)

	// --- HTTP ---
	httpHandler := handler.NewHTTPHandler(sagaStore)
	mux := http.NewServeMux()
	httpHandler.RegisterRoutes(mux)
	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}

	// --- Timeout worker ---
	timeoutWorker := saga.NewTimeoutWorker(sagaStore, orchestrator, cfg.TimeoutCheckInterval, cfg.MaxRetries)

	// --- Start consumers and timeout worker ---
	var wg sync.WaitGroup
	for _, c := range []*kafka.Consumer{sagaCmdConsumer, paymentEvtConsumer, inventoryEvtConsumer} {
		wg.Go(func() {
			if err := c.Start(ctx); err != nil && ctx.Err() == nil {
				slog.ErrorContext(ctx, "consumer error", "error", err)
				stop()
			}
		})
	}
	wg.Go(func() {
		timeoutWorker.Run(ctx)
	})

	go func() {
		slog.InfoContext(ctx, "starting saga-orchestrator",
			"port", cfg.HTTPPort,
			"brokers", cfg.KafkaBrokers,
			"stepTimeout", cfg.StepTimeout,
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.ErrorContext(ctx, "http server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.InfoContext(ctx, "shutting down saga-orchestrator")

	wg.Wait()

	for _, c := range []*kafka.Consumer{sagaCmdConsumer, paymentEvtConsumer, inventoryEvtConsumer} {
		if err := c.Close(); err != nil {
			slog.Error("consumer close error", "error", err)
		}
	}
	if err := producer.Close(); err != nil {
		slog.Error("producer close error", "error", err)
	}
	sagaStore.Close()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.ErrorContext(shutdownCtx, "http server shutdown error", "error", err)
	}

	slog.InfoContext(ctx, "saga-orchestrator stopped")
}

// runMigrations applies all pending SQL migrations using the embedded FS.
// The pgx/v5 migrate driver expects a pgx5:// URL scheme.
func runMigrations(databaseURL string) error {
	migURL := strings.NewReplacer(
		"postgres://", "pgx5://",
		"postgresql://", "pgx5://",
	).Replace(databaseURL)

	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("create migration source: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", src, migURL)
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
