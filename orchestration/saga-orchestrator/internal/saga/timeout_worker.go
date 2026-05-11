package saga

import (
	"context"
	"log/slog"
	"time"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/store"
)

const timeoutEvent = "Timeout"

// TimeoutWorker periodically checks for sagas whose step deadline has expired
// and either retries the current command or transitions to a compensation/failure state.
type TimeoutWorker struct {
	store        store.SagaStore
	orchestrator *Orchestrator
	interval     time.Duration
	maxRetries   int
}

// NewTimeoutWorker creates a TimeoutWorker.
func NewTimeoutWorker(s store.SagaStore, o *Orchestrator, interval time.Duration, maxRetries int) *TimeoutWorker {
	return &TimeoutWorker{
		store:        s,
		orchestrator: o,
		interval:     interval,
		maxRetries:   maxRetries,
	}
}

// Run starts the polling loop. It blocks until ctx is cancelled.
func (w *TimeoutWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.CheckTimeouts(ctx); err != nil {
				slog.ErrorContext(ctx, "timeout check failed", "error", err)
			}
		}
	}
}

// CheckTimeouts fetches all timed-out sagas and processes each one.
func (w *TimeoutWorker) CheckTimeouts(ctx context.Context) error {
	sagas, err := w.store.ListTimedOut(ctx, time.Now())
	if err != nil {
		return err
	}
	for _, saga := range sagas {
		if err := w.processSaga(ctx, saga); err != nil {
			slog.ErrorContext(ctx, "timeout processing failed",
				"correlationId", saga.CorrelationID,
				"state", saga.State,
				"error", err,
			)
		}
	}
	return nil
}

// processSaga decides whether to retry the current command or exhaust retries.
func (w *TimeoutWorker) processSaga(ctx context.Context, saga *model.SagaInstance) error {
	if saga.RetryCount < w.maxRetries {
		return w.retrySaga(ctx, saga)
	}
	return w.exhaustSaga(ctx, saga)
}

// retrySaga resends the command for the current step, increments retry_count,
// and refreshes step_deadline.
func (w *TimeoutWorker) retrySaga(ctx context.Context, saga *model.SagaInstance) error {
	saga.RetryCount++
	deadline := time.Now().Add(w.orchestrator.stepTimeout)
	saga.StepDeadline = &deadline

	ok, err := w.store.Update(ctx, saga)
	if err != nil {
		return err
	}
	if !ok {
		slog.InfoContext(ctx, "timeout retry skipped: saga modified concurrently",
			"correlationId", saga.CorrelationID)
		return nil
	}

	slog.InfoContext(ctx, "retrying timed-out saga step",
		"correlationId", saga.CorrelationID,
		"state", saga.State,
		"retryCount", saga.RetryCount,
	)

	return w.resendCommand(ctx, saga)
}

// resendCommand publishes the command appropriate for the saga's current state.
func (w *TimeoutWorker) resendCommand(ctx context.Context, saga *model.SagaInstance) error {
	switch saga.State {
	case model.SagaPaymentPending:
		return w.orchestrator.publishReservePayment(ctx, saga)
	case model.SagaInventoryPending:
		return w.orchestrator.publishReserveInventory(ctx, saga)
	case model.SagaCancelPaymentPending:
		return w.orchestrator.publishCancelPayment(ctx, saga)
	default:
		return nil
	}
}

// exhaustSaga transitions the saga after all retries are exhausted.
func (w *TimeoutWorker) exhaustSaga(ctx context.Context, saga *model.SagaInstance) error {
	switch saga.State {
	case model.SagaPaymentPending:
		fromState := saga.State
		saga.State = model.SagaFailed
		saga.StepDeadline = nil
		return w.updateAndAct(ctx, saga, fromState, func() error {
			return w.orchestrator.publishSagaFailed(ctx, saga, "payment step timed out after retries exhausted")
		})

	case model.SagaInventoryPending:
		fromState := saga.State
		saga.State = model.SagaCancelPaymentPending
		deadline := time.Now().Add(w.orchestrator.stepTimeout)
		saga.StepDeadline = &deadline
		saga.RetryCount = 0
		return w.updateAndAct(ctx, saga, fromState, func() error {
			return w.orchestrator.publishCancelPayment(ctx, saga)
		})

	case model.SagaCancelPaymentPending:
		fromState := saga.State
		saga.State = model.SagaFailed
		saga.StepDeadline = nil
		return w.updateAndAct(ctx, saga, fromState, func() error {
			return w.orchestrator.publishSagaFailed(ctx, saga, "compensation timed out, manual intervention required")
		})

	default:
		return nil
	}
}

// updateAndAct persists the saga with optimistic locking and, on success,
// executes the follow-up action (publish command/event).
func (w *TimeoutWorker) updateAndAct(
	ctx context.Context,
	saga *model.SagaInstance,
	fromState model.SagaState,
	act func() error,
) error {
	ok, err := w.store.Update(ctx, saga)
	if err != nil {
		return err
	}
	if !ok {
		slog.InfoContext(ctx, "timeout exhaust skipped: saga modified concurrently",
			"correlationId", saga.CorrelationID)
		return nil
	}
	slog.InfoContext(ctx, "saga retries exhausted",
		"correlationId", saga.CorrelationID,
		"state", saga.State,
	)
	w.orchestrator.recordHistory(ctx, saga, fromState, saga.State, timeoutEvent)

	return act()
}
