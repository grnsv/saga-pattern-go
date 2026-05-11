package saga

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/messages"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/store"
)

const (
	topicPaymentCommands   = "payment-commands"
	topicInventoryCommands = "inventory-commands"
	topicSagaEvents        = "saga-events"
)

// CommandPublisher publishes serialised Kafka messages.
type CommandPublisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

// Orchestrator drives the saga state machine: it handles incoming events and
// dispatches commands to the appropriate services.
type Orchestrator struct {
	store       store.SagaStore
	machine     *Machine
	publisher   CommandPublisher
	stepTimeout time.Duration
}

// NewOrchestrator wires the orchestrator together with its store and publisher.
// stepTimeout is used to set per-step deadlines for the timeout worker.
// Transitions are configured as closures over the orchestrator's publish helpers.
func NewOrchestrator(s store.SagaStore, p CommandPublisher, stepTimeout time.Duration) *Orchestrator {
	o := &Orchestrator{store: s, publisher: p, stepTimeout: stepTimeout}
	o.machine = NewMachine(o.buildTransitions())
	return o
}

// buildTransitions returns the complete saga transition table.
// Actions close over `o` so they can publish commands and events.
func (o *Orchestrator) buildTransitions() []Transition {
	return []Transition{
		{
			From:  model.SagaPaymentPending,
			Event: messages.EvtPaymentReserved,
			To:    model.SagaInventoryPending,
			Action: func(ctx context.Context, s *model.SagaInstance) error {
				return o.publishReserveInventory(ctx, s)
			},
		},
		{
			From:  model.SagaPaymentPending,
			Event: messages.EvtPaymentFailed,
			To:    model.SagaFailed,
			Action: func(ctx context.Context, s *model.SagaInstance) error {
				return o.publishSagaFailed(ctx, s, "payment failed")
			},
		},
		{
			From:  model.SagaInventoryPending,
			Event: messages.EvtInventoryReserved,
			To:    model.SagaCompleted,
			Action: func(ctx context.Context, s *model.SagaInstance) error {
				return o.publishSagaCompleted(ctx, s)
			},
		},
		{
			From:  model.SagaInventoryPending,
			Event: messages.EvtInventoryFailed,
			To:    model.SagaCancelPaymentPending,
			Action: func(ctx context.Context, s *model.SagaInstance) error {
				return o.publishCancelPayment(ctx, s)
			},
		},
		{
			From:  model.SagaCancelPaymentPending,
			Event: messages.EvtPaymentCancelled,
			To:    model.SagaFailed,
			Action: func(ctx context.Context, s *model.SagaInstance) error {
				return o.publishSagaFailed(ctx, s, "payment cancelled after inventory failure")
			},
		},
	}
}

// StartSaga creates a new saga for the given order and kicks off the flow by
// sending a ReservePayment command to the payment service.
func (o *Orchestrator) StartSaga(ctx context.Context, correlationID string, payload messages.StartSagaPayload) error {
	now := time.Now()
	deadline := now.Add(o.stepTimeout)
	saga := &model.SagaInstance{
		ID:            uuid.NewString(),
		CorrelationID: correlationID,
		OrderID:       payload.OrderID,
		State:         model.SagaPaymentPending,
		Item:          payload.Item,
		Qty:           payload.Qty,
		Amount:        payload.Amount,
		CreatedAt:     now,
		UpdatedAt:     now,
		StepDeadline:  &deadline,
	}

	// Persist first: if Kafka publish fails, the timeout worker
	// will detect PAYMENT_PENDING sagas with no response and retry.
	if err := o.store.Create(ctx, saga); err != nil {
		return fmt.Errorf("create saga: %w", err)
	}
	o.recordHistory(ctx, saga, model.SagaStarted, saga.State, string(messages.CmdStartSaga))

	if err := o.publishReservePayment(ctx, saga); err != nil {
		return fmt.Errorf("publish ReservePayment: %w", err)
	}

	slog.InfoContext(ctx, "saga started",
		"correlationId", correlationID,
		"orderId", payload.OrderID,
		"state", saga.State,
	)
	return nil
}

// HandleEvent processes an incoming event: it fetches the saga, runs the state
// machine transition, persists the new state using optimistic locking, and only
// then executes the transition action (e.g. publishing a command). This ordering
// ensures that a command is never published for a state that wasn't persisted.
func (o *Orchestrator) HandleEvent(ctx context.Context, event *messages.Event) error {
	saga, err := o.store.Get(ctx, event.CorrelationID)
	if err != nil {
		slog.WarnContext(ctx, "saga not found for event",
			"correlationId", event.CorrelationID,
			"eventType", event.Type,
		)
		return nil //nolint:nilerr // soft failure: unknown saga is not retryable
	}

	fromState := saga.State
	action, err := o.machine.Apply(saga, event.Type)
	if err != nil {
		slog.WarnContext(ctx, "no valid transition, skipping event",
			"correlationId", event.CorrelationID,
			"state", saga.State,
			"event", event.Type,
			"error", err,
		)
		return nil
	}

	o.setStepDeadline(saga)

	ok, err := o.store.Update(ctx, saga)
	if err != nil {
		return fmt.Errorf("update saga: %w", err)
	}
	if !ok {
		slog.WarnContext(ctx, "saga modified concurrently, skipping update",
			"correlationId", event.CorrelationID,
		)
		return nil
	}
	slog.InfoContext(ctx, "saga state updated",
		"correlationId", event.CorrelationID,
		"event", event.Type,
		"state", saga.State,
	)
	o.recordHistory(ctx, saga, fromState, saga.State, string(event.Type))

	// Execute the action after successful persist to avoid publishing commands
	// for states that were never written to the store.
	if action != nil {
		if err := action(ctx, saga); err != nil {
			return fmt.Errorf("execute transition action: %w", err)
		}
	}
	return nil
}

func (o *Orchestrator) recordHistory(
	ctx context.Context,
	saga *model.SagaInstance,
	fromState model.SagaState,
	toState model.SagaState,
	event string,
) {
	if err := o.store.RecordHistory(ctx, newHistoryEntry(saga, fromState, toState, event)); err != nil {
		slog.WarnContext(ctx, "failed to record saga history",
			"correlationId", saga.CorrelationID,
			"sagaId", saga.ID,
			"error", err,
		)
	}
}

func newHistoryEntry(
	saga *model.SagaInstance,
	fromState model.SagaState,
	toState model.SagaState,
	event string,
) *model.SagaHistoryEntry {
	return &model.SagaHistoryEntry{
		SagaID:    saga.ID,
		FromState: fromState,
		ToState:   toState,
		Event:     event,
		CreatedAt: time.Now(),
	}
}

// setStepDeadline sets or clears saga.StepDeadline based on the new state.
// Intermediate states get a fresh deadline; terminal states are cleared.
func (o *Orchestrator) setStepDeadline(saga *model.SagaInstance) {
	switch saga.State {
	case model.SagaInventoryPending, model.SagaCancelPaymentPending:
		deadline := time.Now().Add(o.stepTimeout)
		saga.StepDeadline = &deadline
	default:
		saga.StepDeadline = nil
	}
}

// --- publish helpers ---

func (o *Orchestrator) publishReservePayment(ctx context.Context, saga *model.SagaInstance) error {
	return o.publishCommand(ctx, topicPaymentCommands, saga.CorrelationID,
		messages.CmdReservePayment,
		messages.ReservePaymentPayload{
			OrderID: saga.OrderID,
			Item:    saga.Item,
			Qty:     saga.Qty,
			Amount:  saga.Amount,
		},
	)
}

func (o *Orchestrator) publishReserveInventory(ctx context.Context, saga *model.SagaInstance) error {
	return o.publishCommand(ctx, topicInventoryCommands, saga.CorrelationID,
		messages.CmdReserveInventory,
		messages.ReserveInventoryPayload{
			OrderID: saga.OrderID,
			Item:    saga.Item,
			Qty:     saga.Qty,
		},
	)
}

func (o *Orchestrator) publishCancelPayment(ctx context.Context, saga *model.SagaInstance) error {
	return o.publishCommand(ctx, topicPaymentCommands, saga.CorrelationID,
		messages.CmdCancelPayment,
		messages.CancelPaymentPayload{OrderID: saga.OrderID},
	)
}

func (o *Orchestrator) publishSagaCompleted(ctx context.Context, saga *model.SagaInstance) error {
	return o.publishEvent(ctx, topicSagaEvents, saga.CorrelationID,
		messages.EvtSagaCompleted,
		messages.SagaResultPayload{OrderID: saga.OrderID},
	)
}

func (o *Orchestrator) publishSagaFailed(ctx context.Context, saga *model.SagaInstance, reason string) error {
	return o.publishEvent(ctx, topicSagaEvents, saga.CorrelationID,
		messages.EvtSagaFailed,
		messages.SagaResultPayload{OrderID: saga.OrderID, Reason: reason},
	)
}

func (o *Orchestrator) publishCommand(ctx context.Context, topic, correlationID string, cmdType messages.CommandType, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	cmd := messages.Command{
		ID:            uuid.NewString(),
		CorrelationID: correlationID,
		Type:          cmdType,
		Timestamp:     time.Now().UTC(),
		Payload:       data,
	}
	envelope, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return o.publisher.Publish(ctx, topic, correlationID, envelope)
}

func (o *Orchestrator) publishEvent(ctx context.Context, topic, correlationID string, evtType messages.EventType, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	evt := messages.Event{
		ID:            uuid.NewString(),
		CorrelationID: correlationID,
		Type:          evtType,
		Timestamp:     time.Now().UTC(),
		Payload:       data,
	}
	envelope, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	return o.publisher.Publish(ctx, topic, correlationID, envelope)
}
