package saga

import (
	"context"
	"fmt"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/messages"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
)

// Transition describes a valid state-machine edge: from a given state, on a given
// event type, move to a new state and optionally execute a side-effect action.
type Transition struct {
	From   model.SagaState
	Event  messages.EventType
	To     model.SagaState
	Action func(ctx context.Context, saga *model.SagaInstance) error
}

// Machine is a pure state machine for saga transitions.
// Actions are closures injected at construction time; the machine itself is stateless.
type Machine struct {
	transitions []Transition
}

// NewMachine creates a Machine with the provided transition table.
func NewMachine(transitions []Transition) *Machine {
	return &Machine{transitions: transitions}
}

// Apply finds the matching transition for the current saga state and the given event,
// updates saga.State, and returns the associated action (if any) without executing it.
// The caller must execute the returned action after successfully persisting the new state.
// Returns an error if no transition is defined for the (From, Event) pair.
func (m *Machine) Apply(saga *model.SagaInstance, event messages.EventType) (func(context.Context, *model.SagaInstance) error, error) {
	for _, t := range m.transitions {
		if t.From == saga.State && t.Event == event {
			saga.State = t.To
			return t.Action, nil
		}
	}
	return nil, fmt.Errorf("no transition from state %q on event %q", saga.State, event)
}
