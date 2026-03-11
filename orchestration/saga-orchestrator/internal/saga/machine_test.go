package saga_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/messages"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/saga"
)

// actionRecorder tracks whether a transition action was called.
type actionRecorder struct {
	called bool
	err    error
}

func (r *actionRecorder) record(_ context.Context, _ *model.SagaInstance) error {
	r.called = true
	return r.err
}

func baseSaga(state model.SagaState) *model.SagaInstance {
	return &model.SagaInstance{
		ID:            "test-saga",
		CorrelationID: "test-corr",
		State:         state,
	}
}

func TestMachine_Apply(t *testing.T) {
	tests := []struct {
		name          string
		fromState     model.SagaState
		event         messages.EventType
		expectedState model.SagaState
		actionCalled  bool
	}{
		{
			name:          "payment reserved → inventory pending",
			fromState:     model.SagaPaymentPending,
			event:         messages.EvtPaymentReserved,
			expectedState: model.SagaInventoryPending,
			actionCalled:  true,
		},
		{
			name:          "payment failed → failed",
			fromState:     model.SagaPaymentPending,
			event:         messages.EvtPaymentFailed,
			expectedState: model.SagaFailed,
			actionCalled:  true,
		},
		{
			name:          "inventory reserved → completed",
			fromState:     model.SagaInventoryPending,
			event:         messages.EvtInventoryReserved,
			expectedState: model.SagaCompleted,
			actionCalled:  true,
		},
		{
			name:          "inventory failed → cancel payment pending",
			fromState:     model.SagaInventoryPending,
			event:         messages.EvtInventoryFailed,
			expectedState: model.SagaCancelPaymentPending,
			actionCalled:  true,
		},
		{
			name:          "payment cancelled → failed",
			fromState:     model.SagaCancelPaymentPending,
			event:         messages.EvtPaymentCancelled,
			expectedState: model.SagaFailed,
			actionCalled:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &actionRecorder{}
			m := saga.NewMachine([]saga.Transition{
				{From: tc.fromState, Event: tc.event, To: tc.expectedState, Action: rec.record},
			})

			s := baseSaga(tc.fromState)
			action, err := m.Apply(s, tc.event)

			require.NoError(t, err)
			assert.Equal(t, tc.expectedState, s.State)

			// Apply must not execute the action — caller does so after persist.
			assert.False(t, rec.called, "Apply must not call action directly")

			// Calling the returned action must invoke the recorder.
			if tc.actionCalled {
				require.NotNil(t, action)
				require.NoError(t, action(context.Background(), s))
				assert.True(t, rec.called)
			}
		})
	}
}

func TestMachine_Apply_InvalidTransition(t *testing.T) {
	m := saga.NewMachine([]saga.Transition{
		{
			From:  model.SagaPaymentPending,
			Event: messages.EvtPaymentReserved,
			To:    model.SagaInventoryPending,
		},
	})

	s := baseSaga(model.SagaCompleted)
	_, err := m.Apply(s, messages.EvtPaymentFailed)

	require.Error(t, err)
	assert.Equal(t, model.SagaCompleted, s.State, "state must not change on error")
}

func TestMachine_Apply_NoActionIsValid(t *testing.T) {
	m := saga.NewMachine([]saga.Transition{
		{From: model.SagaPaymentPending, Event: messages.EvtPaymentReserved, To: model.SagaInventoryPending},
	})

	s := baseSaga(model.SagaPaymentPending)
	action, err := m.Apply(s, messages.EvtPaymentReserved)

	require.NoError(t, err)
	assert.Nil(t, action)
	assert.Equal(t, model.SagaInventoryPending, s.State)
}
