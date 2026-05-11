package handler

import (
	"context"
	"errors"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/apperrors"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/messages"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
)

// mockSagaReader is a test double for the SagaReader interface.
type mockSagaReader struct {
	sagas      map[string]*model.SagaInstance
	history    map[string][]*model.SagaHistoryEntry
	getErr     error
	listErr    error
	historyErr error
}

func newMockSagaReader() *mockSagaReader {
	return &mockSagaReader{
		sagas:   make(map[string]*model.SagaInstance),
		history: make(map[string][]*model.SagaHistoryEntry),
	}
}

func (m *mockSagaReader) GetByID(_ context.Context, id string) (*model.SagaInstance, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	for _, s := range m.sagas {
		if s.ID == id {
			return s, nil
		}
	}
	return nil, apperrors.ErrNotFound
}

func (m *mockSagaReader) List(_ context.Context, state *model.SagaState) ([]*model.SagaInstance, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	var result []*model.SagaInstance
	for _, saga := range m.sagas {
		if state != nil && saga.State != *state {
			continue
		}
		result = append(result, saga)
	}
	return result, nil
}

func (m *mockSagaReader) ListHistory(_ context.Context, sagaID string) ([]*model.SagaHistoryEntry, error) {
	if m.historyErr != nil {
		return nil, m.historyErr
	}
	found := false
	for _, s := range m.sagas {
		if s.ID == sagaID {
			found = true
			break
		}
	}
	if !found {
		return nil, apperrors.ErrNotFound
	}
	return m.history[sagaID], nil
}

// mockOrchestrator is a test double for the SagaOrchestrator interface.
type mockOrchestrator struct {
	startErr   error
	handleErr  error
	startCalls []startCall
	eventCalls []*messages.Event
}

type startCall struct {
	correlationID string
	payload       messages.StartSagaPayload
}

func (m *mockOrchestrator) StartSaga(_ context.Context, correlationID string, payload messages.StartSagaPayload) error {
	m.startCalls = append(m.startCalls, startCall{correlationID, payload})
	return m.startErr
}

func (m *mockOrchestrator) HandleEvent(_ context.Context, evt *messages.Event) error {
	m.eventCalls = append(m.eventCalls, evt)
	return m.handleErr
}

// errSagaReader always returns the given error from Get.
type errSagaReader struct{ err error }

func (e *errSagaReader) GetByID(_ context.Context, _ string) (*model.SagaInstance, error) {
	return nil, e.err
}

func (e *errSagaReader) List(_ context.Context, _ *model.SagaState) ([]*model.SagaInstance, error) {
	return nil, e.err
}

func (e *errSagaReader) ListHistory(_ context.Context, _ string) ([]*model.SagaHistoryEntry, error) {
	return nil, e.err
}

var errInternal = errors.New("internal error")
