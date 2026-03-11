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
	sagas  map[string]*model.SagaInstance
	getErr error
}

func newMockSagaReader() *mockSagaReader {
	return &mockSagaReader{sagas: make(map[string]*model.SagaInstance)}
}

func (m *mockSagaReader) Get(_ context.Context, correlationID string) (*model.SagaInstance, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	s, ok := m.sagas[correlationID]
	if !ok {
		return nil, apperrors.ErrNotFound
	}
	return s, nil
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

func (e *errSagaReader) Get(_ context.Context, _ string) (*model.SagaInstance, error) {
	return nil, e.err
}

var errInternal = errors.New("internal error")
