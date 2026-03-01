package handler

import (
	"context"
	"errors"

	"github.com/grnsv/saga-pattern-go/orchestration/payment-service/internal/model"
)

type mockPaymentStore struct {
	payments  map[string]*model.Payment
	createErr error
	deleteErr error
}

func newMockPaymentStore() *mockPaymentStore {
	return &mockPaymentStore{payments: make(map[string]*model.Payment)}
}

func (m *mockPaymentStore) Create(payment *model.Payment) error {
	if m.createErr != nil {
		return m.createErr
	}
	cp := *payment
	m.payments[payment.CorrelationID] = &cp
	return nil
}

func (m *mockPaymentStore) Get(correlationID string) (*model.Payment, error) {
	p, ok := m.payments[correlationID]
	if !ok {
		return nil, errors.New("not found")
	}
	cp := *p
	return &cp, nil
}

func (m *mockPaymentStore) Delete(correlationID string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.payments, correlationID)
	return nil
}

type mockPublisher struct {
	published  []publishedMsg
	publishErr error
}

type publishedMsg struct {
	topic   string
	key     string
	payload []byte
}

func (m *mockPublisher) Publish(_ context.Context, topic, key string, payload []byte) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.published = append(m.published, publishedMsg{topic: topic, key: key, payload: payload})
	return nil
}
