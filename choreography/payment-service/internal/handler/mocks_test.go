package handler

import (
	"context"
	"errors"

	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/model"
)

type mockPaymentStore struct {
	payments        map[string]*model.Payment
	byOrder         map[string]*model.Payment
	createErr       error
	updateStatusErr error
}

func newMockPaymentStore() *mockPaymentStore {
	return &mockPaymentStore{
		payments: make(map[string]*model.Payment),
		byOrder:  make(map[string]*model.Payment),
	}
}

func (m *mockPaymentStore) Create(payment *model.Payment) (string, error) {
	if m.createErr != nil {
		return "", m.createErr
	}
	payment.ID = "pay-test-uuid"
	m.payments[payment.ID] = payment
	m.byOrder[payment.OrderID] = payment
	return payment.ID, nil
}

func (m *mockPaymentStore) Get(id string) (*model.Payment, error) {
	p, ok := m.payments[id]
	if !ok {
		return nil, errors.New("payment not found")
	}
	return p, nil
}

func (m *mockPaymentStore) GetByOrderID(orderID string) (*model.Payment, error) {
	p, ok := m.byOrder[orderID]
	if !ok {
		return nil, errors.New("payment not found")
	}
	return p, nil
}

func (m *mockPaymentStore) UpdateStatus(id string, status model.PaymentStatus) error {
	if m.updateStatusErr != nil {
		return m.updateStatusErr
	}
	p, ok := m.payments[id]
	if !ok {
		return errors.New("payment not found")
	}
	p.Status = status
	return nil
}

type mockProducer struct {
	published []publishedEvent
	err       error
}

type publishedEvent struct {
	topic string
	key   string
	event *events.Event
}

func newMockProducer() *mockProducer {
	return &mockProducer{}
}

func (m *mockProducer) Publish(_ context.Context, topic, key string, event *events.Event) error {
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, publishedEvent{topic: topic, key: key, event: event})
	return nil
}
