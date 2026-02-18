package handler

import (
	"context"
	"errors"

	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/model"
)

type mockOrderStore struct {
	orders    map[string]*model.Order
	createErr error
	updateErr error
}

func newMockStore() *mockOrderStore {
	return &mockOrderStore{orders: make(map[string]*model.Order)}
}

func (m *mockOrderStore) Create(order *model.Order) (string, error) {
	if m.createErr != nil {
		return "", m.createErr
	}
	order.ID = "test-uuid"
	m.orders[order.ID] = order
	return order.ID, nil
}

func (m *mockOrderStore) Get(id string) (*model.Order, error) {
	order, ok := m.orders[id]
	if !ok {
		return nil, errors.New("order not found")
	}
	return order, nil
}

func (m *mockOrderStore) Update(order *model.Order) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	if _, ok := m.orders[order.ID]; !ok {
		return errors.New("order not found")
	}
	m.orders[order.ID] = order
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
