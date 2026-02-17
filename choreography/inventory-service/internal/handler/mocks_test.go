package handler

import (
	"context"
	"errors"

	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/model"
)

type mockInventoryStore struct {
	reservations map[string]*model.InventoryReservation
	createErr    error
}

func newMockInventoryStore() *mockInventoryStore {
	return &mockInventoryStore{
		reservations: make(map[string]*model.InventoryReservation),
	}
}

func (m *mockInventoryStore) Create(reservation *model.InventoryReservation) (string, error) {
	if m.createErr != nil {
		return "", m.createErr
	}
	reservation.ID = "res-test-uuid"
	m.reservations[reservation.ID] = reservation
	return reservation.ID, nil
}

func (m *mockInventoryStore) Get(id string) (*model.InventoryReservation, error) {
	r, ok := m.reservations[id]
	if !ok {
		return nil, errors.New("reservation not found")
	}
	return r, nil
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
