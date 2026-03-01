package handler

import (
	"context"
	"errors"

	"github.com/grnsv/saga-pattern-go/orchestration/inventory-service/internal/model"
)

type mockInventoryStore struct {
	reservations map[string]*model.InventoryReservation
	createErr    error
}

func newMockInventoryStore() *mockInventoryStore {
	return &mockInventoryStore{reservations: make(map[string]*model.InventoryReservation)}
}

func (m *mockInventoryStore) Create(r *model.InventoryReservation) error {
	if m.createErr != nil {
		return m.createErr
	}
	cp := *r
	m.reservations[r.CorrelationID] = &cp
	return nil
}

func (m *mockInventoryStore) Get(correlationID string) (*model.InventoryReservation, error) {
	r, ok := m.reservations[correlationID]
	if !ok {
		return nil, errors.New("not found")
	}
	cp := *r
	return &cp, nil
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
