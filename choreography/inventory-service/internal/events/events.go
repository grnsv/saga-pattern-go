package events

import (
	"encoding/json"
	"time"
)

// EventType represents the type of a saga event.
type EventType string

const (
	OrderCreated      EventType = "OrderCreated"
	PaymentReserved   EventType = "PaymentReserved"
	PaymentFailed     EventType = "PaymentFailed"
	PaymentRolledBack EventType = "PaymentRolledBack"
	InventoryReserved EventType = "InventoryReserved"
	InventoryFailed   EventType = "InventoryFailed"
)

// Event is the standard Kafka message envelope.
type Event struct {
	ID            string          `json:"id"`
	CorrelationID string          `json:"correlationId"`
	Type          EventType       `json:"type"`
	Timestamp     time.Time       `json:"timestamp"`
	Payload       json.RawMessage `json:"payload"`
}

// OrderCreatedPayload is the payload for OrderCreated events.
type OrderCreatedPayload struct {
	OrderID string  `json:"orderId"`
	Item    string  `json:"item"`
	Qty     int     `json:"qty"`
	Amount  float64 `json:"amount"`
}

// PaymentResultPayload is the payload for PaymentReserved, PaymentFailed, and PaymentRolledBack events.
type PaymentResultPayload struct {
	OrderID   string  `json:"orderId"`
	PaymentID string  `json:"paymentId,omitempty"`
	Amount    float64 `json:"amount"`
	Reason    string  `json:"reason,omitempty"`
}

// InventoryResultPayload is the payload for InventoryReserved and InventoryFailed events.
type InventoryResultPayload struct {
	OrderID       string `json:"orderId"`
	ReservationID string `json:"reservationId,omitempty"`
	Item          string `json:"item"`
	Qty           int    `json:"qty"`
	Reason        string `json:"reason,omitempty"`
}
