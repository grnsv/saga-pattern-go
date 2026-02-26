package messages

import (
	"encoding/json"
	"time"
)

// EventType identifies the type of an event message.
type EventType string

const (
	EvtPaymentReserved   EventType = "PaymentReserved"
	EvtPaymentFailed     EventType = "PaymentFailed"
	EvtPaymentCancelled  EventType = "PaymentCancelled"
	EvtInventoryReserved EventType = "InventoryReserved"
	EvtInventoryFailed   EventType = "InventoryFailed"
	EvtSagaCompleted     EventType = "SagaCompleted"
	EvtSagaFailed        EventType = "SagaFailed"
)

// Event is the standard Kafka event envelope.
type Event struct {
	ID            string          `json:"id"`
	CorrelationID string          `json:"correlationId"`
	Type          EventType       `json:"type"`
	Timestamp     time.Time       `json:"timestamp"`
	Payload       json.RawMessage `json:"payload"`
}

// PaymentResultPayload is the payload for payment result events.
type PaymentResultPayload struct {
	OrderID   string  `json:"orderId"`
	PaymentID string  `json:"paymentId,omitempty"`
	Amount    float64 `json:"amount"`
	Reason    string  `json:"reason,omitempty"`
}

// InventoryResultPayload is the payload for inventory result events.
type InventoryResultPayload struct {
	OrderID       string `json:"orderId"`
	ReservationID string `json:"reservationId,omitempty"`
	Item          string `json:"item"`
	Qty           int    `json:"qty"`
	Reason        string `json:"reason,omitempty"`
}

// SagaResultPayload is sent to order-api when a saga reaches a terminal state.
type SagaResultPayload struct {
	OrderID string `json:"orderId"`
	Reason  string `json:"reason,omitempty"`
}
