package messages

import (
	"encoding/json"
	"time"
)

// CommandType identifies the type of a command message.
type CommandType string

const (
	CmdReservePayment CommandType = "ReservePayment"
	CmdCancelPayment  CommandType = "CancelPayment"
)

// Command is the standard Kafka command envelope.
type Command struct {
	ID            string          `json:"id"`
	CorrelationID string          `json:"correlationId"`
	Type          CommandType     `json:"type"`
	Timestamp     time.Time       `json:"timestamp"`
	Payload       json.RawMessage `json:"payload"`
}

// EventType identifies the type of an event message.
type EventType string

const (
	EvtPaymentReserved  EventType = "PaymentReserved"
	EvtPaymentFailed    EventType = "PaymentFailed"
	EvtPaymentCancelled EventType = "PaymentCancelled"
)

// Event is the standard Kafka event envelope.
type Event struct {
	ID            string          `json:"id"`
	CorrelationID string          `json:"correlationId"`
	Type          EventType       `json:"type"`
	Timestamp     time.Time       `json:"timestamp"`
	Payload       json.RawMessage `json:"payload"`
}

// ReservePaymentPayload is the payload for the ReservePayment command.
type ReservePaymentPayload struct {
	OrderID string  `json:"orderId"`
	Item    string  `json:"item"`
	Qty     int     `json:"qty"`
	Amount  float64 `json:"amount"`
}

// CancelPaymentPayload is the payload for the CancelPayment command.
type CancelPaymentPayload struct {
	OrderID string `json:"orderId"`
}

// PaymentResultPayload is the payload for payment result events.
type PaymentResultPayload struct {
	OrderID   string  `json:"orderId"`
	PaymentID string  `json:"paymentId,omitempty"`
	Amount    float64 `json:"amount"`
	Reason    string  `json:"reason,omitempty"`
}
