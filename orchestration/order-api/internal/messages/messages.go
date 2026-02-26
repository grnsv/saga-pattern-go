package messages

import (
	"encoding/json"
	"time"
)

// CommandType identifies the type of a command message.
type CommandType string

const (
	CmdStartSaga CommandType = "StartSaga"
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
	EvtSagaCompleted EventType = "SagaCompleted"
	EvtSagaFailed    EventType = "SagaFailed"
)

// Event is the standard Kafka event envelope.
type Event struct {
	ID            string          `json:"id"`
	CorrelationID string          `json:"correlationId"`
	Type          EventType       `json:"type"`
	Timestamp     time.Time       `json:"timestamp"`
	Payload       json.RawMessage `json:"payload"`
}

// StartSagaPayload is the payload for the StartSaga command.
type StartSagaPayload struct {
	OrderID string  `json:"orderId"`
	Item    string  `json:"item"`
	Qty     int     `json:"qty"`
	Amount  float64 `json:"amount"`
}

// SagaResultPayload is the payload for SagaCompleted and SagaFailed events.
type SagaResultPayload struct {
	OrderID string `json:"orderId"`
	Reason  string `json:"reason,omitempty"`
}
