package messages

import (
	"encoding/json"
	"time"
)

// CommandType identifies the type of a command message.
type CommandType string

const (
	CmdReserveInventory CommandType = "ReserveInventory"
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
	EvtInventoryReserved EventType = "InventoryReserved"
	EvtInventoryFailed   EventType = "InventoryFailed"
)

// Event is the standard Kafka event envelope.
type Event struct {
	ID            string          `json:"id"`
	CorrelationID string          `json:"correlationId"`
	Type          EventType       `json:"type"`
	Timestamp     time.Time       `json:"timestamp"`
	Payload       json.RawMessage `json:"payload"`
}

// ReserveInventoryPayload is the payload for the ReserveInventory command.
type ReserveInventoryPayload struct {
	OrderID string `json:"orderId"`
	Item    string `json:"item"`
	Qty     int    `json:"qty"`
}

// InventoryResultPayload is the payload for inventory result events.
type InventoryResultPayload struct {
	OrderID       string `json:"orderId"`
	ReservationID string `json:"reservationId,omitempty"`
	Item          string `json:"item"`
	Qty           int    `json:"qty"`
	Reason        string `json:"reason,omitempty"`
}
