package messages

import (
	"encoding/json"
	"time"
)

// CommandType identifies the type of a command message.
type CommandType string

const (
	CmdStartSaga        CommandType = "StartSaga"
	CmdReservePayment   CommandType = "ReservePayment"
	CmdCancelPayment    CommandType = "CancelPayment"
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

// StartSagaPayload is sent by order-api to initiate a new saga.
type StartSagaPayload struct {
	OrderID string  `json:"orderId"`
	Item    string  `json:"item"`
	Qty     int     `json:"qty"`
	Amount  float64 `json:"amount"`
}

// ReservePaymentPayload is sent by the orchestrator to payment-service.
type ReservePaymentPayload struct {
	OrderID string  `json:"orderId"`
	Item    string  `json:"item"`
	Qty     int     `json:"qty"`
	Amount  float64 `json:"amount"`
}

// CancelPaymentPayload is sent by the orchestrator to payment-service for compensation.
type CancelPaymentPayload struct {
	OrderID string `json:"orderId"`
}

// ReserveInventoryPayload is sent by the orchestrator to inventory-service.
type ReserveInventoryPayload struct {
	OrderID string `json:"orderId"`
	Item    string `json:"item"`
	Qty     int    `json:"qty"`
}
