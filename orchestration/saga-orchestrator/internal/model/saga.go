package model

import "time"

// SagaState represents the current state of a saga instance.
type SagaState string

const (
	SagaStarted              SagaState = "STARTED"
	SagaPaymentPending       SagaState = "PAYMENT_PENDING"
	SagaInventoryPending     SagaState = "INVENTORY_PENDING"
	SagaCompleted            SagaState = "COMPLETED"
	SagaCancelPaymentPending SagaState = "CANCEL_PAYMENT_PENDING"
	SagaFailed               SagaState = "FAILED"
)

// SagaInstance holds the full state of a running or completed saga.
type SagaInstance struct {
	ID            string     `json:"id"`
	CorrelationID string     `json:"correlationId"`
	OrderID       string     `json:"orderId"`
	State         SagaState  `json:"state"`
	Item          string     `json:"item"`
	Qty           int        `json:"qty"`
	Amount        float64    `json:"amount"`
	CreatedAt     time.Time  `json:"createdAt"`
	UpdatedAt     time.Time  `json:"updatedAt"`
	StepDeadline  *time.Time `json:"stepDeadline,omitempty"` // deadline for the current step; nil when idle
	RetryCount    int        `json:"retryCount"`
}

// SagaHistoryEntry records a persisted saga state transition.
type SagaHistoryEntry struct {
	ID        int64     `json:"id"`
	SagaID    string    `json:"sagaId"`
	FromState SagaState `json:"fromState"`
	ToState   SagaState `json:"toState"`
	Event     string    `json:"event,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}
