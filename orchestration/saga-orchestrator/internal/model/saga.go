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
	ID            string
	CorrelationID string
	OrderID       string
	State         SagaState
	Item          string
	Qty           int
	Amount        float64
	CreatedAt     time.Time
	UpdatedAt     time.Time
	StepDeadline  *time.Time // deadline for the current step; nil when idle
	RetryCount    int
}
