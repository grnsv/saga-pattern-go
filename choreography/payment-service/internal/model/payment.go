package model

// PaymentStatus represents the current state of a payment.
type PaymentStatus string

const (
	PaymentReserved   PaymentStatus = "RESERVED"
	PaymentRolledBack PaymentStatus = "ROLLED_BACK"
)

// Payment represents a payment reservation.
type Payment struct {
	ID      string        `json:"id"`
	OrderID string        `json:"orderId"`
	Amount  float64       `json:"amount"`
	Status  PaymentStatus `json:"status"`
}
