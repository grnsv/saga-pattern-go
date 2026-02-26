package model

// Payment represents a reserved payment for an order.
type Payment struct {
	ID            string  `json:"id"`
	CorrelationID string  `json:"correlationId"`
	OrderID       string  `json:"orderId"`
	Amount        float64 `json:"amount"`
}
