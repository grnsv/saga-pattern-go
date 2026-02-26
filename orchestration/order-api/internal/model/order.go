package model

// OrderStatus represents the current state of an order.
type OrderStatus string

const (
	OrderPending   OrderStatus = "PENDING"
	OrderConfirmed OrderStatus = "CONFIRMED"
	OrderCancelled OrderStatus = "CANCELLED"
)

// Order represents a customer order.
type Order struct {
	ID     string      `json:"id"`
	Item   string      `json:"item"`
	Qty    int         `json:"qty"`
	Amount float64     `json:"amount"`
	Status OrderStatus `json:"status"`
}
