package model

// InventoryReservation represents a reserved inventory slot for an order.
type InventoryReservation struct {
	ID            string `json:"id"`
	CorrelationID string `json:"correlationId"`
	OrderID       string `json:"orderId"`
	Item          string `json:"item"`
	Qty           int    `json:"qty"`
}
