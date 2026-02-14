package model

// ReservationStatus represents the current state of an inventory reservation.
type ReservationStatus string

const (
	ReservationConfirmed ReservationStatus = "CONFIRMED"
	ReservationFailed    ReservationStatus = "FAILED"
)

// InventoryReservation represents a reserved inventory item.
type InventoryReservation struct {
	ID      string            `json:"id"`
	OrderID string            `json:"orderId"`
	Item    string            `json:"item"`
	Qty     int               `json:"qty"`
	Status  ReservationStatus `json:"status"`
}
