package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/model"
)

const pricePerUnit = 9.99

// OrderStore defines the interface for order persistence.
type OrderStore interface {
	Create(order *model.Order) (string, error)
	Get(id string) (*model.Order, error)
	Update(order *model.Order) error
}

// EventPublisher defines the interface for publishing events to Kafka.
type EventPublisher interface {
	Publish(ctx context.Context, topic, key string, event *events.Event) error
}

type createOrderRequest struct {
	Item string `json:"item"`
	Qty  int    `json:"qty"`
}

// HTTPHandler handles HTTP requests for the order service.
type HTTPHandler struct {
	store    OrderStore
	producer EventPublisher
}

// NewHTTPHandler creates a new HTTP handler.
func NewHTTPHandler(s OrderStore, producer EventPublisher) *HTTPHandler {
	return &HTTPHandler{store: s, producer: producer}
}

// RegisterRoutes registers all HTTP routes on the given mux.
func (h *HTTPHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /orders", h.createOrder)
	mux.HandleFunc("GET /orders/{id}", h.getOrder)
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /readyz", h.readyz)
}

func (h *HTTPHandler) createOrder(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	if req.Item == "" || req.Qty <= 0 {
		http.Error(w, `{"error":"item and qty are required (qty > 0)"}`, http.StatusBadRequest)
		return
	}

	order := &model.Order{
		Item:   req.Item,
		Qty:    req.Qty,
		Amount: float64(req.Qty) * pricePerUnit,
		Status: model.OrderPending,
	}

	if _, err := h.store.Create(order); err != nil {
		slog.ErrorContext(r.Context(), "failed to create order", "error", err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	if err := h.publishOrderCreated(r.Context(), order); err != nil {
		slog.ErrorContext(r.Context(), "failed to publish OrderCreated", "orderId", order.ID, "error", err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	slog.InfoContext(r.Context(), "order created", "orderId", order.ID, "item", order.Item, "qty", order.Qty, "amount", order.Amount)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(order); err != nil {
		slog.ErrorContext(r.Context(), "failed to encode response", "error", err)
	}
}

func (h *HTTPHandler) publishOrderCreated(ctx context.Context, order *model.Order) error {
	payload, err := json.Marshal(events.OrderCreatedPayload{
		OrderID: order.ID,
		Item:    order.Item,
		Qty:     order.Qty,
		Amount:  order.Amount,
	})
	if err != nil {
		return fmt.Errorf("marshal OrderCreated payload: %w", err)
	}

	evt := &events.Event{
		ID:            uuid.NewString(),
		CorrelationID: order.ID,
		Type:          events.OrderCreated,
		Timestamp:     time.Now(),
		Payload:       payload,
	}

	return h.producer.Publish(ctx, "order-events", order.ID, evt)
}

func (h *HTTPHandler) getOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	order, err := h.store.Get(id)
	if err != nil {
		http.Error(w, `{"error":"order not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(order); err != nil {
		slog.ErrorContext(r.Context(), "failed to encode response", "error", err)
	}
}

func (h *HTTPHandler) healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (h *HTTPHandler) readyz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
