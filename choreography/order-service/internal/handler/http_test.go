package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/model"
)

func newTestServer(s *mockOrderStore) *http.ServeMux {
	h := NewHTTPHandler(s, newMockProducer())
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux
}

func TestCreateOrder(t *testing.T) {
	s := newMockStore()
	mux := newTestServer(s)

	body := `{"item":"widget","qty":2}`
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)

	var order model.Order
	err := json.Unmarshal(w.Body.Bytes(), &order)
	require.NoError(t, err)
	assert.Equal(t, "test-uuid", order.ID)
	assert.Equal(t, "widget", order.Item)
	assert.Equal(t, 2, order.Qty)
	assert.InDelta(t, 19.98, order.Amount, 0.001)
	assert.Equal(t, model.OrderPending, order.Status)
}

func TestCreateOrder_StoreError(t *testing.T) {
	s := newMockStore()
	s.createErr = errors.New("rng failure")
	mux := newTestServer(s)

	body := `{"item":"widget","qty":1}`
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestCreateOrder_PublishError(t *testing.T) {
	s := newMockStore()
	p := newMockProducer()
	p.err = errors.New("kafka down")

	h := NewHTTPHandler(s, p)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"item":"widget","qty":1}`
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestCreateOrder_InvalidBody(t *testing.T) {
	mux := newTestServer(newMockStore())

	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader("not json"))
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCreateOrder_MissingFields(t *testing.T) {
	mux := newTestServer(newMockStore())

	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"item":"","qty":0}`))
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetOrder(t *testing.T) {
	s := newMockStore()
	s.orders["order-1"] = &model.Order{
		ID:     "order-1",
		Item:   "widget",
		Qty:    1,
		Amount: 9.99,
		Status: model.OrderPending,
	}
	mux := newTestServer(s)

	req := httptest.NewRequest(http.MethodGet, "/orders/order-1", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var got model.Order
	err := json.Unmarshal(w.Body.Bytes(), &got)
	require.NoError(t, err)
	assert.Equal(t, "order-1", got.ID)
	assert.Equal(t, "widget", got.Item)
}

func TestGetOrder_NotFound(t *testing.T) {
	mux := newTestServer(newMockStore())

	req := httptest.NewRequest(http.MethodGet, "/orders/nonexistent", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHealthz(t *testing.T) {
	mux := newTestServer(newMockStore())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
}

func TestReadyz(t *testing.T) {
	mux := newTestServer(newMockStore())

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
}
