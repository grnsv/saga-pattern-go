package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/apperrors"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
)

// SagaReader is the narrow store interface required by HTTPHandler.
type SagaReader interface {
	Get(ctx context.Context, correlationID string) (*model.SagaInstance, error)
}

// HTTPHandler serves the orchestrator's HTTP endpoints.
type HTTPHandler struct {
	store SagaReader
}

// NewHTTPHandler creates a new HTTP handler backed by the given store.
func NewHTTPHandler(s SagaReader) *HTTPHandler {
	return &HTTPHandler{store: s}
}

// RegisterRoutes mounts all HTTP routes on the given mux.
func (h *HTTPHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /sagas/{id}", h.getSaga)
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /readyz", h.readyz)
}

func (h *HTTPHandler) getSaga(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	saga, err := h.store.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, apperrors.ErrNotFound) {
			http.Error(w, `{"error":"saga not found"}`, http.StatusNotFound)
			return
		}
		slog.ErrorContext(r.Context(), "failed to get saga", "id", id, "error", err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(saga); err != nil {
		slog.ErrorContext(r.Context(), "failed to encode saga", "error", err)
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
