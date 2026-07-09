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
	GetByID(ctx context.Context, id string) (*model.SagaInstance, error)
	List(ctx context.Context, state *model.SagaState) ([]*model.SagaInstance, error)
	ListHistory(ctx context.Context, sagaID string) ([]*model.SagaHistoryEntry, error)
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
	mux.HandleFunc("GET /sagas", h.listSagas)
	mux.HandleFunc("GET /sagas/{id}", h.getSaga)
	mux.HandleFunc("GET /sagas/{id}/history", h.getSagaHistory)
	mux.HandleFunc("GET /sagas/{id}/diagram", h.getSagaDiagram)
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /readyz", h.readyz)
}

func (h *HTTPHandler) listSagas(w http.ResponseWriter, r *http.Request) {
	var state *model.SagaState
	if rawState := r.URL.Query().Get("state"); rawState != "" {
		parsed := model.SagaState(rawState)
		if !isValidSagaState(parsed) {
			http.Error(w, `{"error":"invalid state"}`, http.StatusBadRequest)
			return
		}
		state = &parsed
	}

	sagas, err := h.store.List(r.Context(), state)
	if err != nil {
		slog.ErrorContext(r.Context(), "failed to list sagas", "error", err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(sagas); err != nil {
		slog.ErrorContext(r.Context(), "failed to encode sagas", "error", err)
	}
}

func (h *HTTPHandler) getSaga(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	saga, err := h.store.GetByID(r.Context(), id)
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

func (h *HTTPHandler) getSagaHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	history, err := h.store.ListHistory(r.Context(), id)
	if err != nil {
		if errors.Is(err, apperrors.ErrNotFound) {
			http.Error(w, `{"error":"saga not found"}`, http.StatusNotFound)
			return
		}
		slog.ErrorContext(r.Context(), "failed to get saga history", "id", id, "error", err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(history); err != nil {
		slog.ErrorContext(r.Context(), "failed to encode saga history", "error", err)
	}
}

func (h *HTTPHandler) getSagaDiagram(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	saga, err := h.store.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, apperrors.ErrNotFound) {
			http.Error(w, `{"error":"saga not found"}`, http.StatusNotFound)
			return
		}
		slog.ErrorContext(r.Context(), "failed to get saga for diagram", "id", id, "error", err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	history, err := h.store.ListHistory(r.Context(), saga.ID)
	if err != nil {
		slog.ErrorContext(r.Context(), "failed to get saga history for diagram", "id", id, "error", err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if _, err := w.Write([]byte(sagaDiagram(saga, history))); err != nil {
		slog.ErrorContext(r.Context(), "failed to write saga diagram", "error", err)
	}
}

func isValidSagaState(state model.SagaState) bool {
	switch state {
	case model.SagaStarted,
		model.SagaPaymentPending,
		model.SagaInventoryPending,
		model.SagaCompleted,
		model.SagaCancelPaymentPending,
		model.SagaFailed:
		return true
	default:
		return false
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
