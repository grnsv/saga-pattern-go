package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
)

func newTestMux(s SagaReader) *http.ServeMux {
	mux := http.NewServeMux()
	NewHTTPHandler(s).RegisterRoutes(mux)
	return mux
}

func TestGetSaga(t *testing.T) {
	s := newMockSagaReader()
	s.sagas["corr-1"] = &model.SagaInstance{
		ID:            "saga-uuid",
		CorrelationID: "corr-1",
		State:         model.SagaCompleted,
	}
	mux := newTestMux(s)

	req := httptest.NewRequest(http.MethodGet, "/sagas/saga-uuid", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var got model.SagaInstance
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, model.SagaCompleted, got.State)
}

func TestGetSaga_NotFound(t *testing.T) {
	mux := newTestMux(newMockSagaReader())

	req := httptest.NewRequest(http.MethodGet, "/sagas/unknown", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestListSagas(t *testing.T) {
	s := newMockSagaReader()
	s.sagas["corr-1"] = &model.SagaInstance{CorrelationID: "corr-1", State: model.SagaCompleted}
	s.sagas["corr-2"] = &model.SagaInstance{CorrelationID: "corr-2", State: model.SagaFailed}
	mux := newTestMux(s)

	req := httptest.NewRequest(http.MethodGet, "/sagas", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var got []model.SagaInstance
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Len(t, got, 2)
}

func TestListSagas_FilterByState(t *testing.T) {
	s := newMockSagaReader()
	s.sagas["corr-1"] = &model.SagaInstance{CorrelationID: "corr-1", State: model.SagaCompleted}
	s.sagas["corr-2"] = &model.SagaInstance{CorrelationID: "corr-2", State: model.SagaFailed}
	mux := newTestMux(s)

	req := httptest.NewRequest(http.MethodGet, "/sagas?state=COMPLETED", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var got []model.SagaInstance
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got, 1)
	assert.Equal(t, model.SagaCompleted, got[0].State)
}

func TestListSagas_InvalidState(t *testing.T) {
	mux := newTestMux(newMockSagaReader())

	req := httptest.NewRequest(http.MethodGet, "/sagas?state=NOPE", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetSagaHistory(t *testing.T) {
	s := newMockSagaReader()
	s.sagas["corr-1"] = &model.SagaInstance{ID: "saga-1", CorrelationID: "corr-1"}
	s.history["saga-1"] = []*model.SagaHistoryEntry{
		{SagaID: "saga-1", FromState: model.SagaStarted, ToState: model.SagaPaymentPending, Event: "StartSaga"},
	}
	mux := newTestMux(s)

	req := httptest.NewRequest(http.MethodGet, "/sagas/saga-1/history", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var got []model.SagaHistoryEntry
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got, 1)
	assert.Equal(t, "StartSaga", got[0].Event)
}

func TestGetSagaHistory_NotFound(t *testing.T) {
	mux := newTestMux(newMockSagaReader())

	req := httptest.NewRequest(http.MethodGet, "/sagas/unknown/history", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetSaga_StoreError(t *testing.T) {
	mux := newTestMux(&errSagaReader{err: errInternal})

	req := httptest.NewRequest(http.MethodGet, "/sagas/any", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHealthz(t *testing.T) {
	mux := newTestMux(newMockSagaReader())
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestReadyz(t *testing.T) {
	mux := newTestMux(newMockSagaReader())
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}
