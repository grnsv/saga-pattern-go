package handler

import "net/http"

// HTTPHandler handles HTTP requests for the inventory service.
type HTTPHandler struct{}

// NewHTTPHandler creates a new HTTP handler.
func NewHTTPHandler() *HTTPHandler {
	return &HTTPHandler{}
}

// RegisterRoutes registers all HTTP routes on the given mux.
func (h *HTTPHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /readyz", h.readyz)
}

func (h *HTTPHandler) healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (h *HTTPHandler) readyz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
