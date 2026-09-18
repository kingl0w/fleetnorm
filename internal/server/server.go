// package server serves the only HTTP fleetnorm has: a health check and
// metrics. there is no UI and no API.
package server

import (
	"context"
	"io"
	"net/http"
	"time"
)

// New builds the operational server.
func New(addr string, health func(context.Context) error, metrics func(io.Writer)) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if err := health(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, "unhealthy: "+err.Error()+"\n")
			return
		}
		io.WriteString(w, "ok\n")
	})

	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		metrics(w)
	})

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}
