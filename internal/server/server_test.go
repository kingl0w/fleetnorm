package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func request(t *testing.T, srv *http.Server, method, path string) *http.Response {
	t.Helper()
	w := httptest.NewRecorder()
	srv.Handler.ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w.Result()
}

func TestHealthz(t *testing.T) {
	var health error
	srv := New(":0", func(context.Context) error { return health }, func(io.Writer) {})

	resp := request(t, srv, "GET", "/healthz")
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != "ok" {
		t.Errorf("healthy: got %d %q, want 200 ok", resp.StatusCode, body)
	}

	//a fleetnorm that cannot reach its store cannot write the audit log, so it
	//is not healthy however well the HTTP server works.
	health = errors.New("database is locked")
	resp = request(t, srv, "GET", "/healthz")
	body, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("unhealthy: got %d, want 503", resp.StatusCode)
	}
	if !strings.Contains(string(body), "database is locked") {
		t.Errorf("unhealthy body = %q, want it to say why", body)
	}
}

func TestMetrics(t *testing.T) {
	srv := New(":0", func(context.Context) error { return nil }, func(w io.Writer) {
		io.WriteString(w, "fleetnorm_events_polled_total{adapter=\"replay\"} 3\n")
	})

	resp := request(t, srv, "GET", "/metrics")
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") {
		t.Errorf("Content-Type = %q, want Prometheus text", resp.Header.Get("Content-Type"))
	}
	if !strings.Contains(string(body), "fleetnorm_events_polled_total") {
		t.Errorf("body = %q", body)
	}
}

// there is no UI and no API. Anything else is not found.
func TestNothingElseIsServed(t *testing.T) {
	srv := New(":0", func(context.Context) error { return nil }, func(io.Writer) {})
	for _, tt := range []struct {
		method, path string
		want         int
	}{
		{"GET", "/", http.StatusNotFound},
		{"GET", "/events", http.StatusNotFound},
		{"POST", "/healthz", http.StatusMethodNotAllowed},
		{"POST", "/metrics", http.StatusMethodNotAllowed},
	} {
		if got := request(t, srv, tt.method, tt.path).StatusCode; got != tt.want {
			t.Errorf("%s %s = %d, want %d", tt.method, tt.path, got, tt.want)
		}
	}
}
