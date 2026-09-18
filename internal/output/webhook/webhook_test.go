package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ianfrushon/fleetnorm/internal/event"
	"github.com/ianfrushon/fleetnorm/internal/output"
)

const secret = "shop-hmac-key"

func testEvent() event.Event {
	at := time.Date(2026, 9, 14, 13, 4, 5, 0, time.UTC)
	return event.Event{
		SchemaVersion: event.SchemaVersion,
		EventID:       "replay-000002",
		VIN:           "3AKJHHDR8LSLT1234",
		OccurredAt:    at,
		ReceivedAt:    at,
		Source:        "replay",
		SourceType:    event.SourceFile,
		Severity:      event.SeverityCritical,
		Raw:           json.RawMessage(`{"faultCode":"SPN3226-FMI20"}`),
	}
}

func newOutput(t *testing.T, url string) *Output {
	t.Helper()
	o, err := New(Options{Name: "my-shop", URL: url, Secret: secret, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// classification is by class, not by a list of blessed numbers.
func TestSendStatusCodes(t *testing.T) {
	tests := []struct {
		code          int
		wantErr       bool
		wantPermanent bool
	}{
		{code: 200}, {code: 201}, {code: 202}, {code: 204}, {code: 299},

		{code: 408, wantErr: true},
		{code: 429, wantErr: true},
		{code: 500, wantErr: true},
		{code: 502, wantErr: true},
		{code: 503, wantErr: true},
		{code: 504, wantErr: true},

		{code: 400, wantErr: true, wantPermanent: true},
		{code: 401, wantErr: true, wantPermanent: true},
		{code: 403, wantErr: true, wantPermanent: true},
		{code: 404, wantErr: true, wantPermanent: true},
		{code: 405, wantErr: true, wantPermanent: true},
		{code: 410, wantErr: true, wantPermanent: true},
		{code: 422, wantErr: true, wantPermanent: true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.code)
				io.WriteString(w, `{"error":"detail"}`)
			}))
			defer srv.Close()

			err := newOutput(t, srv.URL).Send(t.Context(), testEvent())
			if (err != nil) != tt.wantErr {
				t.Fatalf("Send() = %v, want error: %v", err, tt.wantErr)
			}
			if got := errors.Is(err, output.ErrPermanent); got != tt.wantPermanent {
				t.Errorf("errors.Is(err, ErrPermanent) = %v, want %v (err: %v)", got, tt.wantPermanent, err)
			}
			if tt.wantErr && !strings.Contains(err.Error(), "detail") {
				t.Errorf("error should quote the response body, got %v", err)
			}
		})
	}
}

// a signed POST must never be replayed to another host: the signature goes with
// the body, so following a redirect hands a valid HMAC to whoever asked.
func TestRedirectIsNotFollowed(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var elsewhereHit bool
			elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				elsewhereHit = true
				w.WriteHeader(200)
			}))
			defer elsewhere.Close()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, elsewhere.URL, code)
			}))
			defer srv.Close()

			err := newOutput(t, srv.URL).Send(t.Context(), testEvent())
			if !errors.Is(err, output.ErrPermanent) {
				t.Errorf("Send() = %v, want a permanent failure", err)
			}
			if elsewhereHit {
				t.Error("the redirect was followed: the HMAC was sent to a second host")
			}
			if !strings.Contains(err.Error(), "redirect") {
				t.Errorf("error should say why, got %v", err)
			}
		})
	}
}

func TestRequest(t *testing.T) {
	var got struct {
		method, ctype, ua, eventID, sig, body string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.method, got.body = r.Method, string(b)
		got.ctype = r.Header.Get("Content-Type")
		got.ua = r.Header.Get("User-Agent")
		got.eventID = r.Header.Get("X-Fleetnorm-Event-Id")
		got.sig = r.Header.Get("X-Fleetnorm-Signature")
		w.WriteHeader(202)
	}))
	defer srv.Close()

	e := testEvent()
	if err := newOutput(t, srv.URL).Send(t.Context(), e); err != nil {
		t.Fatal(err)
	}

	if got.method != http.MethodPost {
		t.Errorf("method = %s, want POST", got.method)
	}
	if got.ctype != "application/json" {
		t.Errorf("Content-Type = %q", got.ctype)
	}
	if !strings.HasPrefix(got.ua, "fleetnorm/") {
		t.Errorf("User-Agent = %q, want it to name fleetnorm and a version", got.ua)
	}
	if got.eventID != e.EventID {
		t.Errorf("X-Fleetnorm-Event-Id = %q, want %q", got.eventID, e.EventID)
	}

	//the signature covers exactly the bytes that arrived, computed here from
	//scratch rather than by calling the code under test.
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(got.body))
	if want := hex.EncodeToString(mac.Sum(nil)); got.sig != want {
		t.Errorf("signature = %q, want %q (over %d body bytes)", got.sig, want, len(got.body))
	}

	//and the body is the event.
	var sent event.Event
	if err := json.Unmarshal([]byte(got.body), &sent); err != nil {
		t.Fatalf("body is not an event: %v", err)
	}
	if sent.EventID != e.EventID || string(sent.Raw) != string(e.Raw) {
		t.Errorf("body = %+v, want the event with raw intact", sent)
	}
}

func TestUnsignedWhenNoSecret(t *testing.T) {
	var sig string
	var present bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sig, present = r.Header.Get("X-Fleetnorm-Signature"), r.Header.Values("X-Fleetnorm-Signature") != nil
		w.WriteHeader(200)
	}))
	defer srv.Close()

	o, err := New(Options{Name: "my-shop", URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Send(t.Context(), testEvent()); err != nil {
		t.Fatal(err)
	}
	if present || sig != "" {
		t.Errorf("unsigned output sent a signature header: %q", sig)
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		code      int
		header    string
		wantDelay time.Duration
		wantOK    bool
	}{
		{name: "seconds", code: 429, header: "5", wantDelay: 5 * time.Second, wantOK: true},
		{name: "http date", code: 503, header: now.Add(30 * time.Second).Format(http.TimeFormat), wantDelay: 30 * time.Second, wantOK: true},
		{name: "capped", code: 429, header: "86400", wantDelay: time.Minute, wantOK: true},
		{name: "capped http date", code: 503, header: now.Add(time.Hour).Format(http.TimeFormat), wantDelay: time.Minute, wantOK: true},
		{name: "past date is now", code: 503, header: now.Add(-time.Hour).Format(http.TimeFormat), wantOK: true},
		{name: "negative is now", code: 429, header: "-5", wantOK: true},
		{name: "unparseable is ignored", code: 429, header: "soon"},
		{name: "empty is ignored", code: 503, header: ""},
		{name: "not honored on other codes", code: 500, header: "5"},
		{name: "not honored on permanent failures", code: 400, header: "5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.header != "" {
					w.Header().Set("Retry-After", tt.header)
				}
				w.WriteHeader(tt.code)
			}))
			defer srv.Close()

			o, err := New(Options{Name: "my-shop", URL: srv.URL, Secret: secret, MaxRetryAfter: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			o.now = func() time.Time { return now }

			err = o.Send(t.Context(), testEvent())
			if err == nil {
				t.Fatal("Send() = nil, want an error")
			}
			delay, ok := output.RetryAfter(err)
			if ok != tt.wantOK {
				t.Fatalf("RetryAfter(%v) ok = %v, want %v", err, ok, tt.wantOK)
			}
			if delay != tt.wantDelay {
				t.Errorf("delay = %v, want %v", delay, tt.wantDelay)
			}
		})
	}
}

// slowServer never answers until the test is done with it. Handlers must not
// wait on the request context: the server does not always notice a client
// hanging up, and Close waits for handlers to return.
func slowServer(t *testing.T) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})
	return srv
}

func TestTimeoutPerAttempt(t *testing.T) {
	srv := slowServer(t)

	o, err := New(Options{Name: "my-shop", URL: srv.URL, Secret: secret, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	err = o.Send(context.Background(), testEvent())
	if err == nil {
		t.Fatal("Send() = nil, want a timeout")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Send took %v, want it to give up near the 50ms timeout", elapsed)
	}
	//a timeout is worth another attempt.
	if errors.Is(err, output.ErrPermanent) {
		t.Errorf("timeout classified as permanent: %v", err)
	}
}

func TestContextCancellation(t *testing.T) {
	srv := slowServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := newOutput(t, srv.URL).Send(ctx, testEvent())
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Send() = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Send took %v to notice cancellation", elapsed)
	}
}

// a misconfigured endpoint answering with a page of HTML must not become a page
// of log.
func TestErrorBodyIsTruncated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		io.WriteString(w, strings.Repeat("<div>whoops</div>\n", 100_000))
	}))
	defer srv.Close()

	err := newOutput(t, srv.URL).Send(t.Context(), testEvent())
	if err == nil {
		t.Fatal("Send() = nil, want an error")
	}
	if len(err.Error()) > 2*maxErrorBody {
		t.Errorf("error message is %d bytes, want it capped near %d", len(err.Error()), maxErrorBody)
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Errorf("a truncated body should say so, got %d bytes", len(err.Error()))
	}
	if strings.Contains(err.Error(), "\n") {
		t.Error("error message should stay on one line")
	}
}

func TestNew(t *testing.T) {
	for _, tt := range []struct{ name, url, want string }{
		{name: "", url: "https://shop.example", want: "needs a name"},
		{name: "shop", url: "ftp://shop.example", want: "scheme"},
		{name: "shop", url: "://nope", want: "url"},
	} {
		if _, err := New(Options{Name: tt.name, URL: tt.url}); err == nil {
			t.Errorf("New(%q, %q) = nil error, want one about %q", tt.name, tt.url, tt.want)
		}
	}

	o, err := New(Options{Name: "shop", URL: "https://user:pw@shop.example/ingest?token=abc123"})
	if err != nil {
		t.Fatal(err)
	}
	if o.client.Timeout != DefaultTimeout || o.maxRetryAfter != DefaultMaxRetryAfter {
		t.Errorf("defaults not applied: timeout %v, maxRetryAfter %v", o.client.Timeout, o.maxRetryAfter)
	}
	//credentials in a URL must not reach a log line or an error message.
	for _, leak := range []string{"pw", "abc123"} {
		if strings.Contains(o.safeURL, leak) {
			t.Errorf("safeURL %q leaks %q", o.safeURL, leak)
		}
	}
}
