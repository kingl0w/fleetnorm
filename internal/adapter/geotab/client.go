package geotab

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// the documented MyGeotab limits. we pace ourselves to them rather than
// discovering them by being throttled.
const (
	FeedCallsPerMinute = 60
	GetCallsPerMinute  = 500
	MaxResultsLimit    = 50000
)

// credentials is the MyGeotab authentication object. json tags are the API's
// spelling; the struct never leaves this package.
//
// ponytail: password on every call rather than Authenticate then sessionId.
// MyGeotab accepts it, and it removes a session to refresh and a second failure
// mode. switch to sessions if call volume ever makes the extra auth work
// matter.
type credentials struct {
	Database string `json:"database"`
	UserName string `json:"userName"`
	Password string `json:"password"`
}

// String redacts, so a credentials struct can never reach a log by being
// embedded in something someone printed.
func (c credentials) String() string {
	return fmt.Sprintf("{Database:%s UserName:[REDACTED] Password:[REDACTED]}", c.Database)
}

func (c credentials) GoString() string { return "geotab.credentials" + c.String() }

type client struct {
	http  *http.Client
	url   string
	creds credentials
	feed_ *limiter
	get_  *limiter
}

func newClient(server, database, username, password string, timeout time.Duration, hc *http.Client) *client {
	if hc == nil {
		hc = &http.Client{Timeout: timeout}
	}
	url := server
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "https://" + url
	}
	return &client{
		http:  hc,
		url:   strings.TrimSuffix(url, "/") + "/apiv1",
		creds: credentials{Database: database, UserName: username, Password: password},
		feed_: newLimiter(time.Minute / FeedCallsPerMinute),
		get_:  newLimiter(time.Minute / GetCallsPerMinute),
	}
}

// feedResult is one GetFeed page. data stays raw so every record reaches the
// event's raw field exactly as it arrived.
type feedResult struct {
	Data      []json.RawMessage `json:"data"`
	ToVersion string            `json:"toVersion"`
}

// feed calls GetFeed. fromDate is passed only on a cold start, where the API
// ignores fromVersion and needs a seed date instead, and fromVersion only once
// there is a cursor. sending both is not a documented combination.
func (c *client) feed(ctx context.Context, fromVersion string, fromDate time.Time, limit int) (feedResult, error) {
	params := map[string]any{"typeName": "FaultData", "resultsLimit": limit}
	if fromVersion != "" {
		params["fromVersion"] = fromVersion
	} else {
		params["search"] = map[string]any{"fromDate": fromDate.UTC().Format(time.RFC3339)}
	}
	if err := c.feed_.wait(ctx); err != nil {
		return feedResult{}, err
	}
	var out feedResult
	if err := c.call(ctx, "GetFeed", params, &out); err != nil {
		return feedResult{}, err
	}
	if out.ToVersion == "" {
		return feedResult{}, fmt.Errorf("GetFeed returned no toVersion; refusing to advance a cursor we were not given")
	}
	return out, nil
}

// multicall batches one Get per id into a single request, which is one call
// against the Get budget rather than len(ids).
type multicall struct {
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

// resolve fetches ids of one entity type. an id the server does not know comes
// back as an empty array and is simply left out of the result.
func (c *client) resolve(ctx context.Context, typeName string, ids []string) (map[string]Entity, error) {
	calls := make([]multicall, len(ids))
	for i, id := range ids {
		calls[i] = multicall{
			Method: "Get",
			Params: map[string]any{"typeName": typeName, "search": map[string]any{"id": id}},
		}
	}
	if err := c.get_.wait(ctx); err != nil {
		return nil, err
	}
	//each Get returns an array; a multicall returns an array of those
	var results [][]rawEntity
	if err := c.call(ctx, "ExecuteMultiCall", map[string]any{"calls": calls}, &results); err != nil {
		return nil, fmt.Errorf("resolving %s: %w", typeName, err)
	}
	out := make(map[string]Entity, len(ids))
	for _, got := range results {
		for _, r := range got {
			if r.ID == "" {
				continue
			}
			out[r.ID] = Entity{Name: r.Name, VIN: r.VIN, Code: r.Code, Kind: r.DiagnosticType}
		}
	}
	return out, nil
}

// rawEntity is the union of the fields we read off Diagnostic, FailureMode,
// Controller and Device. absent fields stay zero.
type rawEntity struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	VIN            string `json:"vehicleIdentificationNumber"`
	Code           *int   `json:"code"`
	DiagnosticType string `json:"diagnosticType"`
}

// apiError is the MyGeotab error envelope. the nested name is what says whether
// the credentials were rejected, so it is worth surfacing.
type apiError struct {
	Message string `json:"message"`
	Errors  []struct {
		Name    string `json:"name"`
		Message string `json:"message"`
	} `json:"errors"`
}

func (e apiError) Error() string {
	if len(e.Errors) > 0 && e.Errors[0].Name != "" {
		return e.Errors[0].Name + ": " + e.Errors[0].Message
	}
	return e.Message
}

// call posts one JSON-RPC request and decodes result into out. every failure
// here is a whole poll failure: nothing was read, so nothing can be skipped.
func (c *client) call(ctx context.Context, method string, params map[string]any, out any) error {
	params["credentials"] = c.creds
	body, err := json.Marshal(map[string]any{"method": method, "params": params})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	//MyGeotab answers 200 with an error envelope, but a proxy or an outage
	//answers with a status and no envelope at all
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s: %s", method, resp.Status, snippet(raw))
	}

	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *apiError       `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("%s: undecodable response: %w", method, err)
	}
	if envelope.Error != nil {
		return fmt.Errorf("%s: %w", method, *envelope.Error)
	}
	if err := json.Unmarshal(envelope.Result, out); err != nil {
		return fmt.Errorf("%s: unexpected result shape: %w", method, err)
	}
	return nil
}

// snippet keeps a failing body short enough to log without pasting a whole
// error page into the journal.
func snippet(b []byte) string {
	const max = 200
	s := strings.TrimSpace(string(b))
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}

// limiter spaces calls at least every apart.
//
// ponytail: interval spacing, not a token bucket. 60/min becomes one call a
// second, which is already what a poll loop does; a bucket would only matter if
// we wanted to spend a minute's budget at once, and we do not.
type limiter struct {
	mu    sync.Mutex
	every time.Duration
	next  time.Time
	now   func() time.Time
}

func newLimiter(every time.Duration) *limiter {
	return &limiter{every: every, now: time.Now}
}

func (l *limiter) wait(ctx context.Context) error {
	l.mu.Lock()
	now := l.now()
	delay := l.next.Sub(now)
	if delay < 0 {
		delay = 0
	}
	l.next = now.Add(delay + l.every)
	l.mu.Unlock()

	if delay == 0 {
		return ctx.Err()
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
