package geotab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

// login is the MyGeotab Authenticate payload. it is the only request the
// password ever appears on; every call after it carries a sessionId instead.
type login struct {
	Database string `json:"database"`
	UserName string `json:"userName"`
	Password string `json:"password"`
}

// String redacts, so a login struct can never reach a log by being embedded in
// something someone printed.
func (l login) String() string {
	return fmt.Sprintf("{Database:%s UserName:[REDACTED] Password:[REDACTED]}", l.Database)
}

func (l login) GoString() string { return "geotab.login" + l.String() }

// sessionCreds is the credentials object every call after Authenticate carries.
// no password: it is not needed once a session exists and sending it anyway
// would put it on every request in the journal of anything in between.
type sessionCreds struct {
	Database  string `json:"database"`
	UserName  string `json:"userName"`
	SessionID string `json:"sessionId"`
}

// session pairs a sessionId with the server that issued it. they are one value
// and are never used apart: a sessionId is only valid against its own server,
// so carrying one to a different host fails as an authentication error rather
// than as anything that names the real cause.
type session struct {
	creds sessionCreds
	url   string //the resolved apiv1 endpoint this sessionId belongs to
}

// path values Authenticate returns to mean "the server you asked is correct"
const thisServer = "ThisServer"

type client struct {
	name  string //adapter name, for logs
	http  *http.Client
	url   string //endpoint built from the configured server
	login login
	feed_ *limiter
	get_  *limiter

	//guards sess and serializes authentication. held across the Authenticate
	//call on purpose: that is what makes concurrent callers share one
	//authentication instead of each starting their own.
	mu   sync.Mutex
	sess *session
}

func newClient(name, server, database, username, password string, timeout time.Duration, hc *http.Client) *client {
	if hc == nil {
		hc = &http.Client{Timeout: timeout}
	}
	return &client{
		name:  name,
		http:  hc,
		url:   endpoint(server),
		login: login{Database: database, UserName: username, Password: password},
		feed_: newLimiter(time.Minute / FeedCallsPerMinute),
		get_:  newLimiter(time.Minute / GetCallsPerMinute),
	}
}

// endpoint turns a server name or URL into the apiv1 endpoint to post to.
func endpoint(server string) string {
	u := strings.TrimSpace(server)
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		u = "https://" + u
	}
	u = strings.TrimSuffix(u, "/")
	if !strings.HasSuffix(u, "/apiv1") {
		u += "/apiv1"
	}
	return u
}

// authResult is what Authenticate returns. path is either another server, which
// is the one that must be used from then on, or ThisServer.
type authResult struct {
	Credentials sessionCreds `json:"credentials"`
	Path        string       `json:"path"`
}

// current returns a usable session, authenticating on first use. construction
// does no network IO, so this is where the first call pays for it.
func (c *client) current(ctx context.Context) (*session, error) {
	c.mu.Lock()
	if s := c.sess; s != nil {
		c.mu.Unlock()
		return s, nil
	}
	c.mu.Unlock()
	return c.refresh(ctx, nil)
}

// refresh replaces stale with a new session, unless another caller got there
// first. sessions last up to 14 days, so this runs on first use and then only
// when the server tells us the session is no longer good.
func (c *client) refresh(ctx context.Context, stale *session) (*session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	//someone else already re-authenticated while we were failing
	if c.sess != nil && c.sess != stale {
		return c.sess, nil
	}
	s, err := c.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	c.sess = s
	return s, nil
}

// authenticate exchanges the password for a sessionId and the server that
// sessionId belongs to. caller holds c.mu.
func (c *client) authenticate(ctx context.Context) (*session, error) {
	var res authResult
	if err := c.post(ctx, c.url, "Authenticate", map[string]any{
		"database": c.login.Database, "userName": c.login.UserName, "password": c.login.Password,
	}, &res); err != nil {
		return nil, err
	}
	if res.Credentials.SessionID == "" {
		return nil, fmt.Errorf("Authenticate returned no sessionId")
	}

	url := c.url
	//a database that does not live on the configured server is a normal
	//deployment, not an edge case: the sessionId is only valid against the
	//server that issued it, so everything after this goes there.
	if res.Path != "" && res.Path != thisServer {
		url = endpoint(res.Path)
	}
	if url != c.url {
		slog.Info("geotab session is on a different server than configured",
			"adapter", c.name, "configured", c.url, "server", url)
	}
	return &session{creds: res.Credentials, url: url}, nil
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

// call runs one authenticated request, re-authenticating once if the server
// says the session is no longer good. every failure here is a whole poll
// failure: nothing was read, so nothing can be skipped.
func (c *client) call(ctx context.Context, method string, params map[string]any, out any) error {
	sess, err := c.current(ctx)
	if err != nil {
		return err
	}
	err = c.do(ctx, sess, method, params, out)
	if !isInvalidUser(err) {
		return err
	}

	//the session expired or was revoked. one re-authentication and one retry:
	//if the credentials are simply wrong, retrying forever would just be a
	//login attempt every poll.
	sess, authErr := c.refresh(ctx, sess)
	if authErr != nil {
		return authErr
	}
	if err := c.do(ctx, sess, method, params, out); err != nil {
		if isInvalidUser(err) {
			return fmt.Errorf("%w (re-authenticated and it was still rejected, so the credentials are wrong rather than stale)", err)
		}
		return err
	}
	return nil
}

// do posts one call against a session. the password is not part of this: only
// Authenticate ever sends it.
func (c *client) do(ctx context.Context, sess *session, method string, params map[string]any, out any) error {
	//params is rebuilt per attempt so a retry cannot inherit the last one's
	//credentials
	body := make(map[string]any, len(params)+1)
	for k, v := range params {
		body[k] = v
	}
	body["credentials"] = sess.creds
	return c.post(ctx, sess.url, method, body, out)
}

// post is the raw transport: one JSON-RPC request, one decoded result. it
// knows nothing about sessions, so authenticate can use it too.
func (c *client) post(ctx context.Context, url, method string, params map[string]any, out any) error {
	body, err := json.Marshal(map[string]any{"method": method, "params": params})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
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

// isInvalidUser reports whether err is the server saying the session or the
// credentials are no good, which is the one error worth re-authenticating for.
func isInvalidUser(err error) bool {
	var api apiError
	if !errors.As(err, &api) {
		return false
	}
	for _, e := range api.Errors {
		if e.Name == invalidUser {
			return true
		}
	}
	return strings.Contains(api.Message, invalidUser)
}

const invalidUser = "InvalidUserException"

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
