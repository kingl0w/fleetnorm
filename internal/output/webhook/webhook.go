// package webhook POSTs events to an owner supplied endpoint, signed with HMAC
// SHA256 so the receiver can tell they came from this fleetnorm. one Send is one
// attempt; backoff and giving up belong to the pipeline.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ianfrushon/fleetnorm/internal/event"
	"github.com/ianfrushon/fleetnorm/internal/output"
)

const (
	//identifies this fleetnorm to receivers; wire to a build stamp when there is one
	version = "0.1.0"

	//how much of a failing response is quoted back: enough to read a JSON error,
	//not enough for a page of HTML to become a log line
	maxErrorBody = 4 << 10

	//how much of a body is read before giving up on reusing the connection
	maxDrain = 64 << 10

	//caps what Retry-After can ask for, so a broken endpoint cannot park the
	//queue for a day
	DefaultMaxRetryAfter = 5 * time.Minute

	DefaultTimeout = 10 * time.Second
)

type Options struct {
	Name    string
	URL     string
	Secret  string        //HMAC key; unsigned when empty
	Timeout time.Duration //per attempt; DefaultTimeout when zero

	MaxRetryAfter time.Duration
}

type Output struct {
	name          string
	url           string
	safeURL       string //no credentials, no query: safe to log
	secret        []byte
	maxRetryAfter time.Duration
	client        *http.Client
	now           func() time.Time //swapped in tests
}

func New(o Options) (*Output, error) {
	if o.Name == "" {
		return nil, errors.New("webhook output needs a name")
	}
	u, err := url.Parse(o.URL)
	if err != nil {
		return nil, fmt.Errorf("webhook output %q: %w", o.Name, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("webhook output %q: url scheme must be http or https, got %q", o.Name, u.Scheme)
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.MaxRetryAfter <= 0 {
		o.MaxRetryAfter = DefaultMaxRetryAfter
	}
	return &Output{
		name:          o.Name,
		url:           o.URL,
		safeURL:       safeURL(u),
		secret:        []byte(o.Secret),
		maxRetryAfter: o.MaxRetryAfter,
		client: &http.Client{
			Timeout: o.Timeout,
			//never follow a redirect on a signed POST: the signature travels with
			//the body, so the next host would receive a valid HMAC over fleet data.
			//take the 3xx as the final response and fail on it.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		now: time.Now,
	}, nil
}

func (o *Output) Name() string { return o.name }

func (o *Output) Send(ctx context.Context, e event.Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("%w: %s: encoding event %s: %w", output.ErrPermanent, o.safeURL, e.EventID, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: %s: %w", output.ErrPermanent, o.safeURL, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "fleetnorm/"+version)
	req.Header.Set("X-Fleetnorm-Event-Id", e.EventID)
	if len(o.secret) > 0 {
		req.Header.Set("X-Fleetnorm-Signature", sign(o.secret, body))
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", o.safeURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 == 2 {
		//drain so the connection can be reused
		io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrain))
		return nil
	}
	return o.classify(resp)
}

// classify turns a non 2xx into the right kind of error. the question is whether
// another attempt could plausibly succeed, not how bad the response looks.
func (o *Output) classify(resp *http.Response) error {
	snippet := readSnippet(resp.Body)
	where := fmt.Sprintf("%s: %s", o.safeURL, resp.Status)
	if snippet != "" {
		where += ": " + snippet
	}

	switch {
	case resp.StatusCode/100 == 3:
		return fmt.Errorf("%w: %s: refusing to follow a redirect to %q on a signed POST: the signature would travel to another host",
			output.ErrPermanent, where, resp.Header.Get("Location"))

	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode == http.StatusServiceUnavailable:
		err := fmt.Errorf("%s", where)
		if d, ok := o.retryAfter(resp.Header.Get("Retry-After")); ok {
			return &output.DelayError{Delay: d, Err: err}
		}
		return err

	case resp.StatusCode == http.StatusRequestTimeout, resp.StatusCode/100 == 5:
		return fmt.Errorf("%s", where)

	//every other 4xx: the request itself is wrong, so surface it now
	default:
		return fmt.Errorf("%w: %s", output.ErrPermanent, where)
	}
}

// retryAfter reads the header in either of its forms, capped. an unusable value
// is ignored so the caller falls back to its own backoff.
func (o *Output) retryAfter(h string) (time.Duration, bool) {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0, false
	}
	var d time.Duration
	switch secs, err := strconv.Atoi(h); {
	case err == nil:
		d = time.Duration(secs) * time.Second
	//every other 4xx: the request itself is wrong, so surface it now
	default:
		t, err := http.ParseTime(h)
		if err != nil {
			return 0, false
		}
		d = t.Sub(o.now())
	}
	if d < 0 {
		d = 0
	}
	return min(d, o.maxRetryAfter), true
}

func sign(key, body []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// readSnippet quotes the start of a failing body, flattened to one line.
func readSnippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, maxErrorBody))
	truncated := len(b) == maxErrorBody
	io.Copy(io.Discard, io.LimitReader(r, maxDrain))
	s := strings.TrimSpace(strings.Join(strings.Fields(string(b)), " "))
	if truncated {
		s += "…(truncated)"
	}
	return s
}

// safeURL is the endpoint without the parts that hide credentials, for logs and
// error messages.
func safeURL(u *url.URL) string {
	clean := *u
	clean.User = nil
	clean.RawQuery = ""
	clean.Fragment = ""
	s := clean.String()
	if u.RawQuery != "" {
		s += "?…"
	}
	return s
}
