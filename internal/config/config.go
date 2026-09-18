// package config loads and validates the fleetnorm YAML config. validation is
// strict and happens once, at startup: a config that loads is a config that
// runs, and an unknown key is an error.
package config

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ianfrushon/fleetnorm/internal/event"
)

// applied when a field is omitted
const (
	DefaultPollInterval    = 5 * time.Second
	DefaultTimeout         = 10 * time.Second
	DefaultMaxAttempts     = 5
	DefaultBuffer          = 256
	DefaultStorePath       = "./fleetnorm.db"
	DefaultDedupeRetention = 7 * 24 * time.Hour
	DefaultServerAddr      = ":8080"
	BackoffExponential     = "exponential"
)

type Config struct {
	Adapters []Adapter `yaml:"adapters"`
	Outputs  []Output  `yaml:"outputs"`
	Rules    []Rule    `yaml:"rules"`
	Store    Store     `yaml:"store"`
	Server   Server    `yaml:"server"`
}

type Adapter struct {
	Type         string   `yaml:"type"`
	Name         string   `yaml:"name"`
	PollInterval Duration `yaml:"poll_interval"`

	Path string `yaml:"path"` // file

	//file only, default true: a record that will not normalize fails the poll
	//instead of being skipped. false gives the standard adapter contract.
	Strict *bool `yaml:"strict"`
}

type Output struct {
	Type   string `yaml:"type"`
	Name   string `yaml:"name"`
	Buffer int    `yaml:"buffer"` // bounded queue depth; full means drop, audited

	URL       string   `yaml:"url"`        // webhook
	SecretEnv string   `yaml:"secret_env"` // webhook: env var holding the HMAC key
	Timeout   Duration `yaml:"timeout"`    // webhook
	Retry     Retry    `yaml:"retry"`      // webhook

	//the value of SecretEnv, read once at load so the key Validate checked is the
	//key the output signs with. unexported: never marshaled, printed or logged.
	secret string
}

// Secret is the HMAC key resolved at load time, or "" if the output is
// unsigned. treat it as a credential.
func (o Output) Secret() string { return o.secret }

// String redacts the secret, since a Config lands in startup logs and errors.
func (o Output) String() string {
	secret := ""
	if o.secret != "" {
		secret = "[REDACTED]"
	}
	return fmt.Sprintf("{Type:%s Name:%s Buffer:%d URL:%s SecretEnv:%s Timeout:%s Retry:%+v secret:%s}",
		o.Type, o.Name, o.Buffer, o.URL, o.SecretEnv, o.Timeout, o.Retry, secret)
}

// GoString redacts too: %#v bypasses String, and that output goes into tickets.
func (o Output) GoString() string { return "config.Output" + o.String() }

type Retry struct {
	MaxAttempts int    `yaml:"max_attempts"`
	Backoff     string `yaml:"backoff"`
}

type Rule struct {
	Match Match    `yaml:"match"`
	Route []string `yaml:"route"`
}

// Match is a set of conditions ANDed together. a zero Match matches everything.
type Match struct {
	VIN        string            `yaml:"vin"`
	UnitID     string            `yaml:"unit_id"`
	Source     string            `yaml:"source"`
	SourceType string            `yaml:"source_type"`
	Severity   []string          `yaml:"severity"`
	SPN        SPNSet            `yaml:"spn"`
	FMI        *int              `yaml:"fmi"`
	Tags       map[string]string `yaml:"tags"`
}

type Store struct {
	Path            string   `yaml:"path"`
	DedupeRetention Duration `yaml:"dedupe_retention"`
}

type Server struct {
	Addr string `yaml:"addr"`
}

// Load reads, defaults and validates a config file.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	c, err := parse(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

func parse(r io.Reader) (*Config, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true) //a typo'd key is a bug, not a no-op
	var c Config
	if err := dec.Decode(&c); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("config is empty")
		}
		return nil, err
	}
	c.applyDefaults()
	c.resolveSecrets()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	for i := range c.Adapters {
		a := &c.Adapters[i]
		if a.PollInterval == 0 {
			a.PollInterval = Duration(DefaultPollInterval)
		}
		if a.Type == "file" && a.Strict == nil {
			strict := true
			a.Strict = &strict
		}
	}
	for i := range c.Outputs {
		o := &c.Outputs[i]
		if o.Buffer == 0 {
			o.Buffer = DefaultBuffer
		}
		if o.Type != "webhook" {
			continue
		}
		if o.Timeout == 0 {
			o.Timeout = Duration(DefaultTimeout)
		}
		if o.Retry.MaxAttempts == 0 {
			o.Retry.MaxAttempts = DefaultMaxAttempts
		}
		if o.Retry.Backoff == "" {
			o.Retry.Backoff = BackoffExponential
		}
	}
	if c.Store.Path == "" {
		c.Store.Path = DefaultStorePath
	}
	if c.Store.DedupeRetention == 0 {
		c.Store.DedupeRetention = Duration(DefaultDedupeRetention)
	}
	if c.Server.Addr == "" {
		c.Server.Addr = DefaultServerAddr
	}
}

// resolveSecrets reads each webhook key from the environment once, before
// validation, so a config that passes Validate has its secrets in hand.
func (c *Config) resolveSecrets() {
	for i := range c.Outputs {
		o := &c.Outputs[i]
		if o.Type == "webhook" && o.SecretEnv != "" {
			o.secret = os.Getenv(o.SecretEnv)
		}
	}
}

// Validate reports every problem it finds, not just the first: one run, one list
// of everything to fix.
func (c *Config) Validate() error {
	var errs []error
	bad := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if len(c.Adapters) == 0 {
		bad("no adapters configured")
	}
	adapters := map[string]int{} //name -> first index it appeared at
	for i, a := range c.Adapters {
		where := fmt.Sprintf("adapters[%d]", i)
		if a.Name != "" {
			where = fmt.Sprintf("adapter %q", a.Name)
		}
		if a.Name == "" {
			bad("adapters[%d]: name is required", i)
		} else if first, dup := adapters[a.Name]; dup {
			bad("adapters[%d]: duplicate name %q (also adapters[%d])", first, a.Name, i)
		} else {
			adapters[a.Name] = i
		}
		if a.PollInterval <= 0 {
			bad("%s: poll_interval must be positive", where)
		}
		switch a.Type {
		case "file":
			if a.Path == "" {
				bad("%s: path is required for a file adapter", where)
			}
		case "":
			bad("%s: type is required", where)
		default:
			bad("%s: unknown adapter type %q", where, a.Type)
		}
		if a.Strict != nil && a.Type != "file" {
			bad("%s: strict is only valid for a file adapter", where)
		}
	}

	if len(c.Outputs) == 0 {
		bad("no outputs configured")
	}
	//adapters and outputs are separate namespaces and may share a name
	outputs := map[string]int{} //name -> first index it appeared at
	for i, o := range c.Outputs {
		where := fmt.Sprintf("outputs[%d]", i)
		if o.Name != "" {
			where = fmt.Sprintf("output %q", o.Name)
		}
		if o.Name == "" {
			bad("outputs[%d]: name is required", i)
		} else if first, dup := outputs[o.Name]; dup {
			bad("outputs[%d]: duplicate name %q (also outputs[%d])", first, o.Name, i)
		} else {
			outputs[o.Name] = i
		}
		if o.Buffer <= 0 {
			bad("%s: buffer must be positive", where)
		}
		switch o.Type {
		case "stdout":
			//catches a webhook field landing under the wrong list item
			for field, set := range map[string]bool{
				"url": o.URL != "", "secret_env": o.SecretEnv != "",
				"timeout": o.Timeout != 0, "retry": o.Retry != Retry{},
			} {
				if set {
					bad("%s: %s is not valid for a stdout output", where, field)
				}
			}
		case "webhook":
			errs = append(errs, validateWebhook(where, o)...)
		case "":
			bad("%s: type is required", where)
		default:
			bad("%s: unknown output type %q", where, o.Type)
		}
	}

	if len(c.Rules) == 0 {
		bad("no rules configured: every event would be dropped")
	}
	for i, r := range c.Rules {
		where := fmt.Sprintf("rules[%d]", i)
		if len(r.Route) == 0 {
			bad("%s: route is required, use an explicit list of output names", where)
		}
		for _, name := range r.Route {
			if _, ok := outputs[name]; !ok {
				bad("%s: routes to unknown output %q", where, name)
			}
		}
		errs = append(errs, validateMatch(where, r.Match)...)
	}

	if c.Store.Path == "" {
		bad("store.path is required")
	}
	if c.Store.DedupeRetention <= 0 {
		bad("store.dedupe_retention must be positive")
	}
	if c.Server.Addr == "" {
		bad("server.addr is required")
	}
	return errors.Join(errs...)
}

func validateWebhook(where string, o Output) []error {
	var errs []error
	bad := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}
	switch u, err := url.Parse(o.URL); {
	case o.URL == "":
		bad("%s: url is required for a webhook output", where)
	case err != nil:
		bad("%s: url is not a valid URL: %v", where, err)
	case u.Scheme != "http" && u.Scheme != "https":
		bad("%s: url scheme must be http or https, got %q", where, u.Scheme)
	case u.Host == "":
		bad("%s: url has no host", where)
	}
	//a missing key fails at startup rather than at the first delivery
	if o.SecretEnv != "" && o.secret == "" {
		bad("%s: secret_env %q is unset or empty in the environment", where, o.SecretEnv)
	}
	if o.Timeout <= 0 {
		bad("%s: timeout must be positive", where)
	}
	if o.Retry.MaxAttempts < 1 {
		bad("%s: retry.max_attempts must be at least 1", where)
	}
	if o.Retry.Backoff != BackoffExponential {
		bad("%s: retry.backoff must be %q, got %q", where, BackoffExponential, o.Retry.Backoff)
	}
	return errs
}

func validateMatch(where string, m Match) []error {
	var errs []error
	for _, s := range m.Severity {
		if !validSeverity[event.Severity(s)] {
			errs = append(errs, fmt.Errorf("%s: unknown severity %q", where, s))
		}
	}
	if m.SourceType != "" && !validSourceType[event.SourceType(m.SourceType)] {
		errs = append(errs, fmt.Errorf("%s: unknown source_type %q", where, m.SourceType))
	}
	if m.FMI != nil && (*m.FMI < 0 || *m.FMI > 31) {
		errs = append(errs, fmt.Errorf("%s: fmi must be 0-31, got %d", where, *m.FMI))
	}
	for k := range m.Tags {
		if k == "" {
			errs = append(errs, fmt.Errorf("%s: tag key must not be empty", where))
		}
	}
	return errs
}

var validSeverity = map[event.Severity]bool{
	event.SeverityInfo: true, event.SeverityLow: true, event.SeverityMedium: true,
	event.SeverityHigh: true, event.SeverityCritical: true,
}

var validSourceType = map[event.SourceType]bool{
	event.SourceOEM: true, event.SourceTSP: true, event.SourceFile: true,
}

// Duration is a time.Duration written the Go way in YAML: 5s, 2m, 1h30m.
type Duration time.Duration

func (d Duration) String() string { return time.Duration(d).String() }

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return fmt.Errorf("line %d: duration must be a string like \"5s\"", n.Line)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*d = Duration(parsed)
	return nil
}

// SPNSet matches SPN values. in YAML it is a list whose entries are each a
// number or an inclusive "lo-hi" range: spn: [3226, 3216, "4000-4100"].
type SPNSet []SPNRange

type SPNRange struct{ Lo, Hi int }

func (s SPNSet) Contains(spn int) bool {
	for _, r := range s {
		if spn >= r.Lo && spn <= r.Hi {
			return true
		}
	}
	return false
}

func (s *SPNSet) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.SequenceNode {
		return fmt.Errorf("line %d: spn must be a list of numbers or \"lo-hi\" ranges", n.Line)
	}
	for _, item := range n.Content {
		var spn int
		if err := item.Decode(&spn); err == nil {
			if spn < 0 {
				return fmt.Errorf("line %d: spn must not be negative", item.Line)
			}
			*s = append(*s, SPNRange{Lo: spn, Hi: spn})
			continue
		}
		r, err := parseSPNRange(item.Value)
		if err != nil {
			return fmt.Errorf("line %d: %w", item.Line, err)
		}
		*s = append(*s, r)
	}
	return nil
}

func parseSPNRange(v string) (SPNRange, error) {
	lo, hi, ok := strings.Cut(v, "-")
	if !ok {
		return SPNRange{}, fmt.Errorf("spn entry %q is neither a number nor a \"lo-hi\" range", v)
	}
	l, err := strconv.Atoi(strings.TrimSpace(lo))
	if err != nil {
		return SPNRange{}, fmt.Errorf("spn range %q: bad lower bound", v)
	}
	h, err := strconv.Atoi(strings.TrimSpace(hi))
	if err != nil {
		return SPNRange{}, fmt.Errorf("spn range %q: bad upper bound", v)
	}
	if l < 0 {
		return SPNRange{}, fmt.Errorf("spn range %q: must not be negative", v)
	}
	if l > h {
		return SPNRange{}, fmt.Errorf("spn range %q: lower bound is above upper bound", v)
	}
	return SPNRange{Lo: l, Hi: h}, nil
}
