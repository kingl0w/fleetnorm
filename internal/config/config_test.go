package config

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// a minimal valid config, as a format string the error tests splice into.
const valid = `
adapters:
  - { type: file, name: replay, path: ./testdata/events.json }
outputs:
  - { type: stdout, name: console }
rules:
  - { match: {}, route: [console] }
`

func TestLoadExample(t *testing.T) {
	t.Setenv("SHOP_SECRET", "not-a-real-secret")
	c, err := Load("../../configs/example.yaml")
	if err != nil {
		t.Fatalf("example config must always load: %v", err)
	}

	if len(c.Adapters) != 1 || c.Adapters[0].Name != "replay" {
		t.Fatalf("adapters = %+v", c.Adapters)
	}
	if got, want := time.Duration(c.Adapters[0].PollInterval), 5*time.Second; got != want {
		t.Errorf("poll_interval = %v, want %v", got, want)
	}

	shop := c.Outputs[1]
	if shop.Name != "my-shop" || shop.URL != "https://shop.example/ingest" {
		t.Errorf("webhook output = %+v", shop)
	}
	if shop.Retry != (Retry{MaxAttempts: 5, Backoff: BackoffExponential}) {
		t.Errorf("retry = %+v", shop.Retry)
	}
	if shop.Buffer != DefaultBuffer {
		t.Errorf("buffer = %d, want default %d", shop.Buffer, DefaultBuffer)
	}

	want := []Rule{
		{Match: Match{Severity: []string{"critical", "high"}}, Route: []string{"my-shop", "console"}},
		{Match: Match{Source: "replay", SPN: SPNSet{{3226, 3226}, {3216, 3216}, {4000, 4100}}}, Route: []string{"my-shop"}},
		{Route: []string{"console"}},
	}
	if !reflect.DeepEqual(c.Rules, want) {
		t.Errorf("rules =\n%+v\nwant\n%+v", c.Rules, want)
	}
}

func TestDefaults(t *testing.T) {
	c, err := parse(strings.NewReader(valid))
	if err != nil {
		t.Fatal(err)
	}
	if got := time.Duration(c.Adapters[0].PollInterval); got != DefaultPollInterval {
		t.Errorf("poll_interval = %v, want %v", got, DefaultPollInterval)
	}
	if c.Outputs[0].Buffer != DefaultBuffer {
		t.Errorf("buffer = %v, want %v", c.Outputs[0].Buffer, DefaultBuffer)
	}
	if c.Store.Path != DefaultStorePath || c.Server.Addr != DefaultServerAddr {
		t.Errorf("store/server defaults not applied: %+v %+v", c.Store, c.Server)
	}
	if got := time.Duration(c.Store.DedupeRetention); got != DefaultDedupeRetention {
		t.Errorf("dedupe_retention = %v, want %v", got, DefaultDedupeRetention)
	}
	//defaults are for webhook fields only; stdout must stay bare or the
	//"not valid for a stdout output" check would fire on its own defaults.
	if c.Outputs[0].Timeout != 0 || c.Outputs[0].Retry != (Retry{}) {
		t.Errorf("stdout output picked up webhook defaults: %+v", c.Outputs[0])
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want []string //every one of these must appear in the error
	}{
		{name: "empty", yaml: "", want: []string{"config is empty"}},
		{name: "unknown top-level key", yaml: valid + "\nnonsense: true\n", want: []string{"nonsense"}},
		{name: "unknown adapter key", yaml: `
adapters: [{ type: file, name: replay, path: p, pollinterval: 5s }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`, want: []string{"pollinterval"}},

		{name: "no adapters", yaml: `
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`, want: []string{"no adapters configured"}},
		{name: "no outputs", yaml: `
adapters: [{ type: file, name: replay, path: p }]
rules: [{ match: {}, route: [console] }]
`, want: []string{"no outputs configured", `unknown output "console"`}},
		{name: "no rules", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: stdout, name: console }]
`, want: []string{"no rules configured"}},

		{name: "adapter without name", yaml: `
adapters: [{ type: file, path: p }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`, want: []string{"adapters[0]: name is required"}},
		{name: "adapter without type", yaml: `
adapters: [{ name: replay, path: p }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`, want: []string{`adapter "replay": type is required`}},
		{name: "strict on a non-file adapter", yaml: `
adapters: [{ type: samsara, name: g, strict: false }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`, want: []string{"strict is not valid for a samsara adapter"}},
		{name: "unknown adapter type", yaml: `
adapters: [{ type: samsara, name: g }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`, want: []string{`unknown adapter type "samsara"`}},
		{name: "file adapter without path", yaml: `
adapters: [{ type: file, name: replay }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`, want: []string{"path is required"}},
		{name: "duplicate adapter names", yaml: `
adapters:
  - { type: file, name: replay, path: a }
  - { type: file, name: replay, path: b }
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`, want: []string{`adapters[0]: duplicate name "replay" (also adapters[1])`}},
		{name: "duplicate output names", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs:
  - { type: stdout, name: console }
  - { type: stdout, name: console }
rules: [{ match: {}, route: [console] }]
`, want: []string{`outputs[0]: duplicate name "console" (also outputs[1])`}},
		{name: "duplicate output name, rules still checked", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs:
  - { type: stdout, name: console }
  - { type: stdout, name: quiet }
  - { type: stdout, name: console }
rules: [{ match: {}, route: [console, elsewhere] }]
`, want: []string{
			`outputs[0]: duplicate name "console" (also outputs[2])`,
			`rules[0]: routes to unknown output "elsewhere"`,
		}},

		{name: "bad duration", yaml: `
adapters: [{ type: file, name: replay, path: p, poll_interval: 5 }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`, want: []string{`missing unit in duration "5"`}},
		{name: "duration is not a scalar", yaml: `
adapters: [{ type: file, name: replay, path: p, poll_interval: { every: 5s } }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`, want: []string{"duration must be a string"}},
		{name: "unparseable duration", yaml: `
adapters: [{ type: file, name: replay, path: p, poll_interval: "soon" }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`, want: []string{"invalid duration"}},
		{name: "negative poll_interval", yaml: `
adapters: [{ type: file, name: replay, path: p, poll_interval: -5s }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`, want: []string{"poll_interval must be positive"}},

		{name: "webhook without url", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: webhook, name: shop }]
rules: [{ match: {}, route: [shop] }]
`, want: []string{"url is required"}},
		{name: "webhook bad scheme", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: webhook, name: shop, url: "ftp://shop.example/x" }]
rules: [{ match: {}, route: [shop] }]
`, want: []string{`scheme must be http or https, got "ftp"`}},
		{name: "webhook url without host", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: webhook, name: shop, url: "https:///ingest" }]
rules: [{ match: {}, route: [shop] }]
`, want: []string{"no host"}},
		{name: "webhook secret_env unset", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: webhook, name: shop, url: "https://shop.example/x", secret_env: DEFINITELY_UNSET_9f2 }]
rules: [{ match: {}, route: [shop] }]
`, want: []string{`secret_env "DEFINITELY_UNSET_9f2" is unset`}},
		{name: "webhook bad retry", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs:
  - type: webhook
    name: shop
    url: "https://shop.example/x"
    retry: { max_attempts: -1, backoff: linear }
rules: [{ match: {}, route: [shop] }]
`, want: []string{"max_attempts must be at least 1", `backoff must be "exponential", got "linear"`}},
		{name: "stdout with webhook fields", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: stdout, name: console, url: "https://x.example", timeout: 3s }]
rules: [{ match: {}, route: [console] }]
`, want: []string{"url is not valid for a stdout output", "timeout is not valid for a stdout output"}},
		{name: "zero buffer", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: stdout, name: console, buffer: -1 }]
rules: [{ match: {}, route: [console] }]
`, want: []string{"buffer must be positive"}},

		{name: "route to unknown output", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [my-shop] }]
`, want: []string{`rules[0]: routes to unknown output "my-shop"`}},
		{name: "rule without route", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: { severity: [high] } }]
`, want: []string{"route is required"}},
		{name: "bad severity", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: { severity: [high, URGENT] }, route: [console] }]
`, want: []string{`unknown severity "URGENT"`}},
		{name: "bad source_type", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: { source_type: carrier }, route: [console] }]
`, want: []string{`unknown source_type "carrier"`}},
		{name: "fmi out of range", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: { fmi: 32 }, route: [console] }]
`, want: []string{"fmi must be 0-31"}},

		{name: "spn not a list", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: { spn: 3226 }, route: [console] }]
`, want: []string{"spn must be a list"}},
		{name: "spn inverted range", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: { spn: ["4100-4000"] }, route: [console] }]
`, want: []string{"lower bound is above upper bound"}},
		{name: "spn junk", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: { spn: [banana] }, route: [console] }]
`, want: []string{"neither a number nor"}},
		{name: "spn negative", yaml: `
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: { spn: [-1] }, route: [console] }]
`, want: []string{"must not be negative"}},

		//Validate reports everything at once, it does not stop at the first.
		{name: "several problems at once", yaml: `
adapters: [{ type: samsara, name: g }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: { severity: [nope] }, route: [nowhere] }]
`, want: []string{"unknown adapter type", "unknown severity", "unknown output"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parse(strings.NewReader(tt.yaml))
			if err == nil {
				t.Fatalf("parse() = nil error, want error mentioning %q", tt.want)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error missing %q, got:\n%v", want, err)
				}
			}
		})
	}
}

func TestSPNSetContains(t *testing.T) {
	s := SPNSet{{3226, 3226}, {4000, 4100}}
	for spn, want := range map[int]bool{
		3225: false, 3226: true, 3227: false,
		3999: false, 4000: true, 4050: true, 4100: true, 4101: false,
	} {
		if got := s.Contains(spn); got != want {
			t.Errorf("Contains(%d) = %v, want %v", spn, got, want)
		}
	}
	if (SPNSet{}).Contains(3226) {
		t.Error("empty SPNSet must not match")
	}
}

// Adapters and outputs are separate namespaces, so the same name in each is
// fine. Only a collision within one list is an error.
func TestNamesAcrossKindsDoNotCollide(t *testing.T) {
	c, err := parse(strings.NewReader(`
adapters: [{ type: file, name: archive, path: p }]
outputs: [{ type: stdout, name: archive }]
rules: [{ match: {}, route: [archive] }]
`))
	if err != nil {
		t.Fatalf("adapter and output may share a name: %v", err)
	}
	if c.Adapters[0].Name != "archive" || c.Outputs[0].Name != "archive" {
		t.Errorf("names not preserved: %+v %+v", c.Adapters[0], c.Outputs[0])
	}
}

func TestWebhookSecret(t *testing.T) {
	const secret = "s3kr1t-hmac-key-do-not-print"
	t.Setenv("SHOP_SECRET", secret)
	yaml := `
adapters: [{ type: file, name: replay, path: p }]
outputs:
  - { type: stdout, name: console }
  - { type: webhook, name: shop, url: "https://shop.example/x", secret_env: SHOP_SECRET }
rules: [{ match: {}, route: [shop] }]
`
	c, err := parse(strings.NewReader(yaml))
	if err != nil {
		t.Fatal(err)
	}

	//the output signs with exactly the value Validate checked.
	if got := c.Outputs[1].Secret(); got != secret {
		t.Errorf("Secret() = %q, want the env value", got)
	}
	if got := c.Outputs[0].Secret(); got != "" {
		t.Errorf("unsigned output has a secret: %q", got)
	}

	//changing the environment afterwards must not change what we signed with.
	t.Setenv("SHOP_SECRET", "rotated")
	if got := c.Outputs[1].Secret(); got != secret {
		t.Errorf("Secret() = %q after env change, want the value resolved at load", got)
	}

	//and it must not leak into anything printable.
	for _, f := range []string{"%v", "%+v", "%#v", "%s"} {
		for what, v := range map[string]any{"config": c, "config value": *c, "output": c.Outputs[1]} {
			if out := fmt.Sprintf(f, v); strings.Contains(out, secret) {
				t.Errorf("%s printed with %s leaked the secret:\n%s", what, f, out)
			}
		}
	}
	//the env var name itself is not a secret and stays visible.
	if out := fmt.Sprintf("%v", c.Outputs[1]); !strings.Contains(out, "SHOP_SECRET") {
		t.Errorf("secret_env name should still be printed, got %s", out)
	}
}

func TestSecretMissingStillFails(t *testing.T) {
	for _, env := range []string{"", "   "} {
		t.Setenv("SHOP_SECRET", "")
		if env != "" {
			t.Setenv("SHOP_SECRET", env)
		}
		_, err := parse(strings.NewReader(`
adapters: [{ type: file, name: replay, path: p }]
outputs: [{ type: webhook, name: shop, url: "https://shop.example/x", secret_env: SHOP_SECRET }]
rules: [{ match: {}, route: [shop] }]
`))
		if env == "" && err == nil {
			t.Error("empty secret must fail at load")
		}
		if env != "" && err != nil {
			t.Errorf("a whitespace secret is still a secret, got %v", err)
		}
	}
}

func TestAdapterStrict(t *testing.T) {
	//a file adapter is strict unless it says otherwise.
	c, err := parse(strings.NewReader(valid))
	if err != nil {
		t.Fatal(err)
	}
	if c.Adapters[0].Strict == nil || !*c.Adapters[0].Strict {
		t.Errorf("strict = %v, want it defaulted to true", c.Adapters[0].Strict)
	}

	c, err = parse(strings.NewReader(`
adapters: [{ type: file, name: replay, path: p, strict: false }]
outputs: [{ type: stdout, name: console }]
rules: [{ match: {}, route: [console] }]
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Adapters[0].Strict == nil || *c.Adapters[0].Strict {
		t.Errorf("strict = %v, want the configured false", c.Adapters[0].Strict)
	}
}

// a geotab adapter with everything required, as a format string the error tests
// splice into.
const validGeotab = `
adapters:
  - type: geotab
    name: fleet-geotab
    database: mydb
    username_env: GEOTAB_USER
    password_env: GEOTAB_PASS
    seed_from: 720h
outputs:
  - { type: stdout, name: console }
rules:
  - { match: {}, route: [console] }
`

func geotabEnv(t *testing.T) {
	t.Helper()
	t.Setenv("GEOTAB_USER", "driver@example.com")
	t.Setenv("GEOTAB_PASS", "not-a-real-password")
}

func TestGeotabDefaults(t *testing.T) {
	geotabEnv(t)
	c, err := parse(strings.NewReader(validGeotab))
	if err != nil {
		t.Fatal(err)
	}
	a := c.Adapters[0]
	if a.Server != DefaultGeotabServer {
		t.Errorf("server = %q, want %q", a.Server, DefaultGeotabServer)
	}
	if a.ResultsLimit != DefaultGeotabResultsLimit {
		t.Errorf("results_limit = %d, want %d", a.ResultsLimit, DefaultGeotabResultsLimit)
	}
	if time.Duration(a.CacheRefresh) != DefaultGeotabCacheRefresh {
		t.Errorf("cache_refresh = %v, want %v", a.CacheRefresh, DefaultGeotabCacheRefresh)
	}
	if a.Username() != "driver@example.com" || a.Password() != "not-a-real-password" {
		t.Error("credentials were not resolved from the environment")
	}
	if a.SeedFrom == nil {
		t.Fatal("seed_from was not parsed")
	}
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	if got, want := a.SeedFrom.At(now), now.Add(-720*time.Hour); !got.Equal(want) {
		t.Errorf("seed_from.At = %v, want %v", got, want)
	}
}

func TestGeotabSeedFrom(t *testing.T) {
	geotabEnv(t)
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		value string
		want  time.Time
		bad   string
	}{
		{value: "720h", want: now.Add(-720 * time.Hour)},
		//explicit intent, not omission: only data from here on
		{value: "0s", want: now},
		{value: "2026-01-01T00:00:00Z", want: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{value: "2026-01-01T00:00:00-05:00", want: time.Date(2026, 1, 1, 5, 0, 0, 0, time.UTC)},
		{value: "-1h", bad: "must not be negative"},
		{value: "yesterday", bad: "neither a duration"},
		{value: "2026-01-01", bad: "neither a duration"},
	} {
		t.Run(tc.value, func(t *testing.T) {
			src := strings.Replace(validGeotab, "seed_from: 720h", "seed_from: "+strconv.Quote(tc.value), 1)
			c, err := parse(strings.NewReader(src))
			if tc.bad != "" {
				if err == nil || !strings.Contains(err.Error(), tc.bad) {
					t.Fatalf("err = %v, want it to mention %q", err, tc.bad)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := c.Adapters[0].SeedFrom.At(now); !got.Equal(tc.want) {
				t.Errorf("At = %v, want %v", got, tc.want)
			}
		})
	}
}

// the failure this whole field exists to prevent: a new feed with no seed date
// produces nothing, forever, while every health signal stays green.
func TestGeotabWithoutSeedFromFailsAtLoad(t *testing.T) {
	geotabEnv(t)
	src := strings.Replace(validGeotab, "    seed_from: 720h\n", "", 1)
	_, err := parse(strings.NewReader(src))
	if err == nil {
		t.Fatal("a geotab adapter with no seed_from must not load")
	}
	if !strings.Contains(err.Error(), "seed_from is required") {
		t.Errorf("err = %v, want it to name seed_from", err)
	}
	//and it says what to do about it
	if !strings.Contains(err.Error(), "0s") {
		t.Errorf("err = %v, want it to mention the 0s escape hatch", err)
	}
}

func TestGeotabErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		replace, with, want string
	}{
		"no database":       {"    database: mydb\n", "", "database is required"},
		"no username_env":   {"    username_env: GEOTAB_USER\n", "", "username_env is required"},
		"no password_env":   {"    password_env: GEOTAB_PASS\n", "", "password_env is required"},
		"unset credential":  {"username_env: GEOTAB_USER", "username_env: GEOTAB_NOPE", "unset or empty"},
		"results_limit big": {"seed_from: 720h", "seed_from: 720h\n    results_limit: 60000", "results_limit must be"},
		"poll too fast":     {"seed_from: 720h", "seed_from: 720h\n    poll_interval: 100ms", "poll_interval must be at least"},
		"file field":        {"seed_from: 720h", "seed_from: 720h\n    path: ./x.json", "path is not valid for a geotab adapter"},
	} {
		t.Run(name, func(t *testing.T) {
			geotabEnv(t)
			src := strings.Replace(validGeotab, tc.replace, tc.with, 1)
			_, err := parse(strings.NewReader(src))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// a geotab field on a file adapter is a typo, not a no-op
func TestGeotabFieldsRejectedOnFileAdapter(t *testing.T) {
	src := strings.Replace(valid,
		"- { type: file, name: replay, path: ./testdata/events.json }",
		"- { type: file, name: replay, path: ./testdata/events.json, database: mydb, seed_from: 720h }", 1)
	_, err := parse(strings.NewReader(src))
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{"database is not valid for a file adapter", "seed_from is not valid for a file adapter"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v, want it to mention %q", err, want)
		}
	}
}

func TestAdapterCredentialsAreNotPrintable(t *testing.T) {
	geotabEnv(t)
	c, err := parse(strings.NewReader(validGeotab))
	if err != nil {
		t.Fatal(err)
	}
	//the whole config, since that is what reaches a startup log
	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		out := fmt.Sprintf(format, c.Adapters)
		for _, secret := range []string{"driver@example.com", "not-a-real-password"} {
			if strings.Contains(out, secret) {
				t.Errorf("%s printed a credential: %s", format, out)
			}
		}
		if !strings.Contains(out, "[REDACTED]") {
			t.Errorf("%s = %s, want the credentials shown as redacted", format, out)
		}
	}
}
