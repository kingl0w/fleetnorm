// package file replays events from a JSON or CSV file, so the whole pipeline can
// be exercised with no credentials and no network.
//
// records arrive already in the normalized shape. the adapter fills in what it
// owns, moves unrecognized fields into tags, keeps the record verbatim in raw,
// and validates.
//
// New is strict: one unusable record fails the poll, which is the opposite of
// the adapter contract and deliberate, because this input is a local file its
// owner wrote. NewLenient follows the contract. docs/adapters.md has the
// reasoning, and both paths live here as a reference for each.
package file

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ianfrushon/fleetnorm/internal/adapter"
	"github.com/ianfrushon/fleetnorm/internal/event"
)

// record keys that map onto an event field. everything else becomes a tag, and
// TestKnownFieldsMatchEvent fails if this drifts from the type.
var knownFields = map[string]bool{
	"schema_version": true, "event_id": true, "vin": true,
	"occurred_at": true, "received_at": true, "source": true,
	"source_type": true, "severity": true, "unit_id": true,
	"spn": true, "fmi": true, "occurrence_count": true,
	"lamp_status": true, "odometer_km": true, "location": true,
	"description": true, "tags": true, "raw": true,
}

// CSV columns written to JSON unquoted. lat and lon fold into location instead.
var csvNumeric = map[string]bool{
	"spn": true, "fmi": true, "occurrence_count": true, "odometer_km": true,
}

// CSV columns whose cell is itself JSON
var csvJSON = map[string]bool{"location": true, "tags": true}

type Adapter struct {
	name   string
	path   string
	csv    bool
	strict bool
	onSkip adapter.SkipFunc
	now    func() time.Time //swapped in tests
}

// New returns a strict file adapter, checking now that the file is readable so a
// typo in the config fails at startup.
func New(name, path string) (*Adapter, error) {
	return newFile(name, path, true, nil)
}

// NewLenient returns one that follows the standard contract: unusable records
// are skipped, reported to onSkip and stepped over, up to the ceiling. onSkip
// may be nil, which logs the skip and audits nothing.
func NewLenient(name, path string, onSkip adapter.SkipFunc) (*Adapter, error) {
	return newFile(name, path, false, onSkip)
}

func newFile(name, path string, strict bool, onSkip adapter.SkipFunc) (*Adapter, error) {
	if name == "" {
		return nil, errors.New("file adapter needs a name")
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("file adapter %q: %w", name, err)
	}
	if fi.IsDir() {
		return nil, fmt.Errorf("file adapter %q: %s is a directory", name, path)
	}
	return &Adapter{
		name:   name,
		path:   path,
		csv:    strings.EqualFold(filepath.Ext(path), ".csv"),
		strict: strict,
		onSkip: onSkip,
		now:    func() time.Time { return time.Now().UTC() },
	}, nil
}

func (a *Adapter) Name() string { return a.name }

// Poll returns the records after the cursor, which is the count of records
// already returned.
//
// ponytail: the whole file is re-read every poll. replay files are small and a
// partial read has no cheap correct answer; switch to a byte offset if this ever
// points at something large.
func (a *Adapter) Poll(ctx context.Context, since adapter.Cursor) ([]event.Event, adapter.Cursor, error) {
	if err := ctx.Err(); err != nil {
		return nil, since, err
	}
	offset, err := parseCursor(since)
	if err != nil {
		return nil, since, err
	}

	records, err := a.read()
	if err != nil {
		return nil, since, fmt.Errorf("%s: %w", a.path, err)
	}
	//the file shrank, so it is not the file we were reading. replay it and let
	//dedupe absorb what was already delivered.
	if offset > len(records) {
		slog.Warn("file adapter restarting from the top: file has fewer records than the cursor",
			"adapter", a.name, "path", a.path, "cursor", offset, "records", len(records))
		offset = 0
	}

	batch := records[offset:]
	events := make([]event.Event, 0, len(batch))
	var skips []adapter.Skipped
	for _, r := range batch {
		e, err := a.normalize(r)
		if err == nil {
			events = append(events, e)
			continue
		}
		if a.strict {
			return nil, since, fmt.Errorf("%s: %w", a.path, err)
		}
		skips = append(skips, adapter.Skipped{
			Adapter: a.name,
			ID:      adapter.SyntheticID(a.name, r.locator),
			Reason:  err.Error(),
		})
	}

	//report skips only once the poll has succeeded: a poll that trips the ceiling
	//made no progress and will read these again
	if adapter.SkipCeilingExceeded(len(batch), len(skips)) {
		return nil, since, fmt.Errorf("%s: skipped %d of %d records, which is more than this adapter will call noise: %s",
			a.path, len(skips), len(batch), skips[0].Reason)
	}
	for _, s := range skips {
		slog.Warn("skipping unusable record", "adapter", a.name, "path", a.path, "id", s.ID, "reason", s.Reason)
		if a.onSkip != nil {
			a.onSkip(ctx, s)
		}
	}
	return events, adapter.Cursor(strconv.Itoa(len(records))), nil
}

func parseCursor(c adapter.Cursor) (int, error) {
	if c == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(string(c))
	if err != nil || n < 0 {
		return 0, fmt.Errorf("unusable cursor %q: want a record count", c)
	}
	return n, nil
}

// one source row. doc is the record as JSON, raw is what is preserved, where
// names it in prose, locator names it in a synthetic id. err is set when the
// record could not be parsed at all.
type record struct {
	doc     []byte
	raw     json.RawMessage
	where   string
	locator string
	err     error
}

func (a *Adapter) read() ([]record, error) {
	f, err := os.Open(a.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if a.csv {
		return readCSV(f)
	}
	return readJSON(f)
}

// readJSON accepts a top level array or a stream of objects.
func readJSON(r io.Reader) ([]record, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var items []json.RawMessage
	if bytes.HasPrefix(bytes.TrimSpace(b), []byte("[")) {
		if err := json.Unmarshal(b, &items); err != nil {
			return nil, fmt.Errorf("not a JSON array of events: %w", err)
		}
		//a stream cannot be resynchronized after a syntax error, so a broken one is
		//a whole poll failure rather than a skippable record
	} else {
		dec := json.NewDecoder(bytes.NewReader(b))
		var records []record
		for {
			var item json.RawMessage
			if err := dec.Decode(&item); errors.Is(err, io.EOF) {
				return records, nil
			} else if err != nil {
				return nil, fmt.Errorf("record %d: %w", len(records)+1, err)
			}
			line := 1 + bytes.Count(b[:int(dec.InputOffset())-len(item)], []byte("\n"))
			records = append(records, record{
				doc: item, raw: item,
				where:   fmt.Sprintf("line %d", line),
				locator: fmt.Sprintf("line:%d", line),
			})
		}
	}

	records := make([]record, len(items))
	for i, item := range items {
		records[i] = record{
			doc: item, raw: item,
			where:   fmt.Sprintf("record %d (index %d)", i+1, i),
			locator: fmt.Sprintf("index:%d", i),
		}
	}
	return records, nil
}

func readCSV(r io.Reader) ([]record, error) {
	cr := csv.NewReader(r)
	header, err := cr.Read()
	if errors.Is(err, io.EOF) {
		return nil, nil //an empty file is an empty replay, not an error
	}
	if err != nil {
		return nil, err
	}
	for i, h := range header {
		header[i] = strings.TrimSpace(h)
		if header[i] == "" {
			return nil, fmt.Errorf("header column %d has no name", i+1)
		}
	}

	var records []record
	for {
		row, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		//a malformed row is one bad record; the reader carries on to the next
		var parseErr *csv.ParseError
		if errors.As(err, &parseErr) {
			records = append(records, newRecordErr(parseErr.Line, err))
			continue
		}
		if err != nil {
			return nil, err
		}
		line, _ := cr.FieldPos(0)

		doc, err := csvDoc(header, row)
		if err != nil {
			records = append(records, newRecordErr(line, fmt.Errorf("line %d: %w", line, err)))
			continue
		}
		records = append(records, record{
			doc: doc, raw: csvRaw(header, row),
			where:   fmt.Sprintf("line %d", line),
			locator: fmt.Sprintf("line:%d", line),
		})
	}
}

func newRecordErr(line int, err error) record {
	return record{
		where:   fmt.Sprintf("line %d", line),
		locator: fmt.Sprintf("line:%d", line),
		err:     err,
	}
}

// csvDoc turns a row into the JSON object the normalizer reads. typed columns
// become numbers or nested objects, and anything unrecognized ends up in tags.
func csvDoc(header, row []string) ([]byte, error) {
	doc := map[string]json.RawMessage{}
	var lat, lon *string
	for i, name := range header {
		val := strings.TrimSpace(row[i])
		//an empty cell is an absent field, not an empty one
		if val == "" {
			continue //an empty cell is an absent field, not an empty one
		}
		switch {
		case name == "lat":
			lat = &val
		case name == "lon":
			lon = &val
		case csvNumeric[name]:
			if _, err := strconv.ParseFloat(val, 64); err != nil {
				return nil, fmt.Errorf("column %s: %q is not a number", name, val)
			}
			doc[name] = json.RawMessage(val)
		case csvJSON[name]:
			if !json.Valid([]byte(val)) {
				return nil, fmt.Errorf("column %s: %q is not valid JSON", name, val)
			}
			doc[name] = json.RawMessage(val)
		//a raw cell is JSON if it can be and a plain string otherwise: preserving
		//it matters more than its type
		case name == "raw":
			if json.Valid([]byte(val)) {
				doc[name] = json.RawMessage(val)
			} else {
				doc[name] = mustQuote(val)
			}
		default:
			doc[name] = mustQuote(val)
		}
	}
	if (lat == nil) != (lon == nil) {
		return nil, errors.New("columns lat and lon must be given together")
	}
	if lat != nil {
		for name, v := range map[string]string{"lat": *lat, "lon": *lon} {
			if _, err := strconv.ParseFloat(v, 64); err != nil {
				return nil, fmt.Errorf("column %s: %q is not a number", name, v)
			}
		}
		doc["location"] = json.RawMessage(fmt.Sprintf(`{"lat":%s,"lon":%s}`, *lat, *lon))
	}
	return json.Marshal(doc)
}

// csvRaw preserves the row verbatim, every cell as the string it was.
func csvRaw(header, row []string) json.RawMessage {
	cells := make(map[string]string, len(header))
	for i, name := range header {
		cells[name] = row[i]
	}
	b, err := json.Marshal(cells)
	if err != nil { //a map of strings cannot fail to marshal
		panic(err)
	}
	return b
}

func mustQuote(s string) json.RawMessage {
	b, err := json.Marshal(s)
	if err != nil { //a string cannot fail to marshal
		panic(err)
	}
	return b
}

// normalize fills in what the adapter owns, keeps what it does not understand,
// and checks the result.
func (a *Adapter) normalize(r record) (event.Event, error) {
	if r.err != nil {
		return event.Event{}, r.err //already located by whoever read it
	}
	var e event.Event
	if err := json.Unmarshal(r.doc, &e); err != nil {
		return e, fmt.Errorf("%s: %w", r.where, err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(r.doc, &fields); err != nil {
		return e, fmt.Errorf("%s: record is not a JSON object: %w", r.where, err)
	}
	for k, v := range fields {
		if knownFields[k] {
			continue
		}
		//an explicit tag beats an unknown field of the same name
		if _, taken := e.Tags[k]; taken {
			continue //an explicit tag beats an unknown field of the same name
		}
		if e.Tags == nil {
			e.Tags = map[string]string{}
		}
		e.Tags[k] = tagValue(v)
	}

	if e.SchemaVersion == "" {
		e.SchemaVersion = event.SchemaVersion
	}
	if e.Source == "" {
		e.Source = a.name
	}
	if e.SourceType == "" {
		e.SourceType = event.SourceFile
	}
	if e.ReceivedAt.IsZero() {
		e.ReceivedAt = a.now()
	}
	if len(e.Raw) == 0 {
		e.Raw = r.raw
	}
	//the source may have written an offset; the schema says UTC
	e.OccurredAt = e.OccurredAt.UTC()
	e.ReceivedAt = e.ReceivedAt.UTC()

	e.Annotate()
	if err := e.Validate(); err != nil {
		return e, fmt.Errorf("%s: %w", r.where, err)
	}
	return e, nil
}

// tagValue renders an unknown field as a tag. strings keep their text, anything
// else keeps its JSON, so nothing is lost on the way into a string map.
func tagValue(v json.RawMessage) string {
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		return s
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, v); err != nil {
		return string(v)
	}
	return buf.String()
}
