// package store holds fleetnorm's state: adapter cursors, the dedupe set, and
// the audit log. the audit log is the evidence of where the owner's data went,
// so every routing decision lands in it, drops included.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver: no cgo, cross-compiles clean
)

// fixed width so string comparison in SQL matches chronological order.
// RFC3339Nano would not: ".5Z" sorts before "Z", which would corrupt the
// retention sweep. only timestamps this package generates are stored this way;
// event timestamps never reach it.
const tsFormat = "2006-01-02T15:04:05.000Z"

// Status is the outcome of one routing decision for one event and one output.
type Status string

const (
	StatusDelivered Status = "delivered" // the output accepted it
	StatusFailed    Status = "failed"    // retries exhausted, permanent failure
	StatusDropped   Status = "dropped"   // never attempted: buffer full, no rule matched, shutdown
)

// created with IF NOT EXISTS and no migration framework, which is the whole
// story while the tables are new. the first change that is not additive needs a
// versioned migrations table or a documented export and reimport.
const schema = `
CREATE TABLE IF NOT EXISTS cursors (
	adapter    TEXT PRIMARY KEY,
	cursor     TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS seen (
	adapter       TEXT NOT NULL,
	event_id      TEXT NOT NULL,
	first_seen_at TEXT NOT NULL,
	PRIMARY KEY (adapter, event_id)
);
CREATE INDEX IF NOT EXISTS seen_first_seen_at ON seen (first_seen_at);

CREATE TABLE IF NOT EXISTS audit (
	id        INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id  TEXT NOT NULL,
	vin       TEXT NOT NULL,
	output    TEXT NOT NULL,
	status    TEXT NOT NULL,
	attempts  INTEGER NOT NULL,
	routed_at TEXT NOT NULL,
	error     TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS audit_event_id ON audit (event_id);
CREATE INDEX IF NOT EXISTS audit_routed_at ON audit (routed_at);
`

type Store struct {
	db  *sql.DB
	now func() time.Time //swapped in tests
}

// Open opens or creates the database at path and applies the schema.
func Open(path string) (*Store, error) {
	//synchronous=NORMAL under WAL can lose the last commits to a power cut, not to
	//a fleetnorm crash. set FULL here if an audit gap is unacceptable.
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	//ponytail: one connection, so writers queue instead of racing for the write
	//lock. raise it, and handle SQLITE_BUSY, if audit writes ever become the
	//bottleneck.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db, now: func() time.Time { return time.Now().UTC() }}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Ping reports whether the database is usable. /healthz calls it.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Cursor returns an adapter's stored cursor, or "" if it has never run.
func (s *Store) Cursor(ctx context.Context, adapter string) (string, error) {
	var cursor string
	err := s.db.QueryRowContext(ctx, `SELECT cursor FROM cursors WHERE adapter = ?`, adapter).Scan(&cursor)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return cursor, err
}

// SetCursor records how far an adapter has read.
func (s *Store) SetCursor(ctx context.Context, adapter, cursor string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO cursors (adapter, cursor, updated_at) VALUES (?, ?, ?)
		ON CONFLICT (adapter) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at`,
		adapter, cursor, s.now().Format(tsFormat))
	return err
}

// MarkSeen records an event id and reports whether it is new. false means this
// adapter already delivered that id.
func (s *Store) MarkSeen(ctx context.Context, adapter, eventID string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO seen (adapter, event_id, first_seen_at) VALUES (?, ?, ?)
		ON CONFLICT (adapter, event_id) DO NOTHING`,
		adapter, eventID, s.now().Format(tsFormat))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// SweepSeen forgets dedupe entries first seen before cutoff. an event older than
// the window turning up again is delivered again, which is at least once doing
// what it says.
func (s *Store) SweepSeen(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM seen WHERE first_seen_at < ?`, cutoff.UTC().Format(tsFormat))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Record is one routing decision: what happened to one event at one output.
type Record struct {
	EventID  string
	VIN      string
	Output   string //"" when the event was never routed anywhere
	Status   Status
	Attempts int
	RoutedAt time.Time //defaults to now
	Error    string    //why it failed or was dropped
}

// Audit appends a routing decision, written once the outcome is known.
func (s *Store) Audit(ctx context.Context, r Record) error {
	if r.RoutedAt.IsZero() {
		r.RoutedAt = s.now()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO audit (event_id, vin, output, status, attempts, routed_at, error)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		r.EventID, r.VIN, r.Output, string(r.Status), r.Attempts,
		r.RoutedAt.UTC().Format(tsFormat), r.Error)
	return err
}

// AuditFor returns every recorded decision for an event, oldest first.
func (s *Store) AuditFor(ctx context.Context, eventID string) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT event_id, vin, output, status, attempts, routed_at, error
		FROM audit WHERE event_id = ? ORDER BY id`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Record
	for rows.Next() {
		var r Record
		var routedAt string
		if err := rows.Scan(&r.EventID, &r.VIN, &r.Output, &r.Status, &r.Attempts, &routedAt, &r.Error); err != nil {
			return nil, err
		}
		if r.RoutedAt, err = time.Parse(tsFormat, routedAt); err != nil {
			return nil, fmt.Errorf("audit row for %s has unparseable routed_at %q: %w", r.EventID, routedAt, err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
