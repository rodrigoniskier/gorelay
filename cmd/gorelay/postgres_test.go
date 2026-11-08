package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func integrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("REQUIRE_POSTGRES_TESTS") == "1" || os.Getenv("CI") == "true" {
			t.Fatal("TEST_DATABASE_URL required: integration tests cannot be skipped")
		}
		t.Skip("TEST_DATABASE_URL not set")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid test DSN")
	}
	admin := sql.OpenDB(stdlib.GetConnector(*cfg))
	t.Cleanup(func() { admin.Close() })
	schema := fmt.Sprintf("gorelay_test_%d", time.Now().UnixNano())
	if _, err = admin.ExecContext(t.Context(), `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), `DROP SCHEMA `+schema+` CASCADE`); err != nil {
			t.Error(err)
		}
	})
	cfg.RuntimeParams["search_path"] = schema
	db := sql.OpenDB(stdlib.GetConnector(*cfg))
	db.SetMaxOpenConns(20)
	t.Cleanup(func() { db.Close() })
	return db
}
func TestPostgresIntegration(t *testing.T) {
	db := integrationDB(t)
	ctx := t.Context()
	if readyDatabase(ctx, db) == nil {
		t.Fatal("unmigrated database ready")
	}
	// Simultaneous migrations must serialize and leave one version.
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() { errs <- migrate(ctx, db) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := readyDatabase(ctx, db); err != nil {
		t.Fatal(err)
	}
	handler := routesWithStore(postgresStore{db: db})
	post := func(key, body string) (int, Event) {
		r := httptest.NewRequest("POST", "http://127.0.0.1/v1/events", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		var e Event
		if w.Code == 200 || w.Code == 201 {
			if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
				t.Errorf("decode event: %v", err)
			}
			if w.Header().Get("Location") != "/v1/events/"+e.ID {
				t.Error("POST missing stable Location")
			}
		}
		return w.Code, e
	}
	body := `{"destination_id":"orders","type":"order.created","payload":{"n":9007199254740993,"ok":true}}`
	type result struct {
		status int
		e      Event
	}
	results := make(chan result, 32)
	for range 32 {
		wg.Go(func() { status, e := post("parallel", body); results <- result{status, e} })
	}
	wg.Wait()
	close(results)
	created := 0
	id := ""
	for r := range results {
		if r.status == 201 {
			created++
		} else if r.status != 200 {
			t.Fatalf("concurrent status %d", r.status)
		}
		if id == "" {
			id = r.e.ID
		}
		if r.e.ID != id || r.e.Status != "pending" || r.e.DestinationID == nil || *r.e.DestinationID != "orders" {
			t.Fatalf("inconsistent event %+v", r.e)
		}
	}
	if created != 1 {
		t.Fatalf("created %d", created)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rows %d", count)
	}
	if status, e := post("parallel", `{"destination_id":"orders","payload":{"ok":true,"n":9007199254740993},"type":"order.created"}`); status != 200 || e.ID != id {
		t.Fatal("canonical replay failed")
	}
	if status, _ := post("parallel", `{"destination_id":"orders","type":"different","payload":{}}`); status != 409 {
		t.Fatalf("conflict %d", status)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "http://127.0.0.1/v1/events/"+id, nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), "9007199254740993") {
		t.Fatalf("GET %d %s", w.Code, w.Body)
	}
	var fetched Event
	if err := json.Unmarshal(w.Body.Bytes(), &fetched); err != nil || fetched.ID != id || fetched.DestinationID == nil || *fetched.DestinationID != "orders" {
		t.Fatalf("GET destination lost: %+v %v", fetched, err)
	}
	missing, _ := newEventID()
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "http://127.0.0.1/v1/events/"+missing, nil))
	if w.Code != 404 {
		t.Fatalf("missing %d", w.Code)
	}
	// All fingerprint components must conflict independently and keep one row.
	for _, body := range []string{
		`{"destination_id":"other","type":"order.created","payload":{"n":9007199254740993,"ok":true}}`,
		`{"destination_id":"orders","type":"other","payload":{"n":9007199254740993,"ok":true}}`,
		`{"destination_id":"orders","type":"order.created","payload":{"n":2,"ok":true}}`,
	} {
		if status, _ := post("parallel", body); status != 409 {
			t.Fatalf("fingerprint conflict %d", status)
		}
	}
	var target string
	if err := db.QueryRowContext(ctx, `SELECT destination_id FROM events WHERE id=$1`, id).Scan(&target); err != nil || target != "orders" {
		t.Fatalf("persisted destination %q %v", target, err)
	}
	// A failed transaction must not reserve the key.
	if _, _, err := (postgresStore{db: db}).Create(ctx, "rollback", eventInput{DestinationID: "orders", Type: "", Payload: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("constraint did not reject invalid type")
	}
	if status, _ := post("rollback", body); status != 201 {
		t.Fatalf("rollback reserved key: %d", status)
	}
	// Valid JSON can exceed PostgreSQL jsonb's Unicode/numeric domain.
	// Rejected input must be a client error and must not reserve its key.
	for _, tc := range []struct{ key, payload string }{
		{"invalid-unicode", `{"s":"\u0000"}`},
		{"invalid-number", `{"n":1e999999}`},
	} {
		t.Run(tc.key, func(t *testing.T) {
			invalidBody := `{"destination_id":"orders","type":"x","payload":` + tc.payload + `}`
			if status, _ := post(tc.key, invalidBody); status != 400 {
				t.Fatalf("invalid jsonb input status %d, want 400", status)
			}
			if status, _ := post(tc.key, body); status != 201 {
				t.Fatalf("rejected jsonb input reserved key: %d", status)
			}
		})
	}
	// Concurrent different content: exactly one winner and all other values conflict.
	results = make(chan result, 16)
	for i := range 16 {
		wg.Go(func() {
			status, e := post("mixed", fmt.Sprintf(`{"destination_id":"orders","type":"x","payload":{"i":%d}}`, i))
			results <- result{status, e}
		})
	}
	wg.Wait()
	close(results)
	created = 0
	for r := range results {
		if r.status == 201 {
			created++
		} else if r.status != 409 {
			t.Fatalf("mixed %d", r.status)
		}
	}
	if created != 1 {
		t.Fatalf("mixed winners %d", created)
	}
	if _, err := db.ExecContext(ctx, `UPDATE schema_migrations SET checksum='tampered'`); err != nil {
		t.Fatal(err)
	}
	if migrate(ctx, db) == nil || readyDatabase(ctx, db) == nil {
		t.Fatal("checksum drift accepted")
	}
	db.Close()
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "http://127.0.0.1/readyz", nil))
	if w.Code != 503 {
		t.Fatalf("closed database ready: %d", w.Code)
	}
}
func TestMigrationFailureRollsBack(t *testing.T) {
	db := integrationDB(t)
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, `CREATE TABLE events (sentinel int)`); err != nil {
		t.Fatal(err)
	}
	if migrate(ctx, db) == nil {
		t.Fatal("migration unexpectedly succeeded")
	}
	var absent bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('schema_migrations') IS NULL`).Scan(&absent); err != nil || !absent {
		t.Fatalf("partial schema persisted %v %v", absent, err)
	}
	if _, err := db.ExecContext(ctx, `SELECT sentinel FROM events`); err != nil {
		t.Fatal("existing table modified", err)
	}
}

func TestDestinationMigrationPreservesLegacy(t *testing.T) {
	db := integrationDB(t)
	ctx := t.Context()
	original, err := migrationFiles.ReadFile("migrations/0001_events.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, string(original)); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `CREATE TABLE schema_migrations(version text PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES('0001_events.sql',$1)`, fmt.Sprintf("%x", sha256.Sum256(original))); err != nil {
		t.Fatal(err)
	}
	id, err := newEventID()
	if err != nil {
		t.Fatal(err)
	}
	legacyHash := sha256.Sum256([]byte(`{"type":"legacy","payload":{"x":1}}`))
	if _, err = db.ExecContext(ctx, `INSERT INTO events(id,idempotency_key,event_type,payload,request_hash) VALUES($1,'legacy-key','legacy','{"x":1}',$2)`, id, legacyHash[:]); err != nil {
		t.Fatal(err)
	}
	var before time.Time
	if err = db.QueryRowContext(ctx, `SELECT created_at FROM events WHERE id=$1`, id).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err = migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err = migrate(ctx, db); err != nil {
		t.Fatal("repeat migration", err)
	}
	var destination *string
	var hash []byte
	var version int
	var after time.Time
	if err = db.QueryRowContext(ctx, `SELECT destination_id,request_hash,fingerprint_version,created_at FROM events WHERE id=$1`, id).Scan(&destination, &hash, &version, &after); err != nil {
		t.Fatal(err)
	}
	if destination != nil || version != 1 || !bytes.Equal(hash, legacyHash[:]) || !before.Equal(after) {
		t.Fatal("legacy row altered")
	}
	store := postgresStore{db: db}
	old, err := store.Get(ctx, id)
	if err != nil || old.ID != id || old.DestinationID != nil {
		t.Fatalf("legacy GET %+v %v", old, err)
	}
	if _, _, err = store.Create(ctx, "legacy-key", eventInput{DestinationID: "orders", Type: "legacy", Payload: json.RawMessage(`{"x":1}`)}); err != errIdempotencyConflict {
		t.Fatalf("legacy replay should conflict: %v", err)
	}
	newEvent, created, err := store.Create(ctx, "new-key", eventInput{DestinationID: "orders", Type: "legacy", Payload: json.RawMessage(`{"x":1}`)})
	if err != nil || !created || newEvent.DestinationID == nil || *newEvent.DestinationID != "orders" {
		t.Fatalf("new event %+v %v", newEvent, err)
	}
	for _, target := range []any{nil, "", "https://example.com", "bad space", "-bad"} {
		candidate, _ := newEventID()
		if _, err = db.ExecContext(ctx, `INSERT INTO events(id,idempotency_key,event_type,payload,request_hash,destination_id) VALUES($1,$2,'x','{}',$3,$4)`, candidate, candidate, legacyHash[:], target); err == nil {
			t.Fatalf("database accepted invalid destination %v", target)
		}
	}
	var count int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM events`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("upgrade rows %d %v", count, err)
	}
}
