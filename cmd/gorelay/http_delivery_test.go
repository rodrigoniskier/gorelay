package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func deliveryWorkerConfig() WorkerConfig {
	return WorkerConfig{Workers: 1, LeaseDuration: 5 * time.Second, PollInterval: 10 * time.Millisecond, DBTimeout: 500 * time.Millisecond}
}
func deliveryProcessor(t *testing.T, address string, cfg WorkerConfig) *HTTPProcessor {
	t.Helper()
	p, err := NewHTTPProcessor(HTTPProcessorConfig{Destinations: map[string]string{"local-fixture": address}, RequestTimeout: time.Second, ConnectTimeout: 100 * time.Millisecond, Worker: cfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}
func localDeliveryServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(handler)
	addr := s.Listener.Addr().(*net.TCPAddr)
	if !addr.IP.IsLoopback() {
		s.Close()
		t.Fatal("test server is not loopback")
	}
	t.Cleanup(s.Close)
	return s
}
func startDeliveryPool(t *testing.T, store leaseStore, p *HTTPProcessor, cfg WorkerConfig, onError func(error)) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	pool, err := NewWorkerPool(store, p.Process, cfg, onError)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); awaitWorkerExit(t, done) })
	return cancel, done
}
func awaitDeliveryState(t *testing.T, db *sql.DB, id, status string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		var current string
		if err := db.QueryRowContext(ctx, `SELECT status FROM events WHERE id=$1`, id).Scan(&current); err != nil {
			t.Fatal(err)
		}
		if current == status {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("event did not reach %s", status)
}
func assertNoDelivery(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	var intact bool
	if err := db.QueryRowContext(t.Context(), `SELECT status='processing' AND delivered_at IS NULL AND processed_at IS NULL AND lease_token IS NOT NULL AND payload='{"value":1}'::jsonb FROM events WHERE id=$1`, id).Scan(&intact); err != nil || !intact {
		t.Fatalf("event lost or falsely confirmed: %v %v", intact, err)
	}
}

func TestHTTPDeliverySuccessAndSQL(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "http-success")
	received := make(chan struct{}, 1)
	var wire map[string]json.RawMessage
	var header string
	server := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("bad HTTP contract")
		}
		header = r.Header.Get(DeliveryIDHeader)
		if err := json.NewDecoder(r.Body).Decode(&wire); err != nil {
			t.Error(err)
		}
		w.WriteHeader(200)
		received <- struct{}{}
	}))
	cfg := deliveryWorkerConfig()
	p := deliveryProcessor(t, server.URL, cfg)
	cancel, done := startDeliveryPool(t, deliveryLeaseStore{postgresStore{db: db}}, p, cfg, func(err error) { t.Errorf("unexpected worker failure: %v", err) })
	awaitWorkerSignal(t, received)
	awaitDeliveryState(t, db, e.ID, "delivered")
	cancel()
	awaitWorkerExit(t, done)
	if header != e.ID || len(wire) != 5 || string(wire["id"]) != fmt.Sprintf("%q", e.ID) || string(wire["destination_id"]) != "\"local-fixture\"" {
		t.Fatal("delivery envelope/header mismatch")
	}
	if _, ok := wire["lease_token"]; ok {
		t.Fatal("ownership token exposed")
	}
	var confirmed bool
	if err := db.QueryRowContext(t.Context(), `SELECT delivered_at IS NOT NULL AND processed_at IS NOT NULL AND lease_token IS NULL AND lease_expires_at IS NULL FROM events WHERE id=$1`, e.ID).Scan(&confirmed); err != nil || !confirmed {
		t.Fatalf("delivery not atomically recorded %v", err)
	}
	if _, err := (postgresStore{db: db}).Claim(t.Context(), time.Second); !errors.Is(err, errNoClaimableEvent) {
		t.Fatalf("delivered event reclaimed %v", err)
	}
	fetched, err := (postgresStore{db: db}).Get(t.Context(), e.ID)
	if err != nil || fetched.Status != "delivered" {
		t.Fatalf("GET delivery status %v", err)
	}
	replay, created, err := (postgresStore{db: db}).Create(t.Context(), "http-success", eventInput{DestinationID: "local-fixture", Type: "fixture", Payload: json.RawMessage(`{"value":1}`)})
	if err != nil || created || replay.ID != e.ID {
		t.Fatalf("delivery changed idempotency %v", err)
	}
}
func TestHTTP500PreservesEvent(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "http500")
	var calls atomic.Int64
	server := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(500)
		w.Write([]byte("sensitive response must not be logged"))
	}))
	cfg := deliveryWorkerConfig()
	p := deliveryProcessor(t, server.URL, cfg)
	failed := make(chan struct{}, 1)
	cancel, done := startDeliveryPool(t, deliveryLeaseStore{postgresStore{db: db}}, p, cfg, func(err error) {
		if strings.Contains(err.Error(), "sensitive response") {
			t.Error("response body exposed")
		}
		select {
		case failed <- struct{}{}:
		default:
		}
	})
	awaitWorkerSignal(t, failed)
	cancel()
	awaitWorkerExit(t, done)
	assertNoDelivery(t, db, e.ID)
	if calls.Load() != 1 {
		t.Fatalf("calls %d", calls.Load())
	}
}
func TestHTTPDeliveryTimeoutAndCancellation(t *testing.T) {
	for _, mode := range []string{"timeout", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			db := leaseDB(t)
			e := insertLeaseEvent(t, db, "http-"+mode)
			received := make(chan struct{}, 1)
			requestCanceled := make(chan struct{}, 1)
			server := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				received <- struct{}{}
				<-r.Context().Done()
				requestCanceled <- struct{}{}
			}))
			cfg := deliveryWorkerConfig()
			p, err := NewHTTPProcessor(HTTPProcessorConfig{Destinations: map[string]string{"local-fixture": server.URL}, RequestTimeout: 150 * time.Millisecond, ConnectTimeout: 50 * time.Millisecond, Worker: cfg})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(p.Close)
			failed := make(chan struct{}, 1)
			cancel, done := startDeliveryPool(t, deliveryLeaseStore{postgresStore{db: db}}, p, cfg, func(error) {
				select {
				case failed <- struct{}{}:
				default:
				}
			})
			awaitWorkerSignal(t, received)
			if mode == "timeout" {
				awaitWorkerSignal(t, failed)
			}
			cancel()
			awaitWorkerExit(t, done)
			awaitWorkerSignal(t, requestCanceled)
			assertNoDelivery(t, db, e.ID)
		})
	}
}
func TestUnconfiguredDestinationAndArbitraryURL(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "unknown-target")
	var called atomic.Int64
	server := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called.Add(1); w.WriteHeader(200) }))
	cfg := deliveryWorkerConfig()
	p, err := NewHTTPProcessor(HTTPProcessorConfig{Destinations: map[string]string{"configured": server.URL}, RequestTimeout: time.Second, ConnectTimeout: 100 * time.Millisecond, Worker: cfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	failed := make(chan struct{}, 1)
	cancel, done := startDeliveryPool(t, deliveryLeaseStore{postgresStore{db: db}}, p, cfg, func(error) {
		select {
		case failed <- struct{}{}:
		default:
		}
	})
	awaitWorkerSignal(t, failed)
	cancel()
	awaitWorkerExit(t, done)
	assertNoDelivery(t, db, e.ID)
	for _, body := range []string{
		fmt.Sprintf(`{"destination_id":%q,"type":"x","payload":{}}`, server.URL),
		fmt.Sprintf(`{"destination_id":"configured","destination_url":%q,"type":"x","payload":{}}`, server.URL),
	} {
		r := httptest.NewRequest("POST", "http://127.0.0.1/v1/events", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "arbitrary")
		w := httptest.NewRecorder()
		routesWithStore(postgresStore{db: db}).ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatalf("arbitrary client URL accepted: %d", w.Code)
		}
	}
	if called.Load() != 0 {
		t.Fatal("unconfigured destination contacted")
	}
}

type observedHTTPStore struct {
	deliveryLeaseStore
	observed chan error
}

func (s observedHTTPStore) Claim(ctx context.Context, d time.Duration) (Lease, error) {
	l, err := s.deliveryLeaseStore.Claim(ctx, d)
	select {
	case s.observed <- err:
	default:
	}
	return l, err
}
func TestTwoHTTPInstancesOneEvent(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "two-http-instances")
	received := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int64
	server := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		calls.Add(1)
		received <- struct{}{}
		select {
		case <-release:
			w.WriteHeader(200)
		case <-r.Context().Done():
		}
	}))
	cfg := deliveryWorkerConfig()
	p := deliveryProcessor(t, server.URL, cfg)
	observed := make(chan error, 32)
	store := observedHTTPStore{deliveryLeaseStore{postgresStore{db: db}}, observed}
	cancel1, done1 := startDeliveryPool(t, store, p, cfg, func(err error) { t.Errorf("instance1 %v", err) })
	cancel2, done2 := startDeliveryPool(t, store, p, cfg, func(err error) { t.Errorf("instance2 %v", err) })
	awaitWorkerSignal(t, received)
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	empty := false
	for !empty {
		select {
		case err := <-observed:
			if errors.Is(err, errNoClaimableEvent) {
				empty = true
			} else if err != nil {
				t.Fatal(err)
			}
		case <-timer.C:
			t.Fatal("other instance did not compete")
		}
	}
	close(release)
	awaitDeliveryState(t, db, e.ID, "delivered")
	cancel1()
	cancel2()
	awaitWorkerExit(t, done1)
	awaitWorkerExit(t, done2)
	if calls.Load() != 1 {
		t.Fatalf("duplicate concurrent HTTP %d", calls.Load())
	}
	var attempts int
	if err := db.QueryRowContext(t.Context(), `SELECT processing_attempts FROM events WHERE id=$1`, e.ID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("claims %d %v", attempts, err)
	}
}

func TestLateHTTPResponseCannotConfirmLostLease(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "late-response")
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	server := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		received <- struct{}{}
		select {
		case <-release:
			w.WriteHeader(200)
		case <-r.Context().Done():
		}
	}))
	cfg := deliveryWorkerConfig()
	p := deliveryProcessor(t, server.URL, cfg)
	store := postgresStore{db: db}
	old, err := store.Claim(t.Context(), cfg.LeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	response := make(chan error, 1)
	go func() { response <- p.Process(t.Context(), old.Event) }()
	awaitWorkerSignal(t, received)
	expireLease(t, db, e.ID)
	replacement, err := store.Claim(t.Context(), cfg.LeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Token == old.Token {
		t.Fatal("lease token not rotated")
	}
	close(release)
	if err = <-response; err != nil {
		t.Fatalf("local response failed %v", err)
	}
	if err = store.CompleteDelivery(t.Context(), e.ID, old.Token); !errors.Is(err, errLeaseLost) {
		t.Fatalf("late HTTP confirmed old lease %v", err)
	}
	assertNoDelivery(t, db, e.ID)
	// Even a current owner must not confirm under an already-canceled context.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err = store.CompleteDelivery(ctx, e.ID, replacement.Token); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled confirmation %v", err)
	}
}
func TestHTTPAcceptedButSQLFailedDuplicatesStableID(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "sql-window")
	var mu sync.Mutex
	var ids []string
	server := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ids = append(ids, r.Header.Get(DeliveryIDHeader))
		mu.Unlock()
		w.WriteHeader(200)
	}))
	if _, err := db.ExecContext(t.Context(), `ALTER TABLE events ADD CONSTRAINT fixture_fail_delivery CHECK (status <> 'delivered')`); err != nil {
		t.Fatal(err)
	}
	cfg := deliveryWorkerConfig()
	p := deliveryProcessor(t, server.URL, cfg)
	failed := make(chan struct{}, 1)
	cancel, done := startDeliveryPool(t, deliveryLeaseStore{postgresStore{db: db}}, p, cfg, func(error) {
		select {
		case failed <- struct{}{}:
		default:
		}
	})
	awaitWorkerSignal(t, failed)
	assertNoDelivery(t, db, e.ID)
	if _, err := db.ExecContext(t.Context(), `ALTER TABLE events DROP CONSTRAINT fixture_fail_delivery`); err != nil {
		t.Fatal(err)
	}
	expireLease(t, db, e.ID)
	awaitDeliveryState(t, db, e.ID, "delivered")
	cancel()
	awaitWorkerExit(t, done)
	mu.Lock()
	got := append([]string(nil), ids...)
	mu.Unlock()
	if len(got) != 2 || got[0] != e.ID || got[1] != e.ID {
		t.Fatalf("stable ID not reused across HTTP/SQL window: attempts %d", len(got))
	}
	var attempts int
	if err := db.QueryRowContext(t.Context(), `SELECT processing_attempts FROM events WHERE id=$1`, e.ID).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("SQL attempts %d %v", attempts, err)
	}
}
func TestMigrationPreservesProcessedWithoutCallingItDelivered(t *testing.T) {
	db := integrationDB(t)
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, `CREATE TABLE schema_migrations(version text PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"0001_events.sql", "0002_event_destination.sql", "0003_processing_leases.sql"} {
		b, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, string(b)); err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES($1,$2)`, name, fmt.Sprintf("%x", sha256.Sum256(b))); err != nil {
			t.Fatal(err)
		}
	}
	id, _ := newEventID()
	if _, err := db.ExecContext(ctx, `INSERT INTO events(id,idempotency_key,event_type,payload,request_hash,destination_id,status,processing_attempts,processed_at) VALUES($1,'old-processed','fixture','{}',$2,'local-fixture','processed',1,clock_timestamp())`, id, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	if err := migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	var preserved bool
	if err := db.QueryRowContext(ctx, `SELECT status='processed' AND delivered_at IS NULL AND processed_at IS NOT NULL FROM events WHERE id=$1`, id).Scan(&preserved); err != nil || !preserved {
		t.Fatalf("processed misclassified %v", err)
	}
	if _, err := (postgresStore{db: db}).Claim(ctx, time.Second); !errors.Is(err, errNoClaimableEvent) {
		t.Fatalf("old processed reclaimed %v", err)
	}
}
func TestRuntimeHTTPDemoActivation(t *testing.T) {
	db := leaseDB(t)
	e := insertLeaseEvent(t, db, "runtime-demo")
	var count atomic.Int64
	server := localDeliveryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { count.Add(1); w.WriteHeader(200) }))
	var schema string
	if err := db.QueryRowContext(t.Context(), `SHOW search_path`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal("DSN parse")
		}
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		dsn = u.String()
	} else {
		dsn += " search_path=" + schema
	}
	config, _ := json.Marshal(map[string]string{"local-fixture": server.URL})
	for k, v := range map[string]string{"DATABASE_URL": dsn, "GORELAY_WORKERS": "1", "GORELAY_HTTP_DEMO": "1", "GORELAY_DESTINATIONS": string(config), "GORELAY_LEASE_DURATION": "5s", "GORELAY_POLL_INTERVAL": "10ms", "GORELAY_WORKER_DB_TIMEOUT": "500ms", "GORELAY_HTTP_TIMEOUT": "1s", "GORELAY_HTTP_CONNECT_TIMEOUT": "100ms"} {
		t.Setenv(k, v)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, "127.0.0.1:0"); close(done) }()
	t.Cleanup(func() { cancel(); awaitWorkerExit(t, done) })
	awaitDeliveryState(t, db, e.ID, "delivered")
	cancel()
	awaitWorkerExit(t, done)
	if count.Load() != 1 {
		t.Fatalf("runtime HTTP calls %d", count.Load())
	}
}
