package main

// Opt-in Stage5B instrumentation. No production configuration is changed.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func save5B(t *testing.T, name string, v any) {
	t.Helper()
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join("/results", name), append(b, '\n'), 0644); e != nil {
		t.Fatal(e)
	}
}

func TestStage5BScalability(t *testing.T) {
	if os.Getenv("GORELAY_5B_RUN") != "1" {
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Second)
	defer cancel()
	runs := []perfRun{}
	for rep := 1; rep <= 3; rep++ {
		for j := 0; j < 3; j++ {
			idx := (j + rep - 1) % 3
			r := runPerfExperiment(ctx, []string{"A", "B", "C"}[idx], 1000, []int{1, 4, 8}[idx], rep)
			runs = append(runs, r)
			save5B(t, "scalability.json", map[string]any{"runs": runs, "order": "A1 B1 C1 B2 C2 A2 C3 A3 B3", "scope": "same PID API, recipient, generator, workers; setup excluded; resource deltas include joining"})
			if !r.Completed || r.AttemptsStarted != 1000 || r.HTTPAttempts != 1000 {
				t.Fatalf("profile accounting failed: %+v", r)
			}
		}
	}
}

func TestStage5BChild(t *testing.T) {
	if os.Getenv("GORELAY_5B_CHILD") != "1" {
		return
	}
	ctx, stop := signalContext5B()
	defer stop()
	if err := run(ctx, "127.0.0.1:18080"); err != nil {
		t.Fatal(err)
	}
}

// A child process receives SIGTERM through this helper; SIGKILL is tested by parent.
func signalContext5B() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

type observation5B struct {
	At          string         `json:"at"`
	Label       string         `json:"label"`
	Counts      map[string]int `json:"counts"`
	Attempts    int            `json:"attempts"`
	Consumed    int            `json:"consumed_attempts"`
	InProgress  int            `json:"in_progress"`
	Outcomes    map[string]int `json:"outcomes"`
	HTTP        int            `json:"http_receipts"`
	Unique      int            `json:"unique_http_events"`
	Connections int            `json:"sql_connections"`
}

func TestStage5BFaults(t *testing.T) {
	if os.Getenv("GORELAY_5B_RUN") != "1" {
		return
	}
	for _, name := range []string{"restart", "abrupt", "database", "http503", "concurrent", "stability"} {
		if selected := os.Getenv("GORELAY_5B_CASE"); selected != "" && selected != name {
			continue
		}
		if !t.Run(name, func(t *testing.T) { runFault5B(t, name) }) {
			t.FailNow()
		}
	}
}

func runFault5B(t *testing.T, name string) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	cfg, e := pgx.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal(e)
	}
	admin := sql.OpenDB(stdlib.GetConnector(*cfg))
	defer admin.Close()
	schema := "gorelay_5b_" + name + os.Getenv("GORELAY_5B_SCHEMA_SUFFIX")
	if _, e = admin.ExecContext(ctx, "CREATE SCHEMA "+schema); e != nil {
		t.Fatal(e)
	}
	cfg.RuntimeParams["search_path"] = schema
	db := sql.OpenDB(stdlib.GetConnector(*cfg))
	defer db.Close()
	db.SetMaxOpenConns(3)
	if e = migrate(ctx, db); e != nil {
		t.Fatal(e)
	}
	var mode atomic.Int32
	var mu sync.Mutex
	receipts := map[string]int{}
	receiptRows := []map[string]any{}
	sent := map[string]time.Time{}
	observations := []observation5B{}
	httpClient := &http.Client{Timeout: 3 * time.Second}
	recipient := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event Event
		if e := json.NewDecoder(r.Body).Decode(&event); e != nil {
			t.Error(e)
			return
		}
		m := mode.Load()
		mu.Lock()
		receipts[event.ID]++
		row := map[string]any{"id": event.ID, "at": time.Now().UTC().Format(time.RFC3339Nano), "mode": m}
		receiptRows = append(receiptRows, row)
		mu.Unlock()
		if m == 1 {
			w.WriteHeader(503)
			return
		}
		if m == 2 {
			time.Sleep(800 * time.Millisecond)
		}
		w.WriteHeader(200)
		mu.Lock()
		row["finished_at"] = time.Now().UTC().Format(time.RFC3339Nano)
		row["client_canceled"] = r.Context().Err() != nil
		mu.Unlock()
	}))
	defer recipient.Close()
	var child *exec.Cmd
	stopChild := func(kill bool) {
		if child == nil {
			return
		}
		if kill {
			e = child.Process.Kill()
		} else {
			e = child.Process.Signal(syscall.SIGTERM)
		}
		if e != nil {
			t.Error(e)
		}
		waitErr := child.Wait()
		if !kill && waitErr != nil {
			t.Error(waitErr)
		}
		child = nil
	}
	defer func() { stopChild(true) }()
	log, e := os.Create("/results/" + name + "-worker.log")
	if e != nil {
		t.Fatal(e)
	}
	defer log.Close()
	startChild := func() {
		child = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStage5BChild$", "-test.v")
		child.Env = append(os.Environ(), "GORELAY_5B_CHILD=1", "DATABASE_URL="+childDSN5B(t, os.Getenv("TEST_DATABASE_URL"), schema), "GORELAY_WORKERS=4", "GORELAY_HTTP_DEMO=1", "GORELAY_DESTINATIONS={\"fault\":\""+recipient.URL+"\"}", "GORELAY_LEASE_DURATION=2s", "GORELAY_WORKER_DB_TIMEOUT=500ms", "GORELAY_HTTP_TIMEOUT=300ms", "GORELAY_HTTP_CONNECT_TIMEOUT=100ms", "GORELAY_POLL_INTERVAL=20ms", "GORELAY_MAX_ATTEMPTS=3", "GORELAY_RETRY_BASE_DELAY=200ms", "GORELAY_RETRY_MAX_DELAY=1s")
		child.Stdout = log
		child.Stderr = log
		if e = child.Start(); e != nil {
			t.Fatal(e)
		}
		until5B(t, ctx, func() bool { return status5B(httpClient, "/readyz") == 200 })
	}
	sample := func(label string) observation5B {
		o := observation5B{At: time.Now().UTC().Format(time.RFC3339Nano), Label: label, Counts: map[string]int{}, Outcomes: map[string]int{}}
		rows, e := db.QueryContext(ctx, "SELECT status,count(*) FROM events GROUP BY status")
		if e != nil {
			t.Fatal(e)
		}
		for rows.Next() {
			var s string
			var n int
			if e = rows.Scan(&s, &n); e != nil {
				t.Fatal(e)
			}
			o.Counts[s] = n
		}
		if e = rows.Err(); e != nil {
			t.Fatal(e)
		}
		rows.Close()
		rows, e = db.QueryContext(ctx, "SELECT outcome,count(*) FROM event_attempts GROUP BY outcome")
		if e != nil {
			t.Fatal(e)
		}
		for rows.Next() {
			var s string
			var n int
			if e = rows.Scan(&s, &n); e != nil {
				t.Fatal(e)
			}
			o.Outcomes[s] = n
			o.Attempts += n
		}
		if e = rows.Err(); e != nil {
			t.Fatal(e)
		}
		rows.Close()
		if e = db.QueryRowContext(ctx, "SELECT coalesce(sum(processing_attempts),0) FROM events").Scan(&o.Consumed); e != nil {
			t.Fatal(e)
		}
		if e = db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()").Scan(&o.Connections); e != nil {
			t.Fatal(e)
		}
		o.InProgress = o.Outcomes["in_progress"]
		mu.Lock()
		o.Unique = len(receipts)
		for _, n := range receipts {
			o.HTTP += n
		}
		mu.Unlock()
		observations = append(observations, o)
		save5B(t, name+".json", map[string]any{"schema": schema, "observations": observations})
		return o
	}
	defer func() { mu.Lock(); defer mu.Unlock(); save5B(t, name+"-receipts.json", receiptRows) }()
	enqueue := func(n int) {
		for i := 0; i < n; i++ {
			body := []byte(`{"destination_id":"fault","type":"synthetic","payload":{"data":"` + strings.Repeat("x", 1013) + `"}}`)
			r, e := http.NewRequestWithContext(ctx, "POST", "http://127.0.0.1:18080/v1/events", bytes.NewReader(body))
			if e != nil {
				t.Fatal(e)
			}
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", fmt.Sprintf("%s-%d", schema, len(sent)))
			at := time.Now()
			resp, e := httpClient.Do(r)
			if e != nil {
				t.Fatal(e)
			}
			var event Event
			e = json.NewDecoder(resp.Body).Decode(&event)
			resp.Body.Close()
			if e != nil || resp.StatusCode != 201 {
				t.Fatalf("enqueue %d %v", resp.StatusCode, e)
			}
			sent[event.ID] = at
		}
	}
	count := func(status string) int {
		var n int
		if e = db.QueryRowContext(ctx, "SELECT count(*) FROM events WHERE status=$1", status).Scan(&n); e != nil {
			t.Fatal(e)
		}
		return n
	}
	recoverAll := func(n int) {
		until5B(t, ctx, func() bool { return count("delivered") == n })
		o := sample("recovered")
		if o.Consumed != o.Attempts || o.InProgress != 0 || o.Unique != n {
			t.Fatalf("reconciliation: %+v", o)
		}
	}
	startChild()
	switch name {
	case "restart", "abrupt":
		mode.Store(2)
		enqueue(12)
		until5B(t, ctx, func() bool { mu.Lock(); defer mu.Unlock(); return len(receipts) >= 4 })
		sample("before_stop")
		stopChild(name == "abrupt")
		sample("stopped")
		mode.Store(0)
		startChild()
		recoverAll(12)
	case "database":
		enqueue(4)
		recoverAll(4)
		mode.Store(2)
		enqueue(12)
		until5B(t, ctx, func() bool { mu.Lock(); defer mu.Unlock(); return len(receipts) >= 8 })
		sample("active_before_outage")
		if e = os.WriteFile("/results/db-stop-request", []byte("stop only gorelay-5b-postgres"), 0644); e != nil {
			t.Fatal(e)
		}
		until5B(t, ctx, func() bool { _, e := os.Stat("/results/db-stopped"); return e == nil })
		status := status5B(httpClient, "/readyz")
		save5B(t, "readiness-outage.json", map[string]any{"status": status, "at": time.Now().UTC().Format(time.RFC3339Nano)})
		if status != 503 {
			t.Fatalf("readiness during outage=%d", status)
		}
		until5B(t, ctx, func() bool { _, e := os.Stat("/results/db-restored"); return e == nil })
		until5B(t, ctx, func() bool { return status5B(httpClient, "/readyz") == 200 })
		mode.Store(0)
		recoverAll(16)
	case "http503":
		mode.Store(1)
		enqueue(4)
		until5B(t, ctx, func() bool { return count("dead_letter") == 4 })
		sample("exhausted_no_redrive")
		enqueue(8)
		until5B(t, ctx, func() bool { return count("retry_wait") > 0 })
		sample("retry_scheduled")
		mode.Store(0)
		until5B(t, ctx, func() bool { return count("delivered") == 8 })
		o := sample("eligible_recovered")
		if o.Counts["dead_letter"] != 4 || o.Consumed != o.Attempts || o.InProgress != 0 {
			t.Fatalf("503 accounting: %+v", o)
		}
	case "concurrent":
		mode.Store(2)
		enqueue(12)
		until5B(t, ctx, func() bool { return count("retry_wait") >= 4 })
		sample("concurrent_timeouts")
		mode.Store(0)
		recoverAll(12)
	case "stability":
		start := time.Now()
		for i := 0; i < 30; i++ {
			enqueue(10)
			sample(fmt.Sprintf("interval_%02d", i))
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(time.Until(start.Add(time.Duration(i+1) * time.Second))):
			}
		}
		recoverAll(300)
	}
	stopChild(false)
	// Join delayed HTTP handlers before exporting receiver completion observations.
	recipient.Close()
	// Direct adversarial stale-token confirmation: expires before a new claimant.
	if name == "abrupt" {
		store := retryDeliveryStore{deliveryLeaseStore{postgresStore{db}}, DefaultRetryPolicy()}
		event, _, e := (postgresStore{db}).Create(ctx, "fencing-probe", eventInput{DestinationID: "fault", Type: "synthetic", Payload: json.RawMessage(`{}`)})
		if e != nil {
			t.Fatal(e)
		}
		old, e := store.Claim(ctx, 100*time.Millisecond)
		if e != nil {
			t.Fatal(e)
		}
		time.Sleep(130 * time.Millisecond)
		newer, e := store.Claim(ctx, time.Second)
		if e != nil {
			t.Fatal(e)
		}
		if old.Event.ID != event.ID || newer.Event.ID != event.ID {
			t.Fatal("unexpected fencing target")
		}
		err := store.Complete(ctx, event.ID, old.Token)
		if !errors.Is(err, errLeaseLost) {
			t.Fatalf("late completion=%v", err)
		}
		if e = store.Complete(ctx, event.ID, newer.Token); e != nil {
			t.Fatal(e)
		}
		save5B(t, "fencing-probe.json", map[string]any{"stale_rejected": true, "old_attempt": old.Attempt, "new_attempt": newer.Attempt, "http_sends": 0, "scope": "direct store probe, separate from HTTP deliveries; new owner confirmation is synthetic"})
		sample("after_direct_fencing_probe")
	}
	// Export retained event and attempt rows, omitting internal lease tokens/payload.
	for label, query := range map[string]string{"events": "SELECT coalesce(json_agg(x),'[]') FROM (SELECT id,status,processing_attempts,created_at,delivered_at,next_attempt_at,last_failure_code,dead_letter_reason FROM events ORDER BY created_at,id) x", "attempts": "SELECT coalesce(json_agg(x),'[]') FROM (SELECT event_id,attempt_number,started_at,finished_at,outcome,failure_code,http_status,next_attempt_at FROM event_attempts ORDER BY event_id,attempt_number) x"} {
		var raw json.RawMessage
		if e = db.QueryRowContext(ctx, query).Scan(&raw); e != nil {
			t.Fatal(e)
		}
		save5B(t, name+"-"+label+".json", raw)
	}
}
func status5B(c *http.Client, path string) int {
	r, e := c.Get("http://127.0.0.1:18080" + path)
	if e != nil {
		return 0
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()
	return r.StatusCode
}
func until5B(t *testing.T, ctx context.Context, f func() bool) {
	t.Helper()
	for !f() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// ConnString returns the original input; RuntimeParams mutations are not serialized.
func childDSN5B(t *testing.T, raw, schema string) string {
	t.Helper()
	u, e := url.Parse(raw)
	if e != nil {
		t.Fatal(e)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}
func TestChildDSN5BSearchPath(t *testing.T) {
	raw := "postgres://synthetic:local@db/synthetic?sslmode=disable"
	cfg, e := pgx.ParseConfig(childDSN5B(t, raw, "isolated_probe"))
	if e != nil {
		t.Fatal(e)
	}
	if cfg.RuntimeParams["search_path"] != "isolated_probe" {
		t.Fatal("child schema omitted")
	}
	if cfg.Host != "db" || cfg.RuntimeParams["sslmode"] != "" {
		t.Fatal("unexpected connection rewrite")
	}
}
