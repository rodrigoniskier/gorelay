package main

// This opt-in experiment measures the combined API, generator and recipient PID.
// Migration and eight API connection warmups occur before the measured window.
// No event is inserted during warmup. Schemas are deliberately retained.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type perfResources struct {
	CPUUser   float64 `json:"cpu_user_seconds"`
	CPUSystem float64 `json:"cpu_system_seconds"`
	RSS       uint64  `json:"sampled_rss_bytes"`
	Heap      uint64  `json:"sampled_heap_alloc_bytes"`
	Open      int     `json:"sampled_open_connections"`
	InUse     int     `json:"sampled_in_use_connections"`
	WaitCount int64   `json:"pool_wait_count"`
	WaitMS    float64 `json:"pool_wait_ms"`
}
type perfRun struct {
	Profile            string         `json:"profile"`
	StartedAt          string         `json:"started_at"`
	FinishedAt         string         `json:"finished_at"`
	PayloadBytes       int            `json:"payload_bytes"`
	APIBodyBytes       int            `json:"api_body_bytes"`
	RecipientBodyBytes []int64        `json:"recipient_body_bytes"`
	Repetition         int            `json:"repetition"`
	Workers            int            `json:"workers"`
	Requested          int            `json:"requested"`
	Accepted           int            `json:"accepted"`
	Delivered          int            `json:"delivered"`
	DeadLetter         int            `json:"dead_letter"`
	HTTPAttempts       int            `json:"http_attempts"`
	AttemptsStarted    int            `json:"attempts_started"`
	Window             float64        `json:"window_seconds"`
	Completed          bool           `json:"completed"`
	MeasurementStarted bool           `json:"measurement_started"`
	Schema             string         `json:"schema"`
	API                []float64      `json:"api_acceptance_ms"`
	APIRequests        []float64      `json:"api_request_ms"`
	HTTPAttemptsMS     []float64      `json:"http_attempt_ms"`
	HTTP               []float64      `json:"http_delivery_ms"`
	E2E                []float64      `json:"end_to_end_ms"`
	Resources          perfResources  `json:"resources"`
	Statuses           map[string]int `json:"status_counts"`
	APIStatuses        map[int]int    `json:"api_status_counts"`
	Errors             map[string]int `json:"error_counts"`
}
type perfStore struct {
	retryDeliveryStore
	mu     sync.Mutex
	ends   map[string]time.Time
	done   chan struct{}
	target int
}

func (s *perfStore) Complete(ctx context.Context, id, token string) error {
	err := s.retryDeliveryStore.Complete(ctx, id, token)
	at := time.Now()
	if err == nil {
		s.mu.Lock()
		s.ends[id] = at
		if len(s.ends) == s.target {
			close(s.done)
		}
		s.mu.Unlock()
	}
	return err
}

func TestPerformanceExperiment(t *testing.T) {
	if os.Getenv("GORELAY_PERF_RUN") != "1" {
		return
	}
	output := os.Getenv("GORELAY_PERF_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("absolute GORELAY_PERF_OUTPUT required")
	}
	if _, err := os.Lstat(output); err == nil {
		t.Fatal("GORELAY_PERF_OUTPUT already exists")
	} else if !os.IsNotExist(err) {
		t.Fatal("GORELAY_PERF_OUTPUT unavailable")
	}
	repeats := 3
	if value := os.Getenv("GORELAY_PERF_REPEATS"); value != "" {
		n, e := strconv.Atoi(value)
		if e != nil || n < 1 || n > 3 {
			t.Fatal("invalid repeats")
		}
		repeats = n
	}
	selected := os.Getenv("GORELAY_PERF_PROFILE")
	if selected != "" && selected != "A" && selected != "B" && selected != "C" {
		t.Fatal("invalid profile")
	}
	report := struct {
		Runs   []perfRun `json:"runs"`
		Scope  string    `json:"resource_scope"`
		Method string    `json:"timing_method"`
	}{Runs: []perfRun{}, Scope: "combined same PID: application + 8 API producers + loopback recipient + test harness; CPU getrusage delta; RSS /proc/self/status and heap/pool gauges sampled maxima at 100ms", Method: "monotonic process clock; start before producer release and worker start; finish last successful fenced Complete return; migration and 8 readyz connection warmups outside window; payload object exactly 1024 bytes; schemas retained; serial order A1,A2,A3,B1,B2,B3,C1,C2,C3; initial idle poll (250ms) included; CPU/pool resource deltas include goroutine joining after final Complete; resource samples initial/final plus 100ms"}
	save := func() {
		data, e := json.MarshalIndent(report, "", "  ")
		if e != nil {
			t.Fatal("encode_results")
		}
		if e = os.WriteFile(output, append(data, '\n'), 0600); e != nil {
			t.Fatal("write_results")
		}
	}
	total, cancel := context.WithTimeout(t.Context(), 300*time.Second)
	defer cancel()
	for _, p := range []struct {
		name string
		n, w int
	}{{"A", 100, 1}, {"B", 500, 4}, {"C", 1000, 8}} {
		if selected != "" && selected != p.name {
			continue
		}
		for rep := 1; rep <= repeats; rep++ {
			r := runPerfExperiment(total, p.name, p.n, p.w, rep)
			report.Runs = append(report.Runs, r)
			save()
			if !r.Completed {
				t.Fatal("performance experiment incomplete; partial results saved")
			}
		}
	}
}

func runPerfExperiment(parent context.Context, profile string, n, workers, rep int) (r perfRun) {
	r = perfRun{Profile: profile, Requested: n, Workers: workers, Repetition: rep, API: []float64{}, HTTP: []float64{}, E2E: []float64{}, Statuses: map[string]int{}, APIStatuses: map[int]int{}, Errors: map[string]int{}}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	fail := func(code string) { r.Errors[code]++ }
	cfg, err := pgx.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil || os.Getenv("TEST_DATABASE_URL") == "" {
		fail("database_configuration")
		return
	}
	cfg.RuntimeParams["application_name"] = "gorelay_performance"
	admin := sql.OpenDB(stdlib.GetConnector(*cfg))
	defer admin.Close()
	id, err := newEventID()
	if err != nil {
		fail("schema_identifier")
		return
	}
	r.Schema = "gorelay_perf_" + strings.ReplaceAll(id, "-", "")
	if _, err = admin.ExecContext(ctx, "CREATE SCHEMA "+r.Schema); err != nil {
		fail("create_schema")
		return
	}
	cfg.RuntimeParams["search_path"] = r.Schema
	db := sql.OpenDB(stdlib.GetConnector(*cfg))
	defer db.Close()
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err = migrate(ctx, db); err != nil {
		fail("migration")
		return
	}
	var mu sync.Mutex
	recipient := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		size, _ := io.Copy(io.Discard, q.Body)
		mu.Lock()
		r.RecipientBodyBytes = append(r.RecipientBodyBytes, size)
		r.HTTPAttempts++
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer recipient.Close()
	api := httptest.NewServer(routesWithStore(postgresStore{db}))
	defer api.Close()
	wc := WorkerConfig{Workers: workers, LeaseDuration: 30 * time.Second, PollInterval: 250 * time.Millisecond, DBTimeout: 5 * time.Second}
	hp, err := NewHTTPProcessor(HTTPProcessorConfig{Destinations: map[string]string{"perf": recipient.URL}, RequestTimeout: 5 * time.Second, ConnectTimeout: time.Second, Worker: wc})
	if err != nil {
		fail("processor_configuration")
		return
	}
	defer hp.Close()
	store := &perfStore{retryDeliveryStore: retryDeliveryStore{deliveryLeaseStore{postgresStore{db}}, DefaultRetryPolicy()}, ends: map[string]time.Time{}, done: make(chan struct{}), target: n}
	process := func(c context.Context, e Event) error {
		start := time.Now()
		err := hp.Process(c, e)
		elapsed := time.Since(start)
		mu.Lock()
		r.HTTPAttemptsMS = append(r.HTTPAttemptsMS, float64(elapsed)/float64(time.Millisecond))
		if err == nil {
			r.HTTP = append(r.HTTP, float64(elapsed)/float64(time.Millisecond))
		} else {
			r.Errors["http_process"]++
		}
		mu.Unlock()
		return err
	}
	pool, err := NewWorkerPool(store, process, wc, func(error) { mu.Lock(); r.Errors["worker_operation"]++; mu.Unlock() })
	if err != nil {
		fail("worker_configuration")
		return
	}
	clients := make([]*http.Client, 8)
	for i := range clients {
		tr := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext, MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1}
		defer tr.CloseIdleConnections()
		clients[i] = &http.Client{Transport: tr, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		q, _ := http.NewRequestWithContext(ctx, "GET", api.URL+"/readyz", nil)
		response, e := clients[i].Do(q)
		if e != nil {
			fail("api_warmup")
			return
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != 200 {
			fail("api_warmup")
			return
		}
	}
	payload := `{"data":"` + strings.Repeat("x", 1013) + `"}`
	if len(payload) != 1024 {
		fail("payload_length")
		return
	}
	body := []byte(`{"destination_id":"perf","type":"synthetic","payload":` + payload + `}`)
	r.PayloadBytes = len(payload)
	r.APIBodyBytes = len(body)
	starts := map[string]time.Time{}
	var producers sync.WaitGroup
	gate := make(chan struct{})
	for producer := range 8 {
		producers.Go(func() {
			<-gate
			for j := producer; j < n; j += 8 {
				q, e := http.NewRequestWithContext(ctx, "POST", api.URL+"/v1/events", bytes.NewReader(body))
				if e != nil {
					mu.Lock()
					r.Errors["api_request"]++
					mu.Unlock()
					continue
				}
				q.Header.Set("Content-Type", "application/json")
				q.Header.Set("Idempotency-Key", fmt.Sprintf("%s-%d", r.Schema, j))
				at := time.Now()
				response, e := clients[producer].Do(q)
				if e != nil {
					mu.Lock()
					r.APIRequests = append(r.APIRequests, float64(time.Since(at))/float64(time.Millisecond))
					r.Errors["api_transport"]++
					mu.Unlock()
					continue
				}
				var event Event
				decode := json.NewDecoder(response.Body).Decode(&event)
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				elapsed := time.Since(at)
				mu.Lock()
				r.APIRequests = append(r.APIRequests, float64(elapsed)/float64(time.Millisecond))
				r.APIStatuses[response.StatusCode]++
				if response.StatusCode == 201 && decode == nil && event.ID != "" {
					r.Accepted++
					r.API = append(r.API, float64(elapsed)/float64(time.Millisecond))
					starts[event.ID] = at
				} else {
					r.Errors["api_response"]++
				}
				mu.Unlock()
			}
		})
	}
	var before, after syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	statsBefore := db.Stats()
	sample := func() {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if m.HeapAlloc > r.Resources.Heap {
			r.Resources.Heap = m.HeapAlloc
		}
		if data, e := os.ReadFile("/proc/self/status"); e == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(line, "VmRSS:") {
					fields := strings.Fields(line)
					if len(fields) > 1 {
						v, _ := strconv.ParseUint(fields[1], 10, 64)
						if v*1024 > r.Resources.RSS {
							r.Resources.RSS = v * 1024
						}
					}
				}
			}
		}
		s := db.Stats()
		if s.OpenConnections > r.Resources.Open {
			r.Resources.Open = s.OpenConnections
		}
		if s.InUse > r.Resources.InUse {
			r.Resources.InUse = s.InUse
		}
	}
	sample()
	sampleDone := make(chan struct{})
	stopSample := make(chan struct{})
	go func() {
		defer close(sampleDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sample()
			case <-stopSample:
				return
			}
		}
	}()
	start := time.Now()
	r.MeasurementStarted = true
	r.StartedAt = start.UTC().Format(time.RFC3339Nano)
	close(gate)
	workerCtx, stopWorkers := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() { pool.Run(workerCtx); close(workerDone) }()
	select {
	case <-store.done:
	case <-ctx.Done():
	}
	stopWorkers()
	<-workerDone
	producers.Wait()
	close(stopSample)
	<-sampleDone
	sample()
	syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	// All goroutines writing observations have joined; completion-before-response
	// races are resolved by joining the two event-ID maps here.
	finish := start
	for id, end := range store.ends {
		if end.After(finish) {
			finish = end
		}
		if at, ok := starts[id]; ok {
			r.E2E = append(r.E2E, float64(end.Sub(at))/float64(time.Millisecond))
		}
	}
	if len(store.ends) != n {
		finish = time.Now()
		fail("timeout_or_incomplete")
	}
	r.Window = finish.Sub(start).Seconds()
	r.FinishedAt = finish.UTC().Format(time.RFC3339Nano)
	r.Resources.CPUUser = float64(after.Utime.Nano()-before.Utime.Nano()) / 1e9
	r.Resources.CPUSystem = float64(after.Stime.Nano()-before.Stime.Nano()) / 1e9
	statsAfter := db.Stats()
	r.Resources.WaitCount = statsAfter.WaitCount - statsBefore.WaitCount
	r.Resources.WaitMS = float64(statsAfter.WaitDuration-statsBefore.WaitDuration) / float64(time.Millisecond)
	// Separate bounded accounting context permits reporting after run cancellation.
	accounting, stopAccounting := context.WithTimeout(parent, 5*time.Second)
	defer stopAccounting()
	rows, e := db.QueryContext(accounting, "SELECT status,count(*) FROM events GROUP BY status")
	if e != nil {
		fail("sql_status_accounting")
	} else {
		for rows.Next() {
			var status string
			var count int
			if rows.Scan(&status, &count) != nil {
				fail("sql_status_scan")
				break
			}
			r.Statuses[status] = count
		}
		if rows.Err() != nil {
			fail("sql_status_rows")
		}
		rows.Close()
	}
	r.Delivered = r.Statuses["delivered"]
	r.DeadLetter = r.Statuses["dead_letter"]
	if db.QueryRowContext(accounting, "SELECT count(*) FROM event_attempts").Scan(&r.AttemptsStarted) != nil {
		fail("sql_attempt_accounting")
	}
	r.Completed = r.Accepted == n && r.Delivered == n && len(store.ends) == n && len(r.E2E) == n && len(r.Errors) == 0
	return
}
