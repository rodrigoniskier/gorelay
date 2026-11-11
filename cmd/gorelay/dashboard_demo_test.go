package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func dashboardTestDemo(t *testing.T) *dashboardDemo {
	t.Helper()
	c := deliveryWorkerConfig()
	c.LeaseDuration = 30 * time.Second
	d, err := newDashboardDemo(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	return d
}
func dashboardControl(t *testing.T, h http.Handler, d *dashboardDemo, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("POST", "http://127.0.0.1:8080"+path, strings.NewReader(body))
	r.RemoteAddr = "127.0.0.1:12345"
	r.Header.Set("Origin", "http://127.0.0.1:8080")
	r.Header.Set("Content-Type", "application/json")
	if d != nil {
		r.Header.Set("X-GoRelay-CSRF", d.token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestDashboardDemoSecurityAndDisabled(t *testing.T) {
	d := dashboardTestDemo(t)
	mux := http.NewServeMux()
	registerDashboardDemo(mux, nil, d)
	h := dashboardLocalOnly(mux)
	for _, mutate := range []func(*http.Request){
		func(r *http.Request) { r.RemoteAddr = "192.0.2.1:123" },
		func(r *http.Request) { r.Host = "evil.example" },
		func(r *http.Request) { r.Header.Set("Origin", "http://evil.example") },
		func(r *http.Request) { r.Header.Del("Origin") },
		func(r *http.Request) { r.Header.Del("X-GoRelay-CSRF") },
		func(r *http.Request) { r.Header.Add("X-GoRelay-CSRF", d.token) },
		func(r *http.Request) { r.Header.Add("Origin", "http://127.0.0.1:8080") },
		func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") },
	} {
		r := httptest.NewRequest("POST", "http://127.0.0.1:8080/dashboard/api/demo/mode", strings.NewReader(`{"mode":"503"}`))
		r.RemoteAddr = "127.0.0.1:456"
		r.Header.Set("Origin", "http://127.0.0.1:8080")
		r.Header.Set("X-GoRelay-CSRF", d.token)
		r.Header.Set("Content-Type", "application/json")
		mutate(r)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatalf("unsafe request status %d", w.Code)
		}
	}
	for _, mode := range []string{"200", "429", "503", "timeout"} {
		if w := dashboardControl(t, h, d, "/dashboard/api/demo/mode", `{"mode":"`+mode+`"}`); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, body := range []string{`{"mode":"http://evil.example"}`, `{"mode":"200","url":"http://evil.example"}`, `{"mode":"200"} {}`, strings.Repeat("x", 1100)} {
		if w := dashboardControl(t, h, d, "/dashboard/api/demo/mode", body); w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
	disabled := http.NewServeMux()
	registerDashboardDemo(disabled, nil, nil)
	if w := dashboardControl(t, dashboardLocalOnly(disabled), nil, "/dashboard/api/demo/events", `{"count":1}`); w.Code != 404 {
		t.Fatal(w.Code)
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "http://127.0.0.1/dashboard/api/demo", nil)
	r.RemoteAddr = "127.0.0.1:12"
	dashboardLocalOnly(disabled).ServeHTTP(w, r)
	if w.Code != 200 || strings.Contains(w.Body.String(), "csrf_token") || !strings.Contains(w.Body.String(), `"enabled":false`) {
		t.Fatal(w.Code, w.Body.String())
	}
	t.Setenv("GORELAY_WORKERS", "")
	t.Setenv("GORELAY_DASHBOARD_DEMO", "")
	cfg, err := workerConfigFromEnv()
	if err != nil || cfg.Workers != 0 || dashboardEnabled() {
		t.Fatalf("defaults %+v %v", cfg, err)
	}
	if _, err = newDashboardDemo(cfg); err == nil {
		t.Fatal("demo accepted disabled workers")
	}
	t.Setenv("GORELAY_DASHBOARD_DEMO", "1")
	if err = run(context.Background(), "127.0.0.1:0"); err == nil || !strings.Contains(err.Error(), "GORELAY_WORKERS") {
		t.Fatalf("runtime did not fail closed: %v", err)
	}
}
func TestDashboardSyntheticGenerationBoundsAtomicityAndRate(t *testing.T) {
	db := leaseDB(t)
	d := dashboardTestDemo(t)
	mux := http.NewServeMux()
	registerDashboardDemo(mux, db, d)
	h := dashboardLocalOnly(mux)
	for _, body := range []string{`{"count":0}`, `{"count":101}`, `{"count":1,"destination_id":"evil"}`, `{"count":1} {}`} {
		if w := dashboardControl(t, h, d, "/dashboard/api/demo/events", body); w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
	if w := dashboardControl(t, h, d, "/dashboard/api/demo/events", `{"count":100}`); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := dashboardControl(t, h, d, "/dashboard/api/demo/events", `{"count":1}`); w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatal(w.Code)
	}
	var count, valid int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*),count(*) FILTER(WHERE destination_id='dashboard-demo' AND event_type='demo.delivery' AND fingerprint_version=2 AND status='pending' AND processing_attempts=0 AND payload='{"synthetic":true}'::jsonb) FROM events`).Scan(&count, &valid); err != nil || count != 100 || valid != 100 {
		t.Fatalf("generation count%d valid%d %v", count, valid, err)
	}
	// Fail after the first INSERT using a transactional sequence via a trigger;
	// all events in this batch must roll back, including the first insert.
	_, err := db.ExecContext(t.Context(), `CREATE SEQUENCE demo_test_sequence; CREATE FUNCTION demo_test_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF nextval('demo_test_sequence')>1 THEN RAISE EXCEPTION 'test failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER demo_test_reject BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION demo_test_reject()`)
	if err != nil {
		t.Fatal(err)
	}
	if err = generateDemoEvents(t.Context(), db, 3); err == nil {
		t.Fatal("expected generation rollback")
	}
	if err = db.QueryRowContext(t.Context(), `SELECT count(*) FROM events`).Scan(&count); err != nil || count != 100 {
		t.Fatalf("partial batch persisted %d %v", count, err)
	}
}
func TestDashboardLive503RecoveryAndExhaustion(t *testing.T) {
	db := leaseDB(t)
	d := dashboardTestDemo(t)
	mux := http.NewServeMux()
	registerDashboardDemo(mux, db, d)
	h := dashboardLocalOnly(mux)
	if err := generateDemoEvents(t.Context(), db, 1); err != nil {
		t.Fatal(err)
	}
	var id string
	if err := db.QueryRowContext(t.Context(), `SELECT id FROM events`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	store := retryTestStore(db, retryTestPolicy())
	attempt := func() error {
		l, err := store.Claim(t.Context(), 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		err = d.processor.Process(t.Context(), l.Event)
		if err != nil {
			if ferr := store.Fail(t.Context(), l, err); ferr != nil {
				t.Fatal(ferr)
			}
		} else if err = store.Complete(t.Context(), l.Event.ID, l.Token); err != nil {
			t.Fatal(err)
		}
		return err
	}
	dashboardControl(t, h, d, "/dashboard/api/demo/mode", `{"mode":"503"}`)
	if err := attempt(); err == nil {
		t.Fatal("503 succeeded")
	}
	assertRetryCount(t, db, id, 1)
	forceRetryDue(t, db, id)
	dashboardControl(t, h, d, "/dashboard/api/demo/mode", `{"mode":"200"}`)
	if err := attempt(); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRowContext(t.Context(), `SELECT status FROM events WHERE id=$1`, id).Scan(&state); err != nil || state != "delivered" {
		t.Fatal(state, err)
	}
	if err := generateDemoEvents(t.Context(), db, 1); err != nil {
		t.Fatal(err)
	}
	var doomed string
	if err := db.QueryRowContext(t.Context(), `SELECT id FROM events WHERE status='pending'`).Scan(&doomed); err != nil {
		t.Fatal(err)
	}
	dashboardControl(t, h, d, "/dashboard/api/demo/mode", `{"mode":"503"}`)
	for i := range 5 {
		if err := attempt(); err == nil {
			t.Fatal("503 succeeded")
		}
		if i < 4 {
			forceRetryDue(t, db, doomed)
		}
	}
	assertRetryCount(t, db, doomed, 5)
	if err := db.QueryRowContext(t.Context(), `SELECT status FROM events WHERE id=$1`, doomed).Scan(&state); err != nil || state != "dead_letter" {
		t.Fatal(state, err)
	}
	dashboardControl(t, h, d, "/dashboard/api/demo/mode", `{"mode":"200"}`)
	if _, err := store.Claim(t.Context(), 30*time.Second); !errors.Is(err, errNoClaimableEvent) {
		t.Fatalf("DLQ automatically redriven: %v", err)
	}
}

func TestDashboardLive429AndTimeout(t *testing.T) {
	d := dashboardTestDemo(t)
	mux := http.NewServeMux()
	registerDashboardDemo(mux, nil, d)
	h := dashboardLocalOnly(mux)
	id, err := newEventID()
	if err != nil {
		t.Fatal(err)
	}
	destination := dashboardDestination
	event := Event{ID: id, DestinationID: &destination, Type: "demo.delivery", Payload: []byte(`{"synthetic":true}`)}
	dashboardControl(t, h, d, "/dashboard/api/demo/mode", `{"mode":"429"}`)
	err = d.processor.Process(t.Context(), event)
	var deliveryErr *HTTPDeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.StatusCode != 429 || deliveryErr.RetryAfter != time.Second {
		t.Fatalf("429 result %v", err)
	}
	dashboardControl(t, h, d, "/dashboard/api/demo/mode", `{"mode":"timeout"}`)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err = d.processor.Process(ctx, event); err == nil || ctx.Err() == nil {
		t.Fatalf("timeout result %v ctx %v", err, ctx.Err())
	}
}

// The CSRF token is handed to the dashboard page; it must not double as the
// secret path of the built-in recipient.
func TestDashboardCSRFTokenIsNotTheReceiverSecret(t *testing.T) {
	d := dashboardTestDemo(t)
	if d.token == "" || d.receiver == "" || d.token == d.receiver {
		t.Fatal("CSRF token and receiver secret must be distinct and non-empty")
	}
	for id, endpoint := range d.processor.destinations {
		if strings.Contains(endpoint, d.token) || !strings.HasSuffix(endpoint, "/receive/"+d.receiver) {
			t.Fatalf("destination %s does not use the independent receiver secret", id)
		}
	}
}
