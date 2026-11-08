package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDashboardPostgresMetricsAndPrivacy(t *testing.T) {
	db := leaseDB(t)
	ctx := t.Context()
	s := postgresStore{db: db}
	initial, err := readDashboardOverview(ctx, db)
	if err != nil || initial.Total != 0 || initial.SuccessRate != nil || initial.SuccessfulProcessingMS != nil || len(initial.Events) != 0 {
		t.Fatalf("empty %+v %v", initial, err)
	}
	// Construct each category independently with real fenced transitions.
	delivered := insertLeaseEvent(t, db, "dash-delivered")
	l, err := s.Claim(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteDelivery(ctx, l.Event.ID, l.Token); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `UPDATE event_attempts SET started_at=finished_at-interval '25 milliseconds' WHERE event_id=$1`, delivered.ID); err != nil {
		t.Fatal(err)
	}
	insertLeaseEvent(t, db, "dash-dead")
	l, err = s.Claim(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordFailure(ctx, l, FailureDecision{Terminal: true, Code: "http_terminal", HTTPStatus: 400}); err != nil {
		t.Fatal(err)
	}
	retry := insertLeaseEvent(t, db, "dash-retry")
	l, err = s.Claim(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordFailure(ctx, l, FailureDecision{Code: "http_transient", HTTPStatus: 503, Delay: time.Hour}); err != nil {
		t.Fatal(err)
	}
	insertLeaseEvent(t, db, "dash-processed")
	l, err = s.Claim(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(ctx, l.Event.ID, l.Token); err != nil {
		t.Fatal(err)
	}
	active := insertLeaseEvent(t, db, "dash-active")
	if _, err = s.Claim(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	insertLeaseEvent(t, db, "dash-pending")
	o, err := readDashboardOverview(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if o.Total != 6 || o.Pending != 1 || o.Processing != 1 || o.RetryWait != 1 || o.Delivered != 1 || o.DeadLetter != 1 || o.Processed != 1 || o.ActiveLeases != 1 || o.ExpiredLeases != 0 || o.SuccessNumerator != 1 || o.SuccessDenominator != 2 || o.SuccessRate == nil || *o.SuccessRate != 50 || o.SuccessfulProcessingMS == nil || *o.SuccessfulProcessingMS != 25 {
		t.Fatalf("metrics %+v", o)
	}
	expireLease(t, db, active.ID)
	o, err = readDashboardOverview(ctx, db)
	if err != nil || o.ActiveLeases != 0 || o.ExpiredLeases != 1 {
		t.Fatalf("expired %+v %v", o, err)
	}
	mux := http.NewServeMux()
	registerDashboardData(mux, db)
	for _, path := range []string{"/dashboard/api/overview", "/dashboard/api/events/" + retry.ID} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("HTTP %d %s", w.Code, w.Body.String())
		}
		for _, secret := range []string{"payload", "lease_token", "request_hash", "idempotency_key", "\"value\""} {
			if strings.Contains(w.Body.String(), secret) {
				t.Fatalf("private field %s exposed", secret)
			}
		}
	}
	detail, err := readDashboardDetail(ctx, db, retry.ID)
	if err != nil || len(detail.Attempts) != 1 || detail.Attempts[0].Outcome != "retry_scheduled" || detail.Attempts[0].HTTPStatus == nil || *detail.Attempts[0].HTTPStatus != 503 {
		t.Fatalf("detail %+v %v", detail, err)
	}
}
func TestDashboardBoundedListAndHTTPFailures(t *testing.T) {
	db := leaseDB(t)
	for i := range 103 {
		insertLeaseEvent(t, db, fmt.Sprintf("dash-limit-%d", i))
	}
	o, err := readDashboardOverview(t.Context(), db)
	if err != nil || o.Total != 103 || len(o.Events) != 100 || o.SuccessRate != nil {
		t.Fatalf("bounded metrics %+v %v", o, err)
	}
	for i := 1; i < len(o.Events); i++ {
		if o.Events[i].CreatedAt.After(o.Events[i-1].CreatedAt) {
			t.Fatal("unordered list")
		}
	}
	mux := http.NewServeMux()
	registerDashboardData(mux, db)
	missing, _ := newEventID()
	for path, want := range map[string]int{"/dashboard/api/events/invalid": 400, "/dashboard/api/events/" + missing: 404} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != want {
			t.Fatalf("%s status %d", path, w.Code)
		}
	}
	unavailable := http.NewServeMux()
	registerDashboardData(unavailable, nil)
	w := httptest.NewRecorder()
	unavailable.ServeHTTP(w, httptest.NewRequest("GET", "/dashboard/api/overview", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
	var body map[string]any
	if err = json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
}

// Compile-time guard that the data API stays tied to PostgreSQL, rather than a
// synthetic metrics provider or volatile counters.
var _ func(*http.ServeMux, *sql.DB) = registerDashboardData
