package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeEventStore struct {
	event   Event
	created bool
	err     error
	calls   int
}

func (s *fakeEventStore) Create(_ context.Context, _ string, _ eventInput) (Event, bool, error) {
	s.calls++
	return s.event, s.created, s.err
}
func (s *fakeEventStore) Get(_ context.Context, _ string) (Event, error) {
	s.calls++
	return s.event, s.err
}
func (s *fakeEventStore) Ready(_ context.Context) error { s.calls++; return s.err }
func testEventHandler(s eventStore) http.Handler {
	mux := http.NewServeMux()
	registerEventRoutes(mux, s)
	return mux
}
func TestPostEventValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body, media, key string
		status                 int
	}{
		{"missing destination", `{"type":"x","payload":{}}`, "application/json", "key", 400},
		{"invalid destination", `{"destination_id":"https://example.com","type":"x","payload":{}}`, "application/json", "key", 400},
		{"valid", `{"destination_id":"orders","type":"order.created","payload":{}}`, "application/json", "key", 201},
		{"charset", `{"destination_id":"orders","type":"order.created","payload":{}}`, "application/json; charset=utf-8", "key", 201},
		{"no media", `{}`, "", "key", 415},
		{"no key", `{}`, "application/json", "", 400},
		{"space key", `{}`, "application/json", "bad key", 400},
		{"long key", `{}`, "application/json", strings.Repeat("a", 201), 400},
		{"unknown", `{"destination_id":"orders","type":"x","payload":{},"extra":1}`, "application/json", "key", 400},
		{"missing type", `{"payload":{}}`, "application/json", "key", 400},
		{"blank type", `{"destination_id":"orders","type":" ","payload":{}}`, "application/json", "key", 400},
		{"long type", `{"destination_id":"orders","type":"` + strings.Repeat("a", 129) + `","payload":{}}`, "application/json", "key", 400},
		{"missing payload", `{"destination_id":"orders","type":"x"}`, "application/json", "key", 400},
		{"null payload", `{"destination_id":"orders","type":"x","payload":null}`, "application/json", "key", 400},
		{"array payload", `{"destination_id":"orders","type":"x","payload":[]}`, "application/json", "key", 400},
		{"trailing", `{"destination_id":"orders","type":"x","payload":{}} {}`, "application/json", "key", 400},
		{"broken", `{"destination_id":"orders","type":`, "application/json", "key", 400},
		{"too large", strings.Repeat(" ", maxEventBody+1), "application/json", "key", 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &fakeEventStore{event: Event{ID: "bc08fb73-0ad1-4f71-8bf0-a0458d18a49d"}, created: true}
			r := httptest.NewRequest("POST", "http://127.0.0.1/v1/events", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", tc.media)
			r.Header.Set("Idempotency-Key", tc.key)
			w := httptest.NewRecorder()
			testEventHandler(s).ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status %d want %d; %s", w.Code, tc.status, w.Body.String())
			}
			if tc.status == 201 {
				if s.calls != 1 {
					t.Fatal("store not called")
				}
				if w.Header().Get("Location") != "/v1/events/"+s.event.ID {
					t.Fatal("missing location")
				}
			} else if s.calls != 0 {
				t.Fatal("invalid request reached store")
			}
		})
	}
}
func TestPostEventStoreResults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"replay", nil, 200}, {"invalid storage input", errInvalidEvent, 400}, {"conflict", errIdempotencyConflict, 409}, {"unavailable", errors.New("database secret"), 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &fakeEventStore{err: tc.err}
			r := httptest.NewRequest("POST", "http://127.0.0.1/v1/events", strings.NewReader(`{"destination_id":"orders","type":"x","payload":{}}`))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", "key")
			w := httptest.NewRecorder()
			testEventHandler(s).ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status %d want %d", w.Code, tc.status)
			}
			if strings.Contains(w.Body.String(), "secret") {
				t.Fatal("leaked DB error")
			}
		})
	}
}
func TestGetEventAndReadiness(t *testing.T) {
	id := "bc08fb73-0ad1-4f71-8bf0-a0458d18a49d"
	for _, tc := range []struct {
		path   string
		err    error
		status int
	}{
		{"/v1/events/" + id, nil, 200}, {"/v1/events/no", nil, 400}, {"/v1/events/" + id, errEventNotFound, 404}, {"/v1/events/" + id, errors.New("offline"), 503}, {"/readyz", nil, 200}, {"/readyz", errors.New("offline"), 503},
	} {
		t.Run(tc.path+http.StatusText(tc.status), func(t *testing.T) {
			s := &fakeEventStore{event: Event{ID: id, Payload: json.RawMessage(`{}`)}, err: tc.err}
			w := httptest.NewRecorder()
			testEventHandler(s).ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
			if w.Code != tc.status {
				t.Fatalf("status %d want %d", w.Code, tc.status)
			}
		})
	}
}
func TestCanonicalInputPreservesNumbersAndSortsKeys(t *testing.T) {
	a, err := canonicalInput(eventInput{Type: "x", Payload: json.RawMessage(`{"z":9007199254740993,"a":{"b":2,"a":1}}`)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicalInput(eventInput{Type: "x", Payload: json.RawMessage(`{"a":{"a":1,"b":2},"z":9007199254740993}`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("different canonical forms %s %s", a, b)
	}
	if !strings.Contains(string(a), "9007199254740993") {
		t.Fatal("large integer lost precision")
	}
}
func TestNewEventID(t *testing.T) {
	a, err := newEventID()
	if err != nil {
		t.Fatal(err)
	}
	b, err := newEventID()
	if err != nil {
		t.Fatal(err)
	}
	if !validEventID(a) || !validEventID(b) || a == b || a[14] != '4' {
		t.Fatalf("invalid UUIDs %s %s", a, b)
	}
}
func TestRepeatedIdempotencyHeaderRejected(t *testing.T) {
	s := &fakeEventStore{}
	r := httptest.NewRequest("POST", "http://127.0.0.1/v1/events", strings.NewReader(`{"destination_id":"orders","type":"x","payload":{}}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Add("Idempotency-Key", "one")
	r.Header.Add("Idempotency-Key", "two")
	w := httptest.NewRecorder()
	testEventHandler(s).ServeHTTP(w, r)
	if w.Code != 400 || s.calls != 0 {
		t.Fatalf("status %d calls %d", w.Code, s.calls)
	}
}
func TestBodyAtLimit(t *testing.T) {
	body := `{"destination_id":"orders","type":"` + strings.Repeat("a", 128) + `","payload":{}}`
	body += strings.Repeat(" ", maxEventBody-len(body))
	s := &fakeEventStore{created: true}
	r := httptest.NewRequest("POST", "http://127.0.0.1/v1/events", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", strings.Repeat("a", 200))
	w := httptest.NewRecorder()
	testEventHandler(s).ServeHTTP(w, r)
	if w.Code != 201 || s.calls != 1 {
		t.Fatalf("status %d calls %d body %s", w.Code, s.calls, w.Body.String())
	}
}

func TestCompleteFingerprint(t *testing.T) {
	original := eventInput{DestinationID: "orders", Type: "order.created", Payload: json.RawMessage(`{"a":1,"b":2}`)}
	base, err := canonicalInput(original)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(base), "gorelay:fingerprint:v2:POST:/v1/events") {
		t.Fatal("missing versioned HTTP scope")
	}
	for _, changed := range []eventInput{
		{DestinationID: "other", Type: original.Type, Payload: original.Payload},
		{DestinationID: original.DestinationID, Type: "other", Payload: original.Payload},
		{DestinationID: original.DestinationID, Type: original.Type, Payload: json.RawMessage(`{"a":2,"b":2}`)},
	} {
		next, err := canonicalInput(changed)
		if err != nil {
			t.Fatal(err)
		}
		if string(next) == string(base) {
			t.Fatal("fingerprint component omitted")
		}
	}
	equivalent := original
	equivalent.Payload = json.RawMessage(`{ "b": 2, "a": 1 }`)
	normalized, err := canonicalInput(equivalent)
	if err != nil {
		t.Fatal(err)
	}
	if string(normalized) != string(base) {
		t.Fatal("reordered keys do not replay")
	}
}
func TestDestinationIDBounds(t *testing.T) {
	for _, tc := range []struct {
		id    string
		valid bool
	}{
		{"a", true}, {strings.Repeat("a", 128), true}, {strings.Repeat("a", 129), false},
		{"orders.prod:v1-tenant_2", true}, {"", false}, {"-x", false}, {"space x", false}, {"á", false}, {"https://host", false},
	} {
		if validDestinationID(tc.id) != tc.valid {
			t.Fatalf("destination %q valid=%v", tc.id, tc.valid)
		}
	}
}
