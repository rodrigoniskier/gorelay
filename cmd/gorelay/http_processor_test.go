package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func httpFixtureEvent() Event {
	destination := "demo"
	return Event{ID: "bc08fb73-0ad1-4f71-8bf0-a0458d18a49d", DestinationID: &destination, Type: "order.created", Payload: json.RawMessage(`{"n":9007199254740993}`), Status: "processing", CreatedAt: time.Date(2026, 11, 6, 12, 0, 0, 0, time.FixedZone("fixture", -3*60*60))}
}
func httpFixtureConfig(endpoint string) HTTPProcessorConfig {
	return HTTPProcessorConfig{Destinations: map[string]string{"demo": endpoint}, RequestTimeout: 500 * time.Millisecond, ConnectTimeout: 100 * time.Millisecond, Worker: WorkerConfig{Workers: 2, LeaseDuration: 2 * time.Second, PollInterval: 5 * time.Millisecond, DBTimeout: 100 * time.Millisecond}}
}
func newHTTPFixtureProcessor(t *testing.T, endpoint string) *HTTPProcessor {
	t.Helper()
	processor, err := NewHTTPProcessor(httpFixtureConfig(endpoint))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(processor.Close)
	return processor
}

func TestHTTPProcessorEnvelopeAndStableID(t *testing.T) {
	event := httpFixtureEvent()
	requests := make(chan []byte, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get(DeliveryIDHeader) != event.ID || r.Header.Get("Idempotency-Key") != "" {
			t.Error("unexpected delivery request or identifier")
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
		if err != nil {
			t.Error(err)
		}
		requests <- body
		w.WriteHeader(200)
	}))
	defer server.Close()
	processor := newHTTPFixtureProcessor(t, server.URL+"/events")
	for range 2 {
		if err := processor.Process(t.Context(), event); err != nil {
			t.Fatal(err)
		}
	}
	first, second := <-requests, <-requests
	if string(first) != string(second) {
		t.Fatal("redelivery envelope is not stable")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(first, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 5 || fields["id"] == nil || fields["destination_id"] == nil || fields["type"] == nil || fields["payload"] == nil || fields["created_at"] == nil {
		t.Fatal("unexpected envelope fields")
	}
	if !strings.Contains(string(first), "9007199254740993") || !strings.Contains(string(fields["created_at"]), "Z") {
		t.Fatal("number precision or UTC timestamp lost")
	}
}

func TestHTTPProcessorStatusAndSafeErrors(t *testing.T) {
	for _, status := range []int{200, 204, 299, 300, 400, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				io.WriteString(w, "sensitive-response-must-not-leak")
			}))
			defer server.Close()
			err := newHTTPFixtureProcessor(t, server.URL).Process(t.Context(), httpFixtureEvent())
			if status >= 200 && status < 300 {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var deliveryErr *HTTPDeliveryError
			if !errors.As(err, &deliveryErr) || deliveryErr.Kind != "status" || deliveryErr.StatusCode != status {
				t.Fatalf("status classification: %v", err)
			}
			if strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), server.URL) {
				t.Fatal("delivery error leaked destination or body")
			}
		})
	}
}

func TestHTTPProcessorRedirectDisabled(t *testing.T) {
	var followed atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed.Add(1); w.WriteHeader(200) }))
	defer other.Close()
	for _, location := range []string{other.URL, "http://203.0.113.1:8080/must-never-be-contacted"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Header().Set("Location", location); w.WriteHeader(302) }))
		err := newHTTPFixtureProcessor(t, server.URL).Process(t.Context(), httpFixtureEvent())
		server.Close()
		var deliveryErr *HTTPDeliveryError
		if !errors.As(err, &deliveryErr) || deliveryErr.StatusCode != 302 {
			t.Fatalf("redirect should remain non2xx: %v", err)
		}
	}
	if followed.Load() != 0 {
		t.Fatal("redirect followed")
	}
}

func TestHTTPProcessorTimeoutAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer server.Close()
	cfg := httpFixtureConfig(server.URL)
	cfg.RequestTimeout, cfg.ConnectTimeout = 50*time.Millisecond, 20*time.Millisecond
	processor, err := NewHTTPProcessor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	if err = processor.Process(t.Context(), httpFixtureEvent()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout not classified: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err = processor.Process(ctx, httpFixtureEvent()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation not classified: %v", err)
	}
}

func TestHTTPProcessorCancelInFlight(t *testing.T) {
	started := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		started <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	processor := newHTTPFixtureProcessor(t, server.URL)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- processor.Process(ctx, httpFixtureEvent()) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("request not received")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel in flight: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request did not cancel")
	}
}

func TestHTTPProcessorMissingDestinationAndInvalidEvent(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer server.Close()
	processor := newHTTPFixtureProcessor(t, server.URL)
	for _, change := range []func(*Event){
		func(e *Event) { e.DestinationID = nil },
		func(e *Event) { value := "other"; e.DestinationID = &value },
		func(e *Event) { value := server.URL; e.DestinationID = &value },
		func(e *Event) { e.ID = "bad\r\nHeader" },
		func(e *Event) { e.Payload = json.RawMessage(`null`) },
	} {
		event := httpFixtureEvent()
		change(&event)
		if err := processor.Process(t.Context(), event); err == nil {
			t.Fatal("invalid/unconfigured event accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid/unconfigured event issued a request")
	}
}

func TestHTTPProcessorURLPolicyAndBudget(t *testing.T) {
	for _, endpoint := range []string{
		"", "http://127.0.0.1", "http://127.0.0.1:0", "http://127.0.0.1:65536", "http://127.0.0.1:bad",
		"https://127.0.0.1:8080", "ftp://127.0.0.1:8080", "http://localhost:8080", "http://example.com:8080",
		"http://0.0.0.0:8080", "http://192.168.1.2:8080", "http://[::]:8080", "http://[::1%25lo]:8080",
		"http://127.0.0.1:8080?secret=value", "http://127.0.0.1:8080#part", "http://user:secret@127.0.0.1:8080",
		"http://127.0.0.1:8080?", "http://127.0.0.1:8080#", "http://127.0.0.1:8080\\evil",
	} {
		if _, err := NewHTTPProcessor(httpFixtureConfig(endpoint)); err == nil {
			t.Fatalf("invalid destination accepted: %s", endpoint)
		}
	}
	for _, mutate := range []func(*HTTPProcessorConfig){
		func(c *HTTPProcessorConfig) { c.RequestTimeout = 0 },
		func(c *HTTPProcessorConfig) { c.RequestTimeout = 2 * time.Minute },
		func(c *HTTPProcessorConfig) { c.ConnectTimeout = 0 },
		func(c *HTTPProcessorConfig) { c.ConnectTimeout = c.RequestTimeout + time.Millisecond },
		func(c *HTTPProcessorConfig) { c.RequestTimeout = c.Worker.LeaseDuration - 2*c.Worker.DBTimeout },
		func(c *HTTPProcessorConfig) { c.Destinations = nil },
		func(c *HTTPProcessorConfig) { c.Destinations = map[string]string{"bad space": "http://127.0.0.1:8080"} },
		func(c *HTTPProcessorConfig) { c.Worker.Workers = 0 },
	} {
		cfg := httpFixtureConfig("http://127.0.0.1:8080")
		mutate(&cfg)
		if _, err := NewHTTPProcessor(cfg); err == nil {
			t.Fatal("invalid HTTP budget/config accepted")
		}
	}
	for _, endpoint := range []string{"http://127.0.0.1:8080/events", "http://[::1]:8080/events"} {
		processor, err := NewHTTPProcessor(httpFixtureConfig(endpoint))
		if err != nil {
			t.Fatal(err)
		}
		processor.Close()
	}
}

func TestHTTPProcessorCopiesMapAndIgnoresProxy(t *testing.T) {
	var original, replaced, proxyCalls atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { original.Add(1); w.WriteHeader(200) }))
	defer target.Close()
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { replaced.Add(1); w.WriteHeader(200) }))
	defer other.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { proxyCalls.Add(1); w.WriteHeader(500) }))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("ALL_PROXY", proxy.URL)
	t.Setenv("NO_PROXY", "")
	cfg := httpFixtureConfig(target.URL)
	processor, err := NewHTTPProcessor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	cfg.Destinations["demo"] = other.URL
	if err = processor.Process(t.Context(), httpFixtureEvent()); err != nil {
		t.Fatal(err)
	}
	if original.Load() != 1 || replaced.Load() != 0 || proxyCalls.Load() != 0 || processor.transport.Proxy != nil {
		t.Fatal("destination config mutated or environment proxy used")
	}
}

func TestHTTPProcessorNetworkErrorAndDialGuard(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "http://" + listener.Addr().String()
	listener.Close()
	err = newHTTPFixtureProcessor(t, endpoint).Process(t.Context(), httpFixtureEvent())
	var deliveryErr *HTTPDeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Kind != "network" || strings.Contains(err.Error(), endpoint) {
		t.Fatalf("network error classification/leak: %v", err)
	}
	dial := localHTTPDialer(&net.Dialer{Timeout: time.Second})
	for _, address := range []string{"example.com:80", "203.0.113.1:8080", "0.0.0.0:8080", "127.0.0.1:0"} {
		if conn, err := dial(t.Context(), "tcp", address); err == nil {
			conn.Close()
			t.Fatal("dial guard accepted an unapproved address")
		}
	}
}

func TestHTTPProcessorEnvironment(t *testing.T) {
	for _, name := range []string{"GORELAY_HTTP_DEMO", "GORELAY_DESTINATIONS", "GORELAY_HTTP_TIMEOUT", "GORELAY_HTTP_CONNECT_TIMEOUT"} {
		t.Setenv(name, "")
	}
	worker := httpFixtureConfig("http://127.0.0.1:8080").Worker
	if _, err := httpProcessorFromEnv(worker); err == nil || !strings.Contains(err.Error(), "without a processor") {
		t.Fatal("HTTP demo was not explicit")
	}
	t.Setenv("GORELAY_HTTP_DEMO", "1")
	for _, raw := range []string{"", `not-json`, `[]`, `null`, `{"demo":"http://example.com:8080"}`} {
		t.Setenv("GORELAY_DESTINATIONS", raw)
		if _, err := httpProcessorFromEnv(worker); err == nil {
			t.Fatal("invalid environment destinations accepted")
		}
	}
	t.Setenv("GORELAY_DESTINATIONS", `{"demo":"http://127.0.0.1:8080/events"}`)
	t.Setenv("GORELAY_HTTP_TIMEOUT", "500ms")
	t.Setenv("GORELAY_HTTP_CONNECT_TIMEOUT", "100ms")
	processor, err := httpProcessorFromEnv(worker)
	if err != nil {
		t.Fatal(err)
	}
	processor.Close()
	t.Setenv("GORELAY_HTTP_TIMEOUT", "secret-not-a-duration")
	if _, err = httpProcessorFromEnv(worker); err == nil || strings.Contains(err.Error(), "secret-not-a-duration") {
		t.Fatal("invalid duration accepted or value leaked")
	}
}
