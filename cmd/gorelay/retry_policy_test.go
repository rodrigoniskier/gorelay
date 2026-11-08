package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRetryBackoffExponentialJitterAndCap(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: 5 * time.Second}
	for _, tc := range []struct {
		attempt int64
		ceiling time.Duration
	}{
		{1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second},
		{4, 5 * time.Second}, {100, 5 * time.Second}, {math.MaxInt64, 5 * time.Second},
	} {
		for _, sample := range []float64{0, 0.25, 0.5, 0.999999, 1} {
			delay := p.Backoff(tc.attempt, sample)
			if delay < tc.ceiling/2 || delay > tc.ceiling {
				t.Fatalf("attempt %d sample %g delay %v outside [%v,%v]", tc.attempt, sample, delay, tc.ceiling/2, tc.ceiling)
			}
			if sample == 0 && delay != tc.ceiling/2 || sample == 1 && delay != tc.ceiling {
				t.Fatalf("jitter endpoint attempt %d sample %g delay %v", tc.attempt, sample, delay)
			}
		}
	}
	if got := p.Backoff(3, 0.5); got != 3*time.Second {
		t.Fatalf("manual midpoint %v", got)
	}
	for _, sample := range []float64{-1, math.NaN(), math.Inf(-1)} {
		if got := p.Backoff(1, sample); got != 500*time.Millisecond {
			t.Fatalf("invalid sample lower bound %v", got)
		}
	}
	for _, sample := range []float64{2, math.Inf(1)} {
		if got := p.Backoff(1, sample); got != time.Second {
			t.Fatalf("invalid sample upper bound %v", got)
		}
	}
	large := RetryPolicy{MaxAttempts: 100, BaseDelay: time.Hour, MaxDelay: 24 * time.Hour}
	if got := large.Backoff(math.MaxInt64, 1); got != 24*time.Hour {
		t.Fatalf("saturating backoff overflowed: %v", got)
	}
}

func TestRetryFailureClassificationAndPinnedBudgetSeparation(t *testing.T) {
	p := DefaultRetryPolicy()
	for _, status := range []int{408, 429, 500, 501, 502, 503, 504, 599} {
		d := p.Decide(&HTTPDeliveryError{Kind: "status", StatusCode: status}, 1, 0)
		if d.Terminal || d.Code != "http_transient" || d.HTTPStatus != status || d.Delay != 500*time.Millisecond {
			t.Fatalf("transient status %d decision %+v", status, d)
		}
	}
	for _, status := range []int{300, 301, 302, 307, 308, 400, 401, 403, 404, 405, 409, 422} {
		d := p.Decide(&HTTPDeliveryError{Kind: "status", StatusCode: status, RetryAfter: time.Hour}, 1, 1)
		if !d.Terminal || d.Code != "http_terminal" || d.HTTPStatus != status || d.Delay != 0 {
			t.Fatalf("terminal status %d decision %+v", status, d)
		}
	}
	for _, kind := range []string{"timeout", "network", "canceled"} {
		d := p.Decide(&HTTPDeliveryError{Kind: kind}, 1, 1)
		if d.Terminal || d.Code != kind || d.Delay != time.Second || d.HTTPStatus != 0 {
			t.Fatalf("transient kind %s decision %+v", kind, d)
		}
	}
	for _, kind := range []string{"destination_not_configured", "invalid_event", "configuration"} {
		d := p.Decide(&HTTPDeliveryError{Kind: kind}, 1, 1)
		if !d.Terminal || d.Code != kind || d.Delay != 0 {
			t.Fatalf("terminal kind %s decision %+v", kind, d)
		}
	}
	for _, tc := range []struct {
		err  error
		code string
	}{
		{fmt.Errorf("wrapped: %w", context.DeadlineExceeded), "timeout"},
		{fmt.Errorf("wrapped: %w", context.Canceled), "canceled"},
		{errors.New("do-not-store-sensitive-text"), "processor_error"},
		{&HTTPDeliveryError{Kind: "unknown-sensitive-text"}, "processor_error"},
		{&HTTPDeliveryError{Kind: "status", StatusCode: 999}, "processor_error"},
	} {
		d := p.Decide(tc.err, 2, 0.5)
		if d.Terminal || d.Code != tc.code || d.Delay != 1500*time.Millisecond {
			t.Fatalf("classification decision %+v", d)
		}
		if strings.Contains(d.Code, "sensitive") {
			t.Fatal("unsafe error text retained")
		}
	}
	// SQL uses a pinned row budget. Runtime MaxAttempts must not override it.
	p.MaxAttempts = 1
	d := p.Decide(&HTTPDeliveryError{Kind: "status", StatusCode: 503}, 90, 1)
	if d.Terminal || d.Delay != p.MaxDelay {
		t.Fatal("runtime policy decided SQL exhaustion or failed to cap backoff")
	}
}

func TestRetryAfterValidatedBoundedAndCombined(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		values []string
		want   time.Duration
	}{
		{"seconds", []string{"2"}, 2 * time.Second},
		{"zero", []string{"0"}, 0},
		{"trimmed", []string{" 2 \t"}, 2 * time.Second},
		{"future date", []string{now.Add(4 * time.Second).Format(http.TimeFormat)}, 4 * time.Second},
		{"capped seconds", []string{"60"}, 5 * time.Second},
		{"capped date", []string{now.Add(time.Hour).Format(http.TimeFormat)}, 5 * time.Second},
		{"large safe integer", []string{"18446744073709551615"}, 5 * time.Second},
		{"missing", nil, 0}, {"empty", []string{""}, 0},
		{"negative", []string{"-1"}, 0}, {"signed", []string{"+1"}, 0},
		{"fraction", []string{"0.5"}, 0}, {"bad", []string{"secret-text"}, 0},
		{"overflow", []string{strings.Repeat("9", 40)}, 0},
		{"overlong", []string{strings.Repeat("0", 129)}, 0},
		{"duplicates", []string{"1", "2"}, 0},
		{"past date", []string{now.Add(-time.Second).Format(http.TimeFormat)}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseRetryAfter(tc.values, now, 5*time.Second); got != tc.want {
				t.Fatalf("delay %v want %v", got, tc.want)
			}
		})
	}
	if got := parseRetryAfter([]string{"999999"}, now, 48*time.Hour); got != 24*time.Hour {
		t.Fatalf("hard cap %v", got)
	}
	if got := parseRetryAfter([]string{"1"}, now, 500*time.Millisecond); got != 500*time.Millisecond {
		t.Fatalf("subsecond policy cap %v", got)
	}
	if got := parseRetryAfter([]string{"1"}, now, 0); got != 0 {
		t.Fatalf("invalid cap %v", got)
	}
	p := RetryPolicy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: 5 * time.Second}
	for _, status := range []int{429, 503} {
		if got := p.Decide(&HTTPDeliveryError{Kind: "status", StatusCode: status, RetryAfter: 3 * time.Second}, 1, 0).Delay; got != 3*time.Second {
			t.Fatalf("Retry-After lower bound ignored: %v", got)
		}
		if got := p.Decide(&HTTPDeliveryError{Kind: "status", StatusCode: status, RetryAfter: time.Hour}, 1, 0).Delay; got != p.MaxDelay {
			t.Fatalf("Retry-After cap ignored: %v", got)
		}
		if got := p.Decide(&HTTPDeliveryError{Kind: "status", StatusCode: status, RetryAfter: time.Millisecond}, 3, 1).Delay; got != 4*time.Second {
			t.Fatalf("Retry-After shortened exponential backoff: %v", got)
		}
	}
	if got := p.Decide(&HTTPDeliveryError{Kind: "status", StatusCode: 500, RetryAfter: time.Hour}, 1, 0).Delay; got != 500*time.Millisecond {
		t.Fatalf("unexpected Retry-After for500: %v", got)
	}
}

func TestRetryPolicyEnvironmentAndBounds(t *testing.T) {
	for _, name := range []string{"GORELAY_MAX_ATTEMPTS", "GORELAY_RETRY_BASE_DELAY", "GORELAY_RETRY_MAX_DELAY"} {
		t.Setenv(name, "")
	}
	p, err := retryPolicyFromEnv()
	if err != nil || p != DefaultRetryPolicy() {
		t.Fatalf("defaults %+v %v", p, err)
	}
	for _, tc := range []struct{ name, value string }{
		{"GORELAY_MAX_ATTEMPTS", "0"}, {"GORELAY_MAX_ATTEMPTS", "101"}, {"GORELAY_MAX_ATTEMPTS", "secret-invalid"},
		{"GORELAY_RETRY_BASE_DELAY", "0s"}, {"GORELAY_RETRY_BASE_DELAY", "2h"}, {"GORELAY_RETRY_BASE_DELAY", "secret-invalid"},
		{"GORELAY_RETRY_MAX_DELAY", "1ms"}, {"GORELAY_RETRY_MAX_DELAY", "25h"}, {"GORELAY_RETRY_MAX_DELAY", "secret-invalid"},
	} {
		t.Run(tc.name+tc.value, func(t *testing.T) {
			t.Setenv(tc.name, tc.value)
			_, err := retryPolicyFromEnv()
			if err == nil {
				t.Fatal("invalid policy configuration accepted")
			}
			if strings.Contains(err.Error(), "secret-invalid") {
				t.Fatal("configuration error echoed value")
			}
		})
	}
	t.Setenv("GORELAY_MAX_ATTEMPTS", "2")
	t.Setenv("GORELAY_RETRY_BASE_DELAY", "10ms")
	t.Setenv("GORELAY_RETRY_MAX_DELAY", "100ms")
	p, err = retryPolicyFromEnv()
	if err != nil || p.MaxAttempts != 2 || p.BaseDelay != 10*time.Millisecond || p.MaxDelay != 100*time.Millisecond {
		t.Fatalf("configured %+v %v", p, err)
	}
	for _, p := range []RetryPolicy{
		{1, time.Millisecond, time.Millisecond}, {100, time.Hour, 24 * time.Hour},
	} {
		if err := p.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHTTPRetryAfterOnly429And503(t *testing.T) {
	for _, status := range []int{400, 429, 500, 503} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(status)
		}))
		processor := newHTTPFixtureProcessor(t, server.URL)
		err := processor.Process(t.Context(), httpFixtureEvent())
		server.Close()
		var deliveryErr *HTTPDeliveryError
		if !errors.As(err, &deliveryErr) {
			t.Fatalf("missing status error: %v", err)
		}
		want := time.Duration(0)
		if status == 429 || status == 503 {
			want = 2 * time.Second
		}
		if deliveryErr.RetryAfter != want {
			t.Fatalf("status %d retry hint %v want %v", status, deliveryErr.RetryAfter, want)
		}
	}
}

type policyTimeoutError struct{}

func (policyTimeoutError) Error() string   { return "sensitive-transport-detail" }
func (policyTimeoutError) Timeout() bool   { return true }
func (policyTimeoutError) Temporary() bool { return true }

type policyRoundTripper func(*http.Request) (*http.Response, error)

func (f policyRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHTTPNetworkTimeoutClassificationIsSafe(t *testing.T) {
	processor := newHTTPFixtureProcessor(t, "http://127.0.0.1:8080")
	// A transport-only timeout need not wrap context.DeadlineExceeded. This fake
	// models that branch without relying on slow or external network conditions.
	processor.client.Transport = policyRoundTripper(func(*http.Request) (*http.Response, error) {
		return nil, policyTimeoutError{}
	})
	err := processor.Process(t.Context(), httpFixtureEvent())
	var deliveryErr *HTTPDeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Kind != "timeout" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("network timeout classification: %v", err)
	}
	if strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatal("transport text or URL leaked")
	}
}
