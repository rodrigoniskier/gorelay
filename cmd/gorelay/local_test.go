package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestValidateLocalAddr(t *testing.T) {
	for _, tc := range []struct {
		addr  string
		valid bool
	}{
		{"127.0.0.1:8080", true},
		{"127.10.20.30:0", true},
		{"[::1]:65535", true},
		{"[::ffff:127.0.0.1]:8080", true},
		{"", false},
		{":8080", false},
		{"0.0.0.0:8080", false},
		{"[::]:8080", false},
		{"192.168.1.10:8080", false},
		{"localhost:8080", false},
		{"127.0.0.1", false},
		{"127.0.0.1:", false},
		{"127.0.0.1:http", false},
		{"127.0.0.1:+80", false},
		{"127.0.0.1:-1", false},
		{"127.0.0.1:65536", false},
		{"127.0.0.1:9999999999999999999999", false},
	} {
		t.Run(tc.addr, func(t *testing.T) {
			if err := validateLocalAddr(tc.addr); (err == nil) != tc.valid {
				t.Fatalf("validateLocalAddr(%q) error=%v, valid=%v", tc.addr, err, tc.valid)
			}
		})
	}
}

func TestLocalOnly(t *testing.T) {
	for _, tc := range []struct {
		host   string
		origin *string
		status int
	}{
		{"127.0.0.1", nil, 204},
		{"127.9.8.7:8080", nil, 204},
		{"[::1]", nil, 204},
		{"[::1]:8080", nil, 204},
		{"localhost:8080", nil, 204},
		{"LOCALHOST", nil, 204},
		{"example.com", nil, 403},
		{"localhost.example.com", nil, 403},
		{"127.0.0.1.example.com", nil, 403},
		{"192.168.1.5:8080", nil, 403},
		{"0.0.0.0:8080", nil, 403},
		{"127.0.0.1:", nil, 403},
		{"127.0.0.1:65536", nil, 403},
		{"user@localhost:8080", nil, 403},
		{"::1", nil, 403},
		{"", nil, 403},
		{"127.0.0.1:8080", localString("https://example.com"), 403},
		{"127.0.0.1:8080", localString("http://127.0.0.1:8080"), 403},
		{"127.0.0.1:8080", localString("null"), 403},
		{"127.0.0.1:8080", localString(""), 403},
	} {
		t.Run(tc.host, func(t *testing.T) {
			calls := 0
			h := localOnly(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(http.StatusNoContent)
			}))
			r := httptest.NewRequest("GET", "http://127.0.0.1/healthz", nil)
			r.Host = tc.host
			if tc.origin != nil {
				r.Header.Set("Origin", *tc.origin)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d want %d", w.Code, tc.status)
			}
			if calls != 0 && tc.status == 403 {
				t.Fatal("rejected request reached downstream handler")
			}
			if calls != 1 && tc.status == 204 {
				t.Fatal("valid request did not reach downstream handler exactly once")
			}
			if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Fatal("missing local API response headers")
			}
		})
	}
}

func localString(value string) *string { return &value }

type localTrapStore struct{ calls int }

func (s *localTrapStore) Create(context.Context, string, eventInput) (Event, bool, error) {
	s.calls++
	return Event{}, false, errors.New("unexpected store access")
}
func (s *localTrapStore) Get(context.Context, string) (Event, error) {
	s.calls++
	return Event{}, errors.New("unexpected store access")
}
func (s *localTrapStore) Ready(context.Context) error {
	s.calls++
	return errors.New("unexpected store access")
}

func TestLocalRestrictionPrecedesStore(t *testing.T) {
	for _, tc := range []struct{ host, origin string }{
		{"rebinding.example:8080", ""},
		{"127.0.0.1:8080", "http://untrusted.example"},
	} {
		s := &localTrapStore{}
		r := httptest.NewRequest("GET", "http://127.0.0.1/readyz", nil)
		r.Host = tc.host
		if tc.origin != "" {
			r.Header.Set("Origin", tc.origin)
		}
		w := httptest.NewRecorder()
		routesWithStore(s).ServeHTTP(w, r)
		if w.Code != http.StatusForbidden || s.calls != 0 {
			t.Fatalf("status=%d store calls=%d", w.Code, s.calls)
		}
	}
}

func TestRunRejectsExternalBindBeforeDatabase(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	if err := run(t.Context(), "0.0.0.0:8080"); err == nil || err.Error() == "DATABASE_URL is required" {
		t.Fatalf("local bind validation did not precede database setup: %v", err)
	}
}
