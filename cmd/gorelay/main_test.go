package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRoutes(t *testing.T) {
	handler := routes()
	for _, tc := range []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, "/healthz", http.StatusOK},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed},
		{http.MethodGet, "/healthz/other", http.StatusNotFound},
		{http.MethodGet, "/", http.StatusNotFound},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(tc.method, "http://127.0.0.1"+tc.path, nil))
			if recorder.Code != tc.status {
				t.Fatalf("status = %d, want %d", recorder.Code, tc.status)
			}
			if tc.status == http.StatusOK {
				if got := recorder.Header().Get("Content-Type"); got != "application/json" {
					t.Errorf("Content-Type = %q, want application/json", got)
				}
				if got := recorder.Body.String(); got != "{\"status\":\"ok\"}\n" {
					t.Errorf("body = %q", got)
				}
			}
		})
	}
}

func TestShutdownDrainsActiveRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-release:
			io.WriteString(w, "drained")
		case <-r.Context().Done():
		}
	})}
	defer server.Close()
	shuttingDown := make(chan struct{})
	server.RegisterOnShutdown(func() { close(shuttingDown) })
	stopped := make(chan error, 1)
	go func() { stopped <- serve(ctx, listener, server) }()

	type response struct {
		status int
		body   string
		err    error
	}
	result := make(chan response, 1)
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	go func() {
		res, err := client.Get("http://" + listener.Addr().String())
		if err != nil {
			result <- response{err: err}
			return
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		result <- response{res.StatusCode, string(body), err}
	}()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	select {
	case <-started:
	case <-deadline.C:
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case <-shuttingDown:
	case <-deadline.C:
		t.Fatal("server did not start shutting down")
	}
	select {
	case err := <-stopped:
		t.Fatalf("server stopped before active request finished: %v", err)
	default:
	}
	close(release)
	select {
	case res := <-result:
		if res.err != nil || res.status != http.StatusOK || res.body != "drained" {
			t.Fatalf("active request failed to drain: %+v", res)
		}
	case <-deadline.C:
		t.Fatal("client did not receive a response")
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.C:
		t.Fatal("server did not stop after the request drained")
	}
}
