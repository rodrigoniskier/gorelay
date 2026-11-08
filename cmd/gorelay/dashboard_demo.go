package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

const dashboardDestination = "dashboard-demo"

type dashboardDemo struct {
	mu        sync.Mutex
	mode      string
	token     string
	nextBatch time.Time
	server    *http.Server
	processor *HTTPProcessor
}

func newDashboardDemo(c WorkerConfig) (*dashboardDemo, error) {
	if c.Workers == 0 {
		return nil, errors.New("dashboard demo requires explicit GORELAY_WORKERS > 0")
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, err
	}
	d := &dashboardDemo{mode: "200", token: hex.EncodeToString(secret[:])}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	path := "/receive/" + d.token
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+path, func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() || len(r.Header.Values("Origin")) != 0 {
			http.Error(w, "forbidden", 403)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxEventBody+4096)
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			http.Error(w, "invalid body", 400)
			return
		}
		d.mu.Lock()
		mode := d.mode
		d.mu.Unlock()
		switch mode {
		case "429":
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
		case "503":
			w.WriteHeader(503)
		case "timeout":
			<-r.Context().Done()
		default:
			w.WriteHeader(200)
		}
	})
	d.server = &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 16 << 10}
	d.processor, err = NewHTTPProcessor(HTTPProcessorConfig{Destinations: map[string]string{dashboardDestination: "http://" + listener.Addr().String() + path}, RequestTimeout: 5 * time.Second, ConnectTimeout: time.Second, Worker: c})
	if err != nil {
		listener.Close()
		return nil, err
	}
	go func() { _ = d.server.Serve(listener) }()
	return d, nil
}
func (d *dashboardDemo) Close() {
	if d != nil {
		d.processor.Close()
		d.server.Close()
	}
}

// Dashboard browser access has its own same-origin rules. The original API
// continues using localOnly and does not gain browser mutation access.
func dashboardLocalOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() || !isLocalHost(r.Host) {
			eventError(w, 403, "local dashboard access required")
			return
		}
		origins := r.Header.Values("Origin")
		if len(origins) > 1 || (len(origins) == 1 && origins[0] != "http://"+r.Host) {
			eventError(w, 403, "same origin required")
			return
		}
		site := r.Header.Get("Sec-Fetch-Site")
		if site != "" && site != "same-origin" && site != "none" {
			eventError(w, 403, "same origin required")
			return
		}
		next.ServeHTTP(w, r)
	})
}
func registerDashboardDemo(mux *http.ServeMux, db *sql.DB, d *dashboardDemo) {
	mux.HandleFunc("GET /dashboard/api/demo", func(w http.ResponseWriter, r *http.Request) {
		if d == nil {
			writeJSON(w, 200, map[string]any{"enabled": false})
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		writeJSON(w, 200, map[string]any{"enabled": true, "mode": d.mode, "csrf_token": d.token})
	})
	control := func(fn func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if d == nil {
				http.NotFound(w, r)
				return
			}
			tokens := r.Header.Values("X-GoRelay-CSRF")
			if r.Header.Get("Origin") != "http://"+r.Host || len(r.Header.Values("Origin")) != 1 || len(tokens) != 1 || subtle.ConstantTimeCompare([]byte(tokens[0]), []byte(d.token)) != 1 {
				eventError(w, 403, "demo authorization required")
				return
			}
			if r.Header.Get("Content-Type") != "application/json" {
				eventError(w, 415, "JSON required")
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, 1024)
			fn(w, r)
		}
	}
	decode := func(r *http.Request, v any) bool {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		return dec.Decode(v) == nil && dec.Decode(new(any)) == io.EOF
	}
	mux.HandleFunc("POST /dashboard/api/demo/mode", control(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Mode string `json:"mode"`
		}
		if !decode(r, &in) || (in.Mode != "200" && in.Mode != "429" && in.Mode != "503" && in.Mode != "timeout") {
			eventError(w, 400, "invalid demo mode")
			return
		}
		d.mu.Lock()
		d.mode = in.Mode
		d.mu.Unlock()
		writeJSON(w, 200, map[string]string{"mode": in.Mode})
	}))
	mux.HandleFunc("POST /dashboard/api/demo/events", control(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Count int `json:"count"`
		}
		if !decode(r, &in) || in.Count < 1 || in.Count > 100 {
			eventError(w, 400, "count must be between 1 and 100")
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		if time.Now().Before(d.nextBatch) {
			w.Header().Set("Retry-After", "5")
			eventError(w, 429, "wait before generating another batch")
			return
		}
		d.nextBatch = time.Now().Add(5 * time.Second)
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if db == nil || generateDemoEvents(ctx, db, in.Count) != nil {
			eventError(w, 503, "generation unavailable")
			return
		}
		writeJSON(w, 201, map[string]int{"created": in.Count})
	}))
}
func generateDemoEvents(ctx context.Context, db *sql.DB, count int) error {
	if count < 1 || count > 100 {
		return errors.New("invalid count")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for range count {
		id, err := newEventID()
		if err != nil {
			return err
		}
		// Reuse the canonical Stage2 fingerprint; each synthetic event has a fresh key.
		in := eventInput{DestinationID: dashboardDestination, Type: "demo.delivery", Payload: json.RawMessage(`{"synthetic":true}`)}
		canonical, err := canonicalInput(in)
		if err != nil {
			return err
		}
		hash := sha256.Sum256(canonical)
		_, err = tx.ExecContext(ctx, `INSERT INTO events(id,idempotency_key,event_type,payload,request_hash,status,destination_id,fingerprint_version) VALUES($1,$2,$3,$4,$5,'pending',$6,2)`, id, "demo:"+id, in.Type, string(in.Payload), hash[:], in.DestinationID)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
func dashboardEnabled() bool { return os.Getenv("GORELAY_DASHBOARD_DEMO") == "1" }
