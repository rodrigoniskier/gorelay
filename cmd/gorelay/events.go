package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const maxEventBody = 1 << 20

var errInvalidEvent = errors.New("invalid event")
var errEventNotFound = errors.New("event not found")
var errIdempotencyConflict = errors.New("idempotency conflict")

type Event struct {
	ID            string          `json:"id"`
	DestinationID *string         `json:"destination_id"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
	Status        string          `json:"status"`
	CreatedAt     time.Time       `json:"created_at"`
}
type eventInput struct {
	DestinationID string          `json:"destination_id"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
}
type eventStore interface {
	Create(context.Context, string, eventInput) (Event, bool, error)
	Get(context.Context, string) (Event, error)
	Ready(context.Context) error
}
type postgresStore struct{ db *sql.DB }

// Create commits both the event and its unique idempotency key together. A
// conflicting insert waits for its winner, then the next Read Committed query
// observes that committed row; no separate key reservation can survive rollback.
func (s postgresStore) Create(ctx context.Context, key string, input eventInput) (Event, bool, error) {
	canonical, err := canonicalInput(input)
	if err != nil {
		return Event{}, false, err
	}
	hash := sha256.Sum256(canonical)
	id, err := newEventID()
	if err != nil {
		return Event{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Event{}, false, err
	}
	defer tx.Rollback()
	var event Event
	err = tx.QueryRowContext(ctx, `INSERT INTO events (id,idempotency_key,event_type,payload,request_hash,status,destination_id,fingerprint_version) VALUES ($1,$2,$3,$4,$5,'pending',$6,2) ON CONFLICT (idempotency_key) DO NOTHING RETURNING id,event_type,payload,status,created_at,destination_id`, id, key, input.Type, string(input.Payload), hash[:], input.DestinationID).Scan(&event.ID, &event.Type, &event.Payload, &event.Status, &event.CreatedAt, &event.DestinationID)
	// PostgreSQL jsonb has stricter Unicode and numeric limits than JSON.
	// Only input conversion failures from this INSERT become client errors;
	// connectivity, schema and subsequent query failures remain unavailable.
	var inputError *pgconn.PgError
	if errors.As(err, &inputError) {
		switch inputError.Code {
		case "22P05", "22003", "22021", "22P02":
			return Event{}, false, errInvalidEvent
		}
	}
	created := err == nil
	if errors.Is(err, sql.ErrNoRows) {
		var storedHash []byte
		var version int
		err = tx.QueryRowContext(ctx, `SELECT id,event_type,payload,status,created_at,request_hash,destination_id,fingerprint_version FROM events WHERE idempotency_key=$1`, key).Scan(&event.ID, &event.Type, &event.Payload, &event.Status, &event.CreatedAt, &storedHash, &event.DestinationID, &version)
		if err == nil && (version != 2 || !bytes.Equal(hash[:], storedHash)) {
			return Event{}, false, errIdempotencyConflict
		}
	}
	if err != nil {
		return Event{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return Event{}, false, err
	}
	return event, created, nil
}
func (s postgresStore) Get(ctx context.Context, id string) (Event, error) {
	var e Event
	err := s.db.QueryRowContext(ctx, `SELECT id,event_type,payload,status,created_at,destination_id FROM events WHERE id=$1`, id).Scan(&e.ID, &e.Type, &e.Payload, &e.Status, &e.CreatedAt, &e.DestinationID)
	if errors.Is(err, sql.ErrNoRows) {
		err = errEventNotFound
	}
	return e, err
}
func (s postgresStore) Ready(ctx context.Context) error {
	return readyDatabase(ctx, s.db)
}
func canonicalInput(input eventInput) ([]byte, error) {
	var payload any
	decoder := json.NewDecoder(bytes.NewReader(input.Payload))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Scope         string `json:"scope"`
		DestinationID string `json:"destination_id"`
		Type          string `json:"type"`
		Payload       any    `json:"payload"`
	}{"gorelay:fingerprint:v2:POST:/v1/events", input.DestinationID, input.Type, payload})
}
func newEventID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[:8], h[8:12], h[12:16], h[16:20], h[20:]), nil
}
func validEventID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i, c := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
func eventError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func registerEventRoutes(mux *http.ServeMux, store eventStore) {
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if store == nil || store.Ready(ctx) != nil {
			eventError(w, 503, "not ready")
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /v1/events/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !validEventID(id) {
			eventError(w, 400, "invalid event id")
			return
		}
		if store == nil {
			eventError(w, 503, "storage unavailable")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		event, err := store.Get(ctx, id)
		if errors.Is(err, errEventNotFound) {
			eventError(w, 404, "event not found")
			return
		}
		if err != nil {
			eventError(w, 503, "storage unavailable")
			return
		}
		writeJSON(w, 200, event)
	})
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, r *http.Request) {
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			eventError(w, 415, "Content-Type must be application/json")
			return
		}
		keys := r.Header.Values("Idempotency-Key")
		if len(keys) != 1 || !validIdempotencyKey(keys[0]) {
			eventError(w, 400, "invalid Idempotency-Key")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxEventBody)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var limit *http.MaxBytesError
			if errors.As(err, &limit) {
				eventError(w, 413, "body too large")
			} else {
				eventError(w, 400, "invalid body")
			}
			return
		}
		var input eventInput
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			eventError(w, 400, "invalid event JSON")
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			eventError(w, 400, "expected one JSON value")
			return
		}
		if !validDestinationID(input.DestinationID) {
			eventError(w, 400, "valid destination_id required")
			return
		}
		if strings.TrimSpace(input.Type) == "" || len(input.Type) > 128 || len(input.Payload) == 0 || bytes.TrimSpace(input.Payload)[0] != '{' {
			eventError(w, 400, "type and object payload required")
			return
		}
		if store == nil {
			eventError(w, 503, "storage unavailable")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		event, created, err := store.Create(ctx, keys[0], input)
		if errors.Is(err, errInvalidEvent) {
			eventError(w, 400, "event cannot be represented in storage")
			return
		}
		if errors.Is(err, errIdempotencyConflict) {
			eventError(w, 409, "idempotency key reused with different event")
			return
		}
		if err != nil {
			eventError(w, 503, "storage unavailable")
			return
		}
		w.Header().Set("Location", "/v1/events/"+event.ID)
		status := 200
		if created {
			status = 201
		}
		writeJSON(w, status, event)
	})
}
func validIdempotencyKey(key string) bool {
	if len(key) < 1 || len(key) > 200 {
		return false
	}
	for _, c := range key {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

// Destination IDs are opaque, case-sensitive identifiers, not network addresses.
func validDestinationID(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	for i, c := range id {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		if i > 0 && (c == '.' || c == '_' || c == ':' || c == '-') {
			continue
		}
		return false
	}
	return true
}
