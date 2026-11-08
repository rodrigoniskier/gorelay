package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const DeliveryIDHeader = "GoRelay-Delivery-ID"

// HTTPDeliveryError deliberately excludes the destination URL, response body,
// payload and underlying transport text. Its category and status are safe to log.
type HTTPDeliveryError struct {
	Kind       string
	StatusCode int
	RetryAfter time.Duration
	cause      error
}

func (e *HTTPDeliveryError) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("HTTP delivery failed: %s (status %d)", e.Kind, e.StatusCode)
	}
	return "HTTP delivery failed: " + e.Kind
}
func (e *HTTPDeliveryError) Unwrap() error { return e.cause }

type HTTPProcessorConfig struct {
	Destinations   map[string]string
	RequestTimeout time.Duration
	ConnectTimeout time.Duration
	Worker         WorkerConfig
}

type HTTPProcessor struct {
	destinations map[string]string
	client       *http.Client
	transport    *http.Transport
}

func NewHTTPProcessor(cfg HTTPProcessorConfig) (*HTTPProcessor, error) {
	if err := cfg.Worker.validate(); err != nil {
		return nil, err
	}
	if cfg.RequestTimeout <= 0 || cfg.RequestTimeout > time.Minute {
		return nil, errors.New("HTTP request timeout must be positive and at most 1m")
	}
	if cfg.ConnectTimeout <= 0 || cfg.ConnectTimeout > cfg.RequestTimeout {
		return nil, errors.New("HTTP connect timeout must be positive and at most request timeout")
	}
	if cfg.RequestTimeout+2*cfg.Worker.DBTimeout >= cfg.Worker.LeaseDuration {
		return nil, errors.New("HTTP request timeout plus two DB timeouts must be less than lease duration")
	}
	if len(cfg.Destinations) < 1 || len(cfg.Destinations) > 16 {
		return nil, errors.New("HTTP demo requires between 1 and 16 configured destinations")
	}
	destinations := make(map[string]string, len(cfg.Destinations))
	for id, raw := range cfg.Destinations {
		if !validDestinationID(id) {
			return nil, errors.New("invalid configured destination identifier")
		}
		endpoint, err := validateHTTPDestination(raw)
		if err != nil {
			return nil, err
		}
		destinations[id] = endpoint
	}
	dialer := &net.Dialer{Timeout: cfg.ConnectTimeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            localHTTPDialer(dialer),
		DisableCompression:     true,
		MaxIdleConns:           32,
		MaxIdleConnsPerHost:    cfg.Worker.Workers,
		MaxConnsPerHost:        cfg.Worker.Workers,
		IdleConnTimeout:        30 * time.Second,
		ResponseHeaderTimeout:  cfg.RequestTimeout,
		MaxResponseHeaderBytes: 16 << 10,
	}
	return &HTTPProcessor{
		destinations: destinations,
		transport:    transport,
		client: &http.Client{
			Transport: transport,
			Timeout:   cfg.RequestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func validateHTTPDestination(raw string) (string, error) {
	invalid := errors.New("HTTP demo destinations require an HTTP literal loopback IP and explicit nonzero port without credentials, query or fragment")
	if len(raw) == 0 || len(raw) > 2048 || strings.ContainsAny(raw, "?#\\") {
		return "", invalid
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.Opaque != "" || u.User != nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery {
		return "", invalid
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() || !validLocalPort(u.Port()) || strings.TrimLeft(u.Port(), "0") == "" {
		return "", invalid
	}
	return u.String(), nil
}

func localHTTPDialer(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() || !validLocalPort(port) || strings.TrimLeft(port, "0") == "" || (network != "tcp" && network != "tcp4" && network != "tcp6") {
			return nil, errors.New("HTTP demo dial denied")
		}
		return dialer.DialContext(ctx, network, address)
	}
}

func (p *HTTPProcessor) Process(ctx context.Context, event Event) error {
	if err := ctx.Err(); err != nil {
		return &HTTPDeliveryError{Kind: "canceled", cause: err}
	}
	if p == nil || p.client == nil {
		return &HTTPDeliveryError{Kind: "configuration"}
	}
	if event.DestinationID == nil {
		return &HTTPDeliveryError{Kind: "destination_not_configured"}
	}
	endpoint, exists := p.destinations[*event.DestinationID]
	if !exists {
		return &HTTPDeliveryError{Kind: "destination_not_configured"}
	}
	payload := bytes.TrimSpace(event.Payload)
	if !validEventID(event.ID) || !validDestinationID(*event.DestinationID) || strings.TrimSpace(event.Type) == "" || len(event.Type) > 128 || len(payload) == 0 || payload[0] != '{' || !json.Valid(payload) {
		return &HTTPDeliveryError{Kind: "invalid_event"}
	}
	body, err := json.Marshal(struct {
		ID            string          `json:"id"`
		DestinationID string          `json:"destination_id"`
		Type          string          `json:"type"`
		Payload       json.RawMessage `json:"payload"`
		CreatedAt     time.Time       `json:"created_at"`
	}{event.ID, *event.DestinationID, event.Type, payload, event.CreatedAt.UTC()})
	if err != nil {
		return &HTTPDeliveryError{Kind: "invalid_event"}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return &HTTPDeliveryError{Kind: "configuration"}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(DeliveryIDHeader, event.ID)
	response, err := p.client.Do(request)
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		cause := ctx.Err()
		kind := "network"
		var networkErr net.Error
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &networkErr) && networkErr.Timeout()) {
			cause, kind = context.DeadlineExceeded, "timeout"
		} else if errors.Is(err, context.Canceled) || cause != nil {
			kind = "canceled"
		}
		return &HTTPDeliveryError{Kind: kind, cause: cause}
	}
	// Only response headers determine success. Never drain an unbounded body;
	// closing early sacrifices connection reuse rather than extending the lease.
	response.Body.Close()
	if err := ctx.Err(); err != nil {
		return &HTTPDeliveryError{Kind: "canceled", cause: err}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		retryAfter := time.Duration(0)
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusServiceUnavailable {
			retryAfter = parseRetryAfter(response.Header.Values("Retry-After"), time.Now(), maxPolicyRetryDelay)
		}
		return &HTTPDeliveryError{Kind: "status", StatusCode: response.StatusCode, RetryAfter: retryAfter}
	}
	return nil
}

func (p *HTTPProcessor) Close() {
	if p != nil && p.transport != nil {
		p.transport.CloseIdleConnections()
	}
}

func httpProcessorFromEnv(worker WorkerConfig) (*HTTPProcessor, error) {
	if os.Getenv("GORELAY_HTTP_DEMO") != "1" {
		return nil, errors.New("workers enabled without a processor: HTTP demo requires GORELAY_HTTP_DEMO=1")
	}
	raw := os.Getenv("GORELAY_DESTINATIONS")
	if len(raw) == 0 || len(raw) > 32<<10 {
		return nil, errors.New("GORELAY_DESTINATIONS must contain a configured HTTP demo map")
	}
	cfg := HTTPProcessorConfig{Worker: worker, RequestTimeout: 5 * time.Second, ConnectTimeout: time.Second}
	if err := json.Unmarshal([]byte(raw), &cfg.Destinations); err != nil {
		return nil, errors.New("invalid GORELAY_DESTINATIONS map")
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"GORELAY_HTTP_TIMEOUT", &cfg.RequestTimeout},
		{"GORELAY_HTTP_CONNECT_TIMEOUT", &cfg.ConnectTimeout},
	} {
		if value := os.Getenv(item.name); value != "" {
			duration, err := time.ParseDuration(value)
			if err != nil {
				return nil, fmt.Errorf("invalid %s", item.name)
			}
			*item.target = duration
		}
	}
	return NewHTTPProcessor(cfg)
}
