package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const maxPolicyRetryDelay = 24 * time.Hour

// RetryPolicy is immutable after construction. MaxAttempts is pinned separately
// by the claim transaction; Decide never substitutes its own budget for that row.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// FailureDecision contains only safe categories and bounded durations. It never
// retains an error string, URL, header text, response body or event payload.
type FailureDecision struct {
	Terminal   bool
	Code       string
	HTTPStatus int
	Delay      time.Duration
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: time.Minute}
}

func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 1 || p.MaxAttempts > 100 {
		return errors.New("maximum attempts must be between 1 and 100")
	}
	if p.BaseDelay < time.Millisecond || p.BaseDelay > time.Hour {
		return errors.New("retry base delay must be between 1ms and 1h")
	}
	if p.MaxDelay < p.BaseDelay || p.MaxDelay > maxPolicyRetryDelay {
		return errors.New("retry maximum delay must be at least base delay and at most 24h")
	}
	return nil
}

func retryPolicyFromEnv() (RetryPolicy, error) {
	p := DefaultRetryPolicy()
	if value := os.Getenv("GORELAY_MAX_ATTEMPTS"); value != "" {
		attempts, err := strconv.Atoi(value)
		if err != nil {
			return p, errors.New("invalid GORELAY_MAX_ATTEMPTS")
		}
		p.MaxAttempts = attempts
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"GORELAY_RETRY_BASE_DELAY", &p.BaseDelay},
		{"GORELAY_RETRY_MAX_DELAY", &p.MaxDelay},
	} {
		if value := os.Getenv(item.name); value != "" {
			delay, err := time.ParseDuration(value)
			if err != nil {
				return p, fmt.Errorf("invalid %s", item.name)
			}
			*item.target = delay
		}
	}
	return p, p.Validate()
}

// Backoff returns equal jitter in [ceiling/2, ceiling], where ceiling is a
// saturating base*2^(attempt-1). Explicit samples make scheduling tests independent
// of time and global random seeds. Callers validate policy before use.
func (p RetryPolicy) Backoff(attempt int64, sample float64) time.Duration {
	if p.BaseDelay <= 0 || p.MaxDelay < p.BaseDelay {
		return 0
	}
	ceiling := p.BaseDelay
	for step := int64(1); step < attempt && ceiling < p.MaxDelay; step++ {
		if ceiling > p.MaxDelay/2 {
			ceiling = p.MaxDelay
		} else {
			ceiling *= 2
		}
	}
	if math.IsNaN(sample) || sample < 0 {
		sample = 0
	} else if sample > 1 {
		sample = 1
	}
	half := ceiling / 2
	delay := half + time.Duration(float64(ceiling-half)*sample)
	if delay > ceiling {
		return ceiling
	}
	return delay
}

// Decide classifies one observed failure. Attempts exhaustion is intentionally a
// SQL decision against the persisted event budget, not this runtime config.
func (p RetryPolicy) Decide(err error, attempt int64, sample float64) FailureDecision {
	decision := FailureDecision{Code: "processor_error"}
	var deliveryErr *HTTPDeliveryError
	retryAfter := time.Duration(0)
	if errors.As(err, &deliveryErr) && deliveryErr != nil {
		switch deliveryErr.Kind {
		case "status":
			status := deliveryErr.StatusCode
			if status >= 100 && status <= 599 {
				decision.HTTPStatus = status
				decision.Code = "http_terminal"
				decision.Terminal = true
				if status == 408 || status == 429 || status >= 500 {
					decision.Code, decision.Terminal = "http_transient", false
				}
				if status == 429 || status == 503 {
					retryAfter = deliveryErr.RetryAfter
				}
			}
		case "timeout", "network", "canceled":
			decision.Code = deliveryErr.Kind
		case "destination_not_configured", "invalid_event", "configuration":
			decision.Code, decision.Terminal = deliveryErr.Kind, true
		}
	} else if errors.Is(err, context.DeadlineExceeded) {
		decision.Code = "timeout"
	} else if errors.Is(err, context.Canceled) {
		decision.Code = "canceled"
	}
	if decision.Terminal {
		return decision
	}
	decision.Delay = p.Backoff(attempt, sample)
	if retryAfter > p.MaxDelay {
		retryAfter = p.MaxDelay
	}
	if retryAfter > decision.Delay {
		decision.Delay = retryAfter
	}
	return decision
}

// parseRetryAfter accepts exactly one bounded HTTP Retry-After field. Invalid,
// repeated, overflowing or past values are ignored; callers retain backoff.
func parseRetryAfter(values []string, now time.Time, limit time.Duration) time.Duration {
	if len(values) != 1 || len(values[0]) > 128 || limit <= 0 {
		return 0
	}
	if limit > maxPolicyRetryDelay {
		limit = maxPolicyRetryDelay
	}
	value := strings.TrimSpace(values[0])
	if value == "" {
		return 0
	}
	digits := true
	for _, c := range value {
		if c < '0' || c > '9' {
			digits = false
			break
		}
	}
	if digits {
		seconds, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0
		}
		if seconds > uint64(limit/time.Second) {
			return limit
		}
		return time.Duration(seconds) * time.Second
	}
	date, err := http.ParseTime(value)
	if err != nil || !date.After(now) {
		return 0
	}
	delay := date.Sub(now)
	if delay > limit {
		return limit
	}
	return delay
}
