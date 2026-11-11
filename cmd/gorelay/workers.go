package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Processor is an explicit application callback, not a built-in delivery sender.
// It must honor ctx cancellation and be safe for concurrent calls and redelivery.
type Processor func(context.Context, Event) error

type leaseStore interface {
	Claim(context.Context, time.Duration) (Lease, error)
	Complete(context.Context, string, string) error
}

// Retry-aware stores atomically persist a processor failure under lease fencing.
// Legacy generic stores retain expiry recovery; Claim still enforces its budget.
type failureLeaseStore interface {
	Fail(context.Context, Lease, error) error
}

type WorkerConfig struct {
	Workers       int
	LeaseDuration time.Duration
	PollInterval  time.Duration
	DBTimeout     time.Duration
}

func (c WorkerConfig) validate() error {
	if c.Workers < 1 || c.Workers > 32 {
		return errors.New("workers must be between 1 and 32")
	}
	if c.LeaseDuration < 100*time.Millisecond || c.LeaseDuration > 24*time.Hour {
		return errors.New("lease duration must be between 100ms and 24h")
	}
	if c.PollInterval <= 0 || c.PollInterval > time.Minute {
		return errors.New("poll interval must be positive and at most 1m")
	}
	if c.DBTimeout <= 0 || c.DBTimeout > c.LeaseDuration {
		return errors.New("DB timeout must be positive and at most lease duration")
	}
	return nil
}

func workerConfigFromEnv() (WorkerConfig, error) {
	c := WorkerConfig{Workers: 0, LeaseDuration: 30 * time.Second, PollInterval: 250 * time.Millisecond, DBTimeout: 5 * time.Second}
	if value := os.Getenv("GORELAY_WORKERS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 || n > 32 {
			return c, errors.New("GORELAY_WORKERS must be between 0 and 32")
		}
		c.Workers = n
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"GORELAY_LEASE_DURATION", &c.LeaseDuration}, {"GORELAY_POLL_INTERVAL", &c.PollInterval}, {"GORELAY_WORKER_DB_TIMEOUT", &c.DBTimeout},
	} {
		if value := os.Getenv(item.name); value != "" {
			d, err := time.ParseDuration(value)
			if err != nil {
				return c, fmt.Errorf("invalid %s", item.name)
			}
			*item.target = d
		}
	}
	check := c
	if check.Workers == 0 {
		check.Workers = 1
	}
	if err := check.validate(); err != nil {
		return c, err
	}
	return c, nil
}

// defaultShutdownGrace is how long an in-flight callback may keep running after
// the pool is asked to stop, so its outcome can still be recorded.
const defaultShutdownGrace = 8 * time.Second

type WorkerPool struct {
	store   leaseStore
	process Processor
	config  WorkerConfig
	onError func(error)
	// shutdownGrace bounds an in-flight callback after Run's context is canceled.
	shutdownGrace time.Duration
	errorMu       sync.Mutex
	running       atomic.Bool
}

func NewWorkerPool(store leaseStore, process Processor, c WorkerConfig, onError func(error)) (*WorkerPool, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if store == nil || process == nil {
		return nil, errors.New("active workers require a store and an explicit processor")
	}
	return &WorkerPool{store: store, process: process, config: c, onError: onError, shutdownGrace: defaultShutdownGrace}, nil
}

// Run starts exactly config.Workers loops and joins them on cancellation. It can
// be invoked once at a time. No row is queued while another callback is running.
func (p *WorkerPool) Run(ctx context.Context) error {
	if !p.running.CompareAndSwap(false, true) {
		return errors.New("worker pool is already running")
	}
	defer p.running.Store(false)
	var wg sync.WaitGroup
	for range p.config.Workers {
		wg.Go(func() { p.work(ctx) })
	}
	wg.Wait()
	return nil
}
func (p *WorkerPool) report(operation string, err error) {
	p.errorMu.Lock()
	defer p.errorMu.Unlock()
	if p.onError != nil {
		p.onError(fmt.Errorf("%s: %w", operation, err))
	} else {
		slog.Warn("worker operation failed", "operation", operation)
	}
}
func (p *WorkerPool) wait(ctx context.Context) bool {
	timer := time.NewTimer(p.config.PollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
func (p *WorkerPool) work(ctx context.Context) {
	for ctx.Err() == nil {
		claimCtx, cancel := context.WithTimeout(ctx, p.config.DBTimeout)
		lease, err := p.store.Claim(claimCtx, p.config.LeaseDuration)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !errors.Is(err, errNoClaimableEvent) {
				p.report("claim", err)
			}
			if !p.wait(ctx) {
				return
			}
			continue
		}
		// This interval bounds cooperative callbacks. DB wall-clock expiry and token
		// checks remain authoritative even if the host's clock differs or the callback
		// ignores cancellation. No heartbeat or lease extension in this increment.
		// A stop request does not cancel the callback at once: it gets shutdownGrace
		// to finish, and its outcome is recorded with contexts detached from the stop
		// signal, so a graceful shutdown neither burns the attempt nor loses a
		// confirmation for a recipient that already accepted the event.
		processCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), p.config.LeaseDuration)
		finished := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				grace := time.NewTimer(p.shutdownGrace)
				defer grace.Stop()
				select {
				case <-grace.C:
					stop()
				case <-finished:
				}
			case <-finished:
			}
		}()
		err = p.process(processCtx, lease.Event)
		if err == nil {
			err = processCtx.Err()
		}
		close(finished)
		stop()
		detached := context.WithoutCancel(ctx)
		if err != nil {
			if failures, ok := p.store.(failureLeaseStore); ok {
				failureCtx, finishFailure := context.WithTimeout(detached, p.config.DBTimeout)
				failureErr := failures.Fail(failureCtx, lease, err)
				finishFailure()
				if failureErr != nil {
					p.report("record_failure", failureErr)
				}
			}
			p.report("process", err)
			if !p.wait(ctx) {
				return
			}
			continue // lease recovers after expiry
		}
		completeCtx, finish := context.WithTimeout(detached, p.config.DBTimeout)
		err = p.store.Complete(completeCtx, lease.Event.ID, lease.Token)
		finish()
		if err != nil {
			p.report("complete", err)
			if !p.wait(ctx) {
				return
			}
		}
	}
}
