package main

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const shutdownTimeout = 10 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	addr := os.Getenv("GORELAY_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	if err := run(ctx, addr); err != nil {
		slog.Error("GoRelay stopped with an error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, addr string) error {
	return runConfigured(ctx, addr, nil, true)
}

// Generic composition preserves the injected-processor semantics. The normal command
// selects local HTTP delivery only under explicit demo configuration.
func runWithProcessor(ctx context.Context, addr string, process Processor) error {
	return runConfigured(ctx, addr, process, false)
}

func runConfigured(ctx context.Context, addr string, process Processor, enableHTTP bool) error {
	if err := validateLocalAddr(addr); err != nil {
		return err
	}
	workerConfig, err := workerConfigFromEnv()
	if err != nil {
		return err
	}
	retryPolicy, err := retryPolicyFromEnv()
	if err != nil {
		return err
	}
	var demo *dashboardDemo
	if dashboardEnabled() {
		if process != nil || !enableHTTP {
			return errors.New("dashboard demo requires HTTP runtime")
		}
		demo, err = newDashboardDemo(workerConfig)
		if err != nil {
			return err
		}
		defer demo.Close()
		process = demo.processor.Process
	}
	httpDelivery := demo != nil
	if workerConfig.Workers > 0 && process == nil {
		if !enableHTTP {
			return errors.New("workers enabled without a processor")
		}
		processor, processorErr := httpProcessorFromEnv(workerConfig)
		if processorErr != nil {
			return processorErr
		}
		defer processor.Close()
		process = processor.Process
		httpDelivery = true
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	defer db.Close()
	// Every worker may hold a connection for Claim/Complete/Fail at the same
	// time, and the API and dashboard need headroom of their own; a fixed small
	// pool would starve Claim into spurious DB-timeout errors.
	maxConns := max(20, workerConfig.Workers+8)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(min(maxConns, max(5, workerConfig.Workers)))
	db.SetConnMaxLifetime(30 * time.Minute)
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = migrate(startup, db)
	cancel()
	if err != nil {
		return errors.New("database migration failed")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	var workerDone chan error
	if workerConfig.Workers > 0 {
		var store leaseStore = postgresStore{db: db}
		if httpDelivery {
			store = retryDeliveryStore{deliveryLeaseStore{postgresStore{db: db}}, retryPolicy}
		}
		pool, poolErr := NewWorkerPool(store, process, workerConfig, nil)
		if poolErr != nil {
			listener.Close()
			return poolErr
		}
		workerDone = make(chan error, 1)
		go func() { workerDone <- pool.Run(workerCtx) }()
	}
	slog.Info("GoRelay is listening", "address", listener.Addr().String())
	err = serve(ctx, listener, &http.Server{
		Handler:           routesWithDashboard(db, demo),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	})
	stopWorkers()
	if workerDone != nil {
		<-workerDone
	}
	return err
}

func routes() http.Handler {
	return routesWithStore(nil)
}

func routesWithStore(store eventStore) http.Handler {
	mux := http.NewServeMux()
	registerEventRoutes(mux, store)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, "{\"status\":\"ok\"}\n")
	})
	return localOnly(mux)
}

func serve(ctx context.Context, listener net.Listener, server *http.Server) error {
	result := make(chan error, 1)
	go func() {
		result <- server.Serve(listener)
	}()

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			server.Close()
			return err
		}
		err := <-result
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func routesWithDashboard(db *sql.DB, demo *dashboardDemo) http.Handler {
	mux := http.NewServeMux()
	registerDashboardUI(mux)
	registerDashboardData(mux, db)
	registerDashboardDemo(mux, db, demo)
	api := routesWithStore(postgresStore{db: db})
	dashboard := dashboardLocalOnly(mux)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dashboard" || strings.HasPrefix(r.URL.Path, "/dashboard/") {
			dashboard.ServeHTTP(w, r)
			return
		}
		api.ServeHTTP(w, r)
	})
}
