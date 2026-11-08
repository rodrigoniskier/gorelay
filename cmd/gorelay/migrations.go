package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"strings"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrations are forward-only. The lock and all DDL/version records live in one
// transaction, so simultaneous instances cannot apply the same migration twice.
func migrate(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(716239418)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	files, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return err
	}
	var applied int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&applied); err != nil {
		return err
	}
	known := 0
	for _, path := range files {
		content, err := migrationFiles.ReadFile(path)
		if err != nil {
			return err
		}
		version := strings.TrimPrefix(path, "migrations/")
		checksum := fmt.Sprintf("%x", sha256.Sum256(content))
		var stored string
		err = tx.QueryRowContext(ctx, `SELECT checksum FROM schema_migrations WHERE version=$1`, version).Scan(&stored)
		if err == nil {
			known++
			if stored != checksum {
				return fmt.Errorf("migration checksum mismatch: %s", version)
			}
			continue
		}
		if err != sql.ErrNoRows {
			return err
		}
		if _, err = tx.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("migration %s: %w", version, err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES($1,$2)`, version, checksum); err != nil {
			return err
		}
	}
	if known != applied {
		return fmt.Errorf("database has unknown migrations")
	}
	return tx.Commit()
}
func readyDatabase(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	files, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return err
	}
	var count int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil {
		return err
	}
	if count != len(files) {
		return fmt.Errorf("schema version mismatch")
	}
	for _, path := range files {
		content, err := migrationFiles.ReadFile(path)
		if err != nil {
			return err
		}
		var checksum string
		if err = db.QueryRowContext(ctx, `SELECT checksum FROM schema_migrations WHERE version=$1`, strings.TrimPrefix(path, "migrations/")).Scan(&checksum); err != nil {
			return err
		}
		if checksum != fmt.Sprintf("%x", sha256.Sum256(content)) {
			return fmt.Errorf("schema checksum mismatch")
		}
	}
	rows, err := db.QueryContext(ctx, `SELECT id,idempotency_key,event_type,payload,request_hash,status,created_at,destination_id,fingerprint_version,lease_token,lease_expires_at,processing_attempts,processed_at,delivered_at,next_attempt_at,retry_max_attempts,last_failure_code,dead_letter_at,dead_letter_reason FROM events LIMIT 0`)
	if err != nil {
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}
	rows, err = db.QueryContext(ctx, `SELECT event_id,attempt_number,lease_token,started_at,finished_at,outcome,failure_code,http_status,next_attempt_at FROM event_attempts LIMIT 0`)
	if err != nil {
		return err
	}
	return rows.Close()
}
