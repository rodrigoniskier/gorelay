package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"strings"
	"sync"
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
	// Applied versions must be a contiguous prefix of this binary's list; a hole
	// means the database history diverged and running later DDL would be unsafe.
	appliedSet := map[string]bool{}
	versions, err := tx.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return err
	}
	for versions.Next() {
		var v string
		if err = versions.Scan(&v); err != nil {
			versions.Close()
			return err
		}
		appliedSet[v] = true
	}
	err = versions.Err()
	versions.Close()
	if err != nil {
		return err
	}
	missing := ""
	for _, path := range files {
		version := strings.TrimPrefix(path, "migrations/")
		if !appliedSet[version] {
			if missing == "" {
				missing = version
			}
		} else if missing != "" {
			return fmt.Errorf("migration history has a gap: %s is applied but %s is not", version, missing)
		}
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

// Embedded migrations never change at runtime, so their checksums are computed
// once instead of on every readiness probe.
var embeddedChecksums = sync.OnceValues(func() (map[string]string, error) {
	files, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	sums := make(map[string]string, len(files))
	for _, path := range files {
		content, err := migrationFiles.ReadFile(path)
		if err != nil {
			return nil, err
		}
		sums[strings.TrimPrefix(path, "migrations/")] = fmt.Sprintf("%x", sha256.Sum256(content))
	}
	return sums, nil
})

func readyDatabase(ctx context.Context, db *sql.DB) error {
	want, err := embeddedChecksums()
	if err != nil {
		return err
	}
	// One round trip validates the whole migration history (the first query
	// also proves connectivity, so no separate ping is needed).
	applied, err := db.QueryContext(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return err
	}
	count := 0
	for applied.Next() {
		var version, checksum string
		if err = applied.Scan(&version, &checksum); err != nil {
			applied.Close()
			return err
		}
		count++
		if sum, ok := want[version]; !ok || sum != checksum {
			applied.Close()
			return fmt.Errorf("schema checksum mismatch")
		}
	}
	err = applied.Err()
	applied.Close()
	if err != nil {
		return err
	}
	if count != len(want) {
		return fmt.Errorf("schema version mismatch")
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
