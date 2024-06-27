package repo

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed create_db.sql
var createDBSQL string

func openDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "file:"+dbPath)
	if err == nil {
		// fixes error "database is locked", caused by concurrent access from deal goroutines to a single sqlite3 db connection
		// see: https://github.com/mattn/go-sqlite3#:~:text=Error%3A%20database%20is%20locked
		// see: https://github.com/filecoin-project/boost/pull/657
		db.SetMaxOpenConns(1)
	}

	return db, err
}

func createDB(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, createDBSQL); err != nil {
		return fmt.Errorf("failed to create tables in DB: %w", err)
	}

	return nil
}
