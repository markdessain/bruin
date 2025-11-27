//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/flight/flightsql/driver"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
)

type EphemeralConnection struct {
	config DuckDBConfig
}

func NewEphemeralConnection(c DuckDBConfig) (*EphemeralConnection, error) {
	// _ = &driver.Driver{}
	return &EphemeralConnection{config: c}, nil
}

func (e *EphemeralConnection) driver() string {
	if strings.HasPrefix(e.config.ToDBConnectionURI(), "flight") {
		_ = &driver.Driver{}
		return "flightsql"
	}
	return "duckdb"
}

func (e *EphemeralConnection) withPreQuery(query string) string {
	envMap, err := godotenv.Read(".env")
	if err != nil {
	} else {
		for key, value := range envMap {
			query = strings.ReplaceAll(query, fmt.Sprintf("${%s}", key), value)
		}
	}

	query = strings.ReplaceAll(query, "/*", "\n/*")
	query = strings.ReplaceAll(query, "*/", "*/\n")
	lines := strings.Split(query, "\n")

	setup := ""
	teardown := ""
	output := ""
	startSetup := false
	startTeardown := false
	for _, line := range lines {
		line = strings.TrimSpace(line)

		if line == "/* @setup" {
			startSetup = true
		} else if line == "@setup */" {
			startSetup = false
		} else if line == "/* @teardown" {
			startTeardown = true
		} else if line == "@teardown */" {
			startTeardown = false
		} else if startSetup {
			setup = setup + line + "\n"
		} else if startTeardown {
			teardown = teardown + line + "\n"
		} else {
			output = output + line + "\n"
		}
	}

	return setup + "\n" + output + "\n" + teardown
}

func (e *EphemeralConnection) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	conn, err := sqlx.Open(e.driver(), e.config.ToDBConnectionURI())

	if err != nil {
		return nil, err
	}
	defer func(conn *sqlx.DB) {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}(conn)

	return conn.QueryContext(ctx, e.withPreQuery(query), args...) //nolint
}

func (e *EphemeralConnection) ExecContext(ctx context.Context, sql string, arguments ...any) (sql.Result, error) {
	conn, err := sqlx.Open(e.driver(), e.config.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}
	defer func(conn *sqlx.DB) {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}(conn)

	return conn.ExecContext(ctx, e.withPreQuery(sql), arguments...)
}

func (e *EphemeralConnection) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	conn, err := sqlx.Open(e.driver(), e.config.ToDBConnectionURI())
	if err != nil {
		// Cannot return error from this function signature, so we panic.
		// This is not ideal, but it's the best we can do with the current interface.
		panic(err)
	}
	defer func(conn *sqlx.DB) {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}(conn)

	return conn.QueryRowContext(ctx, e.withPreQuery(query), args...)
}
