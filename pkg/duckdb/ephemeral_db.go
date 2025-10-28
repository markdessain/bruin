//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/flight/flightsql/driver"
	"github.com/jmoiron/sqlx"
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

	loadQuery, err := regexp.Compile("-- (LOAD [a-zA-Z]*;)")
	if err != nil {
		fmt.Println(err)
	}

	if loadQuery != nil {
		for _, m := range loadQuery.FindAllStringSubmatch(query, -1) {
			query = m[1] + "\n" + query
		}
	}

	return query

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

	fmt.Println(sql)
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
