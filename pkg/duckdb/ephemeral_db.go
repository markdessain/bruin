//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
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

	pwd, err := os.Getwd()
	if err != nil {
		fmt.Printf("Error getting working directory: %v\n", err)
	} else {
		fmt.Printf("Current working directory: %s\n", pwd)
	}

	setupQuery, err := regexp.Compile("--[ ]*setup:[ ]*(.*)")
	if err != nil {
		fmt.Println(err)
	}

	if setupQuery != nil {
		for _, m := range setupQuery.FindAllStringSubmatch(query, -1) {
			x := m[1]
			x = strings.TrimPrefix(x, "./")
			query = strings.ReplaceAll(query, m[0], "")
			content, err := os.ReadFile(pwd + "/" + x)
			if err == nil {
				query = query + "\n" + string(content)
			}
		}
	}

	teardownQuery, err := regexp.Compile("--[ ]*teardown:[ ]*(.*)")
	if err != nil {
		fmt.Println(err)
	}

	if teardownQuery != nil {
		for _, m := range teardownQuery.FindAllStringSubmatch(query, -1) {
			x := m[1]
			x = strings.TrimPrefix(x, "./")
			query = strings.ReplaceAll(query, m[0], "")
			content, err := os.ReadFile(pwd + "/" + x)
			if err == nil {
				query = query + "\n" + string(content)
			}
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
