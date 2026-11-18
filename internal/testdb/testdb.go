package testdb

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func Open(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("RELAY_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set RELAY_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	check(t, err)
	t.Cleanup(admin.Close)
	schema := pgx.Identifier{fmt.Sprintf("relay_test_%d", time.Now().UnixNano())}.Sanitize()
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+schema)
	check(t, err)
	t.Cleanup(func() {
		_, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		if err != nil {
			t.Error(err)
		}
	})
	config := admin.Config()
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	check(t, err)
	t.Cleanup(pool.Close)
	files, err := filepath.Glob("../../migrations/*.sql")
	check(t, err)
	for _, file := range files {
		sql, err := os.ReadFile(file)
		check(t, err)
		_, err = pool.Exec(ctx, string(sql))
		check(t, err)
	}
	return pool
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
