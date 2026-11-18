package main

import (
	"log/slog"
	"testing"
)

func TestMissingCredential(t *testing.T) {
	t.Setenv("RELAY_DATABASE_URL", "postgres://localhost/relay")
	t.Setenv("RELAY_API_KEY", "")
	if run(t.Context(), slog.Default()) == nil {
		t.Fatal("accepted missing key")
	}
}
