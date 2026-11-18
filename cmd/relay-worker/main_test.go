package main

import (
	"context"
	"errors"
	"testing"
)

func TestConfiguration(t *testing.T) {
	t.Setenv("RELAY_CONCURRENCY", "7")
	c, err := config()
	if err != nil || c.Concurrency != 7 {
		t.Fatal(c, err)
	}
	t.Setenv("RELAY_HANDLER_TIMEOUT", "never")
	if _, err = config(); err == nil {
		t.Fatal("accepted invalid timeout")
	}
}

func TestDemoWaitCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := demos()["demo.wait"](ctx, []byte(`{"duration":"1h"}`)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
