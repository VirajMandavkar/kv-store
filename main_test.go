package main

import (
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	_ = os.Remove("wal.log")
	_ = time.Second
	code := m.Run()
	os.Exit(code)
}
