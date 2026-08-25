package main

import (
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// 1. Clean the state before any tests run
	os.Remove("wal.log")

	// Temporarily bypass the global server boot so our individual tests
	// (which now handle their own lifecycle) can bind to the port.
	// go main()
	// time.Sleep(1 * time.Second)

	// 2. Boot the server exactly once for the entire test suite
	go main()
	time.Sleep(1 * time.Second) // Give the TCP listener a second to bind

	// 3. Run all the tests (phantom_test.go, wal_consistency_test.go, etc.)
	code := m.Run()

	// 4. Exit gracefully
	os.Exit(code)
}
