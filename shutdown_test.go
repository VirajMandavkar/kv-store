package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestGracefulShutdown(t *testing.T) {
	//Clean slate
	os.Remove("wal.log")

	// 1. Boot the server with a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)

	go func() {
		serverDone <- StartServer(ctx)
	}()

	// Wait for listener to bind
	time.Sleep(500 * time.Millisecond)

	// 2. Blast the server with 50 sequential writes
	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	for i := 0; i < 50; i++ {
		cmd := fmt.Sprintf("SET shutdown_key_%d data_%d", i, i)
		sendTestCmd(t, conn, cmd)
	}
	conn.Close()

	// 3. Trigger the shutdown sequence
	cancel()

	// 4. Wait for the server to cleanly exit (or timeout if deadlocked)
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("Server exited with error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("CRITICAL: Server deadlocked during shutdown sequence")
	}

	// 5. Verify absolute data integrity on the disk
	file, err := os.Open("wal.log")
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	writeCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "SET shutdown_key_") {
			writeCount++
		}
	}
	if err := scanner.Err(); err != nil {
		panic(fmt.Sprintf("Failed to read file: %v\n", err))
	}

	if writeCount != 50 {
		t.Fatalf("DATA LOSS DETECTED: Expected 50 records in WAL, found %d", writeCount)
	}
}
