package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPhantomMemTableVisibility(t *testing.T) {
	tempDir := t.TempDir()
	srv, err := NewServer("127.0.0.1:0", tempDir)
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = srv.StartServer(ctx)
	}()

	addr := waitForServerAddress(t, srv)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	canaryKey := "phantom_key"
	canaryVal := "visible"
	sendTestCmd(t, conn, fmt.Sprintf("SET %s %s", canaryKey, canaryVal))

	junkData := make([]byte, 1024)
	for i := 0; i < 250; i++ {
		sendTestCmd(t, conn, fmt.Sprintf("SET junk_%d %s", i, string(junkData)))
	}

	resp := sendTestCmd(t, conn, fmt.Sprintf("GET %s", canaryKey))
	if resp != canaryVal {
		t.Fatalf("PHANTOM MEMTABLE DETECTED! Expected '%s', got '%s'", canaryVal, resp)
	}
	_ = os.Remove(filepath.Join(tempDir, "wal.log"))
}

func waitForServerAddress(t *testing.T, srv *Server) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		srv.mu.RLock()
		addr := srv.addr
		srv.mu.RUnlock()
		if addr != "" && addr != ":0" && addr != "127.0.0.1:0" && addr != "[::]:0" {
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server address was never assigned")
	return ""
}

func sendTestCmd(t *testing.T, conn net.Conn, cmd string) string {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(cmd)))
	conn.Write(header)
	conn.Write([]byte(cmd))

	respHeader := make([]byte, 4)
	_, err := io.ReadFull(conn, respHeader)
	if err != nil {
		t.Fatalf("Failed to read response header : %v", err)
	}

	respLen := binary.BigEndian.Uint32(respHeader)
	respPayload := make([]byte, respLen)
	_, err = io.ReadFull(conn, respPayload)
	if err != nil {
		t.Fatalf("Failed to read response payload: %v", err)
	}

	return string(respPayload)
}
