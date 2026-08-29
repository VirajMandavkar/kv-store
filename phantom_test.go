package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
)

func TestPhantomMemTableVisibility(t *testing.T) {

	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	canaryKey := "phantom_key"
	canaryVal := "visible"
	sendTestCmd(t, conn, fmt.Sprintf("SET %s %s", canaryKey, canaryVal))

	junkData := make([]byte, 1024) // 1 KB string
	for i := 0; i < 250; i++ {
		sendTestCmd(t, conn, fmt.Sprintf("SET junk_%d %s", i, string(junkData)))
	}

	resp := sendTestCmd(t, conn, fmt.Sprintf("GET %s", canaryKey))

	if resp != canaryVal {
		t.Fatalf("PHANTOM MEMTABLE DETECTED! Expected '%s', got '%s'", canaryVal, resp)
	}
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
