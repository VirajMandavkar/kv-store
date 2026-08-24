package main

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestMaxPayloadLimit(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to bind listener: %v\n", err)
	}

	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		handleConnection(conn)
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Failed to connect to test server: %v\n", err)
	}
	defer clientConn.Close()

	badHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(badHeader, 5*1024*1024)
	_, _ = clientConn.Write(badHeader)

	clientConn.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 1)
	_, err = clientConn.Read(buf)

	if err == nil {
		t.Errorf("Expected server to close connection for oversized payload, but connection remained open")
	}
}
