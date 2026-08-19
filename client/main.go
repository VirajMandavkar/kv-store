package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

func sendCommand(conn net.Conn, cmd string) {
	payload := []byte(cmd)
	// Create a 4-byte header for the length
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))

	// Send the header, then send the payload
	conn.Write(header)
	fmt.Printf("Sent header: %v\n", header)

	conn.Write(payload)
	fmt.Printf("Sent payload string: %s\n", string(payload))

	respHeader := make([]byte, 4)

	_, err := io.ReadFull(conn, respHeader)
	if err != nil {
		fmt.Println("Failed to read response header:", err)
		return
	}

	respLen := binary.BigEndian.Uint32(respHeader)
	respPayload := make([]byte, respLen)
	io.ReadFull(conn, respPayload)

	fmt.Printf("Command: %s | Response: %s\n", cmd, string(respPayload))
}

func main() {
	// Conntecting to server
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// 1. Send a SET command
	sendCommand(conn, "SET profile viraj_devops")

	// 2. Send a GET command to retrieve what we just saved
	sendCommand(conn, "GET profile")

	// 3. Try to GET something that doesn't exist
	sendCommand(conn, "GET missing_key")
}
