package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Printf("New client connected: %s\n", conn.RemoteAddr().String())

	for { // 1. Create a 4-byte buffer for the header
		header := make([]byte, 4)

		// 2. Read exactly 4 bytes from the network stream
		_, err := io.ReadFull(conn, header)
		if err != nil {
			fmt.Printf("Client disconnected or read error: %v\n", err)
			return // Kill the goroutine if the client drops
		}
		// 3. Translate those raw bytes into an actual integer
		msgLength := binary.BigEndian.Uint32(header)
		fmt.Printf("Incoming message length: %d bytes \n", msgLength)

		// Create a new buffer dynamically sized to the exact length of the payload
		payload := make([]byte, msgLength)

		// Block and read exactly that many bytes from the socket
		_, err = io.ReadFull(conn, payload)
		if err != nil {
			fmt.Printf("Failed to read payload: %v\n", err)
			return
		}

		// Print the actual command
		fmt.Printf("Received command: %s\n", string(payload))

		// Parsing the payload
		parts := strings.Split(string(payload), " ")

		if len(parts) == 0 {
			continue
		}

		command := parts[0]
		var response string

		switch command {
		case "SET":
			if len(parts) >= 3 {
				key := parts[1]
				value := strings.Join(parts[2:], " ")

				mu.Lock()
				kvStore[key] = value
				mu.Unlock()

				response = "OK"
				fmt.Printf("Saved to memory: [%s] = %s\n", key, value)
			} else {
				response = "ERROR: syntax"
			}

		case "GET":
			if len(parts) == 2 {
				key := parts[1]

				mu.RLock()
				value, exists := kvStore[key]
				mu.RUnlock()

				if exists {
					response = value
				} else {
					response = "(nil)"
				}
			} else {
				response = "ERROR : syntax"
			}

		default:
			response = "ERROR: unknown command"
		}

		respBytes := []byte(response)
		respHeader := make([]byte, 4)
		binary.BigEndian.PutUint32(respHeader, uint32(len(respBytes)))

		conn.Write(respHeader)
		conn.Write(respBytes)
	}
}

var (
	// The actual database
	kvStore = make(map[string]string)
	// The lock to prevent concurrent write crashes
	mu sync.RWMutex
)

func main() {
	// 1. Start the listener on port 8080
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Printf("Failed to bind to prt: %v\n", err)
		os.Exit(1)
	}
	defer ln.Close()

	fmt.Println("KV Store TCP server listening on :8080")

	//2.The infinite Accept Loop

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}
		go handleConnection(conn)
	}
}
