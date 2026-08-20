package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
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
	start := time.Now()
	var wg sync.WaitGroup

	for i := 1002; i <= 2000; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", "localhost:8080")
			if err != nil {
				fmt.Printf("Connection failed: %v\n", err)
				return
			}
			defer conn.Close()
			cmd := fmt.Sprintf("SET load_key_%d test_value_%d", index, index)
			sendCommand(conn, cmd)
		}(i)

	}
	wg.Wait()

	fmt.Printf("10,000 requests completed in %v\n", time.Since(start))
}
