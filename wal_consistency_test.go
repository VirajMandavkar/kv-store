package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSplitBrainConsistency(t *testing.T) {
	os.Remove("wal.log")

	go main()
	time.Sleep(1 * time.Second)

	var wg sync.WaitGroup
	key := "split_brain_key"

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", "127.0.0.1:8080")
			if err != nil {
				return
			}
			defer conn.Close()

			cmd := fmt.Sprintf("SET %s %d", key, val)
			header := make([]byte, 4)
			binary.BigEndian.PutUint32(header, uint32(len(cmd)))

			conn.Write(header)
			conn.Write([]byte(cmd))

			respHeader := make([]byte, 4)
			conn.Read(respHeader)
		}(i)
	}

	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	file, err := os.Open("wal.log")
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lastWalValue string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "SET "+key) {
			parts := strings.Split(line, " ")
			lastWalValue = parts[2]
		}
	}
	if scanner.Err() != nil {
		t.Fatalf("Failed to scan the file : %v", scanner.Err())
	}

	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("Failed to connect for GET: %v", err)
	}
	defer conn.Close()

	cmd := fmt.Sprintf("GET %s", key)
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(cmd)))

	conn.Write(header)
	conn.Write([]byte(cmd))

	respHeader := make([]byte, 4)
	conn.Read(respHeader)
	respLen := binary.BigEndian.Uint32(respHeader)

	respPayload := make([]byte, respLen)
	conn.Read(respPayload)
	memValue := string(respPayload)

	if memValue != lastWalValue {
		t.Fatalf("SPLIT BRAIN DETECTED! RAM has [%s] but Disk has [%s]", memValue, lastWalValue)
	}
}
