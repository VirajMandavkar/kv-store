package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

type walRequest struct {
	logLine string
	receipt chan error
}

var (
	// The actual database
	kvStore = make(map[string]string)
	// The lock to prevent concurrent write crashes
	mu      sync.RWMutex
	walFile *os.File
)

var walChan = make(chan walRequest, 10000)

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
			//fmt.Printf("Failed to read payload: %v\n", err)
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

				logLine := fmt.Sprintf("SET %s %s\n", key, value)
				rec := make(chan error, 1)

				walReq := walRequest{
					logLine: logLine,
					receipt: rec,
				}

				walChan <- walReq
				err := <-rec

				if err == nil {
					mu.Lock()
					kvStore[key] = value
					mu.Unlock()
					response = "OK"
					fmt.Printf("Saved to memory: [%s] = %s\n", key, value)
				} else {
					response = "ERROR: disk sync failed"
				}
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
					response = "Key Don't Exist"
					fmt.Printf("Key : %s Don't exist in memory\n", key)
				}
			} else {
				response = "ERROR : syntax"
			}

		case "DEL":
			if len(parts) == 2 {
				key := parts[1]

				logLine := fmt.Sprintf("DEL %s\n", key)
				rec := make(chan error, 1)

				walReq := walRequest{
					logLine: logLine,
					receipt: rec,
				}

				walChan <- walReq
				err := <-rec
				if err == nil {
					mu.Lock()

					_, exists := kvStore[key]
					if exists {
						response = "Successful Deletion"
						fmt.Printf("Deleted from memory: %s -[%s]\n", key, kvStore[key])
						delete(kvStore, key)
					} else {
						response = "Key Don't Exist"
						fmt.Printf("Key : %s Don't exist in memory", key)
					}

					mu.Unlock()
				} else {
					response = "ERROR: disk sync failed"
				}

			} else {
				response = "ERROR: Synyax"
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

func loadWAL() {
	file, err := os.Open("wal.log")
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		panic(fmt.Sprintf("Failed to read WAL: %v\n", err))
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) == 0 {
			continue
		}

		command := parts[0]
		if command == "SET" && len(parts) >= 3 {
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			kvStore[key] = value
		} else if command == "DEL" && len(parts) == 2 {
			delete(kvStore, parts[1])
		}
	}
	if err := scanner.Err(); err != nil {
		panic(fmt.Sprintf("Failed to read file: %v\n", err))
	}

	fmt.Printf("Startup: Loaded %d keys from WAL into memory\n", len(kvStore))
}

func main() {
	// 1. Rebuilding memory from disk BEFORE starting the server
	loadWAL()

	// 2. Open the WAL for appending new commands
	var err error
	walFile, err = os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Fatal: failed to open WAL: %v\n", err)
		os.Exit(1)
	}
	defer walFile.Close()

	// 3. SetUp Graceful shutdown trap
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n Recieived shutdown signal . Flushing WAL and exiting ..")
		walFile.Close()
		os.Exit(0)
	}()

	// 4. background WAL flusher

	go func() {
		var batch []walRequest

		for {
			req := <-walChan
			batch = append(batch, req)
		drainLoop:
			for len(batch) < 100 {
				select {
				case nextReq := <-walChan:
					batch = append(batch, nextReq)
				default:
					break drainLoop
				}
			}
			for _, r := range batch {
				walFile.WriteString(r.logLine)
			}
			syncErr := walFile.Sync()

			for _, r := range batch {
				r.receipt <- syncErr
			}
			batch = batch[:0]
		}

	}()

	// 5. Start the listener on port 8080
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Printf("Failed to bind to prt: %v\n", err)
		os.Exit(1)
	}
	defer ln.Close()

	fmt.Println("KV Store TCP server listening on :8080")

	//6.The infinite Accept Loop

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}
		go handleConnection(conn)
	}
}
