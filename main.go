// Package main contains the TCP server and its in-memory and on-disk storage layers.
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

// walRequest is one client mutation waiting for durable WAL persistence.
// receipt lets the WAL worker report the sync result to the requesting connection.
type walRequest struct {
	command string
	key     string
	value   string
	logLine string
	receipt chan error
}

// MemTableLimit is the approximate byte threshold that rotates the active MemTable.
const MemTableLimit = 100 * 1024 // 1 MB limit

var (
	// activeMem accepts new writes after the WAL worker has synced them.
	activeMem = NewSkipList()
	// immutMem holds rotated tables until the background flusher persists them.
	immutMem []*SkipList
	// mu protects all shared memory state and the SSTable ID counter.
	mu sync.RWMutex
	// walFile is the append-only write-ahead log shared by the WAL worker.
	walFile *os.File
	// walChan serializes mutations and allows the worker to batch disk writes.
	walChan = make(chan walRequest, 10000)
	// flushChan transfers full immutable tables to the SSTable writer.
	flushChan = make(chan *SkipList, 1)
	// sstCounter supplies the next SSTable filename suffix.
	sstCounter = 0

	// These wait groups make shutdown wait for active connections and WAL work.
	connWg sync.WaitGroup
	walWg  sync.WaitGroup
)

// handleConnection serves framed commands until the client disconnects.
// Each request starts with a four-byte big-endian payload length.
func handleConnection(conn net.Conn) {
	const MaxPayloadSize = 1 * 1024 * 1024 // 1 MB hard limit

	defer conn.Close()
	fmt.Printf("New client connected: %s\n", conn.RemoteAddr().String())

	for { // Read and process one length-prefixed request at a time.
		header := make([]byte, 4)

		// TCP is a stream, so ReadFull is required to obtain the whole header.
		_, err := io.ReadFull(conn, header)
		if err != nil {
			fmt.Printf("Client disconnected or read error: %v\n", err)
			return // Kill the goroutine if the client drops
		}
		// Decode the network-order length before allocating the payload buffer.
		msgLength := binary.BigEndian.Uint32(header)
		if msgLength > MaxPayloadSize {
			fmt.Printf("[SECURITY] Payload too large: %d bytes. Dropping connection.\n", msgLength)
			return
		}

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

		// Commands are ASCII-like text; the first token selects the operation.
		cleanPayload := strings.TrimSpace(string(payload))
		parts := strings.Split(string(cleanPayload), " ")

		if len(parts) == 0 {
			continue
		}

		command := parts[0]
		var response string

		switch command {
		case "SET":
			// Values may contain spaces, so everything after the key is retained.
			if len(parts) >= 3 {
				key := parts[1]
				value := strings.Join(parts[2:], " ")

				logLine := fmt.Sprintf("SET %s %s\n", key, value)
				rec := make(chan error, 1)

				walReq := walRequest{
					command: "SET",
					key:     key,
					value:   value,
					logLine: logLine,
					receipt: rec,
				}

				// The response is delayed until the WAL worker confirms Sync succeeded.
				walChan <- walReq
				err := <-rec

				if err == nil {
					response = "OK"
					fmt.Printf("Saved to memory: [%s] = %s\n", key, value)
				} else {
					response = "ERROR: disk sync failed"
				}
			} else {
				response = "ERROR: syntax"
			}

		case "GET":
			// Newer memory layers shadow older ones, so search in newest-first order.
			if len(parts) == 2 {
				key := parts[1]
				var value string
				var exists bool

				mu.RLock()
				value, exists = activeMem.Get(key)

				// Immutable tables are checked before disk because they are newer than SSTables.
				if !exists {
					for i := len(immutMem) - 1; i >= 0; i-- {
						value, exists = immutMem[i].Get(key)
						if exists {
							break
						}
					}
				}
				mu.RUnlock()

				// SSTables are also searched newest-first to preserve last-write-wins behavior.
				if !exists {
					mu.RLock()
					maxFiles := sstCounter
					mu.RUnlock()

					for i := maxFiles - 1; i >= 0; i-- {
						filename := fmt.Sprintf("sst_%d.db", i)
						value, exists = searchSSTable(filename, key)
						if exists {
							break
						}
					}
				}
				if exists && value != "<TOMBSTONE>" {
					response = value
				} else {
					response = "Key Don't Exist"
					fmt.Printf("Key : %s Don't exist in memory\n", key)
				}
			} else {
				response = "ERROR : syntax"
			}

		case "DEL":
			// Deletes are represented by a tombstone so older values remain hidden.
			if len(parts) == 2 {
				key := parts[1]

				logLine := fmt.Sprintf("DEL %s\n", key)
				rec := make(chan error, 1)

				walReq := walRequest{
					command: "SET",
					key:     key,
					logLine: logLine,
					receipt: rec,
				}

				// The WAL record is durable before the client receives success.
				walChan <- walReq
				err := <-rec
				if err == nil {
					response = "OK"
				} else {
					response = "ERROR: disk sync failed"
				}

			} else {
				response = "ERROR: Synyax"
			}

		default:
			response = "ERROR: unknown command"
		}

		// Responses use the same four-byte length-prefix framing as requests.
		respBytes := []byte(response)
		respHeader := make([]byte, 4)
		binary.BigEndian.PutUint32(respHeader, uint32(len(respBytes)))

		conn.Write(respHeader)
		conn.Write(respBytes)
	}
}

// loadWAL replays durable mutations into the active MemTable during startup.
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

	// WAL entries are newline-delimited SET and DEL commands.
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
			activeMem.Put(key, value)
		} else if command == "DEL" && len(parts) == 2 {
			key := parts[1]
			activeMem.Put(key, "<TOMBSTONE>")
		}
	}
	if err := scanner.Err(); err != nil {
		panic(fmt.Sprintf("Failed to read file: %v\n", err))
	}

	fmt.Printf("Startup: WAL loaded successfully. MemTable is consuming %d bytes of RAM\n", activeMem.size)
}

// backgroundFlusher persists rotated MemTables and periodically compacts SSTables.
func backgroundFlusher() {
	var uncompactedFiles []int
	for memTOFlush := range flushChan {

		mu.RLock()
		currentID := sstCounter
		mu.RUnlock()

		err := flushMemTable(memTOFlush, currentID)
		if err != nil {
			fmt.Printf("[FATAL] Failed to flush SSTable: %v\n", err)
			continue
		}

		uncompactedFiles = append(uncompactedFiles, sstCounter)
		mu.Lock()
		sstCounter++
		mu.Unlock()

		// Four flushed files trigger a k-way merge to reduce lookup work.
		if len(uncompactedFiles) >= 4 {
			fmt.Println("\n[SYSTEM] Compaction threshold reached. Initiating K-Way Merge...")

			mu.Lock()
			newCompactedID := sstCounter
			sstCounter++
			mu.Unlock()

			err := CompactSSTables(uncompactedFiles, newCompactedID)
			if err != nil {
				fmt.Printf("[ERROR] Compaction failed: %v\n", err)
			} else {
				uncompactedFiles = []int{newCompactedID}
			}
		}

		mu.Lock()
		if len(immutMem) > 0 {
			immutMem = immutMem[1:] // Pop the oldest table from the front
		}
		mu.Unlock()
	}
}

// initSSTCounter scans existing SSTable names so a restart does not reuse an ID.
func initSSTCounter() {
	files, err := os.ReadDir(".")
	if err != nil {
		return
	}

	maxID := -1
	for _, file := range files {
		name := file.Name()
		var id int

		if strings.HasPrefix(name, "sst_") && strings.HasSuffix(name, ".db") {
			_, err := fmt.Sscanf(name, "sst_%d.db", &id)
			if err == nil && id > maxID {
				maxID = id
			}
		}
	}

	if maxID >= 0 {
		mu.Lock()
		sstCounter = maxID + 1
		mu.Unlock()
		fmt.Printf("Startup: Discovered existing SSTables. sstCounter set to %d\n", sstCounter)
	}
}

// main performs recovery, starts background workers, and accepts TCP clients.
func main() {
	// Recover file metadata and the WAL before accepting client requests.
	initSSTCounter()
	loadWAL()

	// Open the WAL in append mode so new records follow existing durable records.
	var err error
	walFile, err = os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Fatal: failed to open WAL: %v\n", err)
		os.Exit(1)
	}

	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Printf("Failed to bind to prt: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("KV Store TCP server listening on :8080")

	// Shutdown closes the listener first, then drains connections and WAL work.
	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n Recieived shutdown signal . Flushing WAL and exiting ..")
		ln.Close()

		connWg.Wait()
		close(walChan)

		walWg.Wait()
		walFile.Sync()
		walFile.Close()

		fmt.Println("[SYSTEM] Data safely persisted to WAL. Shutdown complete.")
		os.Exit(0)
	}()

	// The single WAL worker preserves mutation order while batching fsyncs.

	walWg.Add(1)
	go func() {
		defer walWg.Done()
		var batch []walRequest

		for {
			req, ok := <-walChan
			if !ok {
				if len(batch) > 0 {
					for _, r := range batch {
						walFile.WriteString(r.logLine)
					}
					walFile.Sync()
				}
				return
			}
			batch = append(batch, req)
		drainLoop:
			for len(batch) < 100 {
				select {
				case nextReq, ok := <-walChan:
					if !ok {
						break drainLoop // Channel closed while draining
					}
					batch = append(batch, nextReq)
				default:
					break drainLoop
				}
			}

			// Write the whole batch before syncing once for the group.
			for _, r := range batch {
				walFile.WriteString(r.logLine)
			}
			syncErr := walFile.Sync()

			// Only expose mutations to readers after the corresponding WAL sync succeeds.
			if syncErr == nil {
				var tablesToFlush []*SkipList

				mu.Lock()
				for _, r := range batch {
					if r.command == "SET" {
						activeMem.Put(r.key, r.value)
					} else if r.command == "DEL" {
						activeMem.Put(r.key, "<TOMBSTONE>")
					}

					if activeMem.size >= MemTableLimit {
						tablesToFlush = append(tablesToFlush, activeMem)
						immutMem = append(immutMem, activeMem)
						activeMem = NewSkipList()
					}
				}
				mu.Unlock()

				// Flushing happens asynchronously after the active table has been replaced.
				for _, t := range tablesToFlush {
					flushChan <- t
					fmt.Println("\n[SYSTEM] MemTable frozen! Sent to background flusher.")
				}
			}

			for _, r := range batch {
				r.receipt <- syncErr
			}
			batch = batch[:0]
		}

	}()

	go backgroundFlusher()
	// Accept connections on the fixed service port until shutdown closes the listener.

	for {
		conn, err := ln.Accept()
		if err != nil {
			// When ln.Close() is called by the shutdown trap, Accept() returns an error.
			// We break the loop gracefully instead of printing a failure.
			break
		}

		connWg.Add(1) // Register the new connection
		go func(c net.Conn) {
			defer connWg.Done() // Unregister when the connection closes
			handleConnection(c)
		}(conn)
	}
}
