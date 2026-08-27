package main

import (
	"bufio"
	"context"
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

// walRequest carries data from the connection handler to the centralized WAL worker
type walRequest struct {
	command string
	key     string
	value   string
	logLine string
	receipt chan error
}

const MemTableLimit = 100 * 1024 // 100 KB limit for rapid flushing

type Server struct {
	addr       string
	walChan    chan walRequest
	flushChan  chan *SkipList
	connWg     sync.WaitGroup
	walWg      sync.WaitGroup
	activeMem  *SkipList
	immutMem   []*SkipList
	sstCounter int
	mu         sync.RWMutex
}

var (
	activeMem  = NewSkipList()
	immutMem   []*SkipList // Queue of frozen tables waiting for disk I/O
	mu         sync.RWMutex
	walFile    *os.File
	walChan    = make(chan walRequest, 10000)
	flushChan  = make(chan *SkipList, 1)
	sstCounter = 0

	// Orchestration Mechanics
	connWg sync.WaitGroup // Tracks active client connections
	walWg  sync.WaitGroup // Tracks the background WAL flusher
)

func handleConnection(conn net.Conn) {
	const MaxPayloadSize = 1 * 1024 * 1024 // 1 MB hard limit

	defer conn.Close()
	fmt.Printf("New client connected: %s\n", conn.RemoteAddr().String())

	for {
		// 1. Read the 4-byte length header
		header := make([]byte, 4)
		_, err := io.ReadFull(conn, header)
		if err != nil {
			return // Client disconnected naturally
		}

		msgLength := binary.BigEndian.Uint32(header)
		if msgLength > MaxPayloadSize {
			fmt.Printf("[SECURITY] Payload too large: %d bytes. Dropping connection.\n", msgLength)
			return
		}

		// 2. Read the exact payload
		payload := make([]byte, msgLength)
		_, err = io.ReadFull(conn, payload)
		if err != nil {
			return
		}

		// 3. Parse Command
		cleanPayload := strings.TrimSpace(string(payload))
		parts := strings.Split(string(cleanPayload), " ")
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
					command: "SET",
					key:     key,
					value:   value,
					logLine: logLine,
					receipt: rec,
				}

				walChan <- walReq
				err := <-rec // Block until the WAL worker syncs to disk

				if err == nil {
					response = "OK"
				} else {
					response = "ERROR: disk sync failed"
				}
			} else {
				response = "ERROR: syntax"
			}

		case "GET":
			if len(parts) == 2 {
				key := parts[1]
				var value string
				var exists bool

				// Check active RAM
				mu.RLock()
				value, exists = activeMem.Get(key)

				// Check queued frozen RAM (search backwards for most recent)
				if !exists {
					for i := len(immutMem) - 1; i >= 0; i-- {
						value, exists = immutMem[i].Get(key)
						if exists {
							break
						}
					}
				}
				mu.RUnlock()

				// Check Disk (SSTables)
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

				// Resolve Tombstones
				if exists && value != "<TOMBSTONE>" {
					response = value
				} else {
					response = "Key Don't Exist"
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
					command: "DEL",
					key:     key,
					logLine: logLine,
					receipt: rec,
				}

				walChan <- walReq
				err := <-rec
				if err == nil {
					response = "OK"
				} else {
					response = "ERROR: disk sync failed"
				}
			} else {
				response = "ERROR: Syntax"
			}

		default:
			response = "ERROR: unknown command"
		}

		// 4. Send Response
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
			activeMem.Put(key, value)
		} else if command == "DEL" && len(parts) == 2 {
			key := parts[1]
			activeMem.Put(key, "<TOMBSTONE>")
		}
	}
	if err := scanner.Err(); err != nil {
		panic(fmt.Sprintf("Failed to read file: %v\n", err))
	}
	fmt.Printf("Startup: WAL loaded. MemTable consuming %d bytes\n", activeMem.size)
}

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

		// Pop the oldest frozen table off the queue since it's now on disk
		mu.Lock()
		if len(immutMem) > 0 {
			immutMem = immutMem[1:]
		}
		mu.Unlock()
	}
}

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

// StartServer owns the entire lifecycle of the KV Store.
// It returns an error if startup fails, and blocks until graceful shutdown completes.
func StartServer(ctx context.Context, addr string) error {
	// ==========================================
	// PHASE 1: RECOVERY & INITIALIZATION
	// ==========================================
	initSSTCounter()
	loadWAL()

	var err error
	walFile, err = os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL: %v", err)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind to port %s: %v", addr, err)
	}
	fmt.Printf("KV Store TCP server listening on %s\n", addr)

	// ==========================================
	// PHASE 2: BACKGROUND WORKER DEPLOYMENT
	// ==========================================

	// 2A. The Asynchronous Closer
	// Waits in the background for a stop signal, then forces the network to close.
	go func() {
		<-ctx.Done()
		fmt.Println("\n[SERVER] Shutdown signal received. Breaking network listener...")
		_ = ln.Close()
	}()

	// 2B. The SSTable Disk Flusher
	go backgroundFlusher()

	// 2C. The WAL Worker
	// Must be started BEFORE the network accept loop so clients have someone to talk to.
	walWg.Add(1)
	go func() {
		defer walWg.Done()
		var batch []walRequest

		for {
			req, ok := <-walChan
			if !ok {
				// Channel closed by shutdown trap. Flush final batch.
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
						break drainLoop
					}
					batch = append(batch, nextReq)
				default:
					break drainLoop
				}
			}

			// Sync to disk
			for _, r := range batch {
				walFile.WriteString(r.logLine)
			}
			syncErr := walFile.Sync()

			// Apply to memory
			if syncErr == nil {
				var tablesToFlush []*SkipList
				mu.Lock()
				for _, r := range batch {
					switch r.command {
					case "SET":
						activeMem.Put(r.key, r.value)
					case "DEL":
						activeMem.Put(r.key, "<TOMBSTONE>")
					}

					if activeMem.size >= MemTableLimit {
						tablesToFlush = append(tablesToFlush, activeMem)
						immutMem = append(immutMem, activeMem)
						activeMem = NewSkipList()
					}
				}
				mu.Unlock()

				for _, t := range tablesToFlush {
					flushChan <- t
				}
			}

			for _, r := range batch {
				r.receipt <- syncErr
			}
			batch = batch[:0]
		}
	}()

	// ==========================================
	// PHASE 3: THE MAIN EVENT LOOP
	// ==========================================
	// Blocks here processing clients until ln.Close() is called by the async closer.
	for {
		conn, err := ln.Accept()
		if err != nil {
			break // The async closer shut down the listener.
		}

		connWg.Add(1)
		go func(c net.Conn) {
			defer connWg.Done()
			handleConnection(c)
		}(conn)
	}

	// ==========================================
	// PHASE 4: THE GRACEFUL SHUTDOWN CHAIN
	// ==========================================
	// The accept loop broke. We now carefully wind down the system.

	fmt.Println("[SHUTDOWN] 1. Waiting for active client connections to finish...")
	connWg.Wait()

	fmt.Println("[SHUTDOWN] 2. Closing WAL channel to signal worker...")
	close(walChan)

	fmt.Println("[SHUTDOWN] 3. Waiting for WAL worker to commit final bytes...")
	walWg.Wait()

	fmt.Println("[SHUTDOWN] 4. Synchronizing and closing file descriptor...")
	if err := walFile.Sync(); err != nil {
		fmt.Printf("[ERROR] Failed syncing WAL: %v\n", err)
	}
	walFile.Close()

	fmt.Println("[SYSTEM] Data safely persisted. Shutdown complete.")
	return nil
}

// main now strictly handles OS interaction and context management
func main() {
	// Create a context that automatically cancels on SIGINT or SIGTERM
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := StartServer(ctx, ":8080"); err != nil {
		fmt.Printf("[FATAL] Server failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Process terminated cleanly.")
}
