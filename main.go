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
	"path/filepath"
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
	activeMem  *SkipList
	immutMem   []*SkipList
	mu         sync.RWMutex
	walFile    *os.File
	walChan    chan walRequest
	flushChan  chan *SkipList
	sstCounter int
	connWg     sync.WaitGroup
	walWg      sync.WaitGroup
	dataDir    string
}

func NewServer(addr string, dataDir string) (*Server, error) {

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir %s: %w", dataDir, err)
	}
	srv := &Server{
		addr:      addr,
		activeMem: NewSkipList(),
		walChan:   make(chan walRequest, 10000),
		flushChan: make(chan *SkipList, 1),
		dataDir:   dataDir,
	}
	return srv, nil
}

func (s *Server) handleConnection(conn net.Conn) {
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

				s.walChan <- walReq
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
				var isDeleted bool

				// Check active RAM
				s.mu.RLock()
				value, exists, isDeleted = s.activeMem.Get(key)

				// Check queued frozen RAM (search backwards for most recent)
				if !exists {
					for i := len(s.immutMem) - 1; i >= 0; i-- {
						value, exists, isDeleted = s.immutMem[i].Get(key)
						if exists {
							break
						}
					}
				}
				s.mu.RUnlock()

				// Check Disk (SSTables)
				if !exists {
					s.mu.RLock()
					maxFiles := s.sstCounter
					s.mu.RUnlock()

					for i := maxFiles - 1; i >= 0; i-- {
						filename := fmt.Sprintf("sst_%d.db", i)
						value, exists, isDeleted = s.searchSSTable(filename, key)
						if exists {
							break
						}
					}
				}

				// Resolve Tombstones
				if exists && !isDeleted {
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

				s.walChan <- walReq
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

func (s *Server) loadWAL() {
	file, err := os.Open(filepath.Join(s.dataDir, "wal.log"))
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
			s.activeMem.Put(key, value)
		} else if command == "DEL" && len(parts) == 2 {
			key := parts[1]
			s.activeMem.Delete(key)
		}
	}
	if err := scanner.Err(); err != nil {
		panic(fmt.Sprintf("Failed to read file: %v\n", err))
	}
	fmt.Printf("Startup: WAL loaded. MemTable consuming %d bytes\n", s.activeMem.size)
}

func (s *Server) backgroundFlusher() {
	var uncompactedFiles []int
	for memTOFlush := range s.flushChan {
		s.mu.RLock()
		currentID := s.sstCounter
		s.mu.RUnlock()

		err := s.flushMemTable(memTOFlush, currentID)
		if err != nil {
			fmt.Printf("[FATAL] Failed to flush SSTable: %v\n", err)
			continue
		}

		uncompactedFiles = append(uncompactedFiles, s.sstCounter)

		s.mu.Lock()
		s.sstCounter++
		s.mu.Unlock()

		if len(uncompactedFiles) >= 4 {
			fmt.Println("\n[SYSTEM] Compaction threshold reached. Initiating K-Way Merge...")
			s.mu.Lock()
			newCompactedID := s.sstCounter
			s.sstCounter++
			s.mu.Unlock()

			err := s.CompactSSTables(uncompactedFiles, newCompactedID)
			if err != nil {
				fmt.Printf("[ERROR] Compaction failed: %v\n", err)
			} else {
				uncompactedFiles = []int{newCompactedID}
			}
		}

		// Pop the oldest frozen table off the queue since it's now on disk
		s.mu.Lock()
		if len(s.immutMem) > 0 {
			s.immutMem = s.immutMem[1:]
		}
		s.mu.Unlock()
	}
}

func (s *Server) initSSTCounter() {
	files, err := os.ReadDir(s.dataDir)
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
		s.mu.Lock()
		s.sstCounter = maxID + 1
		s.mu.Unlock()
		fmt.Printf("Startup: Discovered existing SSTables. sstCounter set to %d\n", s.sstCounter)
	}
}

// StartServer owns the entire lifecycle of the KV Store.
// It returns an error if startup fails, and blocks until graceful shutdown completes.
func (s *Server) StartServer(ctx context.Context) error {
	// ==========================================
	// PHASE 1: RECOVERY & INITIALIZATION
	// ==========================================
	s.initSSTCounter()
	s.loadWAL()

	var err error
	s.walFile, err = os.OpenFile(filepath.Join(s.dataDir, "wal.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL: %v", err)
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to bind to port %s: %v", s.addr, err)
	}
	s.mu.Lock()
	s.addr = ln.Addr().String()
	s.mu.Unlock()
	fmt.Printf("KV Store TCP server listening on %s\n", s.addr)

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
	go s.backgroundFlusher()

	// 2C. The WAL Worker
	// Must be started BEFORE the network accept loop so clients have someone to talk to.
	s.walWg.Add(1)
	go func() {
		defer s.walWg.Done()
		var batch []walRequest

		for {
			req, ok := <-s.walChan
			if !ok {
				// Channel closed by shutdown trap. Flush final batch.
				if len(batch) > 0 {
					for _, r := range batch {
						s.walFile.WriteString(r.logLine)
					}
					s.walFile.Sync()
				}
				return
			}
			batch = append(batch, req)

		drainLoop:
			for len(batch) < 100 {
				select {
				case nextReq, ok := <-s.walChan:
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
				s.walFile.WriteString(r.logLine)
			}
			syncErr := s.walFile.Sync()

			// Apply to memory
			if syncErr == nil {
				var tablesToFlush []*SkipList
				s.mu.Lock()
				for _, r := range batch {
					switch r.command {
					case "SET":
						s.activeMem.Put(r.key, r.value)
					case "DEL":
						s.activeMem.Delete(r.key)
					}

					if s.activeMem.size >= MemTableLimit {
						tablesToFlush = append(tablesToFlush, s.activeMem)
						s.immutMem = append(s.immutMem, s.activeMem)
						s.activeMem = NewSkipList()
					}
				}
				s.mu.Unlock()

				for _, t := range tablesToFlush {
					s.flushChan <- t
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

		s.connWg.Add(1)
		go func(c net.Conn) {
			defer s.connWg.Done()
			s.handleConnection(c)
		}(conn)
	}

	// ==========================================
	// PHASE 4: THE GRACEFUL SHUTDOWN CHAIN
	// ==========================================
	// The accept loop broke. We now carefully wind down the system.

	fmt.Println("[SHUTDOWN] 1. Waiting for active client connections to finish...")
	s.connWg.Wait()

	fmt.Println("[SHUTDOWN] 2. Closing WAL channel to signal worker...")
	close(s.walChan)

	fmt.Println("[SHUTDOWN] 3. Waiting for WAL worker to commit final bytes...")
	s.walWg.Wait()

	fmt.Println("[SHUTDOWN] 4. Synchronizing and closing file descriptor...")
	if err := s.walFile.Sync(); err != nil {
		fmt.Printf("[ERROR] Failed syncing WAL: %v\n", err)
	}
	s.walFile.Close()

	fmt.Println("[SYSTEM] Data safely persisted. Shutdown complete.")
	return nil
}

// main now strictly handles OS interaction and context management
func main() {
	// Create a context that automatically cancels on SIGINT or SIGTERM
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	newServer, err := NewServer(":8080", "./data_8080")
	if err != nil {
		fmt.Printf("File Path Not found")
		os.Exit(1)
	}
	if err := newServer.StartServer(ctx); err != nil {
		fmt.Printf("[FATAL] Server failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Process terminated cleanly.")
}
