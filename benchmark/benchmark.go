package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	numWorkers        = 10000
	requestsPerWorker = 1000
)

func worker(workerID int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Printf("Worker %d failed to connect : %v\n", workerID, err)
		return
	}
	defer conn.Close()

	for i := 0; i < requestsPerWorker; i++ {
		cmd := fmt.Sprintf("SET load_key_%d_%d val_%d\n", workerID, i, i)
		payload := []byte(cmd)

		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, uint32(len(payload)))
		conn.Write(header)

		conn.Write(payload)

		respHeader := make([]byte, 4)
		_, err = io.ReadFull(conn, respHeader)
		if err != nil {
			fmt.Printf("Worker %d read header failed: %v\n", workerID, err)
			return
		}
		respLen := binary.BigEndian.Uint32(respHeader)
		respPayload := make([]byte, respLen)
		_, err = io.ReadFull(conn, respPayload)
		if err != nil {
			fmt.Printf("Worker %d read payload failed: %v\n", workerID, err)
			return
		}
	}
}

func main() {
	start := time.Now()
	var wg sync.WaitGroup

	fmt.Printf("Starting benchmark: %d workers, %d requests each (%d total)...\n",
		numWorkers, requestsPerWorker, numWorkers*requestsPerWorker)

	// Launch 100 persistent workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, &wg)
	}

	// Wait for all 100 workers to finish their 1,000 loops
	wg.Wait()

	elapsed := time.Since(start)
	totalRequests := numWorkers * requestsPerWorker
	reqPerSec := float64(totalRequests) / elapsed.Seconds()

	fmt.Printf("\n--- Benchmark Complete ---\n")
	fmt.Printf("Total Requests:  %d\n", totalRequests)
	fmt.Printf("Total Time:      %v\n", elapsed)
	fmt.Printf("Throughput:      %.2f requests/sec\n", reqPerSec)
}
