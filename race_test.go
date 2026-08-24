package main

import (
	"sync"
	"testing"
)

func TestSSTCounterRace(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			mu.Lock()
			sstCounter++
			mu.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			// GET reads the counter with a read-lock
			mu.RLock()
			_ = sstCounter
			mu.RUnlock()
		}
	}()

	wg.Wait()
}
