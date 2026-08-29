package main

import (
	"sync"
	"testing"
)

func (s *Server) TestSSTCounterRace(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			s.mu.Lock()
			s.sstCounter++
			s.mu.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			// GET reads the counter with a read-lock
			s.mu.RLock()
			_ = s.sstCounter
			s.mu.RUnlock()
		}
	}()

	wg.Wait()
}
