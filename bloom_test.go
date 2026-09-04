package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBloomFilterCorrectness(t *testing.T) {
	n := 1000
	p := 0.01
	bf := NewBloomFilter(n, p)

	for i := 0; i < n; i++ {
		bf.Add([]byte(fmt.Sprintf("key_%d", i)))
	}

	for i := 0; i < n; i++ {
		if !bf.Exists([]byte(fmt.Sprintf("key_%d", i))) {
			t.Fatalf("FALSE NEGATIVE DETECTED: key_%d was inserted but reported missing", i)
		}
	}

	falsePositives := 0
	testCount := 1000

	for i := n; i < n+testCount; i++ {
		if bf.Exists([]byte(fmt.Sprintf("key_%d", i))) {
			falsePositives++
		}
	}

	actualRate := float64(falsePositives) / float64(testCount)
	t.Logf("Measured False Positive Rate: %.4f (Target: %.2f)", actualRate, p)

	if actualRate > 0.03 { // Allowing small statistical margin above 1%
		t.Fatalf("False positive rate too high: got %.4f, expected ~0.01", actualRate)
	}
}

func TestBloomFilterMarshalRoundTrip(t *testing.T) {
	bf := NewBloomFilter(5000, 0.01)
	for _, key := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		bf.Add([]byte(key))
	}

	encoded := bf.MarshalBinary()
	decoded, err := UnmarshalBinary(encoded)
	if err != nil {
		t.Fatalf("UnmarshalBinary returned an error for a valid bloom filter: %v", err)
	}

	for _, key := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		if !decoded.Exists([]byte(key)) {
			t.Fatalf("Bloom filter round trip missed inserted key %q", key)
		}
	}
}

func TestSearchSSTableLegacyFormatCompatibility(t *testing.T) {
	tempDir := t.TempDir()
	srv, err := NewServer(":0", tempDir)
	if err != nil {
		t.Fatalf("NewServer returned an error: %v", err)
	}

	filename := "sst_legacy.db"
	legacyPath := filepath.Join(tempDir, filename)
	file, err := os.Create(legacyPath)
	if err != nil {
		t.Fatalf("failed to create legacy SSTable: %v", err)
	}

	key := "legacy_key"
	value := "legacy_value"
	if err := binary.Write(file, binary.LittleEndian, uint32(len(key))); err != nil {
		file.Close()
		t.Fatalf("failed to write key length: %v", err)
	}
	if _, err := file.WriteString(key); err != nil {
		file.Close()
		t.Fatalf("failed to write key: %v", err)
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(len(value))); err != nil {
		file.Close()
		t.Fatalf("failed to write value length: %v", err)
	}
	if _, err := file.WriteString(value); err != nil {
		file.Close()
		t.Fatalf("failed to write value: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("failed to close legacy SSTable: %v", err)
	}

	got, ok, isDeleted := srv.searchSSTable(filename, key)
	if !ok {
		t.Fatalf("legacy SSTable should still be readable without MagicV2 header")
	}
	if isDeleted {
		t.Fatal("legacy put record should not be reported as a tombstone")
	}
	if got != value {
		t.Fatalf("legacy SSTable returned %q, want %q", got, value)
	}
}

func TestSSTableIteratorReadsVersionedFormat(t *testing.T) {
	tempDir := t.TempDir()
	srv, err := NewServer(":0", tempDir)
	if err != nil {
		t.Fatalf("NewServer returned an error: %v", err)
	}

	sl := NewSkipList()
	sl.Put("first", "one")
	sl.Put("second", "two")

	if err := srv.flushMemTable(sl, 7); err != nil {
		t.Fatalf("flushMemTable returned an error: %v", err)
	}

	it, err := srv.NewSSTableItrator(7)
	if err != nil {
		t.Fatalf("NewSSTableItrator returned an error for a versioned table: %v", err)
	}
	if it.EOF {
		t.Fatal("iterator unexpectedly started at EOF")
	}
	if it.currentKey != "first" {
		t.Fatalf("iterator started at %q, want %q", it.currentKey, "first")
	}
	if it.currentValue != "one" {
		t.Fatalf("iterator returned %q, want %q", it.currentValue, "one")
	}

	it.Next()
	if it.currentKey != "second" {
		t.Fatalf("iterator advanced to %q, want %q", it.currentKey, "second")
	}
}
