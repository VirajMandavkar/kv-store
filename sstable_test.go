package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSearchSSTableVersionedFormat(t *testing.T) {
	tempDir := t.TempDir()
	srv, err := NewServer(":0", tempDir)
	if err != nil {
		t.Fatalf("NewServer returned an error: %v", err)
	}

	sl := NewSkipList()
	sl.Put("alpha", "one")
	sl.Put("beta", "two")
	if err := srv.flushMemTable(sl, 1); err != nil {
		t.Fatalf("flushMemTable returned an error: %v", err)
	}

	if got, ok, isDeleted := srv.searchSSTable("sst_1.db", "alpha"); !ok || isDeleted || got != "one" {
		t.Fatalf("searchSSTable returned (%q, %v, %v), want (%q, true, false)", got, ok, isDeleted, "one")
	}
	if _, ok, isDeleted := srv.searchSSTable("sst_1.db", "missing"); ok || isDeleted {
		t.Fatal("searchSSTable should reject a missing key")
	}
}

func TestLegacySSTableTraversalStillWorks(t *testing.T) {
	tempDir := t.TempDir()
	srv, err := NewServer(":0", tempDir)
	if err != nil {
		t.Fatalf("NewServer returned an error: %v", err)
	}

	legacyPath := filepath.Join(tempDir, "sst_99.db")
	file, err := os.Create(legacyPath)
	if err != nil {
		t.Fatalf("failed to create legacy SSTable: %v", err)
	}
	defer file.Close()

	key := "legacy-key"
	value := "legacy-value"
	if err := binary.Write(file, binary.LittleEndian, uint32(len(key))); err != nil {
		t.Fatalf("failed to write key length: %v", err)
	}
	if _, err = file.WriteString(key); err != nil {
		t.Fatalf("failed to write key: %v", err)
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(len(value))); err != nil {
		t.Fatalf("failed to write value length: %v", err)
	}
	if _, err = file.WriteString(value); err != nil {
		t.Fatalf("failed to write value: %v", err)
	}

	it, err := srv.NewSSTableItrator(99)
	if err != nil {
		t.Fatalf("NewSSTableItrator returned an error for a legacy SSTable: %v", err)
	}
	if it.EOF {
		t.Fatal("legacy iterator unexpectedly started at EOF")
	}
	if it.currentKey != key {
		t.Fatalf("iterator currentKey = %q, want %q", it.currentKey, key)
	}
	if it.currentValue != value {
		t.Fatalf("iterator currentValue = %q, want %q", it.currentValue, value)
	}
}

func TestCompactSSTablesProducesReadableOutput(t *testing.T) {
	tempDir := t.TempDir()
	srv, err := NewServer(":0", tempDir)
	if err != nil {
		t.Fatalf("NewServer returned an error: %v", err)
	}

	left := NewSkipList()
	left.Put("a", "1")
	left.Put("c", "3")
	if err := srv.flushMemTable(left, 10); err != nil {
		t.Fatalf("flushMemTable left side returned an error: %v", err)
	}

	right := NewSkipList()
	right.Put("b", "2")
	right.Put("c", "updated")
	if err := srv.flushMemTable(right, 11); err != nil {
		t.Fatalf("flushMemTable right side returned an error: %v", err)
	}

	if err := srv.CompactSSTables([]int{10, 11}, 12); err != nil {
		t.Fatalf("CompactSSTables returned an error: %v", err)
	}

	for key, want := range map[string]string{"a": "1", "b": "2", "c": "updated"} {
		got, ok, isDeleted := srv.searchSSTable("sst_12.db", key)
		if !ok || isDeleted {
			t.Fatalf("compacted SSTable is missing key %q", key)
		}
		if got != want {
			t.Fatalf("compacted SSTable value for %q = %q, want %q", key, got, want)
		}
	}
}

func TestBloomFilterRejectsInvalidInputs(t *testing.T) {
	bf := NewBloomFilter(0, 0.01)
	if bf == nil || bf.m == 0 || bf.k == 0 {
		t.Fatal("NewBloomFilter should sanitize invalid n values")
	}

	bf2 := NewBloomFilter(100, 1.5)
	if bf2 == nil || bf2.m == 0 || bf2.k == 0 {
		t.Fatal("NewBloomFilter should sanitize invalid p values")
	}

	if _, err := UnmarshalBinary([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0}); err == nil {
		t.Fatal("UnmarshalBinary should reject invalid serialized bloom payloads")
	}
}

func TestSSTableMagicCompatibilityHelper(t *testing.T) {
	if len(MagicV2) != 4 {
		t.Fatalf("MagicV2 length = %d, want 4", len(MagicV2))
	}
	if fmt.Sprintf("%s", MagicV2) != "KVS2" {
		t.Fatalf("MagicV2 = %q, want %q", string(MagicV2), "KVS2")
	}
}
