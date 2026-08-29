package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// SSTableItrator acts as a cursor moving line-by-line through a file on disk.
type SSTableItrator struct {
	file         *os.File
	fileID       int
	currentKey   string
	currentValue string
	EOF          bool // Becomes true when the iterator hits the end of the file
}

// flushMemTable takes a frozen SkipList from memory and writes it linearly to an SSTable file.
func (s *Server) flushMemTable(sl *SkipList, fileID int) error {
	filePath := filepath.Join(s.dataDir, fmt.Sprintf("sst_%d.db", fileID))

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	bf := NewBloomFilter(1000, 0.01)

	current := sl.head.next[0]
	for current != nil {
		bf.Add([]byte(current.key))
		current = current.next[0]
	}

	bfBytes := bf.MarshalBinary()
	if err := binary.Write(file, binary.LittleEndian, uint32(len(bfBytes))); err != nil {
		return err
	}
	if _, err := file.Write(bfBytes); err != nil {
		return err
	}

	current = sl.head.next[0]
	count := 0

	for current != nil {
		// 1. Write Key Length (4 bytes)
		keyLen := uint32(len(current.key))
		binary.Write(file, binary.LittleEndian, keyLen)

		// 2. Write Key string
		file.WriteString(current.key)

		// 3. Write Value Length (4 bytes)
		valLen := uint32(len(current.value))
		binary.Write(file, binary.LittleEndian, valLen)

		// 4. Write Value string
		file.WriteString(current.value)

		// Move to the next node in the SkipList
		current = current.next[0]
		count++
	}
	fmt.Printf("[SYSTEM] Flushed %d keys to %s\n", count, filePath)
	return nil
}

// searchSSTable scans a single file from top to bottom looking for a specific key.
func (s *Server) searchSSTable(filename, targetKey string) (string, bool) {
	filePath := filepath.Join(s.dataDir, filename)
	file, err := os.Open(filePath)
	if err != nil {
		return "", false
	}
	defer file.Close()

	var bfLen uint32
	if err := binary.Read(file, binary.LittleEndian, &bfLen); err != nil {
		return "", false
	}

	bfBytes := make([]byte, bfLen)
	if _, err := io.ReadFull(file, bfBytes); err != nil {
		return "", false
	}

	bf, err := UnmarshalBinary(bfBytes)
	if err != nil {
		return "", false
	}

	if !bf.Exists([]byte(targetKey)) {
		return "", false
	}

	for {
		var KeyLen uint32
		err := binary.Read(file, binary.LittleEndian, &KeyLen)
		if err != nil {
			break // EOF reached without finding the key
		}

		keyBytes := make([]byte, KeyLen)
		file.Read(keyBytes)
		currentKey := string(keyBytes)

		var valLen uint32
		binary.Read(file, binary.LittleEndian, &valLen)

		// If this is our key, read the value and return it.
		if currentKey == targetKey {
			valBytes := make([]byte, valLen)
			file.Read(valBytes)
			return string(valBytes), true
		} else {
			// Not our key. Skip ahead by 'valLen' bytes to avoid reading useless data.
			file.Seek(int64(valLen), 1)
		}
	}
	return "", false
}

// NewSSTableItrator opens a file and loads the very first key/value pair into memory.
func (s *Server) NewSSTableItrator(fileID int) (*SSTableItrator, error) {
	filePath := filepath.Join(s.dataDir, fmt.Sprintf("sst_%d.db", fileID))
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	it := &SSTableItrator{file: file, fileID: fileID}

	var bfLen uint32
	if err := binary.Read(file, binary.LittleEndian, &bfLen); err != nil {
		return nil, err
	}

	if _, err := file.Seek(int64(bfLen), io.SeekCurrent); err != nil {
		file.Close()
		return nil, err
	}

	var KeyLen uint32
	err = binary.Read(file, binary.LittleEndian, &KeyLen)
	if err != nil {
		return nil, err // File is likely empty
	}

	keyBytes := make([]byte, KeyLen)
	file.Read(keyBytes)

	var valLen uint32
	err = binary.Read(file, binary.LittleEndian, &valLen)
	if err != nil {
		return nil, err
	}

	valBytes := make([]byte, valLen)
	file.Read(valBytes)

	it.currentKey = string(keyBytes)
	it.currentValue = string(valBytes)

	return it, nil
}

// Next moves the cursor down one line. If it fails to read, it flags itself as dead (EOF = true).
func (it *SSTableItrator) Next() {
	var KeyLen uint32

	err := binary.Read(it.file, binary.LittleEndian, &KeyLen)
	if err != nil {
		it.EOF = true
		it.file.Close() // Clean up the OS resource immediately!
		return
	}

	keyBytes := make([]byte, KeyLen)
	it.file.Read(keyBytes)
	it.currentKey = string(keyBytes)

	var valLen uint32
	err = binary.Read(it.file, binary.LittleEndian, &valLen)
	if err != nil {
		return
	}

	valBytes := make([]byte, valLen)
	it.file.Read(valBytes)
	it.currentValue = string(valBytes)
}

// CompactSSTables performs a K-way merge. It reads multiple sorted files in parallel,
// picks the newest version of each key, and writes a single compacted output file.
func (s *Server) CompactSSTables(fileIDs []int, outputFileID int) error {
	var iterator []*SSTableItrator

	// 1. Initialize an iterator for every file we want to compact
	for _, id := range fileIDs {
		it, err := s.NewSSTableItrator(id)
		if err != nil {
			return fmt.Errorf("failed to initialize iterator for file %d: %w", id, err)
		}
		if !it.EOF {
			iterator = append(iterator, it)
		}
	}

	// 2. Open the new output file
	outFile, err := os.OpenFile(filepath.Join(s.dataDir, fmt.Sprintf("sst_%d.db", outputFileID)), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create compaction output file: %w", err)
	}
	defer outFile.Close()

	bf := NewBloomFilter(5000, 0.01)
	bfBytes := bf.MarshalBinary()

	err = binary.Write(outFile, binary.LittleEndian, uint32(len(bfBytes)))
	if err != nil {
		return err
	}

	_, err = outFile.Write(bfBytes)
	if err != nil {
		return err
	}

	// 3. The Core Merge Loop
	for len(iterator) > 0 {

		// -------------------------------------------------------------
		// SAFETY GATE: The In-Place Filter
		// We sweep the array and physically drop any iterator that hit EOF.
		// This prevents "ghost iterators" from corrupting our comparisons.
		// -------------------------------------------------------------
		validCount := 0
		for _, it := range iterator {
			if !it.EOF {
				iterator[validCount] = it
				validCount++
			}
		}
		iterator = iterator[:validCount] // Truncate the slice to only valid iterators

		// If all iterators are dead, we are done with the compaction.
		if len(iterator) == 0 {
			break
		}

		// -------------------------------------------------------------
		// SELECTION PHASE: Find the smallest key lexicographically
		// -------------------------------------------------------------
		minIndex := 0
		for i := 1; i < len(iterator); i++ {
			if iterator[i].currentKey < iterator[minIndex].currentKey {
				minIndex = i
			} else if iterator[i].currentKey == iterator[minIndex].currentKey {
				// DUPLICATE DETECTED: Two files have the same key.
				// We keep the one with the higher fileID (the newer one) and advance the other.
				if iterator[i].fileID > iterator[minIndex].fileID {
					iterator[minIndex].Next()
					minIndex = i
				} else {
					iterator[i].Next()
				}
			}
		}

		winningIt := iterator[minIndex]

		// -------------------------------------------------------------
		// WRITE PHASE: Save the winning key (unless it's a tombstone)
		// -------------------------------------------------------------
		if winningIt.currentValue != "<TOMBSTONE>" {
			keyBytes := []byte(winningIt.currentKey)
			valBytes := []byte(winningIt.currentValue)

			bf.Add(keyBytes)

			keyLen := uint32(len(keyBytes))
			valLen := uint32(len(valBytes))

			// Write key length, then key data
			err := binary.Write(outFile, binary.LittleEndian, keyLen)
			if err != nil {
				return err
			}
			_, err = outFile.Write(keyBytes)
			if err != nil {
				return err
			}

			// Write value length, then value data
			err = binary.Write(outFile, binary.LittleEndian, valLen)
			if err != nil {
				return err
			}
			_, err = outFile.Write(valBytes)
			if err != nil {
				return err
			}
		}

		// Advance the winner to its next line.
		// If it hits EOF here, the In-Place Filter at the top will remove it on the next loop.
		winningIt.Next()
	}
	_, err = outFile.Seek(4, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek for bloom filter rewrite: %w", err)
	}

	populatedBytes := bf.MarshalBinary()
	_, err = outFile.Write(populatedBytes)
	if err != nil {
		return fmt.Errorf("failed to rewrite populated bloom filter: %w", err)
	}
	// 4. Cleanup Phase: Delete the old, fragmented SSTable files
	for _, id := range fileIDs {
		oldFilename := filepath.Join(s.dataDir, fmt.Sprintf("sst_%d.db", id))
		err := os.Remove(oldFilename)
		if err != nil {
			fmt.Printf("[WARNING] Failed to delete obsolete files %s: %v\n", oldFilename, err)
		}
	}

	outFile.Sync()
	fmt.Printf("[SYSTEM] Compaction complete! Merged files into sst_%d.db\n", outputFileID)
	return nil
}
