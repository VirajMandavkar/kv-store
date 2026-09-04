package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var MagicV2 = []byte{'K', 'V', 'S', '2'}

// SSTableItrator acts as a cursor moving line-by-line through a file on disk.
type SSTableItrator struct {
	file         *os.File
	fileID       int
	currentKey   string
	currentValue string
	isTombStone  bool
	EOF          bool // Becomes true when the iterator hits the end of the file
}

// IndexEntry maps a key to its exact physical byte offset in the file.
type IndexEntry struct {
	Key    string
	Offset int64
}

// SparseIndex holds the in-memory tree for a single SSTable.
type SparseIndex struct {
	Entries []IndexEntry
}

// flushMemTable takes a frozen SkipList from memory and writes it linearly to an SSTable file.
func (s *Server) flushMemTable(sl *SkipList, fileID int) error {
	filePath := filepath.Join(s.dataDir, fmt.Sprintf("sst_%d.db", fileID))

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Wrap the raw file in a 4MB memory buffer for high-throughput writes
	bufOut := bufio.NewWriterSize(file, 4*1024*1024)

	expectedKeys := sl.keyCount
	if expectedKeys <= 0 {
		expectedKeys = 100
	}
	bf := NewBloomFilter(expectedKeys, 0.01)

	current := sl.head.next[0]
	for current != nil {
		bf.Add([]byte(current.key))
		current = current.next[0]
	}

	bfBytes := bf.MarshalBinary()

	// Write Header to Buffer
	if _, err := bufOut.Write(MagicV2); err != nil {
		return err
	}
	if err := binary.Write(bufOut, binary.LittleEndian, uint32(len(bfBytes))); err != nil {
		return err
	}
	if _, err := bufOut.Write(bfBytes); err != nil {
		return err
	}

	// CRITICAL: Flush the header so we can grab the exact physical offset from the OS
	if err := bufOut.Flush(); err != nil {
		return err
	}

	currentOffset, _ := file.Seek(0, io.SeekCurrent)
	var indexEntries []IndexEntry
	const indexInterval = 100

	current = sl.head.next[0]
	count := 0

	for current != nil {
		// Capture the index entry BEFORE writing the record
		if count%indexInterval == 0 {
			indexEntries = append(indexEntries, IndexEntry{
				Key:    current.key,
				Offset: currentOffset,
			})
		}

		// 1. Write the 1-byte flag
		if current.isTombStone {
			bufOut.Write([]byte{RecordDel})
		} else {
			bufOut.Write([]byte{RecordPut})
		}

		// 2. Write Key Length (4 bytes)
		keyLen := uint32(len(current.key))
		binary.Write(bufOut, binary.LittleEndian, keyLen)

		// 3. Write Key string
		bufOut.WriteString(current.key)

		var recordSize int64
		if current.isTombStone {
			binary.Write(bufOut, binary.LittleEndian, uint32(0))
			recordSize = 1 + 4 + int64(len(current.key)) + 4
		} else {
			valLen := uint32(len(current.value))
			binary.Write(bufOut, binary.LittleEndian, valLen)
			bufOut.WriteString(current.value)
			recordSize = 1 + 4 + int64(len(current.key)) + 4 + int64(len(current.value))
		}

		// Advance the tracker purely in math, avoiding slow disk seeks
		currentOffset += recordSize
		current = current.next[0]
		count++
	}

	// Write Index Block & Footer
	indexStartOffset := currentOffset
	binary.Write(bufOut, binary.LittleEndian, uint32(len(indexEntries)))

	for _, entry := range indexEntries {
		binary.Write(bufOut, binary.LittleEndian, uint32(len(entry.Key)))
		bufOut.WriteString(entry.Key)
		binary.Write(bufOut, binary.LittleEndian, uint64(entry.Offset))
	}

	binary.Write(bufOut, binary.LittleEndian, uint64(indexStartOffset))

	// Final flush to ensure all trailing data hits the disk before we close
	if err := bufOut.Flush(); err != nil {
		return err
	}

	fmt.Printf("[SYSTEM] Flushed %d keys to %s\n", count, filePath)
	return nil
}

// searchSSTable scans a single file looking for a specific key.
func (s *Server) searchSSTable(filename, targetKey string) (string, bool, bool) {
	filePath := filepath.Join(s.dataDir, filename)
	file, err := os.Open(filePath)
	if err != nil {
		return "", false, false
	}
	defer file.Close()

	magic := make([]byte, 4)
	legacyFormat := false
	if _, err := io.ReadFull(file, magic); err != nil {
		return "", false, false
	}

	if string(magic) != string(MagicV2) {
		legacyFormat = true
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return "", false, false
		}
	} else {
		var bfLen uint32
		if err := binary.Read(file, binary.LittleEndian, &bfLen); err != nil {
			return "", false, false
		}

		// SANITY CHECK: Prevent massive allocations on corrupted files.
		if bfLen < 9 || bfLen > 10*1024*1024 {
			return "", false, false
		}
		if fileInfo, err := file.Stat(); err == nil {
			if int64(8)+int64(bfLen) > fileInfo.Size() {
				return "", false, false
			}
		}

		bfBytes := make([]byte, bfLen)
		if _, err := io.ReadFull(file, bfBytes); err != nil {
			return "", false, false
		}

		bf, err := UnmarshalBinary(bfBytes)
		if err != nil {
			return "", false, false
		}

		if !bf.Exists([]byte(targetKey)) {
			return "", false, false // Bloom Filter caught it
		}
	}

	// ==========================================
	// V1 READ PATH (Legacy Backward Compatibility)
	// ==========================================
	if legacyFormat {
		for {
			var keyLen uint32
			if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
				break
			}
			keyBytes := make([]byte, keyLen)
			if _, err := io.ReadFull(file, keyBytes); err != nil {
				return "", false, false
			}
			var valueLen uint32
			if err := binary.Read(file, binary.LittleEndian, &valueLen); err != nil {
				return "", false, false
			}
			if string(keyBytes) == targetKey {
				valueBytes := make([]byte, valueLen)
				if _, err := io.ReadFull(file, valueBytes); err != nil {
					return "", false, false
				}
				return string(valueBytes), true, false
			}
			if _, err := file.Seek(int64(valueLen), io.SeekCurrent); err != nil {
				return "", false, false
			}
		}
		return "", false, false
	}

	// ==========================================
	// V2 INDEXED READ PATH
	// ==========================================

	// Capture our current position right after the Bloom Filter.
	dataStartOffset, _ := file.Seek(0, io.SeekCurrent)
	var bestOffset int64 = dataStartOffset

	// 1. Read the 8-byte footer to find where the Index Block starts
	fileInfo, err := file.Stat()
	if err != nil {
		return "", false, false
	}

	_, err = file.Seek(fileInfo.Size()-8, io.SeekStart)
	if err != nil {
		return "", false, false
	}

	var indexStartOffset uint64
	if err := binary.Read(file, binary.LittleEndian, &indexStartOffset); err != nil {
		return "", false, false
	}

	// 2. Jump to the Index Block and load the number of entries
	_, err = file.Seek(int64(indexStartOffset), io.SeekStart)
	if err != nil {
		return "", false, false
	}

	var numEntries uint32
	binary.Read(file, binary.LittleEndian, &numEntries)

	// 3. Scan the index to find the closest offset
	for i := uint32(0); i < numEntries; i++ {
		var kLen uint32
		binary.Read(file, binary.LittleEndian, &kLen)

		kBytes := make([]byte, kLen)
		file.Read(kBytes)
		idxKey := string(kBytes)

		var idxOffset uint64
		binary.Read(file, binary.LittleEndian, &idxOffset)

		if idxKey <= targetKey {
			bestOffset = int64(idxOffset)
		} else {
			// We overshot the target! The bestOffset is now perfectly locked in.
			break
		}
	}

	// 4. Seek directly to the calculated best offset
	file.Seek(bestOffset, io.SeekStart)

	// 5. Execute the heavily bounded Sequential Scan
	for {
		// SAFETY GUARD: Stop if we accidentally scan into the index block
		currentPos, _ := file.Seek(0, io.SeekCurrent)
		if currentPos >= int64(indexStartOffset) {
			break
		}

		var recordType byte
		if err := binary.Read(file, binary.LittleEndian, &recordType); err != nil {
			break
		}

		var KeyLen uint32
		binary.Read(file, binary.LittleEndian, &KeyLen)

		keyBytes := make([]byte, KeyLen)
		file.Read(keyBytes)
		currentKey := string(keyBytes)

		var valLen uint32
		binary.Read(file, binary.LittleEndian, &valLen)

		// EARLY EXIT OPTIMIZATION: SSTables are sorted!
		if currentKey > targetKey {
			break
		}

		if currentKey == targetKey {
			if recordType == RecordDel {
				return "", true, true
			}
			valBytes := make([]byte, valLen)
			file.Read(valBytes)
			return string(valBytes), true, false
		} else {
			// Not our key. Skip the value payload and check the next one.
			file.Seek(int64(valLen), io.SeekCurrent)
		}
	}

	return "", false, false
}

// NewSSTableItrator opens a file and loads the very first key/value pair into memory.
func (s *Server) NewSSTableItrator(fileID int) (*SSTableItrator, error) {
	filePath := filepath.Join(s.dataDir, fmt.Sprintf("sst_%d.db", fileID))
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	it := &SSTableItrator{file: file, fileID: fileID}

	magic := make([]byte, 4)
	legacyFormat := false
	if _, err := io.ReadFull(file, magic); err != nil {
		return nil, err
	}
	if string(magic) != string(MagicV2) {
		legacyFormat = true
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			file.Close()
			return nil, err
		}
	} else {
		var bfLen uint32
		if err := binary.Read(file, binary.LittleEndian, &bfLen); err != nil {
			file.Close()
			return nil, err
		}
		// SANITY CHECK: Prevent massive allocations on corrupted files.
		if bfLen < 9 || bfLen > 10*1024*1024 {
			file.Close()
			return nil, fmt.Errorf("corrupted file header")
		}
		if fileInfo, err := file.Stat(); err == nil {
			if int64(8)+int64(bfLen) > fileInfo.Size() {
				file.Close()
				return nil, fmt.Errorf("corrupted file header")
			}
		}
		if _, err := file.Seek(int64(bfLen), io.SeekCurrent); err != nil {
			file.Close()
			return nil, err
		}
	}

	if legacyFormat {
		var keyLen uint32
		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			file.Close()
			return nil, err
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			file.Close()
			return nil, err
		}
		var valueLen uint32
		if err := binary.Read(file, binary.LittleEndian, &valueLen); err != nil {
			file.Close()
			return nil, err
		}
		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(file, valueBytes); err != nil {
			file.Close()
			return nil, err
		}
		it.currentKey = string(keyBytes)
		it.currentValue = string(valueBytes)
		return it, nil
	}

	var recordType byte
	err = binary.Read(file, binary.LittleEndian, &recordType)
	if err != nil {
		return nil, err // File is likely empty
	}

	var KeyLen uint32
	binary.Read(file, binary.LittleEndian, &KeyLen)

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
	it.isTombStone = (recordType == RecordDel)

	return it, nil
}

// Next moves the cursor down one line. If it fails to read, it flags itself as dead (EOF = true).
func (it *SSTableItrator) Next() {

	var recordType byte
	err := binary.Read(it.file, binary.LittleEndian, &recordType)
	if err != nil {
		it.EOF = true
		it.file.Close() // Clean up the OS resource immediately!
		return
	}

	var KeyLen uint32
	binary.Read(it.file, binary.LittleEndian, &KeyLen)

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
	it.isTombStone = (recordType == RecordDel)
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

	// 4MB Memory Buffer
	bufOut := bufio.NewWriterSize(outFile, 4*1024*1024)

	bf := NewBloomFilter(20000, 0.01)
	bfBytes := bf.MarshalBinary()
	expectedBloomSize := len(bfBytes)

	if _, err := bufOut.Write(MagicV2); err != nil {
		return err
	}
	err = binary.Write(bufOut, binary.LittleEndian, uint32(len(bfBytes)))
	if err != nil {
		return err
	}
	_, err = bufOut.Write(bfBytes)
	if err != nil {
		return err
	}

	// CRITICAL: Flush the header so the offset math is accurate against the disk
	if err := bufOut.Flush(); err != nil {
		return err
	}

	currentOffset, _ := outFile.Seek(0, io.SeekCurrent)
	var indexEntries []IndexEntry
	const indexInterval = 100
	count := 0

	// 3. The Core Merge Loop
	for len(iterator) > 0 {

		// -------------------------------------------------------------
		// SAFETY GATE: The In-Place Filter
		// -------------------------------------------------------------
		validCount := 0
		for _, it := range iterator {
			if !it.EOF {
				iterator[validCount] = it
				validCount++
			}
		}
		iterator = iterator[:validCount]

		if len(iterator) == 0 {
			break
		}

		// -------------------------------------------------------------
		// SELECTION PHASE
		// -------------------------------------------------------------
		minIndex := 0
		for i := 1; i < len(iterator); i++ {
			if iterator[i].currentKey < iterator[minIndex].currentKey {
				minIndex = i
			} else if iterator[i].currentKey == iterator[minIndex].currentKey {
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
		// WRITE PHASE
		// -------------------------------------------------------------
		if !winningIt.isTombStone {
			keyBytes := []byte(winningIt.currentKey)
			valBytes := []byte(winningIt.currentValue)

			bf.Add(keyBytes)

			if count%indexInterval == 0 {
				indexEntries = append(indexEntries, IndexEntry{
					Key:    winningIt.currentKey,
					Offset: currentOffset,
				})
			}

			keyLen := uint32(len(keyBytes))
			valLen := uint32(len(valBytes))

			// Write to the Buffer, NOT the raw outFile
			bufOut.Write([]byte{RecordPut})

			err := binary.Write(bufOut, binary.LittleEndian, keyLen)
			if err != nil {
				return err
			}
			_, err = bufOut.Write(keyBytes)
			if err != nil {
				return err
			}

			err = binary.Write(bufOut, binary.LittleEndian, valLen)
			if err != nil {
				return err
			}
			_, err = bufOut.Write(valBytes)
			if err != nil {
				return err
			}

			recordSize := int64(1 + 4 + len(keyBytes) + 4 + len(valBytes))
			currentOffset += recordSize
			count++
		}

		winningIt.Next()
	}

	// Write Index Block & Footer
	indexStartOffset := currentOffset
	binary.Write(bufOut, binary.LittleEndian, uint32(len(indexEntries)))

	for _, entry := range indexEntries {
		binary.Write(bufOut, binary.LittleEndian, uint32(len(entry.Key)))
		bufOut.WriteString(entry.Key)
		binary.Write(bufOut, binary.LittleEndian, uint64(entry.Offset))
	}

	binary.Write(bufOut, binary.LittleEndian, uint64(indexStartOffset))

	// CRITICAL: Flush ALL buffered RAM to the physical disk before manipulating the file pointer
	if err := bufOut.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer before rewrite: %w", err)
	}

	// Safely seek and overwrite the Bloom filter placeholder
	_, err = outFile.Seek(8, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek for bloom filter rewrite: %w", err)
	}

	populatedBytes := bf.MarshalBinary()
	if len(populatedBytes) != expectedBloomSize {
		return fmt.Errorf("FATAL: bloom filter size changed from %d to %d during compaction", expectedBloomSize, len(populatedBytes))
	}
	_, err = outFile.Write(populatedBytes)
	if err != nil {
		return fmt.Errorf("failed to rewrite populated bloom filter: %w", err)
	}

	// 4. Cleanup Phase
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
