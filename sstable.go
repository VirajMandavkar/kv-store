package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

type SSTableItrator struct {
	file         *os.File
	fileID       int
	currentKey   string
	currentValue string
	EOF          bool
}

func flushMemTable(sl *SkipList, fileID int) error {
	filename := fmt.Sprintf("sst_%d.db", fileID)

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	current := sl.head.next[0]
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

		// Move to the next station
		current = current.next[0]
		count++
	}
	fmt.Printf("[SYSTEM] Flushed %d keys to %s\n", count, filename)
	return nil
}

func searchSSTable(filename, targetKey string) (string, bool) {
	file, err := os.Open(filename)
	if err != nil {
		return "", false
	}
	defer file.Close()

	for {
		var KeyLen uint32
		err := binary.Read(file, binary.LittleEndian, &KeyLen)
		if err != nil {
			break
		}

		keyBytes := make([]byte, KeyLen)
		file.Read(keyBytes)
		currentKey := string(keyBytes)

		var valLen uint32
		binary.Read(file, binary.LittleEndian, &valLen)

		if currentKey == targetKey {
			valBytes := make([]byte, valLen)
			file.Read(valBytes)
			return string(valBytes), true
		} else {
			file.Seek(int64(valLen), 1)
		}
	}
	return "", false
}

func NewSSTableItrator(fileID int) (*SSTableItrator, error) {
	filename := fmt.Sprintf("sst_%d.db", fileID)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	it := &SSTableItrator{file: file, fileID: fileID}

	var KeyLen uint32
	err = binary.Read(file, binary.LittleEndian, &KeyLen)
	if err != nil {
		return nil, err
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

func (it *SSTableItrator) Next() {
	var KeyLen uint32

	err := binary.Read(it.file, binary.LittleEndian, &KeyLen)
	if err != nil {
		it.EOF = true
		it.file.Close() // Clean up the OS resource!
		return          // Stop executing
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

func CompactSSTables(fileIDs []int, outputFileID int) error {
	var iterator []*SSTableItrator
	for _, id := range fileIDs {
		it, err := NewSSTableItrator(id)
		if err != nil {
			return fmt.Errorf("failed to initialize iterator for file %d: %w", id, err)
		}
		if !it.EOF {
			iterator = append(iterator, it)
		}
	}

	outFile, err := os.OpenFile(fmt.Sprintf("sst_%d.db", outputFileID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create compaction output file: %w", err)
	}
	defer outFile.Close()

	for len(iterator) > 0 {
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

		if winningIt.currentValue != "<TOMBSTONE>" {
			keyBytes := []byte(winningIt.currentKey)
			valBytes := []byte(winningIt.currentValue)

			keyLen := uint32(len(keyBytes))
			valLen := uint32(len(valBytes))

			err := binary.Write(outFile, binary.LittleEndian, keyLen)
			if err != nil {
				return err
			}
			_, err = outFile.Write(keyBytes)
			if err != nil {
				return err
			}

			err = binary.Write(outFile, binary.LittleEndian, valLen)
			if err != nil {
				return err
			}
			_, err = outFile.Write(valBytes)
			if err != nil {
				return err
			}
		}
		winningIt.Next()

		if winningIt.EOF {
			iterator = append(iterator[:minIndex], iterator[minIndex+1:]...)
		}
	}

	for _, id := range fileIDs {
		oldFilename := fmt.Sprintf("sst_%d.db", id)
		err := os.Remove(oldFilename)
		if err != nil {
			fmt.Printf("[WARNING] Failed to delete obsolete files %s: %v\n", oldFilename, err)
		}
	}
	outFile.Sync()
	fmt.Printf("[SYSTEM] Compaction complete! Merged files into sst_%d.db\n", outputFileID)
	return nil
}
