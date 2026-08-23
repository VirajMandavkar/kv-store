package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

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
