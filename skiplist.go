package main

import (
	"math/rand"
)

// MaxLevel sets the absolute maximum height our SkipList express lanes can reach.
// 12 levels can efficiently support around 4096 nodes (2^12) with O(log N) speed.

const (
	RecordPut byte = 0x00
	RecordDel byte = 0x01
	MaxLevel       = 12
)

// Node represents a single station (Key-Value pair) in the database.
type Node struct {
	key         string
	value       string
	next        []*Node
	isTombStone bool
}

// SkipList manages the entry point and the current active limits of the tracks.
type SkipList struct {
	head     *Node
	level    int // The current highest active track in the system
	size     int // Exact byte size of all keys and values (used for our 1MB RAM flush)
	keyCount int
}

// NewSkipList initializes a fresh MemTable with a dummy starting line.
func NewSkipList() *SkipList {
	head := &Node{
		key:         "",
		value:       "",
		next:        make([]*Node, MaxLevel),
		isTombStone: false,
	}
	return &SkipList{
		head:     head,
		level:    0,
		size:     0,
		keyCount: 0,
	}
}

// rollLevel simulates flipping a coin to decide how many express lanes a new node gets.
// 50% chance for Level 1, 25% for Level 2, 12.5% for Level 3...
func rollLevel() int {
	lvl := 0
	for rand.Float32() < 0.5 && lvl < MaxLevel-1 {
		lvl++
	}
	return lvl
}

// Put inserts a new key-value pair, or updates an existing one.
// Put inserts a new key-value pair, or updates an existing one.
func (sl *SkipList) Put(key, value string) {
	update := make([]*Node, MaxLevel)
	current := sl.head

	for i := sl.level; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current
	}
	current = current.next[0]

	// Update Existing Key (or resurrect a dead one)
	if current != nil && current.key == key {
		sl.size -= len(current.value)
		sl.size += len(value)
		current.value = value
		current.isTombStone = false // Resurrect the key
		return
	}

	// Insert Brand New Key
	newLevel := rollLevel()
	if newLevel > sl.level {
		for i := sl.level + 1; i <= newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	newNode := &Node{
		key:         key,
		value:       value,
		next:        make([]*Node, newLevel+1),
		isTombStone: false,
	}

	for i := 0; i <= newLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	sl.size += len(key) + len(value)
	sl.keyCount++
}

// Delete inserts a tombstone to mask older versions of this key.
func (sl *SkipList) Delete(key string) {
	update := make([]*Node, MaxLevel)
	current := sl.head

	for i := sl.level; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		update[i] = current
	}
	current = current.next[0]

	// Key is already in memory; turn it into a tombstone
	if current != nil && current.key == key {
		sl.size -= len(current.value) // Reclaim the RAM used by the old value
		current.value = ""            // Wipe the payload
		current.isTombStone = true    // Mark as dead
		return
	}

	// Key is not in memory; insert a fresh tombstone shield
	newLevel := rollLevel()
	if newLevel > sl.level {
		for i := sl.level + 1; i <= newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	newNode := &Node{
		key:         key,
		value:       "", // No payload
		next:        make([]*Node, newLevel+1),
		isTombStone: true, // Born dead
	}

	for i := 0; i <= newLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	sl.size += len(key) // Only the key takes up RAM
	sl.keyCount++
}

// Get traverses the tracks to find a specific key.
func (sl *SkipList) Get(key string) (string, bool, bool) {
	current := sl.head

	// Rapidly skip down the express lanes
	for i := sl.level; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
	}

	// Step onto the final node on the local track
	current = current.next[0]

	// Check if it's the exact match
	if current != nil && current.key == key {
		return current.value, true, current.isTombStone
	}

	return "", false, false // Key does not exist
}
