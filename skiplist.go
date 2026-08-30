package main

import (
	"math/rand"
)

// MaxLevel sets the absolute maximum height our SkipList express lanes can reach.
// 12 levels can efficiently support around 4096 nodes (2^12) with O(log N) speed.
const MaxLevel = 12

// Node represents a single station (Key-Value pair) in the database.
type Node struct {
	key   string
	value string
	// next holds an array of memory addresses.
	// next[0] points to the next station on the local track.
	// next[1] points to the next station on the express track, etc.
	next []*Node
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
		key:   "",
		value: "",
		next:  make([]*Node, MaxLevel),
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
func (sl *SkipList) Put(key, value string) {
	// 1. The Breadcrumb Trail
	// This array stores the last node we visited before dropping down a level.
	update := make([]*Node, MaxLevel)
	current := sl.head

	// 2. The Top-Down Search
	for i := sl.level; i >= 0; i-- {
		// As long as the next station on this track is less than our target key, move forward.
		for current.next[i] != nil && current.next[i].key < key {
			current = current.next[i]
		}
		// We've gone as far as we can on track 'i'. Leave a breadcrumb.
		update[i] = current
	}

	// 3. Drop to the local track (Level 0) to check the station immediately in front of us.
	current = current.next[0]

	// 4. Update Existing Key
	// If the station exists and matches our key, update the value and recalculate RAM usage.
	if current != nil && current.key == key {
		sl.size -= len(current.value) // Remove old string length
		sl.size += len(value)         // Add new string length
		current.value = value
		return // Exit early
	}

	// 5. Insert Brand New Key
	newLevel := rollLevel()

	// If our new node rolled a track higher than the current system max,
	// initialize those upper tracks starting from the HEAD.
	if newLevel > sl.level {
		for i := sl.level + 1; i <= newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel // Raise the system roof
	}

	// Build the physical node in RAM
	newNode := &Node{
		key:   key,
		value: value,
		next:  make([]*Node, newLevel+1),
	}

	// 6. The Splice
	// Use our breadcrumbs to wire the new node into the existing tracks.
	for i := 0; i <= newLevel; i++ {
		newNode.next[i] = update[i].next[i] // New node points to the next station
		update[i].next[i] = newNode         // Breadcrumb points to the new node
	}

	// Add the exact byte weight of the new data to our RAM tracker.
	sl.size += len(key) + len(value)
	sl.keyCount++
}

// Get traverses the tracks to find a specific key.
func (sl *SkipList) Get(key string) (string, bool) {
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
		return current.value, true
	}

	return "", false // Key does not exist
}
