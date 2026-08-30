package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bitset []byte // actual memory bits
	m      uint32 // no. of bits
	k      uint8  // no. of hash functions
}

func NewBloomFilter(n int, p float64) *BloomFilter {
	m := uint32((-float64(n) * math.Log(p)) / math.Pow(math.Log(2), 2))
	k := uint8((float64(m) * math.Log(2)) / float64(n))

	return &BloomFilter{
		bitset: make([]byte, (m+7)/8),
		m:      m,
		k:      k,
	}
}

func hash(key []byte) (uint32, uint32) {
	h := fnv.New64()
	h.Write(key)
	hash64 := h.Sum64()
	h1 := uint32(hash64 & 0xffffffff)
	h2 := uint32(hash64 >> 32)

	return h1, h2
}

func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := hash(key)

	for i := 0; i < int(bf.k); i++ {
		idx := (h1 + uint32(i)*h2) % bf.m
		bf.bitset[idx/8] |= (1 << (idx % 8))
	}
}

func (bf *BloomFilter) Exists(key []byte) bool {
	h1, h2 := hash(key)

	for i := 0; i < int(bf.k); i++ {
		idx := (h1 + uint32(i)*h2) % bf.m
		if (bf.bitset[idx/8] & (1 << (idx % 8))) == 0 {
			return false
		}
	}

	return true
}

func (bf *BloomFilter) MarshalBinary() []byte {
	bitsetLen := len(bf.bitset)
	totalSize := 4 + 1 + 4 + bitsetLen

	buf := make([]byte, totalSize)

	binary.LittleEndian.PutUint32(buf[0:4], bf.m)

	buf[4] = bf.k
	binary.LittleEndian.PutUint32(buf[5:9], uint32(bitsetLen))
	copy(buf[9:], bf.bitset)

	return buf
}

func UnmarshalBinary(data []byte) (*BloomFilter, error) {
	if len(data) < 9 {
		return nil, errors.New("malformed data: byte slice is smaller than minimum header size")
	}

	m := binary.LittleEndian.Uint32(data[0:4])
	k := data[4]
	if m == 0 || k == 0 {
		return nil, errors.New("corrupted bloom filter: m or k is zero")
	}
	bitsetLen := binary.LittleEndian.Uint32(data[5:9])

	expectedTotalSize := 9 + int(bitsetLen)
	if len(data) != expectedTotalSize {
		return nil, fmt.Errorf("malformed data: expected total size of %d bytes, got %d", expectedTotalSize, len(data))
	}

	bitsetPayload := make([]byte, bitsetLen)
	copy(bitsetPayload, data[9:])

	return &BloomFilter{
		m:      m,
		k:      k,
		bitset: bitsetPayload,
	}, nil

}
