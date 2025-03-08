package lsmtree

import (
	"hash/fnv"
)

// BloomFilter is a simple bloom filter implementation.
type BloomFilter struct {
	bitset []bool
	size   uint
}

// NewBloomFilter creates a new BloomFilter with the specified size.
func NewBloomFilter(size uint) *BloomFilter {
	return &BloomFilter{
		bitset: make([]bool, size),
		size:   size,
	}
}

// Add inserts the key into the bloom filter.
func (bf *BloomFilter) Add(key string) {
	indices := bf.getHashes(key)
	for _, idx := range indices {
		bf.bitset[idx] = true
	}
}

// MightContain checks whether the key might be in the bloom filter.
func (bf *BloomFilter) MightContain(key string) bool {
	indices := bf.getHashes(key)
	for _, idx := range indices {
		if !bf.bitset[idx] {
			return false
		}
	}
	return true
}

// getHashes computes hash indices for the given key.
func (bf *BloomFilter) getHashes(key string) []uint {
	h := fnv.New32a()
	h.Write([]byte(key))
	hashVal := h.Sum32()
	// Simulate two hash functions.
	idx1 := uint(hashVal) % bf.size
	idx2 := uint(hashVal>>16) % bf.size
	return []uint{idx1, idx2}
}
