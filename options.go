package cfilter

import (
	"hash"
	"hash/fnv"
)

type option func(*CFilter)

// Size sets the number of buckets in the filter.
// Defaults to ((1 << 18) / BucketSize).
func Size(s uint) option {
	return func(cf *CFilter) {
		cf.size = s
	}
}

// BucketSize sets the size of each bucket in the filter. Defaults to 4.
func BucketSize(s uint8) option {
	return func(cf *CFilter) {
		cf.bSize = s
	}
}

// FingerprintSize sets the size of the fingerprint. Defaults to 2.
func FingerprintSize(s uint8) option {
	return func(cf *CFilter) {
		cf.fpSize = s
	}
}

// MaximumKicks sets the maximum number of times we kick down items/displace
// from their buckets. Defaults to 500.
func MaximumKicks(k uint) option {
	return func(cf *CFilter) {
		cf.kicks = k
	}
}

// HashFn sets the hashing function to be used for fingerprinting. Defaults to
// a 64-bit FNV-1 hash.Hash.
func HashFn(hashfn hash.Hash) option {
	return func(cf *CFilter) {
		cf.hashfn = hashfn
		// A user-supplied hash.Hash is a single mutable instance, so serialize
		// its entire reset/write/sum cycle. Default construction uses a fresh
		// instance per call and needs no extra locking.
		cf.sharedHash = true
		cf.newHash = func() hash.Hash { return hashfn }
	}
}

func configure(cf *CFilter) {
	if cf.hashfn == nil {
		cf.hashfn = fnv.New64()
		cf.newHash = func() hash.Hash { return fnv.New64() }
	}
	if cf.bSize == 0 {
		cf.bSize = 4
	}
	if cf.fpSize == 0 {
		cf.fpSize = 3
	}
	if cf.kicks == 0 {
		cf.kicks = 500
	}
	if cf.size == 0 {
		cf.size = (1 << 18) / uint(cf.bSize)
	}
	// Partial-key cuckoo hashing relies on the two candidate-bucket mapping
	// being an involution, which only holds when the bucket count is a power
	// of two. Round any requested size up to the next power of two.
	cf.size = nextPowerOfTwo(cf.size)
}

// nextPowerOfTwo returns the smallest power of two that is >= n.
func nextPowerOfTwo(n uint) uint {
	if n < 2 {
		return 2
	}
	p := uint(1)
	for p < n {
		p <<= 1
	}
	return p
}
