package cfilter

import (
	"hash"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
)

// CFilter represents a Cuckoo Filter, a probabilistic data store
// for approximated set membership queries.
type CFilter struct {
	count       uint64         // Total number of elements currently in the Filter
	hashfn      hash.Hash      // Prototype hash function used for fingerprinting
	hashPool    sync.Pool      // Reusable independent hash functions
	hashMu      sync.Mutex     // Serializes non-cloneable hash use and pool creation
	bucketLocks []sync.RWMutex // One lock per bucket
	buckets     []bucket       // Buckets where fingerprints are stored

	bSize  uint8 // Bucket size
	fpSize uint8 // Fingerprint size
	size   uint  // Number of buckets in the filter
	kicks  uint  // Maximum number of times we kick down items from buckets
}

type kickRecord struct {
	bucketIndex uint
	slotIndex   int
	original    fingerprint
}

// New returns a new CFilter object. It's Insert, Lookup, Delete and
// Size behave as their names suggest.
// Takes zero or more of the following option functions and applies them in
// order to the Filter:
//   - cfilter.Size(uint) sets the number of buckets in the filter
//   - cfilter.BucketSize(uint8) sets the size of each bucket
//   - cfilter.FingerprintSize(uint8) sets the size of the fingerprint
//   - cfilter.MaximumKicks(uint) sets the maximum number of bucket kicks
//   - cfilter.HashFn(hash.Hash) sets the fingerprinting hashing function
func New(opts ...option) *CFilter {
	cf := new(CFilter)
	for _, opt := range opts {
		opt(cf)
	}
	configure(cf)

	cf.buckets = make([]bucket, cf.size, cf.size)
	for i := range cf.buckets {
		cf.buckets[i] = make([]fingerprint, cf.bSize, cf.bSize)
	}
	cf.bucketLocks = make([]sync.RWMutex, cf.size)
	cf.initHashPool()

	return cf
}

// Insert adds an element (in byte-array form) to the Cuckoo filter,
// returns true if successful and false otherwise.
func (cf *CFilter) Insert(item []byte) bool {
	f := cf.fingerprint(item)
	itemHash := hashfp(item)
	j := itemHash % cf.size
	k := (j ^ hashfp(f)) % cf.size

	for {
		inserted, lockBusy := cf.tryInsert(f, j, k)
		if !lockBusy {
			return inserted
		}
		runtime.Gosched()
	}
}

// Lookup checks if an element (in byte-array form) exists in the Cuckoo
// Filter, returns true if found and false otherwise.
func (cf *CFilter) Lookup(item []byte) bool {
	f := cf.fingerprint(item)
	itemHash := hashfp(item)
	j := itemHash % cf.size
	k := (j ^ hashfp(f)) % cf.size

	low, high := orderedPair(j, k)
	cf.bucketLocks[low].RLock()
	defer cf.bucketLocks[low].RUnlock()
	if high != low {
		cf.bucketLocks[high].RLock()
		defer cf.bucketLocks[high].RUnlock()
	}

	return cf.buckets[j].lookup(f) || cf.buckets[k].lookup(f)
}

// Delete removes an element (in byte-array form) from the Cuckoo Filter,
// returns true if element existed prior and false otherwise.
func (cf *CFilter) Delete(item []byte) bool {
	f := cf.fingerprint(item)
	itemHash := hashfp(item)
	j := itemHash % cf.size
	k := (j ^ hashfp(f)) % cf.size

	low, high := orderedPair(j, k)
	cf.bucketLocks[low].Lock()
	defer cf.bucketLocks[low].Unlock()
	if high != low {
		cf.bucketLocks[high].Lock()
		defer cf.bucketLocks[high].Unlock()
	}

	removed := cf.buckets[j].remove(f)
	if !removed && k != j {
		removed = cf.buckets[k].remove(f)
	}
	if removed {
		atomic.AddUint64(&cf.count, ^uint64(0))
	}
	return removed
}

// Count returns the total number of elements currently in the Cuckoo Filter.
func (cf *CFilter) Count() uint {
	return uint(atomic.LoadUint64(&cf.count))
}

func (cf *CFilter) tryInsert(f fingerprint, j, k uint) (bool, bool) {
	heldLocks := make(map[uint]struct{})
	history := make([]kickRecord, 0, 8)

	unlockHeldLocks := func() {
		for index := range heldLocks {
			cf.bucketLocks[index].Unlock()
		}
	}
	defer unlockHeldLocks()

	tryLock := func(index uint) bool {
		if _, ok := heldLocks[index]; ok {
			return true
		}
		if !cf.bucketLocks[index].TryLock() {
			return false
		}
		heldLocks[index] = struct{}{}
		return true
	}

	if !tryLock(j) {
		return false, true
	}
	if k != j && !tryLock(k) {
		return false, true
	}

	if cf.buckets[j].insert(f) {
		atomic.AddUint64(&cf.count, 1)
		return true, false
	}
	if k != j && cf.buckets[k].insert(f) {
		atomic.AddUint64(&cf.count, 1)
		return true, false
	}

	i := [2]uint{j, k}[rand.Intn(2)]
	for n := uint(0); n < cf.kicks; n++ {
		var slotIndex int
		f, slotIndex = cf.buckets[i].swapTracked(f)
		history = append(history, kickRecord{
			bucketIndex: i,
			slotIndex:   slotIndex,
			original:    f,
		})

		next := (i ^ hashfp(f)) % cf.size
		if !tryLock(next) {
			cf.rollbackFailedInsert(history)
			return false, true
		}
		i = next

		if cf.buckets[i].insert(f) {
			atomic.AddUint64(&cf.count, 1)
			return true, false
		}
	}

	cf.rollbackFailedInsert(history)
	return false, false
}

func (cf *CFilter) rollbackFailedInsert(history []kickRecord) {
	for index := len(history) - 1; index >= 0; index-- {
		record := history[index]
		cf.buckets[record.bucketIndex][record.slotIndex] = record.original
	}
}

func orderedPair(a, b uint) (uint, uint) {
	if a > b {
		return b, a
	}
	return a, b
}
