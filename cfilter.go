package cfilter

import (
	"hash"
	"math/rand"
	"sync"
)

// CFilter represents a Cuckoo Filter, a probabilistic data store
// for approximated set membership queries.
type CFilter struct {
	mu      sync.RWMutex // Guards the table and count
	hashMu  sync.Mutex   // Serializes use of the shared hashfn
	hashfn  hash.Hash    // Hash function used for fingerprinting
	buckets []bucket     // Buckets where fingerprints are stored
	heads   []*entry     // Per-home-bucket displacement chains
	tails   []*entry
	count   uint // Total number of elements currently in the Filter

	bSize  uint8 // Bucket size
	fpSize uint8 // Fingerprint size
	size   uint  // Number of buckets in the filter
	kicks  uint  // Maximum number of times we kick down items from buckets
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
		cf.buckets[i] = make(bucket, cf.bSize, cf.bSize)
	}
	cf.heads = make([]*entry, cf.size)
	cf.tails = make([]*entry, cf.size)

	return cf
}

// Insert adds an element (in byte-array form) to the Cuckoo filter,
// returns true if successful and false otherwise.
func (cf *CFilter) Insert(item []byte) bool {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	f := cf.fingerprint(item)
	j := hashfp(item) % cf.size
	k := (j ^ hashfp(f)) % cf.size

	e := &entry{fp: f, slot: -1}
	if cf.buckets[j].insert(e, j) {
		cf.link(e, j)
		cf.count++
		return true
	}
	if k != j && cf.buckets[k].insert(e, k) {
		cf.link(e, k)
		cf.count++
		return true
	}

	home := j
	if k != j {
		home = [2]uint{j, k}[rand.Intn(2)]
	}
	i := home
	cf.link(e, home)

	type swapChange struct {
		bucket uint
		slot   int
		prev   *entry
	}
	changes := make([]swapChange, 0, cf.kicks)
	current := e

	for n := uint(0); n < cf.kicks; n++ {
		victim, slot := cf.buckets[i].swap(current, i)
		changes = append(changes, swapChange{bucket: i, slot: slot, prev: victim})
		current = victim

		if current == nil {
			cf.count++
			return true
		}

		i = (i ^ hashfp(current.fp)) % cf.size

		if cf.buckets[i].insert(current, i) {
			cf.count++
			return true
		}
	}

	restored := make(map[[2]uint]bool, len(changes))
	for _, change := range changes {
		slot := [2]uint{change.bucket, uint(change.slot)}
		if restored[slot] {
			continue
		}
		restored[slot] = true

		cf.buckets[change.bucket][change.slot] = change.prev
		if change.prev != nil {
			change.prev.bucket = change.bucket
			change.prev.slot = change.slot
		}
	}
	cf.unlink(e)

	return false
}

// Lookup checks if an element (in byte-array form) exists in the Cuckoo
// Filter, returns true if found and false otherwise.
func (cf *CFilter) Lookup(item []byte) bool {
	cf.mu.RLock()
	defer cf.mu.RUnlock()

	f := cf.fingerprint(item)
	j := hashfp(item) % cf.size
	k := (j ^ hashfp(f)) % cf.size

	if cf.buckets[j].lookup(f) || (k != j && cf.buckets[k].lookup(f)) {
		return true
	}
	if cf.chainLookup(j, f) {
		return true
	}

	return k != j && cf.chainLookup(k, f)
}

// Delete removes an element (in byte-array form) from the Cuckoo Filter,
// returns true if element existed prior and false otherwise.
func (cf *CFilter) Delete(item []byte) bool {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	f := cf.fingerprint(item)
	j := hashfp(item) % cf.size
	k := (j ^ hashfp(f)) % cf.size

	if e := cf.buckets[j].remove(f); e != nil {
		cf.removeEntry(e)
		return true
	}
	if k != j {
		if e := cf.buckets[k].remove(f); e != nil {
			cf.removeEntry(e)
			return true
		}
	}

	if e := cf.chainLookupEntry(j, f); e != nil {
		cf.removeEntry(e)
		return true
	}
	if k != j {
		if e := cf.chainLookupEntry(k, f); e != nil {
			cf.removeEntry(e)
			return true
		}
	}

	return false
}

// Count returns the total number of elements currently in the Cuckoo Filter.
func (cf *CFilter) Count() uint {
	cf.mu.RLock()
	defer cf.mu.RUnlock()

	return cf.count
}

// Capacity returns the number of fingerprint slots in the filter.
func (cf *CFilter) Capacity() uint {
	return cf.size * uint(cf.bSize)
}

// LoadFactor returns the fraction of fingerprint slots currently occupied.
func (cf *CFilter) LoadFactor() float64 {
	cf.mu.RLock()
	defer cf.mu.RUnlock()

	capacity := cf.Capacity()
	if capacity == 0 {
		return 0
	}

	return float64(cf.count) / float64(capacity)
}

func (cf *CFilter) link(e *entry, home uint) {
	e.home = home
	e.prev = cf.tails[home]
	e.next = nil

	if cf.tails[home] != nil {
		cf.tails[home].next = e
	} else {
		cf.heads[home] = e
	}
	cf.tails[home] = e
}

func (cf *CFilter) unlink(e *entry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else if cf.heads[e.home] == e {
		cf.heads[e.home] = e.next
	}

	if e.next != nil {
		e.next.prev = e.prev
	} else if cf.tails[e.home] == e {
		cf.tails[e.home] = e.prev
	}

	e.prev = nil
	e.next = nil
}

func (cf *CFilter) removeEntry(e *entry) {
	if e.slot >= 0 && int(e.bucket) < len(cf.buckets) &&
		e.slot < len(cf.buckets[e.bucket]) &&
		cf.buckets[e.bucket][e.slot] == e {
		cf.buckets[e.bucket][e.slot] = nil
		e.slot = -1
	}

	cf.unlink(e)
	if cf.count > 0 {
		cf.count--
	}
}

func (cf *CFilter) chainLookup(home uint, f fingerprint) bool {
	return cf.chainLookupEntry(home, f) != nil
}

func (cf *CFilter) chainLookupEntry(home uint, f fingerprint) *entry {
	for e := cf.heads[home]; e != nil; e = e.next {
		if match(e.fp, f) {
			return e
		}
	}

	return nil
}

func (cf *CFilter) fingerprint(item []byte) fingerprint {
	cf.hashMu.Lock()
	defer cf.hashMu.Unlock()

	return fprint(item, cf.fpSize, cf.hashfn)
}
