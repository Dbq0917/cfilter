package cfilter

import (
	"hash"
	"math/rand"
	"sync"
)

// CFilter represents a Cuckoo Filter, a probabilistic data store
// for approximated set membership queries.
//
// The filter grows itself online: when an insert cannot fit, the bucket table
// is doubled and every existing entry is rehashed into it transparently, under
// the same lock that guards normal operations. Callers never have to rebuild
// the filter or move data themselves, and concurrent readers/writers keep
// working throughout. Each entry additionally retains the full 64-bit hash of
// its item so it can be relocated deterministically; the fingerprint (and hence
// the false-positive rate) is unchanged by growth.
type CFilter struct {
	mu         sync.RWMutex // Guards every table access, including growth
	hashfn     hash.Hash    // Hash function used for fingerprinting
	newHash    func() hash.Hash
	hashMu     sync.Mutex // Serializes a user-supplied shared hash.Hash
	sharedHash bool
	buckets    []bucket // Buckets where fingerprints are stored
	count      uint     // Total number of elements currently in the Filter

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

	cf.buckets = cf.alloc(cf.size)

	return cf
}

func (cf *CFilter) alloc(n uint) []bucket {
	buckets := make([]bucket, n)
	for i := range buckets {
		buckets[i] = make([]entry, cf.bSize)
	}
	return buckets
}

// hashes derives the fingerprint and the full 64-bit item hash (the
// "primary" hash). Bucket indices are computed from these via candidates so
// insert, lookup, delete and growth relocation all share one mapping.
func (cf *CFilter) hashes(item []byte) (fingerprint, uint64) {
	if cf.sharedHash {
		// The configured hash.Hash is one mutable instance; hold the hash
		// lock for the whole reset/write/sum cycle so concurrent callers that
		// only hold the table's read lock cannot race on its internal state.
		cf.hashMu.Lock()
		defer cf.hashMu.Unlock()
	}
	h := cf.newHash()
	h.Write(item)
	raw := sum64(h.Sum(nil))

	// The configured digest (default FNV-1) produces near-consecutive values
	// for sequential short keys, which would pile many items into the same
	// region when only the low bits are used. Run it through an avalanche
	// mixer so both the bucket index and fingerprint spread uniformly.
	home := mix64(raw)

	fp := make(fingerprint, cf.fpSize)
	fpval := mix64(raw ^ fingerprintSalt)
	for i := range fp {
		fp[i] = byte(fpval >> (uint(i) * 8))
	}

	return fp, home
}

// fingerprintSalt decorrelates the fingerprint bits from the bucket-index
// bits derived from the same underlying digest.
const fingerprintSalt uint64 = 0x9e3779b97f4a7c15

// mix64 is the splitmix64 finalizer: a bijective integer mixer with good
// avalanche behavior, turning structured (even sequential) inputs into
// uniformly distributed ones.
func mix64(z uint64) uint64 {
	z += 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// candidates returns the two partial-key cuckoo bucket indices for an entry
// with the given primary hash and fingerprint within a table of 'size' buckets.
func candidates(home uint64, fp fingerprint, size uint) (uint, uint) {
	j := uint(home % uint64(size))
	k := (j ^ hashfp(fp)) % size
	return j, k
}

// Insert adds an element (in byte-array form) to the Cuckoo filter,
// returns true if successful and false otherwise.
func (cf *CFilter) Insert(item []byte) bool {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	f, h := cf.hashes(item)
	e := entry{fp: f, home: h}
	if cf.insertInto(cf.buckets, cf.size, e) {
		cf.count++
		return true
	}

	// Table is full: double it and rehash everything transparently. The
	// caller keeps inserting as if nothing happened; no rebuild, no data move.
	cf.grow()

	if cf.insertInto(cf.buckets, cf.size, e) {
		cf.count++
		return true
	}
	return false
}

// cuckooMove records one step of a cuckoo relocation chain: the entry that
// was displaced from a particular bucket, enabling exact rollback on failure.
type cuckooMove struct {
	bucket uint
	slot   int
	entry  entry
}

// insertInto places a single entry into the given table using the standard
// partial-key cuckoo strategy, including the random-kick relocation loop.
// On failure it rolls the table back to its exact prior state and returns
// false, so callers can grow the table and retry without any entry loss.
func (cf *CFilter) insertInto(table []bucket, size uint, e entry) bool {
	j, k := candidates(e.home, e.fp, size)
	if table[j].insert(e) || table[k].insert(e) {
		return true
	}

	// Record the swap chain so a failed placement can be fully rolled back.
	// A cuckoo insert that gives up must leave the table exactly as it was,
	// otherwise the displaced entry left in hand would be lost and an
	// existing member would become unqueryable.
	var chain []cuckooMove

	i := [2]uint{j, k}[rand.Intn(2)]
	for n := uint(0); n < cf.kicks; n++ {
		slot := rand.Intn(len(table[i]))
		victim := table[i][slot]
		table[i][slot] = e
		chain = append(chain, cuckooMove{bucket: i, slot: slot, entry: victim})
		e = victim
		i = (i ^ hashfp(e.fp)) % size

		if table[i].insert(e) {
			return true
		}
	}

	// Exhausted the kick budget without finding room: undo every swap in
	// reverse so the table is restored and no entry is dropped.
	rollback(table, chain)
	return false
}

// rollback reverses a recorded cuckoo swap chain so a failed insert leaves the
// table unchanged. Each step had swapped one occupant out of its bucket;
// walking backwards restores every displaced occupant to its original slot.
func rollback(table []bucket, chain []cuckooMove) {
	for n := len(chain) - 1; n >= 0; n-- {
		m := chain[n]
		table[m.bucket][m.slot] = m.entry
	}
}

// grow doubles the number of buckets and rehashes every stored entry into the
// new table. Each entry's stored full hash determines its new candidate
// buckets, so no item data is needed from the caller and no fingerprint (and
// thus no false-positive rate) changes. The doubled table has twice as many
// slots, so every existing entry is guaranteed a home.
func (cf *CFilter) grow() {
	old := cf.buckets
	oldSize := cf.size
	newSize := oldSize * 2
	next := cf.alloc(newSize)

	for _, b := range old {
		for _, e := range b {
			if e.fp == nil {
				continue
			}
			cf.relocate(next, newSize, e)
		}
	}

	cf.buckets = next
	cf.size = newSize
}

// relocate places an entry into a table that is guaranteed to have spare
// capacity (the freshly allocated, doubled table). It first tries ordinary
// cuckoo placement; if the kick loop fails, it parks the still-held displaced
// entry into a free slot of one of its own candidate buckets. Because the
// target table can never be full here, that fallback always succeeds and no
// fingerprint is ever dropped.
func (cf *CFilter) relocate(table []bucket, size uint, e entry) {
	if cf.insertInto(table, size, e) {
		return
	}
	j, k := candidates(e.home, e.fp, size)
	if forceInsert(table[j], e) || forceInsert(table[k], e) {
		return
	}
	// Unreachable while doubling: the new table holds at most as many entries
	// as the old one but has twice the slots, so some candidate bucket has room.
	panic("cfilter: no free slot while doubling table")
}

// forceInsert places an entry into the first free slot of a bucket without
// running the kick loop.
func forceInsert(b bucket, e entry) bool {
	for i := range b {
		if b[i].fp == nil {
			b[i] = e
			return true
		}
	}
	return false
}

// Lookup checks if an element (in byte-array form) exists in the Cuckoo
// Filter, returns true if found and false otherwise.
func (cf *CFilter) Lookup(item []byte) bool {
	cf.mu.RLock()
	defer cf.mu.RUnlock()

	f, h := cf.hashes(item)
	j, k := candidates(h, f, cf.size)
	return cf.buckets[j].lookup(f) || cf.buckets[k].lookup(f)
}

// Delete removes an element (in byte-array form) from the Cuckoo Filter,
// returns true if element existed prior and false otherwise.
func (cf *CFilter) Delete(item []byte) bool {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	f, h := cf.hashes(item)
	j, k := candidates(h, f, cf.size)
	if cf.buckets[j].remove(f) || cf.buckets[k].remove(f) {
		cf.count--
		return true
	}

	return false
}

// Count returns the total number of elements currently in the Cuckoo Filter.
func (cf *CFilter) Count() uint {
	cf.mu.RLock()
	defer cf.mu.RUnlock()
	return cf.count
}
