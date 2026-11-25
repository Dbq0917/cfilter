package cfilter

import (
	"hash"
	"math/rand"
)

// CFilter represents a Cuckoo Filter, a probabilistic data store
// for approximated set membership queries.
type CFilter struct {
	hashfn  hash.Hash // Hash function used for fingerprinting
	buckets []bucket  // Buckets where fingerprints are stored
	heads   []*entry  // Oldest-to-newest displacement chains by first candidate bucket
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
	f := fprint(item, cf.fpSize, cf.hashfn)
	j := hashfp(item) % cf.size
	k := (j ^ hashfp(f)) % cf.size
	root := &entry{fp: f, slot: -1}
	current := root

	if cf.buckets[j].insert(root, j) {
		cf.link(root, j)
		cf.count++
		return true
	}
	if cf.buckets[k].insert(root, k) {
		cf.link(root, k)
		cf.count++
		return true
	}

	home := [2]uint{j, k}[rand.Intn(2)]
	i := home
	cf.link(root, home)

	for n := uint(0); n < cf.kicks; n++ {
		victim := cf.buckets[i].swap(current, i)
		current = victim
		if current == nil {
			cf.count++
			return true
		}

		f = current.fp
		i = (i ^ hashfp(f)) % cf.size

		if cf.buckets[i].insert(current, i) {
			cf.count++
			return true
		}
	}

	cf.abortInsert(root, current)
	return false
}

// Lookup checks if an element (in byte-array form) exists in the Cuckoo
// Filter, returns true if found and false otherwise.
func (cf *CFilter) Lookup(item []byte) bool {
	f := fprint(item, cf.fpSize, cf.hashfn)
	j := hashfp(item) % cf.size
	k := (j ^ hashfp(f)) % cf.size

	if cf.buckets[j].lookup(f) || cf.buckets[k].lookup(f) {
		return true
	}

	return cf.chainLookup(j, f) || (k != j && cf.chainLookup(k, f))
}

// Delete removes an element (in byte-array form) from the Cuckoo Filter,
// returns true if element existed prior and false otherwise.
func (cf *CFilter) Delete(item []byte) bool {
	f := fprint(item, cf.fpSize, cf.hashfn)
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

	for _, home := range []uint{j, k} {
		for e := cf.heads[home]; e != nil; e = e.next {
			if match(e.fp, f) {
				cf.removeEntry(e)
				return true
			}
		}
	}

	return false
}

// Count returns the total number of elements currently in the Cuckoo Filter.
func (cf *CFilter) Count() uint {
	return cf.count
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
	cf.unlink(e)

	if e.slot >= 0 && int(e.bucket) < len(cf.buckets) && e.slot < len(cf.buckets[e.bucket]) &&
		cf.buckets[e.bucket][e.slot] == e {
		cf.buckets[e.bucket][e.slot] = nil
	}
	e.slot = -1

	if cf.count > 0 {
		cf.count--
	}
}

func (cf *CFilter) chainLookup(home uint, f fingerprint) bool {
	for e := cf.heads[home]; e != nil; e = e.next {
		if match(e.fp, f) {
			return true
		}
	}

	return false
}

func (cf *CFilter) abortInsert(root, outside *entry) {
	if outside == nil {
		return
	}

	if outside == root {
		cf.unlink(root)
		return
	}

	if root.slot >= 0 && int(root.bucket) < len(cf.buckets) &&
		root.slot < len(cf.buckets[root.bucket]) &&
		cf.buckets[root.bucket][root.slot] == root {
		cf.buckets[root.bucket][root.slot] = outside
		outside.bucket = root.bucket
		outside.slot = root.slot
	}
	cf.unlink(root)
}
