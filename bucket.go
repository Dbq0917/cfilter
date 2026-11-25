package cfilter

import "math/rand"

type entry struct {
	fp     fingerprint
	home   uint
	bucket uint
	slot   int
	prev   *entry
	next   *entry
}

type bucket []*entry

func (b bucket) insert(e *entry, bucketIndex uint) bool {
	for i, current := range b {
		if current == nil {
			b[i] = e
			e.bucket = bucketIndex
			e.slot = i
			return true
		}
	}

	return false
}

func (b bucket) lookup(f fingerprint) bool {
	for _, e := range b {
		if e != nil && match(e.fp, f) {
			return true
		}
	}

	return false
}

func (b bucket) remove(f fingerprint) *entry {
	for i, e := range b {
		if e != nil && match(e.fp, f) {
			b[i] = nil
			return e
		}
	}

	return nil
}

func (b bucket) swap(e *entry, bucketIndex uint) *entry {
	i := rand.Intn(len(b))
	victim := b[i]

	b[i] = e
	e.bucket = bucketIndex
	e.slot = i

	if victim != nil {
		victim.slot = -1
	}

	return victim
}
