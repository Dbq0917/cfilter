package cfilter

// entry pairs a fingerprint with the full 64-bit hash of the originating
// item. The full hash is what lets the filter relocate entries deterministically
// when the table doubles: partial-key cuckoo hashing alone cannot recover the
// high bits of the bucket index from a fingerprint.
type entry struct {
	fp   fingerprint
	home uint64
}

type bucket []entry

func (b bucket) insert(e entry) bool {
	for i := range b {
		if b[i].fp == nil {
			b[i] = e
			return true
		}
	}

	return false
}

func (b bucket) lookup(f fingerprint) bool {
	for _, e := range b {
		if e.fp != nil && match(e.fp, f) {
			return true
		}
	}

	return false
}

func (b bucket) remove(f fingerprint) bool {
	for i := range b {
		if b[i].fp != nil && match(b[i].fp, f) {
			b[i] = entry{}
			return true
		}
	}

	return false
}
