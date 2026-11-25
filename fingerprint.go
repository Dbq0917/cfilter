package cfilter

import (
	"bytes"
)

type fingerprint []byte

func hashfp(f fingerprint) uint {
	var h uint = 5381
	for i := range f {
		h = ((h << 5) + h) + uint(f[i])
	}

	return h
}

func match(a, b fingerprint) bool {
	return bytes.Equal(a, b)
}

// sum64 folds an arbitrary-length hash digest into a uint64. The result is
// passed through mix64 before use so that structured (even sequential)
// digests spread uniformly across bucket indices and fingerprints.
func sum64(sum []byte) uint64 {
	var h uint64
	for i := 0; i < len(sum); i++ {
		h = h<<8 | uint64(sum[i])
		if i >= 7 {
			break
		}
	}
	return h
}
