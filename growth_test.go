package cfilter_test

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/irfansharif/cfilter"
)

// TestGrowthOnFullFilter reproduces the original full-filter scenario. The old
// implementation rejected inserts once saturated; online growth instead keeps
// accepting them, doubling the table and keeping every stored key queryable.
func TestGrowthOnFullFilter(t *testing.T) {
	const startBuckets = 64
	cf := cfilter.New(cfilter.Size(startBuckets), cfilter.BucketSize(4))

	// Fill the first table right up to its physical slot capacity. The next
	// insert is exactly the one that previously returned false.
	initialSlots := startBuckets * 4
	start := make([][]byte, 0, initialSlots)
	for n := 0; n < initialSlots; n++ {
		key := []byte(fmt.Sprintf("full-%d", n))
		if !cf.Insert(key) {
			t.Fatalf("insert %d failed before reaching capacity", n)
		}
		start = append(start, key)
	}

	for _, key := range start {
		if !cf.Lookup(key) {
			t.Fatalf("false negative before growth for %q", key)
		}
	}
	if cf.Count() != uint(initialSlots) {
		t.Fatalf("count %d != %d", cf.Count(), initialSlots)
	}

	// This insert used to return false and force a full rebuild. It must now
	// trigger a doubling transparently and succeed.
	overflow := []byte("full-overflow")
	if !cf.Insert(overflow) {
		t.Fatal("insert into saturated filter must trigger growth, not fail")
	}

	for _, key := range start {
		if !cf.Lookup(key) {
			t.Fatalf("false negative after growth for %q", key)
		}
	}
	if !cf.Lookup(overflow) {
		t.Fatal("overflow key not found after growth")
	}
	if cf.Count() != uint(initialSlots)+1 {
		t.Fatalf("count %d != %d after growth", cf.Count(), initialSlots+1)
	}
}

// TestRepeatedGrowth drives several doublings and verifies no key is ever
// lost and the count tracks the number of successful inserts.
func TestRepeatedGrowth(t *testing.T) {
	cf := cfilter.New(cfilter.Size(32), cfilter.BucketSize(4))
	const total = 4000

	keys := make([][]byte, 0, total)
	for n := 0; n < total; n++ {
		key := []byte(fmt.Sprintf("rep-%d", n))
		if !cf.Insert(key) {
			t.Fatalf("insert %d failed while the filter should keep growing", n)
		}
		keys = append(keys, key)
	}

	if cf.Count() != total {
		t.Fatalf("count %d != %d", cf.Count(), total)
	}
	for _, key := range keys {
		if !cf.Lookup(key) {
			t.Fatalf("false negative after repeated growth for %q", key)
		}
	}

	// Deleting every inserted entry must drain the filter back to zero,
	// including entries relocated across multiple doublings.
	for _, key := range keys {
		if !cf.Delete(key) {
			t.Fatalf("delete failed for %q", key)
		}
	}
	if cf.Count() != 0 {
		t.Fatalf("expected empty filter, got count %d", cf.Count())
	}
}

// measureFPR probes keys drawn from a disjoint namespace and returns the
// observed false-positive fraction.
func measureFPR(cf *cfilter.CFilter, probes int, rng *rand.Rand) float64 {
	fp := 0
	buf := make([]byte, 16)
	for n := 0; n < probes; n++ {
		rng.Read(buf)
		key := append([]byte("probe-"), buf...)
		if cf.Lookup(key) {
			fp++
		}
	}
	return float64(fp) / float64(probes)
}

// TestGrowthDoesNotRaiseFalsePositiveRate verifies doubling preserves the
// fingerprint-derived accuracy instead of masking capacity by relaxing it.
func TestGrowthDoesNotRaiseFalsePositiveRate(t *testing.T) {
	// 1-byte fingerprints keep the baseline FPR high enough to measure
	// reliably in a small, fast test; the same fp size is retained on growth.
	cf := cfilter.New(cfilter.Size(256), cfilter.BucketSize(4), cfilter.FingerprintSize(1))

	members := make([][]byte, 0, 256*4)
	for n := 0; n < 256*4; n++ {
		key := []byte(fmt.Sprintf("member-%d", n))
		if !cf.Insert(key) {
			t.Fatalf("insert %d failed", n)
		}
		members = append(members, key)
	}

	rng := rand.New(rand.NewSource(42))
	const probes = 100000
	before := measureFPR(cf, probes, rng)

	// This insert forces the first doubling; FPR must not structurally rise.
	if !cf.Insert([]byte("member-grow-trigger")) {
		t.Fatal("growth trigger insert failed")
	}

	for _, key := range members {
		if !cf.Lookup(key) {
			t.Fatalf("false negative after growth for %q", key)
		}
	}

	after := measureFPR(cf, probes, rng)
	t.Logf("FPR before growth=%.4f after growth=%.4f (members=%d)", before, after, len(members))

	// Allow only sampling noise between two estimates of the same rate.
	if after > before+0.02 {
		t.Fatalf("false positive rate rose from %.4f to %.4f after growth", before, after)
	}
}

// TestConcurrentReadWriteDuringGrowth hammers the filter with concurrent
// readers and a writer that forces several doublings. Run under the race
// detector; reads and writes must stay safe and never see a false negative.
func TestConcurrentReadWriteDuringGrowth(t *testing.T) {
	cf := cfilter.New(cfilter.Size(32), cfilter.BucketSize(4))

	const total = 5000
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			buf := make([]byte, 12)
			for {
				select {
				case <-stop:
					return
				default:
					rng.Read(buf)
					cf.Lookup(append([]byte("probe-"), buf...))
				}
			}
		}(int64(r + 1))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; n < total; n++ {
			if !cf.Insert([]byte(fmt.Sprintf("cw-%d", n))) {
				t.Errorf("insert %d failed during concurrent growth", n)
				return
			}
		}
	}()

	for cf.Count() < total {
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if cf.Count() != total {
		t.Fatalf("expected %d entries, got %d", total, cf.Count())
	}
	for n := 0; n < total; n++ {
		if !cf.Lookup([]byte(fmt.Sprintf("cw-%d", n))) {
			t.Fatalf("false negative for cw-%d", n)
		}
	}
}

// TestCustomHashFnConcurrent exercises a user-supplied hash.Hash, which is a
// single mutable instance shared across goroutines. It must not race.
func TestCustomHashFnConcurrent(t *testing.T) {
	cf := cfilter.New(cfilter.Size(32), cfilter.HashFn(fnv.New64()))
	var wg sync.WaitGroup
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for n := 0; n < 1000; n++ {
				key := []byte{byte(id), byte(n), byte(n >> 8)}
				cf.Insert(key)
				cf.Lookup(key)
			}
		}(g)
	}
	wg.Wait()
}

// BenchmarkGrowth measures the wall-clock cost of inserting enough items into
// a small filter to force several online doublings (each rehashes the whole
// table). This is the work that previously required an offline full rebuild.
func BenchmarkGrowth(b *testing.B) {
	for n := 0; n < b.N; n++ {
		cf := cfilter.New(cfilter.Size(64), cfilter.BucketSize(4))
		for i := 0; i < 10000; i++ {
			if !cf.Insert([]byte(fmt.Sprintf("bench-%d", i))) {
				b.Fatal("unexpected insert failure during growth")
			}
		}
	}
}
