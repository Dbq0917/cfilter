package cfilter_test

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/irfansharif/cfilter"
)

func TestDeleteMissingAndDisplacedKeys(t *testing.T) {
	rand.Seed(2)

	cf := cfilter.New(
		cfilter.Size(3),
		cfilter.BucketSize(4),
		cfilter.FingerprintSize(8),
		cfilter.MaximumKicks(500),
	)

	keys := [][]byte{
		[]byte("k-1"), []byte("k-4"), []byte("k-7"), []byte("k-11"),
		[]byte("k-14"), []byte("k-17"), []byte("k-21"), []byte("k-24"),
		[]byte("k-27"), []byte("k-31"), []byte("k-34"), []byte("k-37"),
	}
	for _, key := range keys {
		if !cf.Insert(key) {
			t.Fatalf("expected to insert %q", key)
		}
	}

	before := cf.Count()
	beforeLoad := cf.LoadFactor()
	for n := 0; n < 100; n++ {
		key := []byte(fmt.Sprintf("absent-%d", n))
		if cf.Delete(key) {
			t.Fatalf("unexpectedly deleted absent key %q", key)
		}
	}
	if got := cf.Count(); got != before {
		t.Fatalf("missing-key deletes changed count from %d to %d", before, got)
	}
	if got := cf.LoadFactor(); got != beforeLoad {
		t.Fatalf("missing-key deletes changed load factor from %v to %v", beforeLoad, got)
	}
	if got, want := beforeLoad, float64(before)/float64(cf.Capacity()); got != want {
		t.Fatalf("load factor %v does not match count/capacity %v", got, want)
	}

	for i, key := range keys {
		if !cf.Lookup(key) {
			t.Fatalf("expected %q to be findable before deletion", key)
		}
		if !cf.Delete(key) {
			t.Fatalf("expected to delete %q (displaced entries included)", key)
		}
		if cf.Lookup(key) {
			t.Fatalf("expected %q to be absent after deletion", key)
		}
		if got, want := cf.Count(), before-uint(i)-1; got != want {
			t.Fatalf("expected count %d after deleting %q, got %d", want, key, got)
		}
	}

	if got := cf.Count(); got != 0 {
		t.Fatalf("expected empty filter, got count %d", got)
	}
	if got := cf.LoadFactor(); got != 0 {
		t.Fatalf("expected zero load factor, got %v", got)
	}
}

func TestConcurrentDeleteDoesNotUnderflow(t *testing.T) {
	const workers = 32

	cf := cfilter.New(cfilter.Size(16), cfilter.BucketSize(4))
	cf.Insert([]byte("only-key"))
	if got := cf.Count(); got != 1 {
		t.Fatalf("expected count 1 after insert, got %d", got)
	}

	var successes int32
	var wg sync.WaitGroup
	wg.Add(workers)
	for n := 0; n < workers; n++ {
		go func(n int) {
			defer wg.Done()
			if n%2 == 0 {
				if cf.Delete([]byte("only-key")) {
					atomic.AddInt32(&successes, 1)
				}
				return
			}
			if cf.Delete([]byte(fmt.Sprintf("missing-%d", n))) {
				t.Errorf("deleted a key that was never inserted")
			}
		}(n)
	}
	wg.Wait()

	if got := atomic.LoadInt32(&successes); got != 1 {
		t.Fatalf("expected exactly one successful concurrent deletion, got %d", got)
	}
	if got := cf.Count(); got != 0 {
		t.Fatalf("expected count 0, got %d", got)
	}
	if got := cf.LoadFactor(); got != 0 {
		t.Fatalf("expected zero load factor, got %v", got)
	}
}

func TestConcurrentLookupUsesHashSafely(t *testing.T) {
	cf := cfilter.New(cfilter.Size(128), cfilter.BucketSize(4))
	const entries = 128
	for n := 0; n < entries; n++ {
		if !cf.Insert([]byte(fmt.Sprintf("lookup-%d", n))) {
			t.Fatalf("failed to insert lookup-%d", n)
		}
	}

	var wg sync.WaitGroup
	wg.Add(entries)
	for n := 0; n < entries; n++ {
		go func(n int) {
			defer wg.Done()
			if !cf.Lookup([]byte(fmt.Sprintf("lookup-%d", n))) {
				t.Errorf("failed to find lookup-%d", n)
			}
		}(n)
	}
	wg.Wait()
}

func TestFailedInsertKeepsExistingEntries(t *testing.T) {
	cf := cfilter.New(
		cfilter.Size(4),
		cfilter.BucketSize(2),
		cfilter.FingerprintSize(8),
		cfilter.MaximumKicks(500),
	)

	var inserted [][]byte
	for n := 0; n < 1000; n++ {
		key := []byte(fmt.Sprintf("capacity-%d", n))
		if !cf.Insert(key) {
			break
		}
		inserted = append(inserted, key)
	}
	if len(inserted) == 0 {
		t.Fatal("expected to insert some entries before saturation")
	}
	if got := cf.Count(); got != uint(len(inserted)) {
		t.Fatalf("count %d != inserted %d", got, len(inserted))
	}

	for _, key := range inserted {
		if !cf.Lookup(key) {
			t.Fatalf("failed insert corrupted existing entry %q", key)
		}
	}
	for _, key := range inserted {
		if !cf.Delete(key) {
			t.Fatalf("expected to delete %q", key)
		}
	}
	if got := cf.Count(); got != 0 {
		t.Fatalf("expected empty filter, got count %d", got)
	}
}
