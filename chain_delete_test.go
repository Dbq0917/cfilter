package cfilter_test

import (
	"math/rand"
	"testing"

	"github.com/irfansharif/cfilter"
)

func TestDeleteFollowsDisplacementChain(t *testing.T) {
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

	oldest := keys[0]
	if !cf.Lookup(oldest) {
		t.Fatal("expected oldest fingerprint to be findable through its displacement chain")
	}

	if !cf.Delete(oldest) {
		t.Fatal("expected delete to follow the displacement chain and remove oldest fingerprint")
	}

	if cf.Lookup(oldest) {
		t.Fatal("expected oldest fingerprint to be absent after chained deletion")
	}

	if got := cf.Count(); got != uint(len(keys)-1) {
		t.Fatalf("expected count %d after deletion, got %d", len(keys)-1, got)
	}

	for _, key := range keys[1:] {
		if !cf.Delete(key) {
			t.Fatalf("expected to delete %q", key)
		}
		if cf.Lookup(key) {
			t.Fatalf("expected %q to be absent after deletion", key)
		}
	}

	if got := cf.Count(); got != 0 {
		t.Fatalf("expected empty filter, got count %d", got)
	}
}
