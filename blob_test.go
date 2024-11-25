// Copyright Chrono Technologies LLC
// SPDX-License-Identifier: MIT

package arc

import (
	"bytes"
	"testing"
)

func TestBlobStorePut(t *testing.T) {
	store := blobStore{}

	tests := []struct {
		value            []byte
		expectedBlobID   blobID
		expectedRefCount int
	}{
		{[]byte("apple"), makeBlobID([]byte("apple")), 1},
		{[]byte("apple"), makeBlobID([]byte("apple")), 2},
		{[]byte("apple"), makeBlobID([]byte("apple")), 3},
	}

	for _, test := range tests {
		blobID := store.put(test.value)

		if !bytes.Equal(blobID.Slice(), test.expectedBlobID.Slice()) {
			t.Errorf("unexpected blobID: got:%q, want:%q", blobID, test.expectedBlobID)
		}

		value := store.get(blobID[:])

		if !bytes.Equal(value, test.value) {
			t.Errorf("unexpected blob: got:%q, want:%q", value, test.value)
		}

		if got := store[blobID].refCount; got != test.expectedRefCount {
			t.Errorf("unexpected refCount: got:%d, want:%d", got, test.expectedRefCount)
		}
	}
}

func TestBlobThreshold(t *testing.T) {
	arc := New()

	testCases := []struct {
		name string
		key  []byte
		want []byte
	}{
		{"smaller than threshold", []byte("small"), []byte("small value")},
		{"larger than threshold", []byte("large"), bytes.Repeat([]byte("x"), 64)},
		{"exactly at the threshold", []byte("exact"), bytes.Repeat([]byte("x"), 32)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := arc.Put(tc.key, tc.want); err != nil {
				t.Fatalf("unexpected put() error: %v", err)
			}

			got, err := arc.Get(tc.key)

			if err != nil {
				t.Fatalf("unexpected get() error: %v", err)
			}

			if !bytes.Equal(got, tc.want) {
				t.Errorf("incorrect value: key:%q, got:%q, want:%q", tc.key, got, tc.want)
			}
		})
	}

	if len(arc.blobs) != 1 {
		t.Errorf("unexpected blob count: got:%d, want:%d", len(arc.blobs), 1)
	}
}

func TestBlobStoreRelease(t *testing.T) {
	store := blobStore{}
	value := []byte("pineapple")
	refCount := 20

	var blobID blobID

	for i := 0; i < refCount; i++ {
		blobID = store.put(value)
	}

	for i := refCount; i > 0; i-- {
		store.release(blobID.Slice())

		expectedRefCount := i - 1

		if expectedRefCount == 0 {
			if _, found := store[blobID]; found {
				t.Error("expected blob to be removed")
			}
		} else {
			if store[blobID].refCount != expectedRefCount {
				t.Errorf("unexpected refCount: got:%d, want:%d", store[blobID].refCount, expectedRefCount)
			}
		}
	}

	// Test that the store does not panic with an unknown key.
	store.release([]byte("bogus"))

	if len(store) != 0 {
		t.Error("store should be empty")
	}
}
