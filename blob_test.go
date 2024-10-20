package radixdb

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestBlobStorePut(t *testing.T) {
	store := blobStore{}

	tests := []struct {
		value            []byte
		expectedBlobID   blobID
		expectedRefCount int
	}{
		{[]byte("apple"), sha256.Sum256([]byte("apple")), 1},
		{[]byte("apple"), sha256.Sum256([]byte("apple")), 2},
		{[]byte("apple"), sha256.Sum256([]byte("apple")), 3},
	}

	for _, test := range tests {
		blobID := store.put(test.value)

		if !bytes.Equal(blobID.toSlice(), test.expectedBlobID.toSlice()) {
			t.Errorf("unexpected blobID, got:%q, want:%q", blobID, test.expectedBlobID)
		}

		value := store.getValue(blobID)

		if !bytes.Equal(value, test.value) {
			t.Errorf("unexpected blob, got:%q, want:%q", value, test.value)
		}

		if got := store[blobID].refCount; got != test.expectedRefCount {
			t.Errorf("unexpected refCount, got:%d, want:%d", got, test.expectedRefCount)
		}
	}
}
