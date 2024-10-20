package radixdb

import "crypto/sha256"

const (
	// Length of the record value hash in bytes.
	blobIDLen = 32
)

// blobID is a 32-byte fixed length byte array representing the SHA-256 hash of
// a record value. It is an array instead of a slice for map key compatibility.
type blobID [blobIDLen]byte

// blobStoreEntry represents a value and its reference count within the
// blobStore. The value field stores the actual value data, and refCount
// tracks the number of active references to the value.
type blobStoreEntry struct {
	value    []byte
	refCount int
}

// blobStore maps blobIDs to their corresponding byte slices. This type is used
// to store values that exceed the 32-byte length threshold.
type blobStore map[blobID]*blobStoreEntry

// buildBlobID builds a blobID from the given byte slice. It requires that the
// given byte slice length matches the blobID length (32-bytes).
func buildBlobID(src []byte) (blobID, error) {
	var ret blobID

	if len(src) != blobIDLen {
		return ret, ErrInvalidBlobID
	}

	copy(ret[:], src)

	return ret, nil
}

// toSlice returns the given blobID as a byte slice.
func (id blobID) toSlice() []byte {
	return id[:]
}

// getValue returns the blob value that matches the given blobID.
func (b blobStore) getValue(id blobID) []byte {
	blob, found := b[id]

	if !found {
		return nil
	}

	// Create a copy of the value since returning a pointer to the underlying
	// value can have serious implications, such as breaking data integrity.
	ret := make([]byte, len(blob.value))
	copy(ret, blob.value)

	return ret
}

// put adds a new value to the blobStore or increments the reference count
// of an existing value. It returns the blobID of the stored value.
func (b blobStore) put(value []byte) blobID {
	k := blobID(sha256.Sum256(value))

	if entry, found := b[k]; found {
		entry.refCount++
	} else {
		b[k] = &blobStoreEntry{value: value, refCount: 1}
	}

	return k
}
