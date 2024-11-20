// Copyright Chrono Technologies LLC
// SPDX-License-Identifier: MIT

package arc

import "crypto/sha256"

const (
	// Length of the blobID in bytes.
	blobIDLen = 32
)

// blobID is a 32-byte fixed-length byte array representing the SHA-256 hash of
// a blob value. It is an array and not a slice for map key compatibility.
type blobID [blobIDLen]byte

// Slice returns the given blobID as a byte slice.
func (id blobID) Slice() []byte {
	return id[:]
}

// newBlobID builds a blobID from the given src byte slice. It requires that the
// given byte slice length matches the blobID length (32 bytes).
func newBlobID(src []byte) (blobID, error) {
	var ret blobID

	if len(src) != blobIDLen {
		return ret, ErrCorrupted
	}

	copy(ret[:], src)

	return ret, nil
}

// blob represents the blob value and its reference count.
type blob struct {
	value    []byte
	refCount int
}

// blobStore maps blobIDs to their corresponding blobs. It is used to store
// values that exceed the 32-byte value length threshold.
type blobStore map[blobID]*blob

// get returns the blob that matches the blobID.
func (bs blobStore) get(id []byte) []byte {
	blobID, err := newBlobID(id)

	if err != nil {
		return nil
	}

	b, found := bs[blobID]

	if !found {
		return nil
	}

	// Create a copy of the value since returning a pointer to the underlying
	// value can have serious implications, such as breaking data integrity.
	ret := make([]byte, len(b.value))
	copy(ret, b.value)

	return ret
}

// put either creates a new blob and inserts it to the blobStore or increments
// the refCount of an existing blob. It returns a blobID on success.
func (bs blobStore) put(value []byte) blobID {
	k := blobID(sha256.Sum256(value))

	if b, found := bs[k]; found {
		b.refCount++
	} else {
		bs[k] = &blob{value: value, refCount: 1}
	}

	return k
}

// release decrements the refCount of a blob if it exists for the given blobID.
// When the refCount reaches zero, the blob is removed from the blobStore.
func (bs blobStore) release(id []byte) {
	blobID, err := newBlobID(id)

	if err != nil {
		return
	}

	if b, found := bs[blobID]; found {
		if b.refCount > 0 {
			b.refCount--
		}

		if b.refCount == 0 {
			delete(bs, blobID)
		}
	}
}
