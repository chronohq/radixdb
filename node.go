package radixdb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sort"
)

// node represents an in-memory node of a Radix tree. This implementation
// is designed to be memory-efficient by using a minimal set of fields to
// represent each node. In a Radix tree, the node's key inherently carries
// significant information, hence reducing the need to maintain metadata.
// Adding fields to this struct can significantly increase memory overhead.
// Think carefully before adding anything to the struct.
type node struct {
	key      []byte  // Path segment of the node.
	isRecord bool    // True if node is a record; false if path component.
	isBlob   bool    // True if the value is stored in the blob store.
	children []*node // Pointers to child nodes.
	checksum uint32  // CRC32 checksum of the node content.

	// Holds the content of the node. Values less than or equal to 32-bytes
	// are stored directly in this byte slice. Otherwise, it holds the blobID
	// that points to the content in the blobStore.
	data []byte
}

// hasChidren returns true if the receiver node has children.
func (n node) hasChildren() bool {
	return len(n.children) > 0
}

// isLeaf returns true if the receiver node is a leaf node.
func (n node) isLeaf() bool {
	return len(n.children) == 0
}

// value retrieves the record value of the node. If the value is stored in the
// blobStore, it fetches the value using the blobID stored in the data field.
func (n node) value(blobs blobStore) []byte {
	ret := n.data

	if n.isBlob {
		blobID, err := buildBlobID(n.data)

		if err != nil {
			return nil
		}

		ret = blobs.getValue(blobID)
	}

	return ret
}

// findCompatibleChild searches through the child nodes of the receiver node.
// It returns the first child node that shares a common prefix. If no child is
// found, the function returns nil.
func (n node) findCompatibleChild(key []byte) *node {
	for _, child := range n.children {
		prefix := longestCommonPrefix(child.key, key)

		if len(prefix) > 0 {
			return child
		}
	}

	return nil
}

// findChild returns the node's child that matches the given key.
// If not found, an ErrKeyNotFound error is returned.
func (n node) findChild(key []byte) (*node, int, error) {
	index := sort.Search(len(n.children), func(i int) bool {
		return bytes.Compare(n.children[i].key, key) >= 0
	})

	if index >= len(n.children) || longestCommonPrefix(n.children[index].key, key) == nil {
		return nil, -1, ErrKeyNotFound
	}

	return n.children[index], index, nil
}

// addChild efficiently adds the given child to the node's children slice
// while preserving lexicographic order based on the child's key.
func (n *node) addChild(child *node) {
	// Binary search for the correct position to insert the new child.
	// This is faster than appending the child and then calling sort.Slice().
	index := sort.Search(len(n.children), func(i int) bool {
		return bytes.Compare(n.children[i].key, child.key) >= 0
	})

	// Expand the slice by one element, making room for the new child.
	n.children = append(n.children, nil)

	// Shift elements to the right to make space at the index.
	copy(n.children[index+1:], n.children[index:])

	// Insert the child in its correct position.
	n.children[index] = child
}

// removeChild removes a child from the node's (sorted) children slice. It does
// so by identifying the index of the child using binary search.
func (n *node) removeChild(child *node) error {
	index := sort.Search(len(n.children), func(i int) bool {
		return bytes.Compare(n.children[i].key, child.key) >= 0
	})

	if index >= len(n.children) || longestCommonPrefix(n.children[index].key, child.key) == nil {
		return ErrKeyNotFound
	}

	// Remove the child node at the index by shifting the elements after the
	// index to the left. In other words, the shift overwrites the child node.
	// We then truncate the slice by one element to remove the empty space.
	copy(n.children[index:], n.children[index+1:])
	n.children = n.children[:len(n.children)-1]

	return nil
}

// calculateChecksum calculates the CRC32 checksum of the receiver node.
func (n node) calculateChecksum() (uint32, error) {
	h := crc32.NewIEEE()

	if _, err := h.Write(n.key); err != nil {
		return 0, err
	}

	if _, err := h.Write(n.data); err != nil {
		return 0, err
	}

	// Include the isRecord field.
	if n.isRecord {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}

	// Include the isBlob field.
	if n.isBlob {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}

	return h.Sum32(), nil
}

// updateChecksum computes and updates the checksum for the node using CRC32.
func (n *node) updateChecksum() error {
	checksum, err := n.calculateChecksum()

	if err != nil {
		return err
	}

	n.checksum = checksum

	return nil
}

// verifyChecksum verifies the integrity of the node's data by comparing the
// stored checksum based on the current node state.
func (n node) verifyChecksum() bool {
	if n.checksum == 0 {
		return false
	}

	checksum, err := n.calculateChecksum()

	if err != nil {
		return false
	}

	return n.checksum == checksum
}

// shallowCopyFrom copies the properties from the src node to the receiver node.
// This function performs a shallow copy, meaning that the copied fields share
// memory references with the original and are not actual copies. The function
// is intended for cases where sustaining the receiver's address is necessary.
func (n *node) shallowCopyFrom(src *node) {
	n.key = src.key
	n.data = src.data
	n.isBlob = src.isBlob
	n.isRecord = src.isRecord
	n.children = src.children

	n.updateChecksum()
}

// setKey updates the node's key with the provided value and recalculates
// the checksum to reflect the update.
func (n *node) setKey(key []byte) {
	n.key = key
	n.updateChecksum()
}

// setValue sets the given value to the node.
func (n *node) setValue(blobs blobStore, value []byte) {
	if len(value) <= inlineValueThreshold {
		n.data = value
		n.isBlob = false
	} else {
		id := blobs.put(value)
		n.data = id.toSlice()
		n.isBlob = true
	}
}

// prependKey prepends the given prefix to the node's existing key.
func (n *node) prependKey(prefix []byte) {
	if len(prefix) == 0 {
		return
	}

	newKey := make([]byte, len(prefix)+len(n.key))

	copy(newKey, prefix)
	copy(newKey[len(prefix):], n.key)

	n.key = newKey
	n.updateChecksum()
}

// asDescriptor returns the nodeDescriptor representation of the node.
func (n node) asDescriptor() (nodeDescriptor, error) {
	ret := nodeDescriptor{
		isRecord: 0,
		isBlob:   0,
	}

	if !n.verifyChecksum() {
		return ret, ErrInvalidChecksum
	}

	if n.isRecord {
		ret.isRecord = 1
	}

	if n.isBlob {
		ret.isBlob = 1
	}

	if len(n.children) > maxChildPerNode {
		return ret, ErrInvalidIndex
	}

	ret.childOffsets = make([]uint64, 0, len(n.children))
	ret.numChildren = uint16(len(n.children))
	ret.dataLen = uint64(len(n.data))
	ret.data = n.data

	ret.checksum = n.checksum

	return ret, nil
}

// serializeWithoutKey converts the receiver node into a platform-agonostic
// binary representation, and returns it as a byte slice. The returned byte
// slice does not contain the node key.
func (n node) serializeWithoutKey() ([]byte, error) {
	var buf bytes.Buffer

	if !n.verifyChecksum() {
		return nil, ErrInvalidChecksum
	}

	// Step 1: Serialize the node checksum.
	if err := binary.Write(&buf, binary.LittleEndian, n.checksum); err != nil {
		return nil, err
	}

	// Step 2: Serialize the value and its length, if the node holds a record.
	if n.isRecord {
		valLen := uint64(len(n.data))

		if err := binary.Write(&buf, binary.LittleEndian, valLen); err != nil {
			return nil, err
		}

		if valLen > 0 {
			if _, err := buf.Write(n.data); err != nil {
				return nil, err
			}
		}
	} else {
		if err := binary.Write(&buf, binary.LittleEndian, uint64(0)); err != nil {
			return nil, err
		}
	}

	// Step 3: Serialize the number of children.
	numChildren := uint64(len(n.children))

	if err := binary.Write(&buf, binary.LittleEndian, numChildren); err != nil {
		return nil, err
	}

	// Step 4: Reserve the space to hold the child node offsets.
	tmpOffset := uint64(0)

	for i := 0; i < int(numChildren); i++ {
		if err := binary.Write(&buf, binary.LittleEndian, tmpOffset); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}
