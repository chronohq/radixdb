package radixdb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

// node represents an in-memory node of a Radix tree. This implementation
// is designed to be memory-efficient by using a minimal set of fields to
// represent each node. In a Radix tree, the node's key inherently carries
// significant information, hence reducing the need to maintain metadata.
// Adding fields to this struct can significantly increase memory overhead.
// Think carefully before adding anything to the struct.
type node struct {
	key         []byte // Path segment of the node.
	isRecord    bool   // True if node is a record; false if path component.
	isBlob      bool   // True if the value is stored in the blob store.
	firstChild  *node  // Pointer to the first child, if any.
	nextSibling *node  // Pointer to the next sibling, if any.
	numChildren uint16 // Number of children.
	checksum    uint32 // CRC32 checksum of the node content.

	// Holds the content of the node. Values less than or equal to 32-bytes
	// are stored directly in this byte slice. Otherwise, it holds the blobID
	// that points to the content in the blobStore.
	data []byte
}

// hasChidren returns true if the receiver node has children.
func (n node) hasChildren() bool {
	return n.firstChild != nil
}

// isLeaf returns true if the receiver node is a leaf node.
func (n node) isLeaf() bool {
	return n.firstChild == nil
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

// serializedSize returns the size of the serialized node representation.
func (n node) serializedSize() int {
	ret := sizeOfUint8  // flags
	ret += sizeOfUint16 // numChildren
	ret += sizeOfUint16 // keyLen
	ret += sizeOfUint32 // dataLen

	// variable-length key
	ret += len(n.key)

	// variable-length value
	ret += len(n.data)

	// firstChild and nextSibling offsets
	ret += sizeOfUint64
	ret += sizeOfUint64

	// serialized node checksum
	ret += sizeOfUint32

	return ret
}

// findCompatibleChild searches through the child nodes of the receiver node.
// It returns the first child node that shares a common prefix. If no child is
// found, the function returns nil.
func (n node) findCompatibleChild(key []byte) *node {
	for child := n.firstChild; child != nil; child = child.nextSibling {
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
	// TODO: This index likely only makes sense with the legacy children
	// slice. Investigate if it can be removed.
	index := 0

	for child := n.firstChild; child != nil; child = child.nextSibling {
		if bytes.Equal(child.key, key) {
			return child, index, nil
		}

		index++
	}

	return nil, -1, ErrKeyNotFound
}

// addChild efficiently adds the given child to the node's children while
// preserving lexicographic order based on the child's key. The children
// form a singly-linked list starting from firstChild and connected via
// nextSibling pointers.
func (n *node) addChild(child *node) {
	n.numChildren++

	// If the node has no children, the new child becomes the firstChild.
	if n.firstChild == nil {
		n.firstChild = child
		return
	}

	// The new child's key value is less than the firstChild's key. Therefore
	// the new child takes place of the firstChild's place, and the firstChild
	// becomes the nextSibling of the new child.
	if bytes.Compare(child.key, n.firstChild.key) < 0 {
		child.nextSibling = n.firstChild
		n.firstChild = child
		return
	}

	// Reaching here means that the new child's key value is greater than the
	// firstChild's key. Search for a sibling that has a greater value than the
	// new child's key. If found, the new child takes place before that sibling
	// by updating the previous sibling's nextSibling pointer.
	current := n.firstChild

	for current.nextSibling != nil && bytes.Compare(current.nextSibling.key, child.key) < 0 {
		current = current.nextSibling
	}

	child.nextSibling = current.nextSibling
	current.nextSibling = child
}

// removeChild removes a child from the node's linked-list of children slice.
func (n *node) removeChild(child *node) error {
	if n.firstChild == nil {
		return ErrKeyNotFound
	}

	// Removing the first child: nextSibling takes over its place.
	if bytes.Equal(n.firstChild.key, child.key) {
		n.firstChild = n.firstChild.nextSibling
		n.numChildren--

		return nil
	}

	current := n.firstChild

	for current.nextSibling != nil {
		next := current.nextSibling

		if bytes.Equal(next.key, child.key) {
			current.nextSibling = next.nextSibling
			n.numChildren--

			return nil
		}

		current = next
	}

	return ErrKeyNotFound
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
	n.firstChild = src.firstChild
	n.nextSibling = src.nextSibling

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
func (n node) asDescriptor() (persistentNode, error) {
	ret := persistentNode{flags: 0}

	if !n.verifyChecksum() {
		return ret, ErrInvalidChecksum
	}

	if n.isRecord {
		ret.flags |= flagIsRecord
	}

	if n.isBlob {
		ret.flags |= flagHasBlob
	}

	if n.numChildren > maxChildPerNode {
		return ret, ErrInvalidIndex
	}

	if len(n.key) > maxKeyBytes {
		return ret, ErrInvalidIndex
	}

	ret.childOffsets = make([]uint64, n.numChildren)
	ret.numChildren = n.numChildren
	ret.keyLen = uint16(len(n.key))
	ret.dataLen = uint32(len(n.data))
	ret.key = n.key
	ret.data = n.data

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
	numChildren := n.numChildren

	if err := binary.Write(&buf, binary.LittleEndian, numChildren); err != nil {
		return nil, err
	}

	// Step 4: Reserve the space to hold the offsets for firstChild and nextSibling.
	if err := binary.Write(&buf, binary.LittleEndian, uint64(0)); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, uint64(0)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
