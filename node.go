package radixdb

import (
	"bytes"
	"hash/crc32"
	"sort"
)

// node represents an in-memory node of a Radix tree. This implementation
// is designed to be memory-efficient by using a minimal set of fields to
// represent each node. In a Radix tree, the node's key inherently carries
// significant information, hence reducing the need to maintain metadata.
type node struct {
	key      []byte  // Path segment of the node.
	value    []byte  // Data associated with this node, if any.
	isRecord bool    // True if node is a record; false if path component.
	children []*node // Pointers to child nodes.
	checksum uint32
}

// hasChidren returns true if the receiver node has children.
func (n node) hasChildren() bool {
	return len(n.children) > 0
}

// isLeaf returns true if the receiver node is a leaf node.
func (n node) isLeaf() bool {
	return len(n.children) == 0
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

// updateChecksum computes and updates the checksum for the node using CRC32.
func (n *node) updateChecksum() error {
	h := crc32.NewIEEE()

	if _, err := h.Write(n.key); err != nil {
		return err
	}

	if _, err := h.Write(n.value); err != nil {
		return err
	}

	// The isRecord field is equally as important as the key and value
	// fields since an incorrect value entails a corrupt tree.
	if n.isRecord {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}

	n.checksum = h.Sum32()

	return nil
}

// shallowCopyFrom copies the properties from the src node to the receiver node.
// This function performs a shallow copy, meaning that the copied fields share
// memory references with the original and are not actual copies. The function
// is intended for cases where sustaining the receiver's address is necessary.
func (n *node) shallowCopyFrom(src *node) {
	n.key = src.key
	n.value = src.value
	n.isRecord = src.isRecord
	n.children = src.children

	n.updateChecksum()
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
}
