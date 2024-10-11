package radixdb

import (
	"bytes"
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
}

// hasChidren returns true if the receiver node has children.
func (node node) hasChildren() bool {
	return len(node.children) > 0
}

// isLeaf returns true if the receiver node is a leaf node.
func (node node) isLeaf() bool {
	return len(node.children) == 0
}

// findCompatibleChild searches through the child nodes of the receiver node.
// It returns the first child node that shares a common prefix. If no child is
// found, the function returns nil.
func (node node) findCompatibleChild(key []byte) *node {
	for _, child := range node.children {
		prefix := longestCommonPrefix(child.key, key)

		if len(prefix) > 0 {
			return child
		}
	}

	return nil
}

// findChild returns the node's child that matches the given key.
// If not found, an ErrKeyNotFound error is returned.
func (node node) findChild(key []byte) (*node, int, error) {
	index := sort.Search(len(node.children), func(i int) bool {
		return bytes.Compare(node.children[i].key, key) >= 0
	})

	if index >= len(node.children) || longestCommonPrefix(node.children[index].key, key) == nil {
		return nil, -1, ErrKeyNotFound
	}

	return node.children[index], index, nil
}

// addChild efficiently adds the given child to the node's children slice
// while preserving lexicographic order based on the child's key.
func (node *node) addChild(child *node) {
	// Binary search for the correct position to insert the new child.
	// This is faster than appending the child and then calling sort.Slice().
	index := sort.Search(len(node.children), func(i int) bool {
		return bytes.Compare(node.children[i].key, child.key) >= 0
	})

	// Expand the slice by one element, making room for the new child.
	node.children = append(node.children, nil)

	// Shift elements to the right to make space at the index.
	copy(node.children[index+1:], node.children[index:])

	// Insert the child in its correct position.
	node.children[index] = child
}

// removeChild removes a child from the node's (sorted) children slice. It does
// so by identifying the index of the child using binary search.
func (node *node) removeChild(child *node) error {
	index := sort.Search(len(node.children), func(i int) bool {
		return bytes.Compare(node.children[i].key, child.key) >= 0
	})

	if index >= len(node.children) || longestCommonPrefix(node.children[index].key, child.key) == nil {
		return ErrKeyNotFound
	}

	// Remove the child node at the index by shifting the elements after the
	// index to the left. In other words, the shift overwrites the child node.
	// We then truncate the slice by one element to remove the empty space.
	copy(node.children[index:], node.children[index+1:])
	node.children = node.children[:len(node.children)-1]

	return nil
}

// sortChildren sorts the node's children by their keys in lexicographical order.
// The comparison is based on the byte-wise lexicographical order of the keys.
func (node *node) sortChildren() {
	sort.Slice(node.children, func(i, j int) bool {
		return bytes.Compare(node.children[i].key, node.children[j].key) < 0
	})
}
