// Package radixdb provides a persistable Radix tree implementation.
package radixdb

import (
	"errors"
	"sync"
)

var (
	// ErrDuplicateKey is returned when an insertion is attempted using a
	// key that already exists in the database.
	ErrDuplicateKey = errors.New("cannot insert duplicate key")

	// ErrNilKey is returned when an insertion is attempted using a nil key.
	ErrNilKey = errors.New("key cannot be nil")
)

// node represents an in-memory node of a Radix tree. This implementation
// is designed to be memory-efficient by using a minimal set of fields to
// represent each node. In a Radix tree, the node's key inherently carries
// significant information, hence reducing the need to maintain metadata.
type node struct {
	key      []byte  // Path segment of the node.
	value    any     // Data associated with this node, if any.
	children []*node // Pointers to child nodes.
}

// RadixDB represents an in-memory Radix tree, providing concurrency-safe read
// and write APIs. It maintains a reference to the root node and tracks various
// metadata such as the total number of nodes.
type RadixDB struct {
	root     *node        // Pointer to the root node.
	numNodes uint64       // Number of nodes in the tree.
	mu       sync.RWMutex // RWLock for concurrency management.
}

// Empty returns true if the tree is empty. This function is the exported
// concurrency-safe version of empty().
func (rdb *RadixDB) Empty() bool {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	return rdb.empty()
}

// Len returns the number of nodes in the tree as uint64.
func (rdb *RadixDB) Len() uint64 {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	return rdb.numNodes
}

// Insert adds a new key-value pair to the tree. The function returns an error
// if a duplicate or nil key is detected. A write lock is acquired during the
// operation to ensure concurrency safety.
func (rdb *RadixDB) Insert(key []byte, value any) error {
	if key == nil {
		return ErrNilKey
	}

	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	newNode := &node{
		key:      key,
		value:    value,
		children: []*node{},
	}

	// The tree is empty: Simply set newNode as the root.
	if rdb.empty() {
		rdb.root = newNode
		rdb.numNodes = 1

		return nil
	}

	var parent *node
	var current = rdb.root

	for {
		prefix := longestCommonPrefix(current.key, key)

		// Exact match: Duplicate insertion is disallowed.
		if len(prefix) == len(current.key) && len(prefix) == len(newNode.key) {
			return ErrDuplicateKey
		}

		// Partial match: Insert newNode by splitting the curent node.
		// Meeting this condition means that the key has been exhausted.
		if len(prefix) > 0 && len(prefix) < len(current.key) {
			rdb.splitNode(parent, current, newNode, prefix)
			return nil
		}

		// Search for a child node whose key is compatible with the remaining
		// portion of the key. If there is no such child, it means that we are
		// at the deepest level of the tree for the given key.
		key = key[len(prefix):]
		nextNode := current.findCompatibleChild(key)

		if nextNode == nil {
			if len(current.children) > 0 {
				current.children = append(current.children, newNode)
				rdb.numNodes += 1
			} else {
				rdb.splitNode(parent, current, newNode, prefix)
			}

			return nil
		}

		// Reaching this point means that a compatible child was found.
		// Update relevant iterators and continue traversing the tree until
		// we reach a leaf node or no further nodes are available.
		newNode.key = newNode.key[len(prefix):]
		parent = current
		current = nextNode
	}
}

// empty returns true if the tree is empty.
func (rdb *RadixDB) empty() bool {
	return rdb.root == nil && rdb.numNodes == 0
}

// splitNode divides a node into two nodes based on a common prefix, creating
// an intermediate parent node. It does so by updating the keys of the current
// and new nodes to contain only the suffixes after the common prefix.
func (rdb *RadixDB) splitNode(parent *node, current *node, newNode *node, commonPrefix []byte) {
	current.key = current.key[len(commonPrefix):]
	newNode.key = newNode.key[len(commonPrefix):]

	newParent := &node{
		key:      commonPrefix,
		children: []*node{current, newNode},
	}

	// Splitting the root node only requires setting the new branch as root.
	if parent == nil && current == rdb.root {
		rdb.root = newParent
		rdb.numNodes += 1
		return
	}

	// Update the parent of the current node to point at splitNode.
	for i, child := range parent.children {
		if child == current {
			parent.children[i] = newParent
			rdb.numNodes += 1
			return
		}
	}
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

// longestCommonPrefix compares the two given byte slices, and returns the
// longest common prefix. It ensures memory-safety by establishing an index
// boundary based on the length of the shorter byte slice.
func longestCommonPrefix(a, b []byte) []byte {
	minLen := len(a)

	if len(b) < minLen {
		minLen = len(b)
	}

	var i int

	for i = 0; i < minLen; i++ {
		if a[i] != b[i] {
			break
		}
	}

	if i == 0 {
		return nil
	}

	return a[:i]
}
