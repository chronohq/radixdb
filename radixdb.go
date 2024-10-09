// Package radixdb provides a persistable Radix tree implementation.
package radixdb

import (
	"bytes"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrDuplicateKey is returned when an insertion is attempted using a
	// key that already exists in the database.
	ErrDuplicateKey = errors.New("cannot insert duplicate key")

	// ErrKeyNotFound is returned when the key does not exist in the tree.
	ErrKeyNotFound = errors.New("key not found")

	// ErrNilKey is returned when an insertion is attempted using a nil key.
	ErrNilKey = errors.New("key cannot be nil")
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
func (rdb *RadixDB) Insert(key []byte, value []byte) error {
	if key == nil {
		return ErrNilKey
	}

	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	newNode := &node{
		key:      key,
		value:    value,
		isRecord: true,
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

		// Exact match found on the node key. If the current node is marked as
		// a record, enforce the no-duplicate key constraint. Otherwise assign
		// the current node with the given value, and mark it as a record.
		if len(prefix) == len(current.key) && len(prefix) == len(newNode.key) {
			if current.isRecord {
				return ErrDuplicateKey
			} else {
				current.value = value
				current.isRecord = true
				rdb.numNodes++
				return nil
			}
		}

		// newNode's key matches the longest common prefix and is shorter than
		// the current node's key. Therefore, newNode logically becomes the
		// parent of the current node, which requires restructuring the tree.
		//
		// For example, suppose newNode.key is "app" and current.key is "apple".
		// The common prefix is "app", and thus "app" becomes the parent of "le".
		if len(prefix) == len(newNode.key) && len(prefix) < len(current.key) {
			current.key = current.key[len(newNode.key):]
			newNode.children = append(newNode.children, current)

			if parent == nil {
				rdb.root = newNode
			} else {
				for i, child := range parent.children {
					if child == current {
						parent.children[i] = newNode
					}
				}
			}

			rdb.numNodes++
			return nil
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
		newNode.key = newNode.key[len(prefix):]

		if nextNode == nil {
			if current == rdb.root {
				// A root node with nil key means that it's an intermediate node
				// with existing edges to child nodes.
				if current.key == nil && current.hasChildren() {
					current.children = append(current.children, newNode)
				} else if len(current.key) == len(prefix) {
					// Common prefix matches the current node's key.
					// Therefore newNode is a child of the current node.
					current.children = append(current.children, newNode)
				} else {
					rdb.root = &node{
						key:      prefix,
						children: []*node{current, newNode},
					}
				}
			} else {
				current.children = append(current.children, newNode)
			}

			rdb.numNodes++
			return nil
		}

		// Reaching this point means that a compatible child was found.
		// Update relevant iterators and continue traversing the tree until
		// we reach a leaf node or no further nodes are available.
		parent = current
		current = nextNode
	}
}

// Get retrieves the value associated with the given key. It returns the value
// as a byte slice along with any potential errors. For example, if the key does
// not exist, ErrNotKeyFound is returned.
func (rdb *RadixDB) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}

	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	if rdb.empty() {
		return nil, ErrKeyNotFound
	}

	current := rdb.root

	for {
		prefix := longestCommonPrefix(current.key, key)

		// Lack of a common prefix means that the key does not exist in the
		// tree, unless the current node is a root node.
		if prefix == nil && current != rdb.root {
			return nil, ErrKeyNotFound
		}

		// Prefix does not match the current node's key. Radix tree's prefix
		// compression algorithm guarantees that the key does not exist.
		if len(prefix) != len(current.key) {
			return nil, ErrKeyNotFound
		}

		// The prefix matches the current node's key. The value can be returned
		// if the current node is holding a record.
		if len(prefix) == len(key) {
			if current.isRecord {
				return current.value, nil
			} else {
				return nil, ErrKeyNotFound
			}
		}

		// Mild optimization to determine if further traversal is necessary.
		if !current.hasChildren() {
			return nil, ErrKeyNotFound
		}

		// Update the key for the next iteration, and then continue traversing.
		// The key does not exist if a compatible child is not found.
		key = key[len(prefix):]
		current = current.findCompatibleChild(key)

		if current == nil {
			return nil, ErrKeyNotFound
		}
	}
}

// empty returns true if the tree is empty.
func (rdb *RadixDB) empty() bool {
	return rdb.root == nil && rdb.numNodes == 0
}

// clear wipes out the entire in-memory tree. This function is internal and
// is not exported because it is intended for testing purposes.
func (rdb *RadixDB) clear() {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	rdb.root = nil
	rdb.numNodes = 0
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
		rdb.numNodes++
		return
	}

	// Update the parent of the current node to point at splitNode.
	for i, child := range parent.children {
		if child == current {
			parent.children[i] = newParent
			rdb.numNodes++
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

// isLeaf returns true if the receiver node is a leaf node.
func (node node) isLeaf() bool {
	return len(node.children) == 0
}

// hasChidren returns true if the receiver node has children.
func (node node) hasChildren() bool {
	return len(node.children) > 0
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

// sortChildren sorts the node's children by their keys in lexicographical order.
// The comparison is based on the byte-wise lexicographical order of the keys.
func (node *node) sortChildren() {
	sort.Slice(node.children, func(i, j int) bool {
		return bytes.Compare(node.children[i].key, node.children[j].key) < 0
	})
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
