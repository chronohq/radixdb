// Package radixdb provides a persistable Radix tree implementation.
package radixdb

import "sync"

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

// Empty returns true if the tree is empty.
func (rdb *RadixDB) Empty() bool {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	return rdb.root == nil && rdb.numNodes == 0
}

// Len returns the number of nodes in the tree as uint64.
func (rdb *RadixDB) Len() uint64 {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	return rdb.numNodes
}
