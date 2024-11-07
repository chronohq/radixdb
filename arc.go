// Copyright Chrono Technologies LLC
// SPDX-License-Identifier: MIT

// Package arc implements a key-value database based on a Radix tree data
// structure and deduplication-enabled blob storage. The Radix tree provides
// space-efficient key management through prefix compression, while the blob
// storage handles values with automatic deduplication.
package arc

import (
	"errors"
	"sync"
)

var (
	// ErrKeyNotFound is returned when the key does not exist in the index.
	ErrKeyNotFound = errors.New("key not found")
)

// Arc represents the API interface of a space-efficient key-value database that
// combines a Radix tree for key indexing and a space-optimized blob store.
type Arc struct {
	root       *node        // Pointer to the root node.
	numNodes   int          // Number of nodes in the tree.
	numRecords int          // Number of records in the tree.
	mu         sync.RWMutex // RWLock for concurrency management.
}

// Len returns the number of records.
func (a *Arc) Len() int {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.numRecords
}

// splitNode splits a node based on a common prefix by creating an intermediate
// parent node. For the root node, it simply creates a new parent. For non-root
// nodes, it updates the parent-child relationships before modifying the node
// keys to maintain tree consistency. The current and newNode becomes children
// of the intermediate parent, with their keys updated to contain only their
// suffixes after the common prefix.
func (a *Arc) splitNode(parent *node, current *node, newNode *node, commonPrefix []byte) {
	newParent := &node{key: commonPrefix}

	// Splitting the root node only requires setting the new branch as root.
	if current == a.root {
		current.setKey(current.key[len(commonPrefix):])
		newNode.setKey(newNode.key[len(commonPrefix):])

		newParent.addChild(current)
		newParent.addChild(newNode)

		a.root = newParent
		a.numNodes += 2
		a.numRecords++

		return
	}

	// Splitting the non-root node. Update the parent-child relationship
	// before manipulating the node keys of current and newNode.
	parent.removeChild(current)
	parent.addChild(newParent)

	// The current node is unlinked from its parent, and it is now safe to
	// modify its key. Remove the common prefix from current and newNode.
	current.setKey(current.key[len(commonPrefix):])
	newNode.setKey(newNode.key[len(commonPrefix):])

	newParent.addChild(current)
	newParent.addChild(newNode)

	a.numNodes += 2
	a.numRecords++
}

// longestCommonPrefix compares the two given byte slices, and returns the
// longest common prefix. Memory-safety is ensured by establishing an index
// boundary based on the length of the shorter parameter.
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
