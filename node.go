// Copyright Chrono Technologies LLC
// SPDX-License-Identifier: MIT

package arc

import "bytes"

// node represents an in-memory node of a Radix tree. This implementation is
// designed to be memory-efficient by maintaining a minimal set of fields for
// both node representation and persistence metadata. Consider memory overhead
// carefully before adding new fields to this struct.
type node struct {
	key         []byte // Path segment of the node.
	isRecord    bool   // False if the node is a path component.
	numChildren int    // Number of connected child nodes.
	firstChild  *node  // Pointer to the first child node.
	nextSibling *node  // Pointer to the adjacent sibling node.

	// Holds the node's content. For values less than or equal to 32 bytes,
	// it stores the content directly. For larger values, it stores a blobID
	// that references the content in the blobStore.
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

// forEachChild loops over the children of the node, and calls the given
// callback function on each visit.
func (n node) forEachChild(cb func(int, *node) error) error {
	if n.firstChild == nil {
		return nil
	}

	child := n.firstChild

	for i := 0; child != nil; i++ {
		if err := cb(i, child); err != nil {
			return err
		}

		child = child.nextSibling
	}

	return nil
}

// findChild returns the node's child that matches the given key.
func (n node) findChild(key []byte) (*node, error) {
	for child := n.firstChild; child != nil; child = child.nextSibling {
		if bytes.Equal(child.key, key) {
			return child, nil
		}
	}

	return nil, ErrKeyNotFound
}

// findCompatibleChild returns the first child that shares a common prefix.
func (n node) findCompatibleChild(key []byte) *node {
	for child := n.firstChild; child != nil; child = child.nextSibling {
		prefix := longestCommonPrefix(child.key, key)

		if len(prefix) > 0 {
			return child
		}
	}

	return nil
}

// setKey updates the node's key with the provided value.
func (n *node) setKey(key []byte) {
	n.key = key
}

// setValue sets the given value to the node and flags it as a record node.
func (n *node) setValue(value []byte) {
	n.data = value
	n.isRecord = true
}

// addChild inserts the given child into the node's sorted linked-list of
// children. Children are maintained in ascending order by their key values.
func (n *node) addChild(child *node) {
	n.numChildren++

	// Empty list means the given child becomes the firstChild.
	if n.firstChild == nil {
		n.firstChild = child
		return
	}

	// Insert at start if the given child's key is smallest.
	if bytes.Compare(child.key, n.firstChild.key) < 0 {
		child.nextSibling = n.firstChild
		n.firstChild = child
		return
	}

	// Find the insertion point by advancing until we find a node whose next
	// sibling has a key greater than or equal to the given child's key, or
	// until we reach the end of the list.
	current := n.firstChild

	for current.nextSibling != nil && bytes.Compare(current.nextSibling.key, child.key) < 0 {
		current = current.nextSibling
	}

	// Insert the given child between current and its nextSibling.
	// current -> child -> current.nextSibling
	child.nextSibling = current.nextSibling
	current.nextSibling = child
}

// removeChild removes the child node that matches the given child's key.
func (n *node) removeChild(child *node) error {
	if n.firstChild == nil {
		return ErrKeyNotFound
	}

	// Special case: removing first child.
	if bytes.Equal(n.firstChild.key, child.key) {
		n.firstChild = n.firstChild.nextSibling
		n.numChildren--

		return nil
	}

	// Search for a node whose nextSibling matches the given child's key.
	current := n.firstChild

	for current.nextSibling != nil {
		next := current.nextSibling

		if bytes.Equal(next.key, child.key) {
			// Remove the node by updating the link to skip it.
			current.nextSibling = next.nextSibling
			n.numChildren--

			return nil
		}

		current = next
	}

	return ErrKeyNotFound
}
