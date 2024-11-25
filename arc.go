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
	// ErrCorrupted is returned when a database corruption is detected.
	ErrCorrupted = errors.New("database corruption detected")

	// ErrDuplicateKey is returned when an insertion is attempted using a
	// key that already exists in the database.
	ErrDuplicateKey = errors.New("cannot insert duplicate key")

	// ErrKeyNotFound is returned when the key does not exist in the index.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyTooLarge is returned when the key size exceeds the 64KB limit.
	ErrKeyTooLarge = errors.New("key is too large")

	// ErrNilKey is returned when an insertion is attempted using a nil key.
	ErrNilKey = errors.New("key cannot be nil")

	// ErrValueTooLarge is returned when the value size exceeds the 4GB limit.
	ErrValueTooLarge = errors.New("value is too large")
)

const (
	maxUint16     = (1 << 16) - 1 // maxUint16 is the maximum value of uint16.
	maxUint32     = (1 << 32) - 1 // maxUint32 is the maximum value of uint32.
	maxKeyBytes   = maxUint16     // maxKeyBytes is the maximum key size.
	maxValueBytes = maxUint32     // maxValueBytes is the maximum value size.

	inlineValueThreshold = blobIDLen
)

// Arc represents the API interface of a space-efficient key-value database that
// combines a Radix tree for key indexing and a space-optimized blob store.
type Arc struct {
	root       *node        // Pointer to the root node.
	numNodes   int          // Number of nodes in the tree.
	numRecords int          // Number of records in the tree.
	mu         sync.RWMutex // RWLock for concurrency management.

	// Stores deduplicated values that are larger than 32 bytes.
	blobs blobStore
}

// New returns an empty Arc database handler.
func New() *Arc {
	return &Arc{blobs: blobStore{}}
}

// Len returns the number of records.
func (a *Arc) Len() int {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.numRecords
}

// Add inserts a new key-value pair in the database. It returns ErrDuplicateKey
// if the key already exists.
func (a *Arc) Add(key []byte, value []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.insert(key, value, false)
}

// Put inserts or updates a key-value pair in the database.
func (a *Arc) Put(key []byte, value []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.insert(key, value, true)
}

// insert adds a key-value pair to the database. If the key already exists and
// overwrite is true, the existing value is updated. If overwrite is false and
// the key exists, ErrDuplicateKey is returned. It returns nil on success.
func (a *Arc) insert(key []byte, value []byte, overwrite bool) error {
	if key == nil {
		return ErrNilKey
	}

	if len(key) > maxKeyBytes {
		return ErrKeyTooLarge
	}

	if len(value) > maxValueBytes {
		return ErrValueTooLarge
	}

	// Empty tree, set the new record node as the root node.
	if a.empty() {
		a.root = newRecordNode(a.blobs, key, value)
		a.numNodes = 1
		a.numRecords = 1

		return nil
	}

	// Create a common root node for keys with no shared prefix.
	if len(a.root.key) > 0 && longestCommonPrefix(a.root.key, key) == nil {
		oldRoot := a.root

		a.root = &node{key: nil}
		a.root.addChild(oldRoot)
		a.root.addChild(newRecordNode(a.blobs, key, value))

		a.numNodes += 2
		a.numRecords++

		return nil
	}

	var parent *node
	var current = a.root

	for {
		prefix := longestCommonPrefix(current.key, key)
		prefixLen := len(prefix)

		// Found exact match. Put() will overwrite the existing value.
		// Do not update counters because this is an in-place update.
		if prefixLen == len(current.key) && prefixLen == len(key) {
			if !overwrite {
				return ErrDuplicateKey
			}

			if !current.isRecord {
				a.numRecords++
			}

			current.setValue(a.blobs, value)

			return nil
		}

		// The longest common prefix matches the entire key, but is shorter
		// than current's key. The new key becomes the parent of current.
		//
		// For example, suppose the key is "app" and current.key is "apple".
		// The longest common prefix is "app". Therefore "apple" is updated to
		// "le", and then becomes a child of the "app" node, forming the path:
		// ["app"(new node) -> "le"(current)].
		if prefixLen == len(key) && prefixLen < len(current.key) {
			if current == a.root {
				current.setKey(current.key[len(key):])

				a.root = newRecordNode(a.blobs, key, value)
				a.root.addChild(current)
			} else {
				if err := parent.removeChild(current); err != nil {
					return err
				}

				current.setKey(current.key[len(key):])

				n := newRecordNode(a.blobs, key, value)
				n.addChild(current)

				parent.addChild(n)
			}

			a.numNodes++
			a.numRecords++

			return nil
		}

		// Partial match with key exhaustion: Insert via node splitting.
		if prefixLen > 0 && prefixLen < len(current.key) {
			a.splitNode(parent, current, newRecordNode(a.blobs, key, value), prefix)
			return nil
		}

		// Search for a child whose key is compatible with the remaining
		// portion of the key. Start by removing the prefix from the key.
		key = key[prefixLen:]
		nextNode := current.findCompatibleChild(key)

		// No existing path matches the remaining key portion. The new record
		// will be inserted as a leaf node. At this point, current's key must
		// fully match the key prefix because:
		//
		// 1. "No common prefix" cases are handled earlier in the function
		// 2. Partial prefix match would have triggered splitNode()
		//
		// TODO(toru): These conditions can likely be further simplified.
		if nextNode == nil {
			if current == a.root {
				if a.root.key == nil || prefixLen == len(a.root.key) {
					a.root.addChild(newRecordNode(a.blobs, key, value))
				}
			} else {
				current.addChild(newRecordNode(a.blobs, key, value))
			}

			a.numNodes++
			a.numRecords++
			return nil
		}

		// Reaching this point means that a compatible child was found.
		// Update relevant iterators and continue traversing the tree until
		// we reach a leaf node or no further nodes are available.
		parent = current
		current = nextNode
	}
}

// Get retrieves the value that matches the given key. Returns ErrKeyNotFound
// if the key does not exist.
func (a *Arc) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	node, _, err := a.findNodeAndParent(key)

	if err != nil {
		return nil, err
	}

	if !node.isRecord {
		return nil, ErrKeyNotFound
	}

	return node.value(a.blobs), nil
}

// Delete removes a record that matches the given key.
func (a *Arc) Delete(key []byte) error {
	if key == nil {
		return ErrNilKey
	}

	if a.empty() {
		return ErrKeyNotFound
	}

	if len(key) > maxKeyBytes {
		return ErrKeyTooLarge
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	delNode, parent, err := a.findNodeAndParent(key)

	if err != nil {
		return err
	}

	if !delNode.isRecord {
		return ErrKeyNotFound
	}

	// Root node deletion is handled separately to improve code readability.
	if delNode == a.root {
		a.deleteRootNode()
		return nil
	}

	// If the deletion node is not a root node, its parent must be non-nil.
	if parent == nil {
		return ErrCorrupted
	}

	// The deletion node only has one child. Therefore the child will take
	// place of the deletion node, after inheriting deletion node's key.
	if delNode.numChildren == 1 {
		if err := parent.removeChild(delNode); err != nil {
			return err
		}

		child := delNode.firstChild
		child.prependKey(delNode.key)
		parent.addChild(child)

		a.numNodes--
		a.numRecords--

		return nil
	}

	// In most cases, deleting a leaf node is simply a matter of removing it
	// from its parent.  However, if the parent is a non-record node and the
	// deletion leaves it with only a single child, we must merge the nodes.
	if delNode.isLeaf() {
		if err := parent.removeChild(delNode); err != nil {
			return err
		}

		a.numNodes--
		a.numRecords--

		// The deletion had left the non-record parent with one child. This
		// means that the parent node is now redundant. Therefore merge the
		// parent and the only-child nodes.
		if !parent.isRecord && parent.numChildren == 1 {
			child := parent.firstChild
			child.prependKey(parent.key)

			// Save the parent's sibling before overwriting it.
			sibling := parent.nextSibling

			// We do not have access to the grandparent, therefore shallow copy
			// the child node's information to the parent node. This effectively
			// replaces parent with child within the index tree structure.
			parent.shallowCopyFrom(child)
			parent.nextSibling = sibling

			// Decrement for removing the parent node.
			a.numNodes--
		}

		return nil
	}

	// Reaching this point means we are deleting a non-root internal node
	// that has more than one edges. Convert the node to a non-record type.
	delNode.isRecord = false
	delNode.deleteValue(a.blobs)

	a.numRecords--

	return nil
}

// deleteRootNode removes the root node from the tree, while ensuring that
// the tree structure remains valid and consistent.
func (a *Arc) deleteRootNode() {
	if a.root.isLeaf() {
		a.clear()
		return
	}

	if a.root.numChildren == 1 {
		// The root node only has one child, which will become the new root.
		child := a.root.firstChild
		child.prependKey(a.root.key)

		a.root = child

		// Decrement for the original root node removal.
		a.numNodes--

	} else {
		// The root node has multiple children, thus it must continue to exist
		// for the tree to sustain its structure. Convert it to a non-record
		// node by removing its value and flagging it as a non-record node.
		a.root.isRecord = false
		a.root.deleteValue(a.blobs)
	}

	a.numRecords--
}

// clear wipes the in-memory tree, and resets metadata. This function is
// intended for development and testing purposes only.
func (a *Arc) clear() {
	a.root = nil
	a.numNodes = 0
	a.numRecords = 0
	a.blobs = blobStore{}
}

// empty returns true if the database is empty.
func (a *Arc) empty() bool {
	return a.root == nil && a.numRecords == 0
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

	// Reset current's nextSibling in prep for becoming a child of newParent.
	current.nextSibling = nil

	// Remove the common prefix from current and newNode.
	current.setKey(current.key[len(commonPrefix):])
	newNode.setKey(newNode.key[len(commonPrefix):])

	newParent.addChild(current)
	newParent.addChild(newNode)

	a.numNodes += 2
	a.numRecords++
}

// findNodeAndParent returns the node that matches the given key and its parent.
// The parent is nil if the discovered node is a root node.
func (a *Arc) findNodeAndParent(key []byte) (current *node, parent *node, err error) {
	if key == nil {
		return nil, nil, ErrNilKey
	}

	if a.empty() {
		return nil, nil, ErrKeyNotFound
	}

	current = a.root

	for {
		prefix := longestCommonPrefix(current.key, key)
		prefixLen := len(prefix)

		// Lack of a common prefix means that the key does not exist in the
		// tree, unless the current node is a root node.
		if prefix == nil && current != a.root {
			return nil, nil, ErrKeyNotFound
		}

		// Common prefix must be at least the length of the current key.
		// If not, the search key cannot exist in a Radix tree.
		if prefixLen != len(current.key) {
			return nil, nil, ErrKeyNotFound
		}

		// The prefix matches the current node's key.
		if prefixLen == len(key) {
			return current, parent, nil
		}

		if !current.hasChildren() {
			return nil, nil, ErrKeyNotFound
		}

		// Update the key for the next iteration, and then continue traversing.
		key = key[len(prefix):]
		parent = current
		current = current.findCompatibleChild(key)

		// The key does not exist if a compatible child is not found.
		if current == nil {
			return nil, nil, ErrKeyNotFound
		}
	}
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
