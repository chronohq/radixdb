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

	// ErrFileCorrupt is returned when a database file corruption is detected.
	ErrFileCorrupt = errors.New("database file corruption detected")

	// ErrInvalidBlobID is returned when an invalid blobID is detected.
	ErrInvalidBlobID = errors.New("invalid blob id detected")

	// ErrInvalidChecksum is returned when the checksum of a node does not match
	// the expected value, indicating potential data corruption or tampering.
	ErrInvalidChecksum = errors.New("checksum mismatch detected")

	// ErrInvalidIndex is returned when an index error is detected.
	ErrInvalidIndex = errors.New("invalid index detected")

	// ErrKeyNotFound is returned when the key does not exist in the tree.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyTooLarge is returned when the key size exceeds the 64KB limit.
	ErrKeyTooLarge = errors.New("key is too large")

	// ErrNilKey is returned when an insertion is attempted using a nil key.
	ErrNilKey = errors.New("key cannot be nil")

	// ErrValueTooLarge is returned when the value size exceeds the 4GB limit.
	ErrValueTooLarge = errors.New("value is too large")
)

const (
	inlineValueThreshold = blobIDLen
	maxKeyBytes          = maxUint16
	maxValueBytes        = maxUint32
)

// RadixDB represents an in-memory Radix tree, providing concurrency-safe read
// and write APIs. It maintains a reference to the root node and tracks various
// metadata such as the total number of nodes.
type RadixDB struct {
	root       *node        // Pointer to the root node.
	numNodes   uint64       // Number of nodes in the tree.
	numRecords uint64       // Number of records in the tree.
	mu         sync.RWMutex // RWLock for concurrency management.
	header     fileHeader   // Header region of the database file.

	// Maps each SHA-256 hash of record values that are larger than
	// 32-bytes to their corresponding unstructured value data.
	blobs blobStore
}

// New initializes and returns a new instance of RadixDB.
func New() *RadixDB {
	ret := &RadixDB{
		header: fileHeader{
			magic:   magicByte,
			version: fileFormatVersion,
		},
		blobs: map[blobID]*blobStoreEntry{},
	}

	return ret
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

	return rdb.numRecords
}

// Insert adds a new key-value pair to the tree. The function returns an
// error if a duplicate or nil key is detected.
func (rdb *RadixDB) Insert(key []byte, value []byte) error {
	if key == nil {
		return ErrNilKey
	}

	if len(key) > maxKeyBytes {
		return ErrKeyTooLarge
	}

	if len(value) > maxValueBytes {
		return ErrValueTooLarge
	}

	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	newNode := &node{
		key:      key,
		isRecord: true,
	}

	newNode.setValue(rdb.blobs, value)
	newNode.updateChecksum()

	// The tree is empty: Simply set newNode as the root.
	if rdb.empty() {
		rdb.root = newNode
		rdb.numNodes = 1
		rdb.numRecords = 1

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
				current.isRecord = true
				current.setValue(rdb.blobs, value)
				current.updateChecksum()

				rdb.numRecords++

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
			current.setKey(current.key[len(newNode.key):])
			newNode.addChild(current)

			if parent == nil {
				rdb.root = newNode
			} else {
				if err := parent.removeChild(current); err != nil {
					return err
				}

				parent.addChild(newNode)
			}

			rdb.numNodes++
			rdb.numRecords++

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
		newNode.setKey(newNode.key[len(prefix):])

		if nextNode == nil {
			if current == rdb.root {
				// A root node with nil key means that it's an intermediate node
				// with existing edges to child nodes.
				if current.key == nil && current.hasChildren() {
					current.addChild(newNode)
					rdb.numNodes++
				} else if len(current.key) == len(prefix) {
					// Common prefix matches the current node's key.
					// Therefore newNode is a child of the current node.
					current.addChild(newNode)
					rdb.numNodes++
				} else {
					rdb.root = &node{key: prefix}
					rdb.root.addChild(current)
					rdb.root.addChild(newNode)
					rdb.root.updateChecksum()

					// Account for the new root node.
					rdb.numNodes += 2
				}
			} else {
				current.addChild(newNode)
				rdb.numNodes++
			}

			rdb.numRecords++
			return nil
		}

		// Reaching this point means that a compatible child was found.
		// Update relevant iterators and continue traversing the tree until
		// we reach a leaf node or no further nodes are available.
		parent = current
		current = nextNode
	}
}

// Get retrieves the value that matches the given key. It returns the value as
// a byte slice along with any potential errors. For example, if the key does
// not exist, ErrNotKeyFound is returned.
func (rdb *RadixDB) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}

	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	node, _, err := rdb.findNodeAndParent(key)

	if err != nil {
		return nil, err
	}

	if !node.isRecord {
		return nil, ErrKeyNotFound
	}

	if !node.verifyChecksum() {
		return nil, ErrInvalidChecksum
	}

	return node.value(rdb.blobs), nil
}

// Delete removes the node that matches the given key.
func (rdb *RadixDB) Delete(key []byte) error {
	if key == nil {
		return ErrNilKey
	}

	if len(key) > maxKeyBytes {
		return ErrKeyTooLarge
	}

	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	node, parent, err := rdb.findNodeAndParent(key)
	if err != nil {
		return err
	}

	if !node.isRecord {
		return ErrKeyNotFound
	}

	if !node.verifyChecksum() {
		return ErrInvalidChecksum
	}

	// Determine how to remove the current node based on the number
	// of children. If there are no children, simply remove the node
	// from the parent. If the node is a root node then clear the tree.
	if !node.hasChildren() {
		if parent == nil && node == rdb.root {
			rdb.clear()
		} else {
			if err := parent.removeChild(node); err != nil {
				return err
			}

			rdb.numRecords--

			// Inform the blobStore if the deleted node had carried a blob.
			if node.isBlob {
				blobID, err := buildBlobID(node.data)

				if err != nil {
					return err
				}

				rdb.blobs.release(blobID)
			}

			// If the deletion had left the parent node with only one child,
			// it means that the child can take its place in the tree.
			if parent.numChildren == 1 {
				onlyChild := parent.firstChild

				// If the parent is the root node, we can simply promote
				// the onlyChild node as the new root node.
				if parent == rdb.root {
					onlyChild.prependKey(parent.key)
					rdb.root = onlyChild

					return nil
				}

				// Reaching here means that we can replace the parent with the
				// onlyChild. Because we don't have immediate access to the
				// grandparent, instead of switching pointers, we will recycle
				// the parent node by overwriting it.
				//
				// Taking place of the parent also means we need to be careful
				// with the key. If the new parent either has children or is a
				// record node, it needs to inherit the parent's key.
				if onlyChild.hasChildren() || onlyChild.isRecord {
					onlyChild.prependKey(parent.key)
				}

				parent.shallowCopyFrom(onlyChild)
			}
		}

		return nil
	}

	// If the node only has one child, the child's key is reconstructed,
	// and then it takes over the node's position in the tree.
	if node.numChildren == 1 {
		onlyChild := node.firstChild
		onlyChild.prependKey(node.key)

		if parent == nil && node == rdb.root {
			rdb.root = onlyChild
		} else {
			if err := parent.removeChild(node); err != nil {
				return err
			}

			parent.addChild(onlyChild)
		}

		rdb.numRecords--
		return nil
	}

	// Reaching here means that the node has multiple children. Because the
	// children are guaranteed to share the node's key as their prefix, we
	// can simply convert the node to a path compression node.
	if node.isBlob {
		if id, err := buildBlobID(node.data); err != nil {
			return err
		} else {
			rdb.blobs.release(id)
		}
	}

	node.isBlob = false
	node.isRecord = false
	node.data = nil
	rdb.numRecords--

	return nil
}

// Clear wipes the entire RadixDB tree from memory, effectively resetting the
// database. This operation is irreversible, so use it with caution.
func (rdb *RadixDB) Clear() {
	rdb.mu.Lock()
	defer rdb.mu.Unlock()

	rdb.clear()
}

// empty returns true if the tree is empty.
func (rdb *RadixDB) empty() bool {
	return rdb.root == nil && rdb.numRecords == 0
}

// clear wipes out the entire in-memory tree. This function is internal and
// is not exported because it is intended for testing purposes.
func (rdb *RadixDB) clear() {
	rdb.root = nil
	rdb.numNodes = 0
	rdb.numRecords = 0
	rdb.blobs = make(blobStore)
}

// splitNode divides a node into two nodes based on a common prefix, creating
// an intermediate parent node. It does so by updating the keys of the current
// and new nodes to contain only the suffixes after the common prefix.
func (rdb *RadixDB) splitNode(parent *node, current *node, newNode *node, commonPrefix []byte) {
	current.setKey(current.key[len(commonPrefix):])
	newNode.setKey(newNode.key[len(commonPrefix):])

	newParent := &node{key: commonPrefix}
	newParent.addChild(current)
	newParent.addChild(newNode)
	newParent.updateChecksum()

	// Account for the newParent + newNode.
	rdb.numNodes += 2

	// Splitting the root node only requires setting the new branch as root.
	if parent == nil && current == rdb.root {
		rdb.root = newParent
		rdb.numRecords++

		return
	}

	// Update the parent of the current node to point at newParent.
	for child := parent.firstChild; child != nil; child = child.nextSibling {
		if child == current {
			parent.removeChild(child)
			parent.addChild(newParent)
			rdb.numRecords++

			return
		}
	}
}

// findNodeAndParent returns both the node that matches the given key and its
// parent. The parent is nil if the discovered node is a root node.
func (rdb *RadixDB) findNodeAndParent(key []byte) (*node, *node, error) {
	if key == nil {
		return nil, nil, ErrNilKey
	}

	if rdb.empty() {
		return nil, nil, ErrKeyNotFound
	}

	var parent *node
	var current = rdb.root

	for {
		prefix := longestCommonPrefix(current.key, key)

		// Lack of a common prefix means that the key does not exist in the
		// tree, unless the current node is a root node.
		if prefix == nil && current != rdb.root {
			return nil, nil, ErrKeyNotFound
		}

		// Prefix does not match the current node's key. Radix tree's prefix
		// compression algorithm guarantees that the key does not exist.
		if len(prefix) != len(current.key) {
			return nil, nil, ErrKeyNotFound
		}

		// The prefix matches the current node's key.
		if len(prefix) == len(key) {
			return current, parent, nil
		}

		// Mild optimization to determine if further traversal is necessary.
		if !current.hasChildren() {
			return nil, nil, ErrKeyNotFound
		}

		// Update the key for the next iteration, and then continue traversing.
		// The key does not exist if a compatible child is not found.
		key = key[len(prefix):]
		parent = current
		current = current.findCompatibleChild(key)

		if current == nil {
			return nil, nil, ErrKeyNotFound
		}
	}
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

// traverse performs a depth-first search (DFS) traversal on the tree.
// It uses a stack-based technique instead of recursion, and also accepts
// a callback function, which is executed on each node visit.
func (rdb *RadixDB) traverse(cb func(*node) error) error {
	if rdb.root == nil {
		return nil
	}

	stack := []*node{rdb.root}

	for len(stack) > 0 {
		// Pop the next node from the stack.
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if current == nil {
			continue
		}

		if err := cb(current); err != nil {
			return err
		}

		// Collect all children, and stack them in reverse order.
		var children []*node

		for child := current.firstChild; child != nil; child = child.nextSibling {
			children = append(children, child)
		}

		for i := len(children) - 1; i >= 0; i-- {
			stack = append(stack, children[i])
		}
	}

	return nil
}
