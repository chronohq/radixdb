// Package radixdb provides a persistable Radix tree implementation.
package radixdb

// node represents an in-memory node of a Radix tree. This implementation
// is designed to be memory-efficient by using a minimal set of fields to
// represent each node. In a Radix tree, the node's key inherently carries
// significant information, hence reducing the need to maintain metadata.
type node struct {
	key      []byte  // Path segment of the node.
	value    any     // Data associated with this node, if any.
	children []*node // Pointers to child nodes.
}
