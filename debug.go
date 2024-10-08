package radixdb

import (
	"fmt"
)

// DebugPrint prints the RadixDB structure in a directory tree format.
// This function is only for development and debugging purposes.
func (rdb *RadixDB) DebugPrint() {
	rdb.mu.RLock()
	defer rdb.mu.RUnlock()

	printTree(rdb.root, "", true, true, rdb.Len())
}

func printTree(node *node, prefix string, isLast bool, isRoot bool, treeSize uint64) {
	if node == nil {
		return
	}

	if isRoot && treeSize == 1 {
		fmt.Printf("%s\n", string(node.key))
		return
	}

	if isRoot {
		if node.value != nil {
			fmt.Printf("%s (%q)\n", string(node.key), node.value)
		} else {
			fmt.Println(".")
		}
	} else {
		var val string

		if node.value != nil {
			val = string(node.value)
		} else {
			val = "<nil>"
		}

		if isLast {
			fmt.Printf("%s└─ %s (%q)\n", prefix, string(node.key), val)
			prefix += "  "
		} else {
			fmt.Printf("%s├─ %s (%q)\n", prefix, string(node.key), val)
			prefix += "│  "
		}
	}

	for i, child := range node.children {
		printTree(child, prefix, i == len(node.children)-1, false, treeSize)
	}
}
