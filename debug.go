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

	var val string

	if node.data != nil {
		val = string(node.data)
	} else {
		val = "<nil>"
	}

	if isRoot && treeSize == 1 {
		fmt.Printf("%s (%q)\n", string(node.key), val)
		return
	}

	if isRoot {
		if node.key != nil {
			fmt.Printf("%s (%q)\n", string(node.key), val)
		} else {
			fmt.Println(".")
		}
	} else {

		if isLast {
			fmt.Printf("%s└─ %s (%q)\n", prefix, string(node.key), val)
			prefix += "  "
		} else {
			fmt.Printf("%s├─ %s (%q)\n", prefix, string(node.key), val)
			prefix += "│  "
		}
	}

	var i uint16
	for child := node.firstChild; child != nil; child = child.nextSibling {
		printTree(child, prefix, i == node.numChildren-1, false, treeSize)
		i++
	}
}
