package arc

import "fmt"

// DebugPrint prints the Arc index structure in a directory tree format.
// Use this function only for development and debugging purposes.
func (a *Arc) DebugPrint() {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.Len() == 1 {
		fmt.Printf("%s (%q)\n", string(a.root.key), a.root.data)
		return
	}

	printTree(a.root, "", true, true)
}

func printTree(current *node, prefix string, isLast bool, isRoot bool) {
	if current == nil {
		return
	}

	var val string

	if current.data != nil {
		val = string(current.data)
	} else {
		val = "<nil>"
	}

	if isRoot {
		if current.key != nil {
			fmt.Printf("%s (%q)\n", string(current.key), val)
		} else {
			fmt.Println(".")
		}
	} else {
		if isLast {
			fmt.Printf("%s└─ %s (%q)\n", prefix, string(current.key), val)
			prefix += "  "
		} else {
			fmt.Printf("%s├─ %s (%q)\n", prefix, string(current.key), val)
			prefix += "│  "
		}
	}

	current.forEachChild(func(i int, n *node) error {
		printTree(n, prefix, i == current.numChildren-1, false)
		return nil
	})
}
