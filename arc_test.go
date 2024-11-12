// Copyright Chrono Technologies LLC
// SPDX-License-Identifier: MIT

package arc

import (
	"bytes"
	"fmt"
	"testing"
)

func TestLongestCommonPrefix(t *testing.T) {
	tests := []struct {
		a, b     []byte
		expected []byte
	}{
		// Basic cases.
		{[]byte("apple"), []byte("app"), []byte("app")},
		{[]byte("banana"), []byte("band"), []byte("ban")},
		{[]byte("cat"), []byte("candy"), []byte("ca")},
		{[]byte("no"), []byte("match"), nil},
		{[]byte("fullmatch"), []byte("fullmatch"), []byte("fullmatch")},

		// Edge cases.
		{[]byte(""), []byte(""), nil},
		{nil, nil, nil},
	}

	for _, test := range tests {
		subject := longestCommonPrefix(test.a, test.b)

		if !bytes.Equal(subject, test.expected) {
			t.Errorf("unexpected prefix: got:%q, want:%q", subject, test.expected)
		}
	}
}

func TestSplitNode(t *testing.T) {
	arc := &Arc{
		root:       &node{key: []byte("apple")},
		numNodes:   1,
		numRecords: 1,
	}

	// Test root split: "app" -> ["le", "store"]
	{
		newNode := &node{key: []byte("appstore")}
		commonPrefix := longestCommonPrefix(arc.root.key, newNode.key)

		arc.splitNode(nil, arc.root, newNode, commonPrefix)

		// Root and its two children.
		if arc.numNodes != 3 {
			t.Errorf("unexpected node count: got:%d, want:3", arc.numNodes)
		}

		// "apple" and "appstore" records.
		if arc.Len() != 2 {
			t.Errorf("unexpected record count: got:%d, want:2", arc.numRecords)
		}

		if !bytes.Equal(arc.root.key, commonPrefix) {
			t.Errorf("invalid root key: got:%q, want:%q", commonPrefix, arc.root.key)
		}

		expectedKeys := [][]byte{[]byte("le"), []byte("store")}

		arc.root.forEachChild(func(i int, n *node) error {
			if !bytes.Equal(n.key, expectedKeys[i]) {
				t.Fatalf("unexpected key: got:%q, want:%q", n.key, expectedKeys[i])
			}

			if !n.isLeaf() {
				t.Fatalf("expected %q to be a leaf node", n.key)
			}

			return nil
		})
	}

	// Test non-root split using the "app" -> "store" node.
	{
		current, _ := arc.root.findChild([]byte("store"))
		strawberryNode := &node{key: []byte("strawberry")}
		commonPrefix := longestCommonPrefix(strawberryNode.key, current.key)

		arc.splitNode(arc.root, current, strawberryNode, commonPrefix)

		// "app" -> ["le", "st" -> ["ore", "rawberry"]]
		if arc.numNodes != 5 {
			t.Errorf("unexpected node count: got:%d, want:3", arc.numNodes)
		}

		// "apple", "appstore" and "strawberry" records.
		if arc.Len() != 3 {
			t.Errorf("unexpected record count: got:%d, want:2", arc.Len())
		}

		// "store" should now be further split to "st" -> "ore".
		expectedKey := []byte("ore")
		if !bytes.Equal(current.key, expectedKey) {
			t.Errorf("unexpected key: got:%q, want:%q", current.key, expectedKey)
		}

		// "strawberry" should now be split to "st" -> "rawberry".
		expectedKey = []byte("rawberry")
		if !bytes.Equal(strawberryNode.key, expectedKey) {
			t.Errorf("unexpected key: got:%q, want:%q", current.key, expectedKey)
		}
	}
}
func TestPut(t *testing.T) {
	// Test using keys that do not share any prefix.
	{
		records := []struct {
			key   []byte
			value []byte
		}{
			{[]byte("apple"), []byte("1")},
			{[]byte("citron"), []byte("3")},
			{[]byte("durian"), []byte("4")},
			{[]byte("banana"), []byte("2")},
		}

		arc := New()

		for _, record := range records {
			if err := arc.Put(record.key, record.value); err != nil {
				t.Errorf("unexpected error:%v", err)
			}
		}

		expectedKeys := [][]byte{[]byte("apple"), []byte("banana"), []byte("citron"), []byte("durian")}

		if arc.numNodes != len(expectedKeys)+1 {
			t.Fatalf("unexpected numNodes, got:%d, want:%d", arc.numNodes, len(expectedKeys)+1)
		}

		if arc.numRecords != len(expectedKeys) {
			t.Fatalf("unexpected numNodes, got:%d, want:%d", arc.numRecords, len(expectedKeys))
		}

		if arc.root.numChildren != len(expectedKeys) {
			t.Fatalf("unexpected numChildren, got:%d, want:3", arc.root.numChildren)
		}

		arc.root.forEachChild(func(i int, n *node) error {
			if !bytes.Equal(n.key, expectedKeys[i]) {
				t.Fatalf("unexpected key: got:%q, want:%q", n.key, expectedKeys[i])
			}

			if !n.isLeaf() {
				t.Fatalf("expected %q to be a leaf node", n.key)
			}

			return nil
		})

		printIndex(arc)
	}

	// Test using similar keys.
	{
		records := []struct {
			key   []byte
			value []byte
		}{
			{[]byte("a"), []byte("1")},
			{[]byte("app"), []byte("6")},
			{[]byte("apple"), []byte("7")},
			{[]byte("approved"), []byte("12")},
			{[]byte("apply"), []byte("10")},
			{[]byte("apex"), []byte("4")},
			{[]byte("application"), []byte("9")},
			{[]byte("apology"), []byte("5")},
			{[]byte("appointment"), []byte("11")},
			{[]byte("appliance"), []byte("8")},
			{[]byte("ap"), []byte("3")},
			{[]byte("android"), []byte("2")},
		}

		arc := New()

		for _, record := range records {
			if err := arc.Put(record.key, record.value); err != nil {
				t.Errorf("unexpected error:%v", err)
			}
		}

		// Expected tree structure:
		//
		// a ("1")
		// ├─ ndroid ("2")
		// └─ p ("3")
		//    ├─ ex ("4")
		//    ├─ ology ("5")
		//    └─ p ("6")
		//       ├─ l ("<nil>")
		//       │  ├─ e ("7")
		//       │  ├─ i ("<nil>")
		//       │  │  ├─ ance ("8")
		//       │  │  └─ cation ("9")
		//       │  └─ y ("10")
		//       ├─ ointment ("11")
		//       └─ roved ("12")
		tests := [][]nodeTestCase{
			// Level 0
			{
				{key: []byte("a"), isLeaf: false, isRecord: true, numChildren: 2},
			},
			// Level 1
			{
				{key: []byte("ndroid"), isLeaf: true, isRecord: true, numChildren: 0},
				{key: []byte("p"), isLeaf: false, isRecord: true, numChildren: 3},
			},
			// Level 2
			{
				{key: []byte("ex"), isLeaf: true, isRecord: true, numChildren: 0},
				{key: []byte("ology"), isLeaf: true, isRecord: true, numChildren: 0},
				{key: []byte("p"), isLeaf: false, isRecord: true, numChildren: 3},
			},
			// Level 3
			{
				{key: []byte("l"), isLeaf: false, isRecord: false, numChildren: 3},
				{key: []byte("ointment"), isLeaf: true, isRecord: true, numChildren: 0},
				{key: []byte("roved"), isLeaf: true, isRecord: true, numChildren: 0},
			},
			// Level 4
			{
				{key: []byte("e"), isLeaf: true, isRecord: true, numChildren: 0},
				{key: []byte("i"), isLeaf: false, isRecord: false, numChildren: 2},
				{key: []byte("y"), isLeaf: true, isRecord: true, numChildren: 0},
			},
			// Level 5
			{
				{key: []byte("ance"), isLeaf: true, isRecord: true, numChildren: 0},
				{key: []byte("cation"), isLeaf: true, isRecord: true, numChildren: 0},
			},
		}

		levels := collectNodesByLevel(arc.root)

		for level, testNodes := range tests {
			if level >= len(levels) {
				t.Fatalf("invalid level: %d", level)
			}

			if len(levels[level]) != len(testNodes) {
				t.Fatalf("unexpected level (%d) node count, got:%d, want:%d",
					level, len(levels[level]), len(testNodes))
			}

			for i, want := range testNodes {
				got := levels[level][i]

				if !bytes.Equal(got.key, want.key) {
					t.Fatalf("unexpected key: got:%q, want:%q", got.key, want.key)
				}

				if got.firstChild != nil && want.isLeaf {
					t.Fatalf("expected %q to be a leaf node", got.key)
				}

				if got.firstChild == nil && !want.isLeaf {
					t.Fatalf("expected %q to be a non-leaf node", got.key)
				}

				if got.isRecord != want.isRecord {
					t.Fatalf("unexpected isRecord value (%q), got: %t, want:%t", got.key, got.isRecord, want.isRecord)
				}

				if got.numChildren != want.numChildren {
					t.Fatalf("unexpected child count (%q), got:%d, want:%d", got.key, got.numChildren, want.numChildren)
				}
			}
		}

		printIndex(arc)
	}

	// Test using similar keys that require tricky restructing and splitting.
	{
		records := []struct {
			key   []byte
			value []byte
		}{
			{[]byte("ax"), []byte("1")},
			{[]byte("axb"), []byte("2")},
			{[]byte("axby"), []byte("3")},
			{[]byte("axbyz"), []byte("4")},
			{[]byte("axbyza"), []byte("5")},
			{[]byte("axbyzab"), []byte("6")},

			// Start right branch.
			{[]byte("axy"), []byte("7")},
			{[]byte("axyb"), []byte("8")},
			{[]byte("axybz"), []byte("9")},
			{[]byte("axybza"), []byte("10")},

			// Build out right branch.
			{[]byte("axyz"), []byte("11")},
			{[]byte("axyza"), []byte("12")},
			{[]byte("axyzab"), []byte("13")},
			{[]byte("axyzb"), []byte("14")},
			{[]byte("axyzba"), []byte("15")},
		}

		arc := New()

		for _, record := range records {
			if err := arc.Put(record.key, record.value); err != nil {
				t.Errorf("unexpected error:%v", err)
			}
		}

		// Expected tree structure:
		//
		// ax ("1")
		// ├─ b ("2")
		// │  └─ y ("3")
		// │     └─ z ("4")
		// │        └─ a ("5")
		// │           └─ b ("6")
		// └─ y ("7")
		//    ├─ b ("8")
		//    │  └─ z ("9")
		//    │     └─ a ("10")
		//    └─ z ("11")
		//       ├─ a ("12")
		//       │  └─ b ("13")
		//       └─ b ("14")
		//          └─ a ("15")
		tests := [][]nodeTestCase{
			// Level 0
			{
				{key: []byte("ax"), value: []byte("1"), isLeaf: false, isRecord: true, numChildren: 2},
			},
			// Level 1
			{
				{key: []byte("b"), value: []byte("2"), isLeaf: false, isRecord: true, numChildren: 1},
				{key: []byte("y"), value: []byte("7"), isLeaf: false, isRecord: true, numChildren: 2},
			},
			// Level 2
			{
				{key: []byte("y"), value: []byte("3"), isLeaf: false, isRecord: true, numChildren: 1},
				{key: []byte("b"), value: []byte("8"), isLeaf: false, isRecord: true, numChildren: 1},
				{key: []byte("z"), value: []byte("11"), isLeaf: false, isRecord: true, numChildren: 2},
			},
			// Level 3
			{
				{key: []byte("z"), value: []byte("4"), isLeaf: false, isRecord: true, numChildren: 1},
				{key: []byte("z"), value: []byte("9"), isLeaf: false, isRecord: true, numChildren: 1},
				{key: []byte("a"), value: []byte("12"), isLeaf: false, isRecord: true, numChildren: 1},
				{key: []byte("b"), value: []byte("14"), isLeaf: false, isRecord: true, numChildren: 1},
			},
			// Level 4
			{
				{key: []byte("a"), value: []byte("5"), isLeaf: false, isRecord: true, numChildren: 1},
				{key: []byte("a"), value: []byte("10"), isLeaf: true, isRecord: true, numChildren: 0},
				{key: []byte("b"), value: []byte("13"), isLeaf: true, isRecord: true, numChildren: 0},
				{key: []byte("a"), value: []byte("15"), isLeaf: true, isRecord: true, numChildren: 0},
			},
			// Level 4
			{
				{key: []byte("b"), value: []byte("6"), isLeaf: true, isRecord: true, numChildren: 0},
			},
		}

		levels := collectNodesByLevel(arc.root)

		for level, testNodes := range tests {
			if level >= len(levels) {
				t.Fatalf("invalid level: %d", level)
			}

			if len(levels[level]) != len(testNodes) {
				t.Fatalf("unexpected level (%d) node count, got:%d, want:%d",
					level, len(levels[level]), len(testNodes))
			}

			for i, want := range testNodes {
				got := levels[level][i]

				if !bytes.Equal(got.key, want.key) {
					t.Fatalf("unexpected key: got:%q, want:%q", got.key, want.key)
				}

				if !bytes.Equal(got.data, want.value) {
					t.Fatalf("unexpected value: got:%q, want:%q", got.data, want.value)
				}

				if got.firstChild != nil && want.isLeaf {
					t.Fatalf("expected %q to be a leaf node", got.key)
				}

				if got.firstChild == nil && !want.isLeaf {
					t.Fatalf("expected %q to be a non-leaf node", got.key)
				}

				if got.isRecord != want.isRecord {
					t.Fatalf("unexpected isRecord value (%q), got: %t, want:%t", got.key, got.isRecord, want.isRecord)
				}

				if got.numChildren != want.numChildren {
					t.Fatalf("unexpected child count (%q), got:%d, want:%d", got.key, got.numChildren, want.numChildren)
				}
			}
		}

		printIndex(arc)
	}
}

func printIndex(a *Arc) {
	if testing.Verbose() {
		fmt.Println("---")
		a.DebugPrint()
	}
}

func collectNodesByLevel(root *node) [][]*node {
	if root == nil {
		return nil
	}

	var levels [][]*node
	currentLevel := []*node{root}

	for len(currentLevel) > 0 {
		levels = append(levels, currentLevel)

		var nextLevel []*node

		for _, n := range currentLevel {
			n.forEachChild(func(_ int, child *node) error {
				nextLevel = append(nextLevel, child)
				return nil
			})
		}

		currentLevel = nextLevel
	}

	return levels
}

type nodeTestCase struct {
	key         []byte
	value       []byte
	isLeaf      bool
	isRecord    bool
	numChildren int
}
