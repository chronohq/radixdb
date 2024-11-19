// Copyright Chrono Technologies LLC
// SPDX-License-Identifier: MIT

package arc

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
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
	testCases := []struct {
		name           string
		records        []testNode
		expectedLevels [][]testNode
		numNodes       int
		numRecords     int
	}{
		{
			name: "with no common prefix",
			records: []testNode{
				{key: []byte("apple"), value: []byte("1")},
				{key: []byte("citron"), value: []byte("3")},
				{key: []byte("durian"), value: []byte("4")},
				{key: []byte("banana"), value: []byte("2")},
			},
			// Expected tree structure:
			//
			// .
			// ├─ apple ("1")
			// ├─ banana ("2")
			// ├─ citron ("3")
			// └─ durian ("4")
			expectedLevels: [][]testNode{
				{
					{key: nil, isLeaf: false, isRecord: false, numChildren: 4},
				},
				{
					{key: []byte("apple"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("banana"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("citron"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("durian"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   5,
			numRecords: 4,
		},
		{
			name: "with similar keys",
			records: []testNode{
				{key: []byte("a"), value: []byte("1")},
				{key: []byte("app"), value: []byte("6")},
				{key: []byte("apple"), value: []byte("7")},
				{key: []byte("approved"), value: []byte("12")},
				{key: []byte("apply"), value: []byte("10")},
				{key: []byte("apex"), value: []byte("4")},
				{key: []byte("application"), value: []byte("9")},
				{key: []byte("apology"), value: []byte("5")},
				{key: []byte("appointment"), value: []byte("11")},
				{key: []byte("appliance"), value: []byte("8")},
				{key: []byte("ap"), value: []byte("3")},
				{key: []byte("android"), value: []byte("2")},
			},
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
			expectedLevels: [][]testNode{
				{
					{key: []byte("a"), isLeaf: false, isRecord: true, numChildren: 2},
				},
				{
					{key: []byte("ndroid"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("p"), isLeaf: false, isRecord: true, numChildren: 3},
				},
				{
					{key: []byte("ex"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("ology"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("p"), isLeaf: false, isRecord: true, numChildren: 3},
				},
				{
					{key: []byte("l"), isLeaf: false, isRecord: false, numChildren: 3},
					{key: []byte("ointment"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("roved"), isLeaf: true, isRecord: true, numChildren: 0},
				},
				{
					{key: []byte("e"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("i"), isLeaf: false, isRecord: false, numChildren: 2},
					{key: []byte("y"), isLeaf: true, isRecord: true, numChildren: 0},
				},
				{
					{key: []byte("ance"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("cation"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   14,
			numRecords: 12,
		},
		{
			name: "with complex keys",
			records: []testNode{
				{key: []byte("ax"), value: []byte("1")},
				{key: []byte("axb"), value: []byte("2")},
				{key: []byte("axby"), value: []byte("3")},
				{key: []byte("axbyz"), value: []byte("4")},
				{key: []byte("axbyza"), value: []byte("5")},
				{key: []byte("axbyzab"), value: []byte("6")},
				{key: []byte("axy"), value: []byte("7")},
				{key: []byte("axyb"), value: []byte("8")},
				{key: []byte("axybz"), value: []byte("9")},
				{key: []byte("axybza"), value: []byte("10")},
				{key: []byte("axyz"), value: []byte("11")},
				{key: []byte("axyza"), value: []byte("12")},
				{key: []byte("axyzab"), value: []byte("13")},
				{key: []byte("axyzb"), value: []byte("14")},
				{key: []byte("axyzba"), value: []byte("15")},
			},
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
			expectedLevels: [][]testNode{
				{
					{key: []byte("ax"), value: []byte("1"), isLeaf: false, isRecord: true, numChildren: 2},
				},
				{
					{key: []byte("b"), value: []byte("2"), isLeaf: false, isRecord: true, numChildren: 1},
					{key: []byte("y"), value: []byte("7"), isLeaf: false, isRecord: true, numChildren: 2},
				},
				{
					{key: []byte("y"), value: []byte("3"), isLeaf: false, isRecord: true, numChildren: 1},
					{key: []byte("b"), value: []byte("8"), isLeaf: false, isRecord: true, numChildren: 1},
					{key: []byte("z"), value: []byte("11"), isLeaf: false, isRecord: true, numChildren: 2},
				},
				{
					{key: []byte("z"), value: []byte("4"), isLeaf: false, isRecord: true, numChildren: 1},
					{key: []byte("z"), value: []byte("9"), isLeaf: false, isRecord: true, numChildren: 1},
					{key: []byte("a"), value: []byte("12"), isLeaf: false, isRecord: true, numChildren: 1},
					{key: []byte("b"), value: []byte("14"), isLeaf: false, isRecord: true, numChildren: 1},
				},
				{
					{key: []byte("a"), value: []byte("5"), isLeaf: false, isRecord: true, numChildren: 1},
					{key: []byte("a"), value: []byte("10"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("b"), value: []byte("13"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("a"), value: []byte("15"), isLeaf: true, isRecord: true, numChildren: 0},
				},
				{
					{key: []byte("b"), value: []byte("6"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   15,
			numRecords: 15,
		},
		{
			name: "with single byte prefix difference",
			records: []testNode{
				{key: []byte("35e2ac5f198beea10f1e8abf296b9bb9"), value: nil},
				{key: []byte("35642e6d587bcdffeb28a33bd1cb6c73"), value: nil},
				{key: []byte("e28a9e6d2f747e3a421646ca5c8f3c0b"), value: nil},
			},
			// Expected tree structure:
			//
			// .
			// ├─ 35 ("<nil>")
			// │  ├─ 642e6d587bcdffeb28a33bd1cb6c73 ("<nil>")
			// │  └─ e2ac5f198beea10f1e8abf296b9bb9 ("<nil>")
			// └─ e28a9e6d2f747e3a421646ca5c8f3c0b ("<nil>")
			expectedLevels: [][]testNode{
				{
					{key: []byte(nil), isLeaf: false, isRecord: false, numChildren: 2},
				},
				{
					{key: []byte("35"), isLeaf: false, isRecord: false, numChildren: 2},
					{key: []byte("e28a9e6d2f747e3a421646ca5c8f3c0b"), isLeaf: true, isRecord: true, numChildren: 0},
				},
				{
					{key: []byte("642e6d587bcdffeb28a33bd1cb6c73"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("e2ac5f198beea10f1e8abf296b9bb9"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   5,
			numRecords: 3,
		},
		{
			name:           "with basic test nodes",
			records:        basicTestNodes(),
			expectedLevels: basicTreeLevels(),
			numNodes:       basicTreeNumNodes(),
			numRecords:     len(basicTestNodes()),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			arc := New()

			for _, record := range tc.records {
				if err := arc.Put(record.key, record.value); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			if arc.numNodes != tc.numNodes {
				t.Fatalf("unexpected numNodes, got:%d, want:%d", arc.numNodes, tc.numNodes)
			}

			if arc.numRecords != tc.numRecords {
				t.Fatalf("unexpected numRecords, got:%d, want:%d", arc.numRecords, tc.numRecords)
			}

			nodesByLevel := collectNodesByLevel(arc.root)

			if len(nodesByLevel) != len(tc.expectedLevels) {
				t.Fatalf("unexpected tree depth: got:%d, want:%d", len(nodesByLevel), len(tc.expectedLevels))
			}

			for level, wantNodes := range tc.expectedLevels {
				if len(wantNodes) != len(nodesByLevel[level]) {
					t.Fatalf("invalid node count on level:%d, got:%d, want:%d", level, len(wantNodes), len(nodesByLevel[level]))
				}

				for i, want := range wantNodes {
					got := nodesByLevel[level][i]

					if !bytes.Equal(got.key, want.key) {
						t.Fatalf("unexpected key: got:%q, want:%q", got.key, want.key)
					}

					if got.isLeaf() != want.isLeaf {
						t.Fatalf("unexpected isLeaf: got:%t, want:%t", got.isLeaf(), want.isLeaf)
					}

					if got.isRecord != want.isRecord {
						t.Fatalf("unexpected isRecord: got: %t, want:%t", got.isRecord, want.isRecord)
					}

					if got.numChildren != want.numChildren {
						t.Fatalf("unexpected numChildren: got:%d, want:%d", got.numChildren, want.numChildren)
					}
				}
			}
		})
	}
}

func TestGet(t *testing.T) {
	arc := basicTestTree()

	// Test that all known keys are available.
	for _, known := range basicTestTreeData() {
		value, err := arc.Get(known.key)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if !bytes.Equal(value, known.data) {
			t.Errorf("unexpected value: got:%q, want:%q", value, known.data)
		}
	}

	// Test a key that do not exist.
	if _, err := arc.Get([]byte("bogus")); err != ErrKeyNotFound {
		t.Errorf("unexpected error: got:%v, want:%v", err, ErrKeyNotFound)
	}

	// Test nil key.
	if _, err := arc.Get(nil); err != ErrNilKey {
		t.Errorf("unexpected error: got:%v, want:%v", err, ErrNilKey)
	}
}

func TestDelete(t *testing.T) {
	testCases := []struct {
		name           string
		deletionKeys   [][]byte
		records        []testNode
		expectedLevels [][]testNode
		numNodes       int
		numRecords     int
	}{
		{
			name:         "root:with no children",
			deletionKeys: [][]byte{[]byte("a")},
			records: []testNode{
				{key: []byte("a"), value: []byte("1")},
			},
			expectedLevels: [][]testNode{},
			numNodes:       0,
			numRecords:     0,
		},
		{
			name:         "root:with single leaf child",
			deletionKeys: [][]byte{[]byte("a")},
			records: []testNode{
				{key: []byte("a"), value: []byte("1")},
				{key: []byte("aa"), value: []byte("2")},
			},
			// Expected tree structure after deletion:
			//
			// aa ("2")
			expectedLevels: [][]testNode{
				{
					{key: []byte("aa"), value: []byte("2"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   1,
			numRecords: 1,
		},
		{
			name:         "root:with single non-leaf child",
			deletionKeys: [][]byte{[]byte("a")},
			records: []testNode{
				{key: []byte("a"), value: []byte("1")},
				{key: []byte("ab"), value: []byte("2")},
				{key: []byte("abc"), value: []byte("3")},
			},
			// Expected tree structure after deletion:
			//
			//ab ("2")
			//└─ c ("3")
			expectedLevels: [][]testNode{
				{
					{key: []byte("ab"), value: []byte("2"), isLeaf: false, isRecord: true, numChildren: 1},
				},
				{
					{key: []byte("c"), value: []byte("3"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   2,
			numRecords: 2,
		},
		{
			name:         "root:with multiple children",
			deletionKeys: [][]byte{[]byte("a")},
			records: []testNode{
				{key: []byte("a"), value: []byte("1")},
				{key: []byte("ab"), value: []byte("2")},
				{key: []byte("ac"), value: []byte("3")},
				{key: []byte("ad"), value: []byte("4")},
			},
			// Expected tree structure after deletion. Structure remains the
			// same, but the root node is no longer a record node:
			//
			// a ("<nil>")
			// ├─ b ("2")
			// ├─ c ("3")
			// └─ d ("4")
			expectedLevels: [][]testNode{
				{
					{key: []byte("a"), value: nil, isLeaf: false, isRecord: false, numChildren: 3},
				},
				{
					{key: []byte("b"), value: []byte("2"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("c"), value: []byte("3"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("d"), value: []byte("4"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   4,
			numRecords: 3,
		},
		{
			name:         "root:node promotion",
			deletionKeys: [][]byte{[]byte("a"), []byte("c"), []byte("d")},
			// Test tree structure:
			//
			// .
			// ├─ a ("1")
			// ├─ b ("2")
			// │  └─ x ("3")
			// ├─ c ("4")
			// └─ d ("5")
			records: []testNode{
				{key: []byte("a"), value: []byte("1")},
				{key: []byte("b"), value: []byte("2")},
				{key: []byte("c"), value: []byte("4")},
				{key: []byte("d"), value: []byte("5")},
				{key: []byte("bx"), value: []byte("3")},
			},
			// Expected tree structure after deletion:
			//
			// b ("2")
			// └─ x ("3")
			expectedLevels: [][]testNode{
				{
					{key: []byte("b"), value: []byte("2"), isLeaf: false, isRecord: true, numChildren: 1},
				},
				{
					{key: []byte("x"), value: []byte("3"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   2,
			numRecords: 2,
		},
		{
			name:         "internal:single child node",
			deletionKeys: [][]byte{[]byte("app")},
			// Test tree structure:
			//
			// .
			// ├─ ap ("1")
			// │  └─ p ("2")
			// │    └─ le ("3")
			// │      └─ sauce ("4")
			// └─ banana ("5")
			records: []testNode{
				{key: []byte("ap"), value: []byte("1")},
				{key: []byte("app"), value: []byte("2")},
				{key: []byte("apple"), value: []byte("3")},
				{key: []byte("applesauce"), value: []byte("4")},
				{key: []byte("banana"), value: []byte("5")},
			},
			// Expected tree structure after deletion:
			// .
			// ├─ ap ("1")
			// │  └─ ple ("3")
			// │    └─ sauce ("4")
			// └─ banana ("5")
			expectedLevels: [][]testNode{
				{
					{key: nil, value: nil, isLeaf: false, isRecord: false, numChildren: 2},
				},
				{
					{key: []byte("ap"), value: []byte("1"), isLeaf: false, isRecord: true, numChildren: 1},
					{key: []byte("banana"), value: []byte("5"), isLeaf: true, isRecord: true, numChildren: 0},
				},
				{
					{key: []byte("ple"), value: []byte("3"), isLeaf: false, isRecord: true, numChildren: 1},
				},
				{
					{key: []byte("sauce"), value: []byte("4"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   5,
			numRecords: 4,
		},
		{
			name: "internal:nodes with multiple children",
			deletionKeys: [][]byte{
				[]byte("app"), []byte("ap"),
			},
			records: []testNode{
				{key: []byte("a"), value: []byte("1")},
				{key: []byte("app"), value: []byte("6")},
				{key: []byte("apple"), value: []byte("7")},
				{key: []byte("approved"), value: []byte("12")},
				{key: []byte("apply"), value: []byte("10")},
				{key: []byte("apex"), value: []byte("4")},
				{key: []byte("application"), value: []byte("9")},
				{key: []byte("apology"), value: []byte("5")},
				{key: []byte("appointment"), value: []byte("11")},
				{key: []byte("appliance"), value: []byte("8")},
				{key: []byte("ap"), value: []byte("3")},
				{key: []byte("android"), value: []byte("2")},
			},
			expectedLevels: [][]testNode{
				{
					{key: []byte("a"), value: []byte("1"), isLeaf: false, isRecord: true, numChildren: 2},
				},
				{
					{key: []byte("ndroid"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("p"), isLeaf: false, isRecord: false, numChildren: 3},
				},
				{
					{key: []byte("ex"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("ology"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("p"), isLeaf: false, isRecord: false, numChildren: 3},
				},
				{
					{key: []byte("l"), isLeaf: false, isRecord: false, numChildren: 3},
					{key: []byte("ointment"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("roved"), isLeaf: true, isRecord: true, numChildren: 0},
				},
				{
					{key: []byte("e"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("i"), isLeaf: false, isRecord: false, numChildren: 2},
					{key: []byte("y"), isLeaf: true, isRecord: true, numChildren: 0},
				},
				{
					{key: []byte("ance"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("cation"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   14,
			numRecords: 10,
		},
		{
			name:         "leaf:with single-child root parent",
			deletionKeys: [][]byte{[]byte("aa")},
			records: []testNode{
				{key: []byte("a"), value: []byte("1")},
				{key: []byte("aa"), value: []byte("2")},
			},
			// Expected tree structure after deletion:
			//
			// a ("1")
			expectedLevels: [][]testNode{
				{
					{key: []byte("a"), value: []byte("1"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   1,
			numRecords: 1,
		},
		{
			name:         "leaf:with multi-child root parent",
			deletionKeys: [][]byte{[]byte("ab")},
			// Test tree structure:
			//
			// a ("1")
			// ├─ a ("2")
			// ├─ b ("3")
			// └─ c ("4")
			records: []testNode{
				{key: []byte("a"), value: []byte("1")},
				{key: []byte("aa"), value: []byte("2")},
				{key: []byte("ab"), value: []byte("3")},
				{key: []byte("ac"), value: []byte("4")},
			},
			// Expected tree structure after deletion:
			//
			// a ("1")
			// ├─ a ("2")
			// └─ c ("4"
			expectedLevels: [][]testNode{
				{
					{key: []byte("a"), value: []byte("1"), isLeaf: false, isRecord: true, numChildren: 2},
				},
				{
					{key: []byte("a"), value: []byte("2"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("c"), value: []byte("4"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   3,
			numRecords: 3,
		},
		{
			name:         "leaf:from multi level tree",
			deletionKeys: [][]byte{[]byte("aac"), []byte("aba")},
			records: []testNode{
				{key: []byte("a"), value: []byte("1")},
				{key: []byte("aa"), value: []byte("2")},
				{key: []byte("aab"), value: []byte("3")},
				{key: []byte("aac"), value: []byte("4")},
				{key: []byte("ab"), value: []byte("5")},
				{key: []byte("aba"), value: []byte("6")},
			},
			// Expected tree structure after deletion:
			//
			// a ("1")
			// ├─ a ("2")
			// │  └─ b ("3")
			// └─ b ("5")
			expectedLevels: [][]testNode{
				{
					{key: []byte("a"), value: []byte("1"), isLeaf: false, isRecord: true, numChildren: 2},
				},
				{
					{key: []byte("a"), value: []byte("2"), isLeaf: false, isRecord: true, numChildren: 1},
					{key: []byte("b"), value: []byte("5"), isLeaf: true, isRecord: true, numChildren: 0},
				},
				{
					{key: []byte("b"), value: []byte("3"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   4,
			numRecords: 4,
		},
		{
			name:         "leaf:non-record parent with two children",
			deletionKeys: [][]byte{[]byte("apple")},
			// Test tree structure:
			//
			// a ("1")
			// └─ p ("<nil>")
			//    ├─ ple ("2")
			//    └─ ricot ("3")
			records: []testNode{
				{key: []byte("a"), value: []byte("1")},
				{key: []byte("apple"), value: []byte("2")},
				{key: []byte("apricot"), value: []byte("3")},
			},
			// Removal of the "ple" node has left the non-record parent: "p"
			// with one child: "ricot". This means that the parent node is
			// now reundant, and therefore the "p" and "ricot" nodes should
			// be merged, forming a "pricot" node containing "ricot" data.
			//
			// Expected tree structure after deletion:
			//
			// a ("1")
			// └─ pricot ("3")
			expectedLevels: [][]testNode{
				{
					{key: []byte("a"), value: []byte("1"), isLeaf: false, isRecord: true, numChildren: 1},
				},
				{
					{key: []byte("pricot"), value: []byte("3"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   2,
			numRecords: 2,
		},
		{
			name:         "leaf:all nodes",
			deletionKeys: [][]byte{[]byte("aa"), []byte("ab"), []byte("ac")},
			// Test tree structure:
			//
			// a ("1")
			// ├─ a ("2")
			// ├─ b ("3")
			// └─ c ("4")
			records: []testNode{
				{key: []byte("a"), value: []byte("1")},
				{key: []byte("aa"), value: []byte("2")},
				{key: []byte("ab"), value: []byte("3")},
				{key: []byte("ac"), value: []byte("4")},
			},
			// Expected tree structure after deletion:
			//
			// a ("1")
			expectedLevels: [][]testNode{
				{
					{key: []byte("a"), value: []byte("1"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   1,
			numRecords: 1,
		},
		{
			name:         "bottom up branch restructuring",
			deletionKeys: [][]byte{[]byte("apple"), []byte("applet")},
			// Test tree structure:
			//
			// ap ("<nil>")
			// ├─ pl ("<nil>")
			// │  ├─ e ("1")
			// │  │  └─ t ("2")
			// │  └─ ication ("3")
			// └─ ricot ("4")
			records: []testNode{
				{key: []byte("apple"), value: []byte("1")},
				{key: []byte("applet"), value: []byte("2")},
				{key: []byte("application"), value: []byte("3")},
				{key: []byte("apricot"), value: []byte("4")},
			},
			// Expected tree structure after deletion:
			//
			// ap ("<nil>")
			// ├─ plication ("3")
			// └─ ricot ("4")
			expectedLevels: [][]testNode{
				{
					{key: []byte("ap"), value: nil, isLeaf: false, isRecord: false, numChildren: 2},
				},
				{
					{key: []byte("plication"), value: []byte("3"), isLeaf: true, isRecord: true, numChildren: 0},
					{key: []byte("ricot"), value: []byte("4"), isLeaf: true, isRecord: true, numChildren: 0},
				},
			},
			numNodes:   3,
			numRecords: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			arc := New()

			for _, record := range tc.records {
				if err := arc.Put(record.key, record.value); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			for _, deletionKey := range tc.deletionKeys {
				if err := arc.Delete(deletionKey); err != nil {
					t.Fatalf("unexpected deletion error: %v", err)
				}
			}

			if arc.numNodes != tc.numNodes {
				t.Fatalf("unexpected numNodes, got:%d, want:%d", arc.numNodes, tc.numNodes)
			}

			if arc.numRecords != tc.numRecords {
				t.Fatalf("unexpected numRecords, got:%d, want:%d", arc.numRecords, tc.numRecords)
			}

			nodesByLevel := collectNodesByLevel(arc.root)

			if len(nodesByLevel) != len(tc.expectedLevels) {
				t.Fatalf("unexpected tree depth: got:%d, want:%d", len(nodesByLevel), len(tc.expectedLevels))
			}

			for level, wantNodes := range tc.expectedLevels {
				if len(wantNodes) != len(nodesByLevel[level]) {
					t.Fatalf("invalid node count on level:%d, got:%d, want:%d", level, len(wantNodes), len(nodesByLevel[level]))
				}
				for i, want := range wantNodes {
					got := nodesByLevel[level][i]

					if !bytes.Equal(got.key, want.key) {
						t.Fatalf("unexpected key: got:%q, want:%q", got.key, want.key)
					}

					if got.isLeaf() != want.isLeaf {
						t.Fatalf("unexpected isLeaf: got:%t, want:%t", got.isLeaf(), want.isLeaf)
					}

					if got.isRecord != want.isRecord {
						t.Fatalf("unexpected isRecord: got: %t, want:%t", got.isRecord, want.isRecord)
					}

					if got.numChildren != want.numChildren {
						t.Fatalf("unexpected numChildren: got:%d, want:%d", got.numChildren, want.numChildren)
					}
				}
			}

			// Ensure that all known keys are fetchable via the public API.
			for _, record := range tc.records {
				var expected error
				var deletedKey bool

				for _, deletionKey := range tc.deletionKeys {
					if bytes.Equal(deletionKey, record.key) {
						deletedKey = true
					}
				}

				if deletedKey {
					expected = ErrKeyNotFound
				}

				if _, err := arc.Get(record.key); err != expected {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}

	t.Run("with nil key", func(t *testing.T) {
		arc := New()

		if err := arc.Delete(nil); err != ErrNilKey {
			t.Fatalf("unexpected result, got:%v, want:%v", err, ErrNilKey)
		}
	})
}

func TestDeleteWithBasicTree(t *testing.T) {
	t.Run("delete multi-child internal node", func(t *testing.T) {
		arc := basicTestTree()
		testKey := []byte("band")

		// Test multi-child internal node deletion.
		if err := arc.Delete(testKey); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		node, _, err := arc.findNodeAndParent(testKey)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if node.isRecord {
			t.Error("expected node to be non-record type")
		}
	})
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

func FuzzPutGet(f *testing.F) {
	f.Fuzz(func(t *testing.T, n uint32, keySeed []byte) {
		if len(keySeed) == 0 {
			t.Skip("empty keySeed: skipping fuzz case")
		}

		arc := New()

		keys := make([][]byte, 0, n)

		// Build test keys deterministically.
		for i := 0; i < int(n); i++ {
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, uint32(i))
			seed := append(keySeed, buf...)
			key := sha256.Sum256(seed)
			keys = append(keys, key[:])
		}

		// Build the database.
		for _, key := range keys {
			if err := arc.Put(key, nil); err != nil {
				t.Fatalf("fuzzing Put() failed: %v", err)
			}
		}

		// Test that all keys are retrievable.
		for _, key := range keys {
			_, err := arc.Get(key)

			if err != nil {
				if errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("missing key: %q", hex.EncodeToString(key))
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}
		}
	})
}

// Expected tree structure:
// .
// ├─ ap ("<nil>")
// │  ├─ pl ("<nil>")
// │  │  ├─ e ("cider")
// │  │  │  └─ t ("java")
// │  │  └─ ication ("framework")
// │  └─ ricot ("fruit")
// ├─ b ("<nil>")
// │  ├─ an ("<nil>")
// │  │  ├─ ana ("ripe")
// │  │  └─ d ("practice")
// │  │    ├─ age ("medical")
// │  │    └─ saw ("cut")
// │  ├─ erry ("sweet")
// │  └─ lueberry ("jam")
// ├─ grape ("vine")
// │  └─ fruit ("citrus")
// ├─ l ("<nil>")
// │  ├─ emon ("sour")
// │  │  └─ ade ("refreshing")
// │  └─ ime ("green")
// │    └─ stone ("concrete")
// └─ orange ("juice")
func basicTestTree() *Arc {
	arc := New()

	for _, row := range basicTestTreeData() {
		arc.Put(row.key, row.data)
	}

	return arc
}

func basicTestTreeData() []node {
	return []node{
		{key: []byte("grape"), data: []byte("vine")},
		{key: []byte("bandsaw"), data: []byte("cut")},
		{key: []byte("applet"), data: []byte("java")},
		{key: []byte("grapefruit"), data: []byte("citrus")},
		{key: []byte("apple"), data: []byte("cider")},
		{key: []byte("banana"), data: []byte("ripe")},
		{key: []byte("apricot"), data: []byte("fruit")},
		{key: []byte("bandage"), data: []byte("first-aid")},
		{key: []byte("blueberry"), data: []byte("jam")},
		{key: []byte("lemon"), data: []byte("sour")},
		{key: []byte("berry"), data: []byte("sweet")},
		{key: []byte("lime"), data: []byte("green")},
		{key: []byte("lemonade"), data: []byte("refreshing")},
		{key: []byte("application"), data: []byte("framework")},
		{key: []byte("limestone"), data: []byte("concrete")},
		{key: []byte("orange"), data: []byte("juice")},
		{key: []byte("band"), data: []byte("practice")},
	}
}

func basicTestNodes() []testNode {
	return []testNode{
		{key: []byte("grape"), value: []byte("vine")},
		{key: []byte("bandsaw"), value: []byte("cut")},
		{key: []byte("applet"), value: []byte("java")},
		{key: []byte("grapefruit"), value: []byte("citrus")},
		{key: []byte("apple"), value: []byte("cider")},
		{key: []byte("banana"), value: []byte("ripe")},
		{key: []byte("apricot"), value: []byte("fruit")},
		{key: []byte("bandage"), value: []byte("first-aid")},
		{key: []byte("blueberry"), value: []byte("jam")},
		{key: []byte("lemon"), value: []byte("sour")},
		{key: []byte("berry"), value: []byte("sweet")},
		{key: []byte("lime"), value: []byte("green")},
		{key: []byte("lemonade"), value: []byte("refreshing")},
		{key: []byte("application"), value: []byte("framework")},
		{key: []byte("limestone"), value: []byte("concrete")},
		{key: []byte("orange"), value: []byte("juice")},
		{key: []byte("band"), value: []byte("practice")},
	}
}

func basicTreeLevels() [][]testNode {
	return [][]testNode{
		{
			{key: []byte(nil), isLeaf: false, isRecord: false, numChildren: 5},
		},
		{
			{key: []byte("ap"), isLeaf: false, isRecord: false, numChildren: 2},
			{key: []byte("b"), isLeaf: false, isRecord: false, numChildren: 3},
			{key: []byte("grape"), isLeaf: false, isRecord: true, numChildren: 1},
			{key: []byte("l"), isLeaf: false, isRecord: false, numChildren: 2},
			{key: []byte("orange"), isLeaf: true, isRecord: true, numChildren: 0},
		},
		{
			{key: []byte("pl"), isLeaf: false, isRecord: false, numChildren: 2},
			{key: []byte("ricot"), isLeaf: true, isRecord: true, numChildren: 0},
			{key: []byte("an"), isLeaf: false, isRecord: false, numChildren: 2},
			{key: []byte("erry"), isLeaf: true, isRecord: true, numChildren: 0},
			{key: []byte("lueberry"), isLeaf: true, isRecord: true, numChildren: 0},
			{key: []byte("fruit"), isLeaf: true, isRecord: true, numChildren: 0},
			{key: []byte("emon"), isLeaf: false, isRecord: true, numChildren: 1},
			{key: []byte("ime"), isLeaf: false, isRecord: true, numChildren: 1},
		},
		{
			{key: []byte("e"), isLeaf: false, isRecord: true, numChildren: 1},
			{key: []byte("ication"), isLeaf: true, isRecord: true, numChildren: 0},
			{key: []byte("ana"), isLeaf: true, isRecord: true, numChildren: 0},
			{key: []byte("d"), isLeaf: false, isRecord: true, numChildren: 2},
			{key: []byte("ade"), isLeaf: true, isRecord: true, numChildren: 0},
			{key: []byte("stone"), isLeaf: true, isRecord: true, numChildren: 0},
		},
		{
			{key: []byte("t"), isLeaf: true, isRecord: true, numChildren: 0},
			{key: []byte("age"), isLeaf: true, isRecord: true, numChildren: 0},
			{key: []byte("saw"), isLeaf: true, isRecord: true, numChildren: 0},
		},
	}
}

func basicTreeNumNodes() int {
	ret := 0

	for _, level := range basicTreeLevels() {
		ret += len(level)
	}

	return ret
}

type testNode struct {
	key         []byte
	value       []byte
	isLeaf      bool
	isRecord    bool
	numChildren int
}
