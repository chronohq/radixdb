// Copyright Chrono Technologies LLC
// SPDX-License-Identifier: MIT

package arc

import (
	"bytes"
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
