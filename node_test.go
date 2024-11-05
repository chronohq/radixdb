// Copyright Chrono Technologies LLC
// SPDX-License-Identifier: MIT

package arc

import (
	"bytes"
	"errors"
	"testing"
)

func TestForEachChild(t *testing.T) {
	subject := node{}

	expectedKeys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("durian"),
	}

	subject.addChild(&node{key: expectedKeys[2]})
	subject.addChild(&node{key: expectedKeys[0]})
	subject.addChild(&node{key: expectedKeys[3]})
	subject.addChild(&node{key: expectedKeys[1]})

	subject.forEachChild(func(idx int, n *node) error {
		got := n.key
		want := expectedKeys[idx]

		if !bytes.Equal(got, want) {
			t.Fatalf("unexpected node, got:%q, want:%q", got, want)
		}

		return nil
	})
}

func TestFindChild(t *testing.T) {
	subject := node{}

	expectedKeys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("durian"),
	}

	for _, key := range expectedKeys {
		subject.addChild(&node{key: key})
	}

	for _, key := range expectedKeys {
		node, err := subject.findChild(key)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !bytes.Equal(node.key, key) {
			t.Fatalf("unexpected node, got:%q, want:%q", node.key, key)
		}
	}

	for _, key := range [][]byte{[]byte("apricot"), []byte("crisp")} {
		_, err := subject.findChild(key)

		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("unexpected error, got:%v, want:%v", err, ErrKeyNotFound)
		}
	}
}

func TestRemoveChild(t *testing.T) {
	subject := node{}

	expectedKeys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("durian"),
	}

	for _, key := range expectedKeys {
		subject.addChild(&node{key: key})
	}

	// Test basic removal operations.
	{
		// Node exists before removal.
		if _, err := subject.findChild(expectedKeys[2]); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := subject.removeChild(&node{key: expectedKeys[2]}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Child count has been updated.
		if subject.numChildren != len(expectedKeys)-1 {
			t.Fatalf("unexpected numChildren, got:%d, want:%d", subject.numChildren, len(expectedKeys)-1)
		}

		// Node unavailable after removal.
		if _, err := subject.findChild(expectedKeys[2]); err != ErrKeyNotFound {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Test removal of non-existent node.
	{
		if err := subject.removeChild(&node{key: []byte("bogus")}); err != ErrKeyNotFound {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}
