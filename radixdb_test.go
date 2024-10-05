package radixdb

import (
	"bytes"
	"crypto/rand"
	mrand "math/rand/v2"
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
		{[]byte("match"), []byte("match"), []byte("match")},

		// Edge cases.
		{[]byte(""), []byte(""), nil},
		{nil, nil, nil},
	}

	for _, test := range tests {
		subject := longestCommonPrefix(test.a, test.b)

		if !bytes.Equal(subject, test.expected) {
			t.Errorf("(%q,%q): got:%q, want:%q", test.a, test.b, subject, test.expected)
		}
	}
}

func TestFindCompatibleChild(t *testing.T) {
	root := &node{
		children: []*node{
			{key: []byte("apple")},
			{key: []byte("banana")},
			{key: []byte("citron")},
		},
	}

	tests := []struct {
		key      []byte
		expected []byte
	}{
		{[]byte("apple"), []byte("apple")},
		{[]byte("applet"), []byte("apple")},
		{[]byte("bandage"), []byte("banana")},
		{[]byte("coconut"), []byte("citron")},
		{[]byte("durian"), nil},
		{[]byte("orange"), nil},
	}

	for _, test := range tests {
		child := root.findCompatibleChild([]byte(test.key))
		if (child == nil && test.expected != nil) || (child != nil && !bytes.Equal(child.key, test.expected)) {
			t.Errorf("findCompatibleChild(%q): got:%q, want:%q", test.key, child.key, test.expected)
		}
	}
}

func TestSplitNode(t *testing.T) {
	rdb := &RadixDB{
		root: &node{
			key:   []byte("apple"),
			value: "juice",
		},
	}

	newNode := &node{
		key:   []byte("appstore"),
		value: "registry",
	}

	// Test root split.
	commonPrefix := longestCommonPrefix(rdb.root.key, newNode.key)
	rdb.splitNode(nil, rdb.root, newNode, commonPrefix)

	if rdb.Len() != 1 && len(rdb.root.children) != 1 {
		t.Errorf("Len(): got:%d, want:1", rdb.Len())
	}

	if !bytes.Equal(rdb.root.key, commonPrefix) {
		t.Errorf("invalid root key: got:%q, want:%q", commonPrefix, rdb.root.key)
	}

	expectedKey := []byte("store")
	if !bytes.Equal(newNode.key, expectedKey) {
		t.Errorf("invalid newNode key: got:%q, want:%q", newNode.key, expectedKey)
	}

	// Test non-root split: newNode(app[store]) is the parent.
	strawberryNode := &node{
		key:   []byte("strawberry"),
		value: "jam",
	}

	commonPrefix = longestCommonPrefix(newNode.key, strawberryNode.key)
	rdb.splitNode(rdb.root, newNode, strawberryNode, commonPrefix)

	if rdb.Len() != 2 && len(rdb.root.children) != 2 {
		t.Errorf("Len(): got:%d, want:2", rdb.Len())
	}

	// newNode should now be further split to "st[ore]".
	expectedKey = []byte("ore")

	if !bytes.Equal(newNode.key, expectedKey) {
		t.Errorf("invalid newNode.key: got:%q, want:%q", newNode.key, expectedKey)
	}

	// strawberryNode should now be split to "st[rawberry]".
	expectedKey = []byte("rawberry")
	if !bytes.Equal(strawberryNode.key, expectedKey) {
		t.Errorf("invalid strawberryNode.key: got:%q, want:%q", newNode.key, expectedKey)
	}
}

func TestInsert(t *testing.T) {
	rdb := &RadixDB{}

	// Test nil key insertion.
	if err := rdb.Insert(nil, "nil-key"); err != ErrNilKey {
		t.Errorf("expected error: got:nil, want:%v", ErrNilKey)
	}

	// Test standard insertion.
	if err := rdb.Insert([]byte("apple"), "juice"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Test duplicate key insertion.
	if err := rdb.Insert([]byte("apple"), "cider"); err != ErrDuplicateKey {
		t.Errorf("expected error: got:nil, want:%v", ErrDuplicateKey)
	}

	if len := rdb.Len(); len != 1 {
		t.Errorf("Len(): got:%d, want:1", len)
	}

	// Test non-common key insertion. The node should be a direct child of root.
	if err := rdb.Insert([]byte("banana"), "smoothie"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	var found bool
	for _, node := range rdb.root.children {
		if bytes.Equal(node.key, []byte("banana")) {
			found = true
			break
		}
	}

	if !found {
		t.Error("banana expected to be a child of root")
	}

	if len := rdb.Len(); len != 2 {
		t.Errorf("Len(): got:%d, want:2", len)
	}

	// Test common prefix insertion.
	if err := rdb.Insert([]byte("applet"), "app"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if err := rdb.Insert([]byte("apricot"), "farm"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if err := rdb.Insert([]byte("baking"), "show"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len := rdb.Len(); len != 5 {
		t.Errorf("Len(): got:%d, want:5", len)
	}

	// Mild fuzzing: Insert random keys for memory errors.
	numRandomInserts := 3000
	numRecordsBefore := rdb.Len()
	numRecordsExpected := uint64(numRandomInserts + int(numRecordsBefore))

	for i := 0; i < numRandomInserts; i++ {
		// Random key length between 4 and 128 bytes.
		keyLength := mrand.IntN(128-4) + 4
		randomKey := make([]byte, keyLength)

		if _, err := rand.Read(randomKey); err != nil {
			t.Fatal(err)
		}

		if err := rdb.Insert(randomKey, randomKey); err != nil {
			t.Fatalf("%v: %v", randomKey, err)
		}
	}

	if len := rdb.Len(); len != numRecordsExpected {
		t.Errorf("Len(): got:%d, want:%d", len, numRecordsExpected)
	}
}
