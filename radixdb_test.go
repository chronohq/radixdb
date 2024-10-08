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
			value: []byte("juice"),
		},
	}

	newNode := &node{
		key:   []byte("appstore"),
		value: []byte("registry"),
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
		value: []byte("jam"),
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

// TODO(toru): Reorganize the tests once all the basic cases are defined.
// In the meantime, keep adding test cases to catch regressions.
func TestInsert(t *testing.T) {
	rdb := &RadixDB{}

	// Test nil key insertion.
	if err := rdb.Insert(nil, []byte("nil-key")); err != ErrNilKey {
		t.Errorf("expected error: got:nil, want:%v", ErrNilKey)
	}

	// Test duplicate key insertion.
	if err := rdb.Insert([]byte("apple"), []byte("juice")); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := rdb.Insert([]byte("apple"), []byte("cider")); err != ErrDuplicateKey {
		t.Errorf("expected error: got:nil, want:%v", ErrDuplicateKey)
	}

	if len := rdb.Len(); len != 1 {
		t.Errorf("Len(): got:%d, want:1", len)
	}

	// Test non-common key insertion. The node should be a direct child of root.
	if err := rdb.Insert([]byte("banana"), []byte("smoothie")); err != nil {
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
	if err := rdb.Insert([]byte("applet"), []byte("app")); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if err := rdb.Insert([]byte("apricot"), []byte("farm")); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if err := rdb.Insert([]byte("baking"), []byte("show")); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len := rdb.Len(); len != 5 {
		t.Errorf("Len(): got:%d, want:5", len)
	}

	// Test insertion on path component node (e.g. split/intermediate node).
	if err := rdb.Insert([]byte("ba"), []byte("flights")); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Second insertion must fail.
	if err := rdb.Insert([]byte("ba"), []byte("flights")); err == nil {
		t.Errorf("expected error: got:%v, want:%v", err, ErrDuplicateKey)
	}

	// Expected tree structure at this point:
	// .
	// ├─ ap ("<nil>")
	// │  ├─ ple ("juice")
	// │  │  └─ t ("app")
	// │  └─ ricot ("farm")
	// └─ ba ("flights")
	//   ├─ nana ("smoothie")
	//   └─ king ("show")

	// Test path component node awareness.
	if len := len(rdb.root.children); len != 2 {
		t.Errorf("len(rdb.root.children): got:%d, want:2", len)
	}

	apNode := rdb.root.children[0]
	baNode := rdb.root.children[1]

	if bytes.Equal(apNode.key, []byte("ap")) {
		if apNode.isRecord {
			t.Errorf("node.isRecord: got:%t, want:%t", apNode.isRecord, false)
		}

		if len := len(apNode.children); len != 2 {
			t.Errorf("len(apNode.children): got:%d, want:2", len)
		}

		pleNode := apNode.children[0]
		ricotNode := apNode.children[1]

		if !bytes.Equal(pleNode.key, []byte("ple")) {
			t.Errorf("got:%q, want:%q", pleNode.key, "ple")
		}

		if !bytes.Equal(ricotNode.key, []byte("ricot")) {
			t.Errorf("got:%q, want:%q", ricotNode.key, "ricot")
		}

		if !pleNode.isRecord {
			t.Errorf("pleRecord.isRecord: got:%t, want:%t", pleNode.isRecord, true)
		}

		if !ricotNode.isRecord {
			t.Errorf("ricotNode.isRecord: got:%t, want:%t", ricotNode.isRecord, true)
		}
	} else {
		t.Errorf("got:%q, want:%q", apNode.key, "ap")
	}

	if bytes.Equal(baNode.key, []byte("ba")) {
		if !baNode.isRecord {
			t.Errorf("node.isRecord: got:%t, want:%t", baNode.isRecord, true)
		}

		if len := len(baNode.children); len != 2 {
			t.Errorf("len(baNode.children): got:%d, want:2", len)
		}
	} else {
		t.Errorf("got:%q, want:%q", baNode.key, "ba")
	}

	// Mild fuzzing: Insert random keys for memory errors.
	numRandomInserts := 80000
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

func TestClear(t *testing.T) {
	rdb := &RadixDB{}

	rdb.Insert([]byte("k1"), []byte("v1"))
	rdb.Insert([]byte("k2"), []byte("v2"))
	rdb.Insert([]byte("k3"), []byte("v3"))

	rdb.clear()

	if len := rdb.Len(); len != 0 {
		t.Errorf("Len(): got:%d, want:0", len)
	}

	if rdb.root != nil {
		t.Error("expected root to be nil")
	}
}
