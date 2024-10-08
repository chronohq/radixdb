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

func TestInsert(t *testing.T) {
	rdb := &RadixDB{}

	// Test the tree structure using keys of varying length and common prefix.
	//
	// Expected tree structure:
	//
	// a ("team")
	// └─ p ("nic")
	//    ├─ p ("store")
	//    │  ├─ l ("<nil>")
	//    │  │  ├─ e ("sauce")
	//    │  │  ├─ y ("force")
	//    │  │  └─ i ("<nil>")
	//    │  │     ├─ cation ("framework")
	//    │  │     └─ ance ("shopping")
	//    │  ├─ roved ("style")
	//    │  └─ ointment ("time")
	//    ├─ ex ("summit")
	//    └─ ology ("accepted")
	{
		tests := []struct {
			key         []byte
			value       []byte
			expectedErr error
		}{
			{[]byte("a"), []byte("team"), nil},
			{[]byte("app"), []byte("store"), nil},
			{[]byte("apple"), []byte("sauce"), nil},
			{[]byte("approved"), []byte("style"), nil},
			{[]byte("apply"), []byte("force"), nil},
			{[]byte("apex"), []byte("summit"), nil},
			{[]byte("application"), []byte("framework"), nil},
			{[]byte("apology"), []byte("accepted"), nil},
			{[]byte("appointment"), []byte("time"), nil},
			{[]byte("appliance"), []byte("shopping"), nil},
			{[]byte("ap"), []byte("nic"), nil},

			// Throw in some intentional insertion errors.
			{nil, []byte("boo"), ErrNilKey},
			{[]byte("ap"), []byte("news"), ErrDuplicateKey},
			{[]byte("apple"), []byte("cider"), ErrDuplicateKey},
		}

		for _, test := range tests {
			if err := rdb.Insert(test.key, test.value); err != test.expectedErr {
				t.Errorf("rdb.Insert: got:%v, want:%v", err, test.expectedErr)
			}
		}

		if !bytes.Equal(rdb.root.key, []byte("a")) {
			t.Errorf("rdb.root.key: got:%q, want:%q", rdb.root.key, "a")
		}

		// Root must only have one children: "p".
		if len := len(rdb.root.children); len != 1 {
			t.Errorf("len(rdb.root.children): got:%d, want:3", len)
		}

		// "a->p" node must only have three children: "p", "ex", "ology",
		apNode := rdb.root.children[0]
		{
			if !bytes.Equal(apNode.key, []byte("p")) {
				t.Errorf("apNode.key: got:%q, want:%q", apNode.key, "p")
			}

			if len := len(apNode.children); len != 3 {
				t.Errorf("len(apNode.children): got:%d, want:3", len)
			}

			for i, expectedKey := range [][]byte{[]byte("p"), []byte("ex"), []byte("ology")} {
				if !bytes.Equal(apNode.children[i].key, expectedKey) {
					t.Errorf("unexpected key: got:%q, want:%q", apNode.children[i].key, expectedKey)
				}
			}

			// "a->p" node _was_ a non-record row, but it became one when
			// "ap/nic" pair was inserted into the tree.
			if !apNode.isRecord {
				t.Errorf("apNode.isRecord: got:%t, want:true", apNode.isRecord)
			}
		}

		// "a->p->p" node must only have three children: "l", "roved", "ointment".
		appNode := apNode.children[0]
		{
			if !bytes.Equal(appNode.key, []byte("p")) {
				t.Errorf("appNode.key: got:%q, want:%q", appNode.key, "p")
			}

			if len := len(appNode.children); len != 3 {
				t.Errorf("len(appNode.children): got:%d, want:3", len)
			}

			for i, expectedKey := range [][]byte{[]byte("l"), []byte("roved"), []byte("ointment")} {
				if !bytes.Equal(appNode.children[i].key, expectedKey) {
					t.Errorf("unexpected key: got:%q, want:%q", appNode.children[i].key, expectedKey)
				}
			}

			// "a->p->p->l" node was produced during node split.
			{
				applNode := appNode.children[0]

				if !bytes.Equal(applNode.key, []byte("l")) {
					t.Errorf("applNode.key: got:%q, want:%q", applNode.key, "l")
				}

				if applNode.isRecord {
					t.Errorf("applNode.isRecord: got:%t, want:false", applNode.isRecord)
				}
			}
		}

		rdb.clear()
	}

	// Mild fuzzing: Insert random keys for memory errors.
	{
		numRandomInserts := 100000
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
