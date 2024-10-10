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

func TestGet(t *testing.T) {
	rdb := &RadixDB{}

	// Expected tree structure:
	// .
	// ├─ grape ("vine")
	// │  └─ fruit ("citrus")
	// ├─ b ("<nil>")
	// │  ├─ an ("<nil>")
	// │  │  ├─ d ("practice")
	// │  │  │  ├─ saw ("cut")
	// │  │  │  └─ age ("medical")
	// │  │  └─ ana ("ripe")
	// │  ├─ lueberry ("fruit")
	// │  └─ erry ("sweet")
	// ├─ ap ("<nil>")
	// │  ├─ pl ("<nil>")
	// │  │  ├─ e ("cider")
	// │  │  │  └─ t ("java")
	// │  │  └─ ication ("framework")
	// │  └─ ricot ("fruit")
	// ├─ l ("<nil>")
	// │  ├─ emon ("sour")
	// │  └─ ime ("green")
	// └─ orange ("juice")
	{
		var testCases = []struct {
			key, value []byte
		}{
			{key: []byte("grape"), value: []byte("vine")},
			{key: []byte("bandsaw"), value: []byte("cut")},
			{key: []byte("applet"), value: []byte("java")},
			{key: []byte("grapefruit"), value: []byte("citrus")},
			{key: []byte("apple"), value: []byte("cider")},
			{key: []byte("banana"), value: []byte("ripe")},
			{key: []byte("apricot"), value: []byte("fruit")},
			{key: []byte("bandage"), value: []byte("medical")},
			{key: []byte("blueberry"), value: []byte("jam")},
			{key: []byte("lemon"), value: []byte("sour")},
			{key: []byte("berry"), value: []byte("sweet")},
			{key: []byte("lime"), value: []byte("green")},
			{key: []byte("application"), value: []byte("framework")},
			{key: []byte("orange"), value: []byte("juice")},
			{key: []byte("band"), value: []byte("practice")},
		}

		// Load the test cases.
		for _, test := range testCases {
			if err := rdb.Insert(test.key, test.value); err != nil {
				t.Fatal(err)
			}
		}

		// Replay the insertion. Every key/value pair must match.
		for _, test := range testCases {
			result, err := rdb.Get(test.key)

			if err != nil {
				t.Errorf("failed Get(): %v", err)
			}

			if !bytes.Equal(result, test.value) {
				t.Errorf("unexpected value: got:%q, want:%q", result, test.value)
			}
		}

		// Test path component nodes that do not hold records.
		for _, key := range []string{"b", "ban", "ap", "appl", "l"} {
			if _, err := rdb.Get([]byte(key)); err == nil {
				t.Errorf("unexpected result: got:%v, want:%v", err, ErrKeyNotFound)
			}
		}

		// Test keys that do not exist.
		for _, key := range []string{"papaya", "pitaya", "durian"} {
			if _, err := rdb.Get([]byte(key)); err == nil {
				t.Errorf("unexpected result: got:%v, want:%v", err, ErrKeyNotFound)
			}
		}
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
	//    │  │  ├─ i ("<nil>")
	//    │  │  │  ├─ cation ("framework")
	//    │  │  │  └─ ance ("shopping")
	//    │  │  └─ y ("job")
	//    │  ├─ ointment ("time")
	//    │  └─ roved ("style")
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
			{[]byte("apply"), []byte("job"), nil},
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
			t.Errorf("len(rdb.root.children): got:%d, want:1", len)
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

			for i, expected := range [][]byte{[]byte("p"), []byte("ex"), []byte("ology")} {
				if !bytes.Equal(apNode.children[i].key, expected) {
					t.Errorf("unexpected key: got:%q, want:%q", apNode.children[i].key, expected)
				}
			}

			// "a->p" node started off as a non-record node, but it became a
			// data node when "ap/nic" pair was inserted into the tree.
			if !apNode.isRecord {
				t.Errorf("apNode.isRecord: got:%t, want:true", apNode.isRecord)
			}

			// "a->p->ex" and "a->p->ology" child nodes are leaf nodes.
			apexNode := apNode.children[1]
			apologyNode := apNode.children[2]

			if !bytes.Equal(apexNode.key, []byte("ex")) {
				t.Errorf("unexpected key: got:%q, want:%q", apexNode.key, "ex")
			}

			if !bytes.Equal(apologyNode.key, []byte("ology")) {
				t.Errorf("unexpected key: got:%q, want:%q", apexNode.key, "ology")
			}

			if !apexNode.isLeaf() {
				t.Errorf("isLeaf(ex): got:%t, want:true", apexNode.isLeaf())
			}

			if !apologyNode.isLeaf() {
				t.Errorf("isLeaf(ology): got:%t, want:true", apologyNode.isLeaf())
			}
		}

		// "a->p->p" node must only have three children: "l", "ointment", "roved".
		appNode := apNode.children[0]
		{
			if !bytes.Equal(appNode.key, []byte("p")) {
				t.Errorf("appNode.key: got:%q, want:%q", appNode.key, "p")
			}

			if len := len(appNode.children); len != 3 {
				t.Errorf("len(appNode.children): got:%d, want:3", len)
			}

			for i, expectedKey := range [][]byte{[]byte("l"), []byte("ointment"), []byte("roved")} {
				if !bytes.Equal(appNode.children[i].key, expectedKey) {
					t.Errorf("unexpected key: got:%q, want:%q", appNode.children[i].key, expectedKey)
				}
			}
		}

		// "a->p->p->l" node must only have three children: "e", "i", "y".
		applNode := appNode.children[0]
		{
			if !bytes.Equal(applNode.key, []byte("l")) {
				t.Errorf("applNode.key: got:%q, want:%q", applNode.key, "l")
			}

			if len := len(applNode.children); len != 3 {
				t.Errorf("len(applNode.children): got:%d, want:3", len)
			}

			// applNode is a path component produced by split.
			if applNode.isRecord {
				t.Errorf("applNode.isRecord: got:%t, want:false", applNode.isRecord)
			}

			for i, expectedKey := range [][]byte{[]byte("e"), []byte("i"), []byte("y")} {
				if !bytes.Equal(applNode.children[i].key, expectedKey) {
					t.Errorf("unexpected key: got:%q, want:%q", applNode.children[i].key, expectedKey)
				}
			}

			// "e" and "y" are leaf nodes.
			if !applNode.children[0].isLeaf() {
				t.Errorf("isLeaf(e): got:%t, want:true", applNode.children[0].isLeaf())
			}

			if !applNode.children[2].isLeaf() {
				t.Errorf("isLeaf(y): got:%t, want:0", applNode.children[1].isLeaf())
			}
		}

		// "a->p->p->l->i" node must only have two children: "cation", "ance".
		appliNode := applNode.children[1]
		{
			if !bytes.Equal(appliNode.key, []byte("i")) {
				t.Errorf("applNode.key: got:%q, want:%q", applNode.key, "l")
			}

			if len := len(appliNode.children); len != 2 {
				t.Errorf("len(appliNode.children): got:%d, want:2", len)
			}

			// "a->p->p->l->i" node is a path component produced by split.
			if appliNode.isRecord {
				t.Errorf("applNode.isRecord: got:%t, want:false", appliNode.isRecord)
			}

			for i, expectedKey := range [][]byte{[]byte("cation"), []byte("ance")} {
				if !bytes.Equal(appliNode.children[i].key, expectedKey) {
					t.Errorf("unexpected key: got:%q, want:%q", appliNode.children[i].key, expectedKey)
				}

				// Every child: "cation" and "ance" are leaf nodes.
				isLeaf := appliNode.children[i].isLeaf()
				if !isLeaf {
					t.Errorf("isLeaf(%q): got:%t, want:true", appliNode.children[i].key, isLeaf)
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
