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

		if rdb.numNodes != 21 {
			t.Errorf("unexpected node count: got:%d, want:21", rdb.numNodes)
		}

		if rdb.numRecords != 15 {
			t.Errorf("unexpected record count: got:%d, want:15", rdb.numRecords)
		}

		// Replay the insertion. Every key/value pair must match.
		for _, test := range testCases {
			result, err := rdb.Get(test.key)

			if err != nil {
				t.Errorf("failed Get(): %v", err)
			}

			if !bytes.Equal(result, test.value) {
				t.Errorf("unexpected value (%q): got:%q, want:%q", test.key, result, test.value)
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
	//    ├─ ex ("summit")
	//    ├─ ology ("accepted")
	//    └─ p ("store")
	//       ├─ l ("<nil>")
	//       │  ├─ e ("sauce")
	//       │  ├─ i ("<nil>")
	//       │  │  ├─ ance ("shopping")
	//       │  │  └─ cation ("framework")
	//       │  └─ y ("job")
	//       ├─ ointment ("time")
	//       └─ roved ("style")
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

		if rdb.numNodes != 13 {
			t.Errorf("unexpected node count: got:%d, want:13", rdb.numNodes)
		}

		if rdb.numRecords != 11 {
			t.Errorf("unexpected record count: got:%d, want:11", rdb.numRecords)
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

			for i, expected := range [][]byte{[]byte("ex"), []byte("ology"), []byte("p")} {
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
			apexNode := apNode.children[0]
			apologyNode := apNode.children[1]

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
		appNode := apNode.children[2]
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

			for i, expectedKey := range [][]byte{[]byte("ance"), []byte("cation")} {
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

func TestDelete(t *testing.T) {
	// Basic cases.
	{
		rdb := basicTestTree()

		tests := []struct {
			key         []byte
			expectedErr error
		}{
			{nil, ErrNilKey},
			{[]byte("yyy"), ErrKeyNotFound},
			{[]byte("zzz"), ErrKeyNotFound},

			// Nodes that exist but are non-record nodes.
			{[]byte("ap"), ErrKeyNotFound},
			{[]byte("appl"), ErrKeyNotFound},
			{[]byte("b"), ErrKeyNotFound},
			{[]byte("ban"), ErrKeyNotFound},
			{[]byte("l"), ErrKeyNotFound},

			// Nodes that exist.
			{[]byte("grape"), nil},
			{[]byte("grapefruit"), nil},
			{[]byte("orange"), nil},
			{[]byte("lemonade"), nil},

			// The "band" node technically exists after deletion because it
			// becomes a node that splits "bandage" and "bandsaw". Therefore
			// explicitly test that the second deletion fails.
			{[]byte("band"), nil},
			{[]byte("band"), ErrKeyNotFound},

			// Removing "banana" from the following subtree must result in
			// merged "b->an" and "b->an->d" nodes.
			//
			// ├─ b ("<nil>")
			// │  ├─ an ("<nil>")
			// │  │  ├─ ana ("ripe")
			// │  │  └─ d ("<nil>")
			// │  │    ├─ age ("medical")
			// │  │    └─ saw ("cut")
			// │  ├─ erry ("sweet")
			// │  └─ lueberry ("jam")
			//
			// Expected subtree after deleting "banana":
			//
			// ├─ b ("<nil>")
			// │  ├─ and ("<nil>")
			// │  │  ├─ age ("medical")
			// │  │  └─ saw ("cut")
			// │  ├─ erry ("sweet")
			// │  └─ lueberry ("jam")
			{[]byte("banana"), nil},

			// This is another tricky case. Deleting the "ime" node from the
			// following subtree must convert the "stone" node to "imestone",
			// because the "ime" node was compressing the path prefix.
			//
			// └─ l ("<nil>")
			//   ├─ emon ("sour")
			//   └─ ime ("green")
			//     └─ stone ("concrete")
			//
			// Expected subtree after deleting "banana":
			//
			// └─ l ("<nil>")
			//   ├─ emon ("sour")
			//   └─ imestone ("concrete")
			{[]byte("lime"), nil},

			{[]byte("limestone"), nil},
			{[]byte("lemon"), nil},

			// Removing "berry" and "blueberry" from the following subtree must
			// result in merged "b" and "b->and" nodes.
			//
			// └─ b ("<nil>")
			//   ├─ and ("<nil>")
			//   │  ├─ age ("first-aid")
			//   │  └─ saw ("cut")
			//   ├─ erry ("sweet")
			//   └─ lueberry ("jam")
			//
			// Expected subtree after the deletions:
			//
			// └─ band ("<nil>")
			//   ├─ age ("first-aid")
			//   └─ saw ("cut")
			{[]byte("berry"), nil},
			{[]byte("blueberry"), nil},

			{[]byte("bandage"), nil},
			{[]byte("bandsaw"), nil},

			// There should now only be one subtree ("ap" prefix), therefore
			// the entire tree should be flat.
			{[]byte("apricot"), nil},
			{[]byte("apple"), nil},
			{[]byte("application"), nil},
			{[]byte("applet"), nil},
		}

		var err error
		expectedTreeLen := rdb.Len()

		for _, test := range tests {
			if err = rdb.Delete(test.key); err != test.expectedErr {
				t.Errorf("failed Delete(%q): got:%v, want:%v", test.key, err, test.expectedErr)
			}

			if err == nil {
				expectedTreeLen--

				if len := rdb.Len(); len != expectedTreeLen {
					t.Errorf("unexpected tree size: got:%d, want:%d", len, expectedTreeLen)
				}
			}

			// Spin-off test suite for a complicated auto parent converstion behavior.
			if bytes.Equal(test.key, []byte("banana")) {
				subTreeRoot := rdb.root.children[1]

				if !bytes.Equal(subTreeRoot.key, []byte("b")) {
					t.Errorf("unexpected key: got:%q, want:%q", subTreeRoot.key, []byte("b"))
				}

				expected := []struct {
					key         []byte
					numChildren int
				}{
					{[]byte("and"), 2},
					{[]byte("erry"), 0},
					{[]byte("lueberry"), 0},
				}

				for i, exp := range expected {
					subject := subTreeRoot.children[i]

					if !bytes.Equal(subject.key, exp.key) {
						t.Errorf("unexpected key: got:%q, want:%q", subTreeRoot.children[i].key, exp.key)
					}

					if len := len(subject.children); len != exp.numChildren {
						t.Errorf("unexpected child count: got:%d, want:%d", len, exp.numChildren)
					}
				}

				subject := subTreeRoot.children[0]
				keys := [][]byte{[]byte("age"), []byte("saw")}

				for i, k := range keys {
					if !bytes.Equal(subject.children[i].key, k) {
						t.Errorf("unexpected key: got:%q, want:%q", subject.children[i].key, k)
					}

					if !subject.children[i].isLeaf() {
						t.Errorf("expected leaf node: got:%t", subject.children[i].isLeaf())
					}
				}
			}

			if bytes.Equal(test.key, []byte("lime")) {
				subTreeRoot := rdb.root.children[2]

				if !bytes.Equal(subTreeRoot.key, []byte("l")) {
					t.Errorf("unexpected key: got:%q, want:%q", subTreeRoot.key, []byte("l"))
				}

				if len := len(subTreeRoot.children); len != 2 {
					t.Errorf("unexpected child count: got:%d, want:2", len)
				}

				expectations := [][]byte{[]byte("emon"), []byte("imestone")}

				for i, expected := range expectations {
					if !bytes.Equal(subTreeRoot.children[i].key, expected) {
						t.Errorf("unexpected key: got:%q, want:%q", subTreeRoot.children[i].key, expected)
					}

					if !subTreeRoot.children[i].isLeaf() {
						t.Errorf("expected (%q) to be a leaf node", subTreeRoot.children[i].key)
					}
				}
			}

			if bytes.Equal(test.key, []byte("blueberry")) {
				subTreeRoot := rdb.root.children[1]

				if !bytes.Equal(subTreeRoot.key, []byte("band")) {
					t.Errorf("unexpected key: got:%q, want:%q", subTreeRoot.key, []byte("band"))
				}

				if len := len(subTreeRoot.children); len != 2 {
					t.Errorf("unexpected child count: got:%d, want:2", len)
				}

				expectations := [][]byte{[]byte("age"), []byte("saw")}

				for i, expected := range expectations {
					if !bytes.Equal(subTreeRoot.children[i].key, expected) {
						t.Errorf("unexpected key: got:%q, want:%q", subTreeRoot.children[i].key, expected)
					}

					if !subTreeRoot.children[i].isLeaf() {
						t.Errorf("expected (%q) to be a leaf node", subTreeRoot.children[i].key)
					}
				}
			}

			// Expected tree structure at this point:
			//
			// appl ("<nil>")
			//  ├─ et ("java")
			//  └─ ication ("framework")
			if bytes.Equal(test.key, []byte("apple")) {
				if !bytes.Equal(rdb.root.key, []byte("appl")) {
					t.Errorf("unexpected key: got:%q, want:%q", rdb.root.key, []byte("appl"))
				}

				if rdb.root.isRecord {
					t.Errorf("unexpected isRecord, got:%t, want:false", rdb.root.isRecord)
				}

				if len := len(rdb.root.children); len != 2 {
					t.Errorf("unexpected child count: got:%d, want:2", len)
				}

				left := rdb.root.children[0]
				right := rdb.root.children[1]

				if !bytes.Equal(left.key, []byte("et")) {
					t.Errorf("unexpected key: got:%q, want:%q", left.key, []byte("et"))
				}

				if !bytes.Equal(right.key, []byte("ication")) {
					t.Errorf("unexpected key: got:%q, want:%q", right.key, []byte("ication"))
				}
			}

			// There is only one key left in the tree.
			if bytes.Equal(test.key, []byte("application")) {
				if !bytes.Equal(rdb.root.key, []byte("applet")) {
					t.Errorf("unexpected key: got:%q, want:%q", rdb.root.key, []byte("applet"))
				}

				if len := len(rdb.root.children); len > 0 {
					t.Errorf("unexpected child count: got:%d, want:0", len)
				}

				if len := rdb.Len(); len != 1 {
					t.Errorf("unexpected tree size: got:%d, want:1", len)
				}
			}
		}

		// The tree must be empty at this point.
		if len := rdb.Len(); len != 0 {
			t.Errorf("unexpected tree size: got:%d, want:0", len)
		}
	}

	// Test redundant parent node deletion. It is common for a parent node to
	// become redundant after a node is deleted. In this case, we are targeting
	// the "pl" node of the "ap" branch, which becomes redundant after deleting
	// "apple" and "applet".
	//
	// ap ("<nil>")
	// ├─ pl ("<nil>")
	// │  ├─ e ("apple")
	// │  │  └─ t ("applet")
	// │  └─ ication ("application")
	// └─ ricot ("apricot")
	{
		rdb := basicTestTree()
		originalLen := rdb.Len()

		// Expected branch structure after the deletion.
		//
		// .
		// ├─ ap ("<nil>")
		// │  ├─ plication ("framework")
		// │  └─ ricot ("fruit")
		rdb.Delete([]byte("apple"))
		rdb.Delete([]byte("applet"))

		subject := rdb.root.children[0]

		if len := rdb.Len(); len != originalLen-2 {
			t.Errorf("unexpected tree size: got:%d, want:2", len)
		}

		if !bytes.Equal(subject.key, []byte("ap")) {
			t.Errorf("unexpected key: got:%q, want:%q", rdb.root.key, []byte("ap"))
		}

		if len := len(subject.children); len != 2 {
			t.Errorf("unexpected child count: got:%d, want:2", len)
		}

		leftNode := subject.children[0]
		rightNode := subject.children[1]

		if !bytes.Equal(leftNode.key, []byte("plication")) {
			t.Errorf("unexpected key: got:%q, want:%q", leftNode.key, []byte("plication"))
		}

		if !bytes.Equal(rightNode.key, []byte("ricot")) {
			t.Errorf("unexpected key: got:%q, want:%q", rightNode.key, []byte("ricot"))
		}

		if !leftNode.isLeaf() {
			t.Errorf("expected (%q) to be a leaf node", leftNode.key)
		}

		if !rightNode.isLeaf() {
			t.Errorf("expected (%q) to be a leaf node", rightNode.key)
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
func basicTestTree() *RadixDB {
	rdb := &RadixDB{}

	rdb.Insert([]byte("grape"), []byte("vine"))
	rdb.Insert([]byte("bandsaw"), []byte("cut"))
	rdb.Insert([]byte("applet"), []byte("java"))
	rdb.Insert([]byte("grapefruit"), []byte("citrus"))
	rdb.Insert([]byte("apple"), []byte("cider"))
	rdb.Insert([]byte("banana"), []byte("ripe"))
	rdb.Insert([]byte("apricot"), []byte("fruit"))
	rdb.Insert([]byte("bandage"), []byte("first-aid"))
	rdb.Insert([]byte("blueberry"), []byte("jam"))
	rdb.Insert([]byte("lemon"), []byte("sour"))
	rdb.Insert([]byte("berry"), []byte("sweet"))
	rdb.Insert([]byte("lime"), []byte("green"))
	rdb.Insert([]byte("lemonade"), []byte("refreshing"))
	rdb.Insert([]byte("application"), []byte("framework"))
	rdb.Insert([]byte("limestone"), []byte("concrete"))
	rdb.Insert([]byte("orange"), []byte("juice"))
	rdb.Insert([]byte("band"), []byte("practice"))

	return rdb
}
