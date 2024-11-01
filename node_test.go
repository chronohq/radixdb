package radixdb

import (
	"bytes"
	"encoding/binary"
	"testing"
)

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

func TestFindChild(t *testing.T) {
	subject := &node{}

	subject.addChild(&node{key: []byte("durian")})
	subject.addChild(&node{key: []byte("apple")})
	subject.addChild(&node{key: []byte("cherry")})
	subject.addChild(&node{key: []byte("banana")})

	tests := []struct {
		key           []byte
		expectedIndex int
		expectedErr   error
	}{
		{[]byte("apple"), 0, nil},
		{[]byte("banana"), 1, nil},
		{[]byte("cherry"), 2, nil},
		{[]byte("durian"), 3, nil},
		{[]byte("orange"), -1, ErrKeyNotFound},
	}

	for _, test := range tests {
		child, index, err := subject.findChild(test.key)

		if err != test.expectedErr {
			t.Errorf("unexpected error: got:%v, want:%v", err, test.expectedErr)
		}

		if index != test.expectedIndex {
			t.Errorf("unexpected index (%q): got:%d, want:%d", test.key, index, test.expectedIndex)
		}

		if test.expectedErr == nil {
			if !bytes.Equal(child.key, test.key) {
				t.Errorf("unexpected child: got:%q, want:%q", child.key, test.key)
			}
		}
	}
}

func TestAddChild(t *testing.T) {
	parent := &node{}

	child1 := &node{key: []byte("apple")}
	child2 := &node{key: []byte("banana")}
	child3 := &node{key: []byte("avocado")}
	child4 := &node{key: []byte("alpha")}
	child5 := &node{key: []byte("carrot")}

	// Test with 1 child.
	{
		parent.addChild(child1)

		if len(parent.children) != 1 {
			t.Errorf("unexpected len: got:%d, want:1", len(parent.children))
		}

		if !bytes.Equal(parent.children[0].key, child1.key) {
			t.Errorf("unexpected key: got:%q, want:%q", parent.children[0].key, child1.key)
		}
	}

	// Test with 2 children.
	{
		parent.addChild(child2)

		if len(parent.children) != 2 {
			t.Errorf("unexpected len: got:%d, want:2", len(parent.children))
		}

		expected := [][]byte{[]byte("apple"), []byte("banana")}

		for i, child := range parent.children {
			if !bytes.Equal(child.key, expected[i]) {
				t.Errorf("unexpected child, got:%q, want:%q", child.key, expected[i])
			}
		}

	}

	// Test with a child that should sit in-between the 2 existing nodes.
	{
		parent.addChild(child3)

		if len(parent.children) != 3 {
			t.Errorf("unexpected len: got:%d, want:3", len(parent.children))
		}

		expected := [][]byte{[]byte("apple"), []byte("avocado"), []byte("banana")}

		for i, child := range parent.children {
			if !bytes.Equal(child.key, expected[i]) {
				t.Errorf("unexpected child, got:%q, want:%q", child.key, expected[i])
			}
		}
	}

	// Test with a child that should be in the 0th index.
	{
		parent.addChild(child4)

		if len(parent.children) != 4 {
			t.Errorf("unexpected len: got:%d, want:4", len(parent.children))
		}

		expected := [][]byte{[]byte("alpha"), []byte("apple"), []byte("avocado"), []byte("banana")}

		for i, child := range parent.children {
			if !bytes.Equal(child.key, expected[i]) {
				t.Errorf("unexpected child, got:%q, want:%q", child.key, expected[i])
			}
		}
	}

	// Test with a child that should go at the end.
	{
		parent.addChild(child5)

		if len(parent.children) != 5 {
			t.Errorf("unexpected len: got:%d, want:5", len(parent.children))
		}

		expected := [][]byte{[]byte("alpha"), []byte("apple"), []byte("avocado"), []byte("banana"), []byte("carrot")}

		for i, child := range parent.children {
			if !bytes.Equal(child.key, expected[i]) {
				t.Errorf("unexpected child, got:%q, want:%q", child.key, expected[i])
			}
		}
	}

	// Test with a child that has a duplicate key. Technically this would not
	// happen since the key would be rejected before a child is inserted.
	{
		parent.addChild(&node{key: []byte("apple")})

		if len(parent.children) != 6 {
			t.Errorf("unexpected len: got:%d, want:5", len(parent.children))
		}

		expected := [][]byte{[]byte("alpha"), []byte("apple"), []byte("apple"), []byte("avocado"), []byte("banana"), []byte("carrot")}

		for i, child := range parent.children {
			if !bytes.Equal(child.key, expected[i]) {
				t.Errorf("unexpected child, got:%q, want:%q", child.key, expected[i])
			}
		}
	}
}

func TestRemoveChild(t *testing.T) {
	subject := &node{}

	appleNode := &node{key: []byte("apple")}
	bananaNode := &node{key: []byte("banana")}
	cherryNode := &node{key: []byte("cherry")}
	durianNode := &node{key: []byte("durian")}
	orangeNode := &node{key: []byte("orange")}

	subject.addChild(bananaNode)
	subject.addChild(durianNode)
	subject.addChild(appleNode)
	subject.addChild(cherryNode)

	// Test removal of exising child.
	{
		if err := subject.removeChild(bananaNode); err != nil {
			t.Errorf("unexpected error: got:%v, want:nil", err)
		}

		expected := [][]byte{[]byte("apple"), []byte("cherry"), []byte("durian")}

		if len(subject.children) != len(expected) {
			t.Errorf("unexpected child count: got:%d, want:%d", len(subject.children), len(expected))
		}

		for i, child := range subject.children {
			if !bytes.Equal(child.key, expected[i]) {
				t.Errorf("unexpected child, got:%q, want:%q", child.key, expected[i])
			}
		}
	}

	// Test removal of a child that does not exist.
	{
		if err := subject.removeChild(orangeNode); err != ErrKeyNotFound {
			t.Errorf("unexpected error: got:%v, want:%v", err, ErrKeyNotFound)
		}
	}

	// Test removal until only one node remains.
	{
		if err := subject.removeChild(durianNode); err != nil {
			t.Errorf("unexpected error: got:%v, want:nil", err)
		}

		if err := subject.removeChild(appleNode); err != nil {
			t.Errorf("unexpected error: got:%v, want:nil", err)
		}

		expected := [][]byte{[]byte("cherry")}

		if len(subject.children) != len(expected) {
			t.Errorf("unexpected child count: got:%d, want:%d", len(subject.children), len(expected))
		}

		for i, child := range subject.children {
			if !bytes.Equal(child.key, expected[i]) {
				t.Errorf("unexpected child, got:%q, want:%q", child.key, expected[i])
			}
		}
	}

	// Test removal of last child.
	{
		if err := subject.removeChild(cherryNode); err != nil {
			t.Errorf("unexpected error: got:%v, want:nil", err)
		}

		if len(subject.children) != 0 {
			t.Errorf("unexpected child count: got:%d, want:0", len(subject.children))
		}
	}
}

func TestUpdateChecksum(t *testing.T) {
	n := &node{
		key:      []byte("apple"),
		data:     []byte("sauce"),
		isRecord: true,
	}

	// Manually compute the correct checksum.
	expectedChecksum, _ := n.calculateChecksum()

	// Compute the test subject.
	if err := n.updateChecksum(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if n.checksum != expectedChecksum {
		t.Errorf("checksum mismatch, got:%d, want:%d", n.checksum, expectedChecksum)
	}

	if !n.verifyChecksum() {
		t.Errorf("checksum verification failed, node:%q", n.key)
	}
}

func TestPrependKey(t *testing.T) {
	subject := &node{key: []byte("child")}

	prefix := []byte("parent-")
	expected := []byte("parent-child")

	subject.prependKey(prefix)

	if !bytes.Equal(subject.key, expected) {
		t.Errorf("testPrepend(): got:%q, want:%q", subject.key, expected)
	}

	subject.prependKey(nil)

	if !bytes.Equal(subject.key, expected) {
		t.Errorf("testPrepend(): got:%q, want:%q", subject.key, expected)
	}

	if !subject.verifyChecksum() {
		t.Errorf("checksum verification failed, node:%q", subject.key)
	}
}

func TestSetValue(t *testing.T) {
	tests := []struct {
		key           []byte
		value         []byte
		isBlob        bool
		blobStoreSize int
	}{
		{[]byte("apple"), []byte("sauce"), false, 0},
		{[]byte("banana"), make([]byte, 33), true, 1},
	}

	rdb := New()

	for _, test := range tests {
		n := &node{}
		n.setKey(test.key)
		n.setValue(rdb.blobs, test.value)

		if n.isBlob != test.isBlob {
			t.Errorf("unexpected isBlob, got:%t, want:%t", n.isBlob, test.isBlob)
		}

		if len(rdb.blobs) != test.blobStoreSize {
			t.Errorf("unexpected blobStore size, got:%d, want:%d", len(rdb.blobs), test.blobStoreSize)
		}

		// Test that the blobID is stored in the value slice.
		if test.isBlob {
			blobID, err := buildBlobID(n.data)

			if err != nil {
				t.Errorf("failed to buildBlobID: %v", err)
			}

			blob, found := rdb.blobs[blobID]

			if !found {
				t.Error("cound not find blob")
				return
			}

			if !bytes.Equal(blob.value, test.value) {
				t.Errorf("value mismatch, got:%q, want:%q", blob.value, test.value)
			}
		}
	}
}

func TestSerializeWithoutKey(t *testing.T) {
	subject := node{
		key:      []byte("apple"),
		data:     []byte("sauce"),
		isRecord: true,
		children: nil,
	}

	subject.addChild(&node{key: []byte("test-1")})
	subject.addChild(&node{key: []byte("test-2")})

	subject.updateChecksum()

	rawBytes, err := subject.serializeWithoutKey()

	if err != nil {
		t.Errorf("node serialization failed: %v", err)
	}

	// Manually decode the raw binary representation.
	reader := bytes.NewReader(rawBytes)

	// Reconstruct the node checksum.
	var checksum uint32

	if err := binary.Read(reader, binary.LittleEndian, &checksum); err != nil {
		t.Errorf("failed to read checksum: %v", err)
	}

	if checksum != subject.checksum {
		t.Errorf("unexpected checksum, got:%v, want:%v", checksum, subject.checksum)
	}

	// Reconstruct the value and its length.
	var valLen uint64

	if err := binary.Read(reader, binary.LittleEndian, &valLen); err != nil {
		t.Fatalf("failed to read value length: %v", err)
	}

	if want := uint64(len(subject.data)); want != valLen {
		t.Errorf("unexpected value length, got:%d, want:%d", valLen, want)
	}

	valData := make([]byte, valLen)

	if _, err := reader.Read(valData); err != nil {
		t.Fatalf("failed to read value data: %v", err)
	}

	if !bytes.Equal(valData, subject.data) {
		t.Errorf("unexpected value data, got:%q, want:%q", valData, subject.data)
	}

	// Reconstruct the child count.
	var numChildren uint64

	if err := binary.Read(reader, binary.LittleEndian, &numChildren); err != nil {
		t.Fatalf("failed to read child count: %v", err)
	}

	if want := uint64(len(subject.children)); want != numChildren {
		t.Errorf("unexpected child count, got:%d, want:%d", numChildren, want)
	}

	// Verify that the child offset region is reserved.
	expectedReservedSpace := numChildren * sizeOfUint64

	if remaining := reader.Len(); remaining != int(expectedReservedSpace) {
		t.Errorf("unexpected child offset region size, got:%d, want:%d", remaining, expectedReservedSpace)
	}

	// Test a node with invalid checksum.
	{
		subject := node{
			key:      []byte("banana"),
			data:     []byte("smoothie"),
			isRecord: true,
			children: nil,
		}

		subject.updateChecksum()

		// Tamper with the key.
		subject.key = []byte("bandana")

		if _, err := subject.serializeWithoutKey(); err != ErrInvalidChecksum {
			t.Error("expected node serialization failure")
		}
	}
}

func TestAsDescriptor(t *testing.T) {
	src := node{
		key:      []byte("apple"),
		data:     []byte("sauce"),
		isRecord: true,
		children: nil,
	}

	src.addChild(&node{key: []byte("cherry")})
	src.addChild(&node{key: []byte("durian")})

	subject, err := src.asDescriptor()

	if err != ErrInvalidChecksum {
		t.Fatalf("unexpected error, got:%v, want:%v", err, ErrInvalidChecksum)
	}

	src.updateChecksum()

	subject, err = src.asDescriptor()

	if err != nil {
		t.Fatal(err)
	}

	if int(subject.numChildren) != len(src.children) {
		t.Fatalf("numChildren mismatch, got:%d, want:%d", subject.numChildren, len(src.children))
	}

	if cap(subject.childOffsets) != len(src.children) {
		t.Fatalf("unexpected childOffsets capacity, got:%d, want:%d", cap(subject.childOffsets), len(src.children))
	}

	if int(subject.dataLen) != len(src.data) {
		t.Fatalf("dataLen mismatch, got:%d, want:%d", subject.dataLen, len(src.data))
	}

	// Test that the underlying pointer of the data is the same.
	if &subject.data[0] != &src.data[0] {
		t.Fatalf("data address mismatch, got:%p, want:%p", &subject.data[0], &src.data[0])
	}

	if subject.isRecord != 1 {
		t.Fatalf("invalid isRecord, got:%d, want:1", subject.isRecord)
	}

	if subject.isBlob == 1 {
		t.Fatalf("invalid isBlob, got:%d, want:0", subject.isBlob)
	}
}
