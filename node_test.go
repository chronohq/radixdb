package radixdb

import (
	"bytes"
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

func TestSortChildren(t *testing.T) {
	node := &node{
		children: []*node{
			{key: []byte("banana")},
			{key: []byte("apple")},
			{key: []byte("cherry")},
			{key: []byte("Banana")},
			{key: []byte("applet")},
			{key: []byte("Apple")},
			{key: []byte("Bananas")},
		},
	}

	node.sortChildren()

	expected := [][]byte{
		[]byte("Apple"),
		[]byte("Banana"),
		[]byte("Bananas"),
		[]byte("apple"),
		[]byte("applet"),
		[]byte("banana"),
		[]byte("cherry"),
	}

	for i, child := range node.children {
		if !bytes.Equal(child.key, expected[i]) {
			t.Errorf("sortChildren(): expected key %q, got %q", expected[i], child.key)
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
