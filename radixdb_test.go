package radixdb

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
