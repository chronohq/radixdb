package arc

import (
	"bytes"
	"testing"
)

func TestArcHeaderSerialize(t *testing.T) {
	testCases := []struct {
		name   string
		header arcHeader
	}{
		{
			name:   "with a new default header",
			header: newArcHeader(),
		},
		{
			name: "with arcFileOpened status",
			header: arcHeader{
				magic:   magicByte,
				version: fileFormatVersion,
				status:  arcFileOpened,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bytes, err := tc.header.serialize()

			if err != nil {
				t.Fatal(err)
			}

			subject, err := newArcHeaderFromBytes(bytes)

			if err != nil {
				t.Fatalf("newArcHeaderFromBytes(): %v", err)
			}

			if subject.magic != tc.header.magic {
				t.Errorf("unexpected magicByte: got:%d, want:%d", subject.magic, tc.header.magic)
			}

			if subject.version != tc.header.version {
				t.Errorf("unexpected version: got:%d, want:%d", subject.version, tc.header.version)
			}

			if subject.status != tc.header.status {
				t.Errorf("unexpected status: got:%d, want:%d", subject.status, tc.header.status)
			}
		})
	}
}

func TestMakePersistentNode(t *testing.T) {
	testCases := []struct {
		name     string
		src      node
		children []node
	}{
		{
			name: "with record node",
			src: node{
				key:      []byte("app"),
				data:     []byte("band"),
				isRecord: true,
			},
			children: []node{
				{key: []byte("le")},
				{key: []byte("store")},
			},
		},
		{
			name: "with blob record node",
			src: node{
				key:       []byte("x"),
				data:      []byte("y"),
				isRecord:  true,
				blobValue: true,
			},
		},
		{
			name: "with non-record node",
			src: node{
				key:      []byte("prefix-"),
				data:     nil,
				isRecord: false,
			},
			children: []node{
				{key: []byte("a")},
				{key: []byte("b")},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, child := range tc.children {
				tc.src.addChild(&child)
			}

			subject := makePersistentNode(tc.src)

			if int(subject.numChildren) != int(tc.src.numChildren) {
				t.Errorf("unexpected numChildren: got:%d, want:%d", subject.numChildren, tc.src.numChildren)
			}

			if int(subject.keyLen) != len(tc.src.key) {
				t.Errorf("unexpected keyLen: got:%d, want:%d", subject.keyLen, len(tc.src.key))
			}

			if &subject.key[0] != &tc.src.key[0] {
				t.Errorf("unexpected key address: got:%p, want:%p", &subject.data[0], &tc.src.data[0])
			}

			if int(subject.dataLen) != len(tc.src.data) {
				t.Errorf("unexpected dataLen: got:%d, want:%d", subject.dataLen, len(tc.src.data))
			}

			if subject.isRecord() != tc.src.isRecord {
				t.Errorf("unexpected isRecord: got:%t, want:%t", subject.isRecord(), tc.src.isRecord)
			}

			if subject.hasBlob() != tc.src.blobValue {
				t.Errorf("unexpected hasBlob: got:%t, want:%t", subject.hasBlob(), tc.src.blobValue)
			}

			if tc.src.isRecord {
				if &subject.data[0] != &tc.src.data[0] {
					t.Errorf("unexpected data address: got:%p, want:%p", &subject.data[0], &tc.src.data[0])
				}
			}
		})
	}
}

func TestPersistentNodeSerialize(t *testing.T) {
	testCases := []struct {
		name        string
		node        node
		children    []node
		numChildren int
	}{
		{
			name: "with record node",
			node: node{
				key:      []byte("app"),
				data:     []byte("band"),
				isRecord: true,
			},
			children: []node{
				{key: []byte("le")},
				{key: []byte("store")},
			},
		},
		{
			name: "with non-record node",
			node: node{
				key:      []byte("app"),
				data:     nil,
				isRecord: false,
			},
			children: []node{
				{key: []byte("le")},
				{key: []byte("store")},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, child := range tc.children {
				tc.node.addChild(&child)
			}

			pn := makePersistentNode(tc.node)

			// Set non-zero offsets for test purpose.
			pn.firstChildOffset = 128
			pn.nextSiblingOffset = 256

			serializedNode, err := pn.serialize()

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got, err := makePersistentNodeFromBytes(serializedNode)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got.flags != pn.flags {
				t.Errorf("unexpected flags: got:%d, want:%d", got.flags, pn.flags)
			}

			if got.numChildren != pn.numChildren {
				t.Errorf("unexpected numChildren: got:%d, want:%d", got.numChildren, pn.numChildren)
			}

			if got.keyLen != pn.keyLen {
				t.Errorf("unexpected keyLen: got:%d, want:%d", got.keyLen, pn.keyLen)
			}

			if got.dataLen != pn.dataLen {
				t.Errorf("unexpected dataLen: got:%d, want:%d", got.dataLen, pn.dataLen)
			}

			if got.firstChildOffset != pn.firstChildOffset {
				t.Errorf("unexpected firstChildOffset: got:%d, want:%d", got.firstChildOffset, pn.firstChildOffset)
			}

			if got.nextSiblingOffset != pn.nextSiblingOffset {
				t.Errorf("unexpected nextSiblingOffset: got:%d, want:%d", got.nextSiblingOffset, pn.nextSiblingOffset)
			}

			if !bytes.Equal(got.key, pn.key) {
				t.Errorf("unexpected key: got:%q, want:%q", got.key, pn.key)
			}

			if !bytes.Equal(got.data, pn.data) {
				t.Errorf("unexpected key: got:%q, want:%q", got.data, pn.data)
			}
		})
	}
}
