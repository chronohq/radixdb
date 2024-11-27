package arc

import "testing"

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
