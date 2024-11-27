package arc

const (
	flagIsRecord = 1 << iota // 0b00000001
	flagHasBlob              // 0b00000010
)

// persistentNode is the on-disk structure of Arc's radix tree node.
// All fields in this struct are persisted in the same order.
type persistentNode struct {
	flags             uint8
	numChildren       uint16
	keyLen            uint16
	dataLen           uint32
	key               []byte
	data              []byte
	firstChildOffset  uint64
	nextSiblingOffset uint64
}

func makePersistentNode(n node) persistentNode {
	var ret persistentNode

	if n.isRecord {
		ret.flags |= flagIsRecord
	}

	if n.blobValue {
		ret.flags |= flagHasBlob
	}

	ret.numChildren = uint16(n.numChildren)
	ret.keyLen = uint16(len(n.key))
	ret.dataLen = uint32(len(n.data))
	ret.key = n.key
	ret.data = n.data

	// Node offsets are unknown at initialization phase.
	ret.firstChildOffset = 0
	ret.nextSiblingOffset = 0

	return ret
}

// isRecord returns true if the isRecord flag is set.
func (pn persistentNode) isRecord() bool {
	return pn.flags&flagIsRecord != 0
}

// hasBlob returns true if the hasBlob flag is set.
func (pn persistentNode) hasBlob() bool {
	return pn.flags&flagHasBlob != 0
}
