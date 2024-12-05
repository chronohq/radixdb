package arc

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

const (
	// magicByte is the first byte of an Arc file.
	magicByte = byte(0x41)

	// fileFormatVersion is the database file format version.
	fileFormatVersion = uint8(1)

	// sizeOfUint8 is the size of uint8 in bytes.
	sizeOfUint8 = 1

	// sizeOfUint16 is the size of uint16 in bytes.
	sizeOfUint16 = 2

	// sizeOfUint32 is the size of uint32 in bytes.
	sizeOfUint32 = 4

	// sizeOfUint64 is the size of uint64 in bytes.
	sizeOfUint64 = 8

	// checksumLen is the length of a checksum in bytes.
	checksumLen = sizeOfUint32

	// minNodeBytesLen is the minimum length of a serialized node.
	minNodeBytesLen = sizeOfUint8 + sizeOfUint16 + sizeOfUint16 + sizeOfUint32 + sizeOfUint64 + sizeOfUint64

	// arcHeaderBytesLen is the length of the arc file header.
	arcHeaderBytesLen = sizeOfUint8 + sizeOfUint8 + sizeOfUint8 + checksumLen
)

// Index node flags.
const (
	flagIsRecord = 1 << iota // 0b00000001
	flagHasBlob              // 0b00000010
)

const (
	arcFileClosed = 0
	arcFileOpened = 1
)

type arcHeader struct {
	magic   byte
	version byte
	status  byte
}

func newArcHeader() arcHeader {
	return arcHeader{
		magic:   magicByte,
		version: fileFormatVersion,
		status:  arcFileClosed,
	}
}

func (ah *arcHeader) serialize() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte(ah.magic)
	buf.WriteByte(ah.version)
	buf.WriteByte(ah.status)

	checksum, err := computeChecksum(buf.Bytes())

	if err != nil {
		return nil, err
	}

	if err = binary.Write(&buf, binary.LittleEndian, checksum); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func newArcHeaderFromBytes(src []byte) (arcHeader, error) {
	var ret arcHeader

	if len(src) != arcHeaderBytesLen {
		return ret, ErrCorrupted
	}

	reader := bytes.NewReader(src)

	if err := binary.Read(reader, binary.LittleEndian, &ret.magic); err != nil {
		return ret, err
	}

	if err := binary.Read(reader, binary.LittleEndian, &ret.version); err != nil {
		return ret, err
	}

	if err := binary.Read(reader, binary.LittleEndian, &ret.status); err != nil {
		return ret, err
	}

	return ret, nil
}

// persistentNode is the on-disk structure of Arc's radix tree node.
// All fields in this struct are persisted in the same order.
type persistentNode struct {
	flags             uint8
	numChildren       uint16
	keyLen            uint16
	dataLen           uint32
	firstChildOffset  uint64
	nextSiblingOffset uint64
	key               []byte
	data              []byte
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

func makePersistentNodeFromBytes(src []byte) (persistentNode, error) {
	var ret persistentNode

	if len(src) < minNodeBytesLen {
		return ret, ErrNodeCorrupted
	}

	var err error
	var gotChecksum uint32
	var wantChecksum uint32

	checksumPos := src[len(src)-sizeOfUint32:]
	checksumBuf := bytes.NewReader(checksumPos)

	if err = binary.Read(checksumBuf, binary.LittleEndian, &wantChecksum); err != nil {
		return ret, err
	}

	nodeData := src[:len(src)-sizeOfUint32]

	if gotChecksum, err = computeChecksum(nodeData); err != nil {
		return ret, err
	}

	if gotChecksum != wantChecksum {
		return ret, ErrInvalidChecksum
	}

	nodeRegion := src[:len(src)-sizeOfUint32]
	nodeReader := bytes.NewReader(nodeRegion)

	if err := binary.Read(nodeReader, binary.LittleEndian, &ret.flags); err != nil {
		return ret, err
	}

	if err := binary.Read(nodeReader, binary.LittleEndian, &ret.numChildren); err != nil {
		return ret, err
	}

	if err := binary.Read(nodeReader, binary.LittleEndian, &ret.keyLen); err != nil {
		return ret, err
	}

	if err := binary.Read(nodeReader, binary.LittleEndian, &ret.dataLen); err != nil {
		return ret, err
	}

	if err := binary.Read(nodeReader, binary.LittleEndian, &ret.firstChildOffset); err != nil {
		return ret, err
	}

	if err := binary.Read(nodeReader, binary.LittleEndian, &ret.nextSiblingOffset); err != nil {
		return ret, err
	}

	// Done reading fixed length fields. Ensure that the dynamic length
	// regions are available. If not, the node is corrupted.
	remaining := nodeReader.Len()
	expectedRemaining := int(ret.keyLen) + int(ret.dataLen)

	if expectedRemaining != remaining {
		return ret, ErrNodeCorrupted
	}

	ret.key = make([]byte, ret.keyLen)
	if _, err := nodeReader.Read(ret.key); err != nil {
		return ret, err
	}

	if ret.isRecord() {
		ret.data = make([]byte, ret.dataLen)
		if _, err := nodeReader.Read(ret.data); err != nil {
			return ret, err
		}
	}

	return ret, nil
}

// isRecord returns true if the isRecord flag is set.
func (pn persistentNode) isRecord() bool {
	return pn.flags&flagIsRecord != 0
}

// hasBlob returns true if the hasBlob flag is set.
func (pn persistentNode) hasBlob() bool {
	return pn.flags&flagHasBlob != 0
}

// serialize serializes the persistentNode into a standardized byte slice.
func (pn persistentNode) serialize() ([]byte, error) {
	var buf bytes.Buffer

	if err := buf.WriteByte(pn.flags); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, pn.numChildren); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, pn.keyLen); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, pn.dataLen); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, pn.firstChildOffset); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, pn.nextSiblingOffset); err != nil {
		return nil, err
	}

	if _, err := buf.Write(pn.key); err != nil {
		return nil, err
	}

	if _, err := buf.Write(pn.data); err != nil {
		return nil, err
	}

	// Append the checksum at the end of the serialized node.
	checksum, err := computeChecksum(buf.Bytes())

	if err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, checksum); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func computeChecksum(src []byte) (uint32, error) {
	h := crc32.NewIEEE()

	if _, err := h.Write(src); err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}
