package radixdb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	// magicByte is the first byte of a RadixDB file, used to identify the format
	// and detect pontential corruption. It is represented by 'R' for RadixDB.
	magicByte = byte(0x52)

	// fileFormatVersion represents the version of the database file format.
	// It is used to ensure that the database file is compatible with the
	// RadixDB software version.
	fileFormatVersion = uint8(1)

	// sizeOfUint8 is the size of uint8 in bytes.
	sizeOfUint8 = 1

	// sizeOfUint16 is the size of uint16 in bytes.
	sizeOfUint16 = 2

	// sizeOfUint32 is the size of uint32 in bytes.
	sizeOfUint32 = 4

	// sizeOfUint64 is the size of uint64 in bytes.
	sizeOfUint64 = 8

	// maxChildPerNode is the maximum possible number of children per node.
	maxChildPerNode = 256

	// maxUint16 is the maximum value of uint16.
	maxUint16 = (1 << 16) - 1

	// maxUint32 is the maximum value of uint32.
	maxUint32 = (1 << 32) - 1

	// magicByteLen represents the size of magicByte in bytes.
	magicByteLen = sizeOfUint8

	// fileFormatVersionLen represents the size of fileFormatVersion in bytes.
	fileFormatVersionLen = sizeOfUint8

	// compressionAlgoLen represents the size of compressionAlgo in bytes.
	compressionAlgoLen = sizeOfUint8

	// nodeCountLen represents the size of nodeCount in bytes.
	nodeCountLen = sizeOfUint64

	// recordCountLen represents the size of recordCount in bytes.
	recordCountLen = sizeOfUint64

	// blobCountLen represents the size of blobCount in bytes.
	blobCountLen = sizeOfUint64

	// radixIndexOffsetLen represents the size of radixIndexOffset in bytes.
	radixIndexOffsetLen = sizeOfUint64

	// radixIndexSizeLen represents the size of the serialized radix index in bytes.
	radixIndexSizeLen = sizeOfUint64

	// blobIndexOffsetLen represents the size of blobIndexOffset in bytes.
	blobIndexOffsetLen = sizeOfUint64

	// blobIndexSizeLen represents the size of the serialized blob index in bytes.
	blobIndexSizeLen = sizeOfUint64

	// createdAtLen represents the size of createdAt in bytes.
	createdAtLen = sizeOfUint64

	// updatedAtLen represents the size of updatedAt in bytes.
	updatedAtLen = sizeOfUint64

	// headerChecsumLen represents the size of the checksum in bytes.
	headerChecksumLen = sizeOfUint32

	// reservedTotalLen represents the total size of the reserved region.
	reservedTotalLen = sizeOfUint8

	// minNodeDescriptorLen is the minimum size of a serialized node descriptor.
	// It is the accumulated size of the fixed length fields.
	minNodeDescriptorLen = sizeOfUint8 + sizeOfUint8 + sizeOfUint16 + sizeOfUint16 + sizeOfUint32 + sizeOfUint32
)

// nodeOffsetInfo holds the serialized offset and size of a node.
type nodeOffset struct {
	offset uint64 // Offset to the node in the file.
	size   uint64 // Size of the raw node data.
}

// nodeDescriptor represents the data structure of a node as it is stored on
// disk, except the checksum field. All fields in this struct is persisted in
// the same order. The checksum is transparently appended by the serializer.
type nodeDescriptor struct {
	isRecord     uint8
	isBlob       uint8
	numChildren  uint16
	keyLen       uint16
	dataLen      uint32
	key          []byte
	data         []byte
	childOffsets []uint64
}

type fileHeader struct {
	magic            byte
	version          byte
	compressionAlgo  byte
	nodeCount        uint64
	recordCount      uint64
	blobCount        uint64
	radixIndexOffset uint64
	radixIndexSize   uint64
	blobIndexOffset  uint64
	blobIndexSize    uint64
	createdAt        time.Time
	updatedAt        time.Time
	checksum         uint32
}

// fileHeaderSize returns the total size of the binary header of the database
// file. The size is returned as an int representing the total number of bytes.
func fileHeaderSize() int {
	return magicByteLen +
		fileFormatVersionLen +
		compressionAlgoLen +
		reservedTotalLen +
		nodeCountLen +
		recordCountLen +
		blobCountLen +
		radixIndexOffsetLen +
		radixIndexSizeLen +
		blobIndexOffsetLen +
		blobIndexSizeLen +
		createdAtLen +
		updatedAtLen +
		headerChecksumLen
}

func (fh fileHeader) serialize() ([]byte, error) {
	// Expected binary format of the file header:
	//     0               1               2               3
	//     0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
	//    +---------------+---------------+---------------+---------------+
	//  0 | Magic ('R')   | Version       | Compression   | Reserved      |
	//    +---------------+---------------+---------------+---------------+
	//  4 | Node Count                                                    |
	//    +                                                               +
	//  8 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 12 | Record Count                                                  |
	//    +                                                               +
	// 16 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 20 | Blob Count                                                    |
	//    +                                                               +
	// 24 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 28 | Radix Index Offset                                            |
	//    +                                                               +
	// 32 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 36 | Radix Index Size                                              |
	//    +                                                               +
	// 40 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 44 | Blob Index Offset                                             |
	//    +                                                               +
	// 48 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 52 | Blob Index Size                                               |
	//    +                                                               +
	// 56 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 60 | Creation Timestamp                                            |
	//    +                                                               +
	// 64 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 68 | Update Timestamp                                              |
	//    +                                                               +
	// 74 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 80 | Header Checksum                                               |
	//    +---------------+---------------+---------------+---------------+
	var buf bytes.Buffer
	var err error

	buf.WriteByte(fh.magic)
	buf.WriteByte(fh.version)
	buf.WriteByte(fh.compressionAlgo)
	buf.WriteByte(byte(0)) // reserved space

	binary.Write(&buf, binary.LittleEndian, fh.nodeCount)
	binary.Write(&buf, binary.LittleEndian, fh.recordCount)
	binary.Write(&buf, binary.LittleEndian, fh.blobCount)

	binary.Write(&buf, binary.LittleEndian, fh.radixIndexOffset)
	binary.Write(&buf, binary.LittleEndian, fh.radixIndexSize)

	binary.Write(&buf, binary.LittleEndian, fh.blobIndexOffset)
	binary.Write(&buf, binary.LittleEndian, fh.blobIndexSize)

	binary.Write(&buf, binary.LittleEndian, uint64(fh.createdAt.Unix()))
	binary.Write(&buf, binary.LittleEndian, uint64(fh.updatedAt.Unix()))

	// Compute the CRC32 checksum of the header up until the checksum field.
	if fh.checksum, err = calculateChecksum(buf.Bytes()); err != nil {
		return nil, err
	}

	binary.Write(&buf, binary.LittleEndian, fh.checksum)

	return buf.Bytes(), nil
}

// buildOffsetTable builds a map of node pointers to their offsets within the
// file. Offsets are determined by traversing the tree in depth-first search
// order. The function returns an error if node serialization fails.
func (rdb *RadixDB) buildOffsetTable() (map[*node]nodeOffset, error) {
	offsetTable := make(map[*node]nodeOffset)

	// Start at the end of the file header region.
	currentOffset := uint64(fileHeaderSize())

	err := rdb.traverse(func(current *node) error {
		// TODO(toru): There is no need to do full node serialization.
		// Write a function that computes the node size without serializing.
		rawNode, err := current.serializeWithoutKey()

		if err != nil {
			return nil
		}

		nodeSize := uint64(len(rawNode))
		offsetTable[current] = nodeOffset{offset: currentOffset, size: nodeSize}
		currentOffset += nodeSize

		return nil
	})

	if err != nil {
		return nil, err
	}

	return offsetTable, nil
}

// calculateChecksum returns the CRC32 checksum of the given byte slice.
func calculateChecksum(src []byte) (uint32, error) {
	h := crc32.NewIEEE()

	if _, err := h.Write(src); err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}

// serialize converts the nodeDescriptor into a byte slice for storage.
func (nd nodeDescriptor) serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Step 1: Serialize the fixed length metadata.
	if err := buf.WriteByte(nd.isRecord); err != nil {
		return nil, err
	}

	if err := buf.WriteByte(nd.isBlob); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, nd.numChildren); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, nd.keyLen); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, nd.dataLen); err != nil {
		return nil, err
	}

	// Step 2: Serialize the dynamic length fields.
	if _, err := buf.Write(nd.key); err != nil {
		return nil, err
	}

	if _, err := buf.Write(nd.data); err != nil {
		return nil, err
	}

	for _, offset := range nd.childOffsets {
		if err := binary.Write(&buf, binary.LittleEndian, offset); err != nil {
			return nil, err
		}
	}

	// Step 3: Compute the checksum and serialize the valaue.
	checksum, err := calculateChecksum(buf.Bytes())

	if err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, checksum); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// deserializeNodeDescriptor reconstructs a nodeDescriptor from its serialized
// byte representation. It reads the data in the same order as serialization,
// verifies the data, and returns the nodeDescriptor.
func deserializeNodeDescriptor(data []byte) (nodeDescriptor, error) {
	var ret nodeDescriptor

	// The raw data must be at least the length of the fixed-length fields.
	if len(data) < minNodeDescriptorLen {
		return ret, ErrInvalidIndex
	}

	// Determine the buffer positions of the descriptor and checksum.
	descriptorPos := data[:len(data)-sizeOfUint32]
	checksumPos := data[len(data)-sizeOfUint32:]

	// Read the checksum from the serialized data.
	var checksum uint32
	checksumBuf := bytes.NewReader(checksumPos)

	if err := binary.Read(checksumBuf, binary.LittleEndian, &checksum); err != nil {
		return ret, err
	}

	// Compute the checksum of the descriptor content.
	descriptorChecksum, err := calculateChecksum(descriptorPos)
	if err != nil {
		return ret, err
	}

	if checksum != descriptorChecksum {
		return ret, ErrInvalidChecksum
	}

	// Reaching here means that we can start deserializing.
	buf := bytes.NewReader(descriptorPos)

	// Decode the fixed length metadata.
	if err := binary.Read(buf, binary.LittleEndian, &ret.isRecord); err != nil {
		return ret, err
	}

	if err := binary.Read(buf, binary.LittleEndian, &ret.isBlob); err != nil {
		return ret, err
	}

	if err := binary.Read(buf, binary.LittleEndian, &ret.numChildren); err != nil {
		return ret, err
	}

	if err := binary.Read(buf, binary.LittleEndian, &ret.keyLen); err != nil {
		return ret, err
	}

	if err := binary.Read(buf, binary.LittleEndian, &ret.dataLen); err != nil {
		return ret, err
	}

	// Reaching here means that the fixed length metadata is loaded on memory.
	// Compute the total length of the node descriptor using the metadata, and
	// verify the length of the given data buffer.
	expectedLen := minNodeDescriptorLen
	expectedLen += int(ret.keyLen)
	expectedLen += int(ret.dataLen)
	expectedLen += int(ret.numChildren) * sizeOfUint64

	if len(data) != expectedLen {
		return ret, ErrFileCorrupt
	}

	// Read the variable length fields.
	ret.key = make([]byte, ret.keyLen)
	if _, err := buf.Read(ret.key); err != nil {
		return ret, err
	}

	ret.data = make([]byte, ret.dataLen)
	if _, err := buf.Read(ret.data); err != nil {
		return ret, err
	}

	ret.childOffsets = make([]uint64, ret.numChildren)
	for i := 0; i < int(ret.numChildren); i++ {
		if err := binary.Read(buf, binary.LittleEndian, &ret.childOffsets[i]); err != nil {
			return ret, err
		}
	}

	return ret, nil
}
