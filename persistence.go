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

	// sizeOfUint32 is the size of uint32 in bytes.
	sizeOfUint32 = 4

	// sizeOfUint64 is the size of uint64 in bytes.
	sizeOfUint64 = 8

	// magicByteLen represents the size of magicByte in bytes.
	magicByteLen = sizeOfUint8

	// fileFormatVersionLen represents the size of fileFormatVersion in bytes.
	fileFormatVersionLen = sizeOfUint8

	// nodeCountLen represents the size of nodeCount in bytes.
	nodeCountLen = sizeOfUint64

	// recordCountLen represents the size of recordCount in bytes.
	recordCountLen = sizeOfUint64

	// blobCountLen represents the size of blobCount in bytes.
	blobCountLen = sizeOfUint64

	// radixTreeOffsetLen represents the size of radixTreeOffset in bytes.
	radixTreeOffsetLen = sizeOfUint64

	// blobStoreOffsetLen represents the size of blobStoreOffset in bytes.
	blobStoreOffsetLen = sizeOfUint64

	// createdAtLen represents the size of createdAt in bytes.
	createdAtLen = sizeOfUint64

	// updatedAtLen represents the size of updatedAt in bytes.
	updatedAtLen = sizeOfUint64

	// headerChecsumLen represents the size of the checksum in bytes.
	headerChecksumLen = sizeOfUint32

	// reservedTotalLen represents the total size of the reserved region.
	reservedTotalLen = sizeOfUint8 + sizeOfUint8

	// createdAtOffset represents the starting position of the createdAt field.
	createdAtOffset = magicByteLen + fileFormatVersion + reservedTotalLen + nodeCountLen + recordCountLen + blobCountLen + radixTreeOffsetLen + blobStoreOffsetLen

	// updatedAtOffset represents the starting position of the updatedAt field.
	updatedAtOffset = magicByteLen + fileFormatVersion + reservedTotalLen + nodeCountLen + recordCountLen + blobCountLen + radixTreeOffsetLen + blobStoreOffsetLen + createdAtLen

	// headerChecksumOffset represents the starting position of the checksum field.
	headerChecksumOffset = magicByteLen + fileFormatVersion + reservedTotalLen + nodeCountLen + recordCountLen + blobCountLen + radixTreeOffsetLen + blobStoreOffsetLen + createdAtLen + updatedAtLen
)

// nodeOffsetInfo holds the serialized offset and size of a node.
type nodeOffset struct {
	offset uint64 // Offset to the node in the file.
	size   uint64 // Size of the raw node data.
}

type fileHeader []byte

// fileHeaderSize returns the total size of the binary header of the database
// file. The size is returned as an int representing the total number of bytes.
func fileHeaderSize() int {
	return magicByteLen +
		fileFormatVersionLen +
		reservedTotalLen +
		nodeCountLen +
		recordCountLen +
		blobCountLen +
		radixTreeOffsetLen +
		blobStoreOffsetLen +
		createdAtLen +
		updatedAtLen +
		headerChecksumLen
}

// newFileHeader returns a new binary header for the database file.
// Non predetermined values are initially set to zero.
func newFileHeader() fileHeader {
	// Expected binary format of the file header:
	//     0               1               2               3
	//     0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
	//    +---------------+---------------+---------------+---------------+
	//  0 | Magic ('R')   | Version       | Reserverd     | Reserved      |
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
	// 28 | Radix Tree Offset                                             |
	//    +                                                               +
	// 32 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 36 | Blob Store Offset                                             |
	//    +                                                               +
	// 40 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 44 | Creation Timestamp                                            |
	//    +                                                               +
	// 48 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 52 | Update Timestamp                                              |
	//    +                                                               +
	// 56 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 60 | Header Checksum                                               |
	//    +---------------+---------------+---------------+---------------+
	var buf bytes.Buffer

	buf.WriteByte(magicByte)
	buf.WriteByte(fileFormatVersion)

	// Reserve space for future use.
	buf.WriteByte(byte(0)) // reserved
	buf.WriteByte(byte(0)) // reserved

	// Reserve space for nodeCount, recordCount and blobCount.
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // nodeCount
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // recordCount
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // blobCount

	// Reserve space for radixTreeOffset and blobStoreOffset.
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // radixTreeOffset
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // blobStoreOffset

	// Reserve space for the createdAt and updatedAt timestamps.
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // createdAt
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // updatedAt

	// Reserve space for the CRC32 header checksum.
	binary.Write(&buf, binary.LittleEndian, uint32(0)) // checksum

	return buf.Bytes()
}

// setCreatedAt updates the createdAt field in the fileHeader. It writes
// the given time as a uint64 Unix timestamp in little-endian byte order.
func (fh fileHeader) setCreatedAt(t time.Time) {
	ts := uint64(t.Unix())
	binary.LittleEndian.PutUint64(fh[createdAtOffset:], ts)
}

// setUpdatedAt updates the updatedAt field in the fileHeader. It writes
// the given time as a uint64 Unix timestamp in little-endian byte order.
func (fh fileHeader) setUpdatedAt(t time.Time) {
	ts := uint64(t.Unix())
	binary.LittleEndian.PutUint64(fh[updatedAtOffset:], ts)
}

// getCreatedAt decodes the createdAt field in the fileHeader, and returns
// it as Go's standard time.Time value.
func (fh fileHeader) getCreatedAt() (time.Time, error) {
	var ts uint64

	buf := bytes.NewReader(fh[createdAtOffset:])

	if err := binary.Read(buf, binary.LittleEndian, &ts); err != nil {
		return time.Time{}, err
	}

	return time.Unix(int64(ts), 0), nil
}

// getUpdatedAt decodes the updatedAt field in the fileHeader, and returns
// it as Go's standard time.Time value.
func (fh fileHeader) getUpdatedAt() (time.Time, error) {
	var ts uint64

	buf := bytes.NewReader(fh[updatedAtOffset:])

	if err := binary.Read(buf, binary.LittleEndian, &ts); err != nil {
		return time.Time{}, err
	}

	return time.Unix(int64(ts), 0), nil
}

// updateChecksum computes and updates the header checksum using CRC32.
func (fh fileHeader) updateChecksum() {
	h := crc32.NewIEEE()

	h.Write(fh[:headerChecksumOffset])

	binary.LittleEndian.PutUint32(fh[headerChecksumOffset:], h.Sum32())
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
		rawNode, err := current.serialize()

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
