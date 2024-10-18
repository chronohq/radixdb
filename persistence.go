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

	// createdAtLen represents the size of createdAt in bytes.
	createdAtLen = sizeOfUint64

	// updatedAtLen represents the size of updatedAt in bytes.
	updatedAtLen = sizeOfUint64

	// headerChecsumLen represents the size of the checksum in bytes.
	headerChecksumLen = sizeOfUint32

	// reservedTotalLen represents the total size of the reserved region.
	reservedTotalLen = sizeOfUint8 + sizeOfUint8

	// createdAtOffset represents the starting position of the createdAt field.
	createdAtOffset = magicByteLen + fileFormatVersion + reservedTotalLen + nodeCountLen + recordCountLen

	// updatedAtOffset represents the starting position of the updatedAt field.
	updatedAtOffset = magicByteLen + fileFormatVersion + reservedTotalLen + nodeCountLen + recordCountLen + createdAtLen

	// headerChecksumOffset represents the starting position of the checksum field.
	headerChecksumOffset = magicByteLen + fileFormatVersion + reservedTotalLen + nodeCountLen + recordCountLen + createdAtLen + updatedAtLen
)

type fileHeader []byte

// fileHeaderSize returns the total size of the binary header of the database
// file. The size is returned as an int representing the total number of bytes.
func fileHeaderSize() int {
	return magicByteLen +
		fileFormatVersionLen +
		reservedTotalLen +
		nodeCountLen +
		recordCountLen +
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
	// 20 | Creation Timestamp                                            |
	//    +                                                               +
	// 24 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 28 | Update Timestamp                                              |
	//    +                                                               +
	// 32 |                                                               |
	//    +---------------+---------------+---------------+---------------+
	// 36 | Checksum                                                      |
	//    +---------------+---------------+---------------+---------------+
	var buf bytes.Buffer

	buf.WriteByte(magicByte)
	buf.WriteByte(fileFormatVersion)

	// Reserve space for future use.
	buf.WriteByte(byte(0)) // reserved
	buf.WriteByte(byte(0)) // reserved

	// Reserve space for numNodes and numRecords.
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // nodeCount
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // recordCount

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
