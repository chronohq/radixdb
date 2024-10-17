package radixdb

import (
	"bytes"
	"encoding/binary"
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

	// createdAtOffset represents the starting position of the createdAt field.
	createdAtOffset = magicByteLen + fileFormatVersion + nodeCountLen + recordCountLen

	// updatedAtOffset represents the starting position of the updatedAt field.
	updatedAtOffset = magicByteLen + fileFormatVersion + nodeCountLen + recordCountLen + createdAtLen
)

type fileHeader []byte

// fileHeaderSize returns the total size of the binary header of the database
// file. The size is returned as an int representing the total number of bytes.
func fileHeaderSize() int {
	return (magicByteLen + fileFormatVersionLen + nodeCountLen + recordCountLen + createdAtLen + updatedAtLen)
}

// newFileHeader returns a new binary header for the database file.
func newFileHeader() fileHeader {
	var buf bytes.Buffer

	buf.WriteByte(magicByte)
	buf.WriteByte(fileFormatVersion)

	// Reserve space for numNodes and numRecords.
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // nodeCount
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // recordCount

	// Reserve space for the createdAt and updatedAt timestamps.
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // createdAt
	binary.Write(&buf, binary.LittleEndian, uint64(0)) // updatedAt

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
