package radixdb

import (
	"bytes"
	"encoding/binary"
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

	// checksumEnabledLen represents the size of checksumEnabled in bytes.
	checksumEnabledLen = sizeOfUint8

	// nodeCountLen represents the size of nodeCount in bytes.
	nodeCountLen = sizeOfUint64

	// recordCountLen represents the size of recordCount in bytes.
	recordCountLen = sizeOfUint64
)

// binaryHeaderSize returns the total size of the binary header of the database
// file. The size is returned as an int representing the total number of bytes.
func fileHeaderSize() int {
	return (magicByteLen + fileFormatVersionLen + checksumEnabledLen + nodeCountLen + recordCountLen)
}

// buildFileHeader builds and returns a binary header for the RadixDB database
// file. This function encodes multi-byte native types, such as the number of
// nodes (uint64) using little-endian encoding.
func (rdb *RadixDB) buildFileHeader() []byte {
	var buf bytes.Buffer
	var checksumEnabled uint8

	if rdb.checksumEnabled {
		checksumEnabled = 1
	}

	buf.WriteByte(magicByte)
	buf.WriteByte(fileFormatVersion)
	buf.WriteByte(checksumEnabled)
	binary.Write(&buf, binary.LittleEndian, rdb.numNodes)
	binary.Write(&buf, binary.LittleEndian, rdb.numRecords)

	return buf.Bytes()
}
