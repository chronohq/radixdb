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
)

// binaryHeaderSize returns the total size of the binary header of the database
// file. The size is returned as an int representing the total number of bytes.
func binaryHeaderSize() int {
	magicByteLen := 1         // byte
	fileFormatVersionLen := 1 // uint8
	recordCountLen := 8       // uint64

	return (magicByteLen + fileFormatVersionLen + recordCountLen)
}

// buildFileHeader builds and returns a binary header for the RadixDB database
// file. This function encodes multi-byte native types, such as the number of
// nodes (uint64) using little-endian encoding.
func (rdb *RadixDB) buildFileHeader() []byte {
	var buf bytes.Buffer

	buf.WriteByte(magicByte)
	buf.WriteByte(fileFormatVersion)
	binary.Write(&buf, binary.LittleEndian, rdb.numNodes)

	return buf.Bytes()
}
