package radixdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

func TestBuildOffsetTable(t *testing.T) {
	rdb := basicTestTree()

	offsetTable, err := rdb.buildOffsetTable()

	if err != nil {
		t.Fatalf("failed to build offset table: %v", err)
	}

	expectedOffset := fileHeaderSize()

	err = rdb.traverse(func(current *node) error {
		offsetInfo, found := offsetTable[current]

		if !found {
			return fmt.Errorf("missing offset: %q", current.key)
		}

		raw, _ := current.serializeWithoutKey()
		nodeSize := len(raw)

		if offsetInfo.offset != uint64(expectedOffset) {
			return fmt.Errorf("incorrect offset (%q), got:%d, want:%d", current.key, offsetInfo.offset, expectedOffset)
		}

		if offsetInfo.size != uint64(nodeSize) {
			return fmt.Errorf("unexpected node size, got:%d, want:%d", offsetInfo.size, nodeSize)
		}

		expectedOffset += nodeSize

		return nil
	})

	if err != nil {
		t.Errorf("invalid offset table: %v", err)
	}
}

func TestFileHeaderSerialize(t *testing.T) {
	rdb := New()
	rdb.header.createdAt = time.Date(1969, time.July, 20, 20, 17, 0, 0, time.UTC)

	buf, _ := rdb.header.serialize()

	if len(buf) != fileHeaderSize() {
		t.Fatalf("invalid fileHeader size, got:%d, want:%d", len(buf), fileHeaderSize())
	}

	got := binary.LittleEndian.Uint32(buf[fileHeaderSize()-sizeOfUint32:])
	want, _ := calculateChecksum(buf[:fileHeaderSize()-sizeOfUint32])

	if got != want {
		t.Fatalf("invalid header checksum, got:%d, want:%d", got, want)
	}
}

func TestPersistentNodeSerialize(t *testing.T) {
	rdb := basicTestTree()

	subject, err := rdb.root.asDescriptor()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Inject test child offsets.
	for i := 0; i < int(subject.numChildren); i++ {
		subject.childOffsets[i] = uint64(i)
	}

	rawDescriptor, err := subject.serialize()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decoded, err := deserializePersistentNode(rawDescriptor)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decoded.isRecord() != subject.isRecord() {
		t.Fatalf("isRecord mismatch, got:%t, want:%t", decoded.isRecord(), subject.isRecord())
	}

	if decoded.hasBlob() != subject.hasBlob() {
		t.Fatalf("isBlob mismatch, got:%t, want:%t", decoded.hasBlob(), subject.hasBlob())
	}

	if decoded.numChildren != subject.numChildren {
		t.Fatalf("numChildren mismatch, got:%d, want:%d", decoded.numChildren, subject.numChildren)
	}

	if decoded.keyLen != subject.keyLen {
		t.Fatalf("keyLen mismatch, got:%d, want:%d", decoded.keyLen, subject.keyLen)
	}

	if decoded.dataLen != subject.dataLen {
		t.Fatalf("dataLen mismatch, got:%d, want:%d", decoded.dataLen, subject.dataLen)
	}

	if !bytes.Equal(decoded.key, subject.key) {
		t.Fatalf("key mismatch, got:%q, want:%q", decoded.key, subject.key)
	}

	if !bytes.Equal(decoded.data, subject.data) {
		t.Fatalf("data mismatch, got:%q, want:%q", decoded.data, subject.data)
	}

	if len(decoded.childOffsets) != len(subject.childOffsets) {
		t.Fatalf("childOffsets length mismatch, got:%d, want:%d", len(decoded.childOffsets), len(subject.childOffsets))
	}

	for i := 0; i < int(decoded.numChildren); i++ {
		if decoded.childOffsets[i] != subject.childOffsets[i] {
			t.Fatalf("childOffset mismatch, got:%d, want:%d", decoded.childOffsets[i], subject.childOffsets[i])
		}
	}
}
