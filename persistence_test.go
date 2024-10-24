package radixdb

import (
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

		raw, _ := current.serialize()
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
