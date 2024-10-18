package radixdb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
	"time"
)

func TestSetCreatedAt(t *testing.T) {
	rdb := New()

	if len := len(rdb.header); len != fileHeaderSize() {
		t.Errorf("unexpected header size, got:%d, want:%d", len, fileHeaderSize())
	}

	want := time.Date(1969, time.July, 20, 20, 17, 0, 0, time.UTC)
	rdb.header.setCreatedAt(want)

	got, err := rdb.header.getCreatedAt()

	if err != nil {
		t.Fatalf("failed to getCreatedAt: %v", err)
	}

	if got.Unix() != want.Unix() {
		t.Errorf("unexpected createdAt, got:%d, want:%d", got.Unix(), want.Unix())
	}
}

func TestSetUpdatedAt(t *testing.T) {
	rdb := New()

	if len := len(rdb.header); len != fileHeaderSize() {
		t.Errorf("unexpected header size, got:%d, want:%d", len, fileHeaderSize())
	}

	want := time.Date(1969, time.July, 20, 20, 17, 0, 0, time.UTC)
	rdb.header.setUpdatedAt(want)

	got, err := rdb.header.getUpdatedAt()

	if err != nil {
		t.Fatalf("failed to getUpdatedAt: %v", err)
	}

	if got.Unix() != want.Unix() {
		t.Errorf("unexpected updatedAt, got:%d, want:%d", got.Unix(), want.Unix())
	}
}

func TestUpdateHeaderChecksum(t *testing.T) {
	header := newFileHeader()
	header.setCreatedAt(time.Date(1969, time.July, 20, 20, 17, 0, 0, time.UTC))
	header.updateChecksum()

	h := crc32.NewIEEE()
	h.Write(header[:headerChecksumOffset])

	want := h.Sum32()
	got := binary.LittleEndian.Uint32(header[headerChecksumOffset:])

	if got != want {
		t.Errorf("checksum mismatch, got:%d, want:%d", got, want)
	}
}

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
