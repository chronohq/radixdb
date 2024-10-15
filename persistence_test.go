package radixdb

import "testing"

func TestBuildFileHeader(t *testing.T) {
	rdb := &RadixDB{}
	rdb.Insert([]byte("apple"), []byte("sauce"))

	header := rdb.buildFileHeader()

	expectedSize := fileHeaderSize()

	if len := len(header); len != expectedSize {
		t.Errorf("unexpected header size, got:%d, want:%d", len, expectedSize)
	}
}
