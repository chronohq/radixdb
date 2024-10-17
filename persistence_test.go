package radixdb

import (
	"testing"
	"time"
)

func TestSetCreatedAt(t *testing.T) {
	rdb := New()

	if rdb.header == nil {
		t.Error("binary file header is not initialized")
	}

	if len := len(rdb.header); len != fileHeaderSize() {
		t.Errorf("unexpected header size, got:%d, want:%d", len, fileHeaderSize())
	}

	want := time.Now()
	rdb.header.setCreatedAt(want)

	got, err := rdb.header.getCreatedAt()

	if err != nil {
		t.Fatalf("failed to getCreatedAt: %v", err)
	}

	if got.Unix() != want.Unix() {
		t.Errorf("unexpected createdAt, got:%d, want:%d", got.Unix(), want.Unix())
	}
}
