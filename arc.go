// Copyright Chrono Technologies LLC
// SPDX-License-Identifier: MIT

// Package arc implements a key-value database based on a Radix tree data
// structure and deduplication-enabled blob storage. The Radix tree provides
// space-efficient key management through prefix compression, while the blob
// storage handles values with automatic deduplication.
package arc

import "errors"

var (
	// ErrKeyNotFound is returned when the key does not exist in the index.
	ErrKeyNotFound = errors.New("key not found")
)

// longestCommonPrefix compares the two given byte slices, and returns the
// longest common prefix. Memory-safety is ensured by establishing an index
// boundary based on the length of the shorter parameter.
func longestCommonPrefix(a, b []byte) []byte {
	minLen := len(a)

	if len(b) < minLen {
		minLen = len(b)
	}

	var i int

	for i = 0; i < minLen; i++ {
		if a[i] != b[i] {
			break
		}
	}

	if i == 0 {
		return nil
	}

	return a[:i]
}
