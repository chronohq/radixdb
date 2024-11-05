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
