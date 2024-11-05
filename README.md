# Arc: Space-Optimized Database

[![go workflow](https://github.com/chronohq/arc/actions/workflows/go.yml/badge.svg)](https://github.com/chronohq/arc/actions/workflows/go.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/chronohq/arc.svg)](https://pkg.go.dev/github.com/chronohq/arc)
[![mit license](https://img.shields.io/badge/license-MIT-green)](/LICENSE)

Arc is a lightweight key-value database optimized for storage efficiency.
It combines a [Radix tree](https://en.wikipedia.org/wiki/Radix_tree) for space-efficient
key indexing with content-aware blob storage that stores only unique content. 
While implemented in Go, Arc's simple file format is designed to be platform-agnostic and
easily usable from any programming language.

🚧 Arc is currently in active development. We plan to announce its readiness for
general availability when appropriate. Stay tuned for updates.

## Design Principles

Arc is designed on two core principles: space efficiency and universal accessibility.
The space efficiency goal is achieved through prefix compression in the Radix tree,
content-aware blob storage that stores only unique content, and lazy-loading to reduce 
memory footprint. The accessibility goal is tackled through a simple, platform-agnostic
file format that any programming language can read and write without special bindings.

## Concurrency Model

The Go implementation of Arc employs a [single-writer, multi-reader](https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock)
concurrency model. This allows concurrent read access without locking, while ensuring
data consistency by serializing write operations. Other implementations of Arc may
adopt different concurrency models to better support certain performance characteristics.

## Persistence Model

Arc employs a dual-representation persistence model. It can maintain a complete in-memory
representation of the Radix tree for fast operations, while also supporting lazy-loading
from its platform-agnostic file format. The goal for the initial version is to persist
in-memory changes by writing the entire tree to disk in a single operation. Future versions
will support in-place and partial flushing while maintaining backwards compatibility with
the existing file format.

## Data Integrity

Arc ensures data integrity using [IEEE CRC32](https://en.wikipedia.org/wiki/Cyclic_redundancy_check)
checksums. These checksums detect potential corruption in both the in-memory Radix tree
structure and the persisted data. The verification occurs during regular operations and
data persistence, providing comprehensive coverage across all tree nodes. While CRC32
is robust for detecting accidental corruption, it is not designed to detect deliberate
tampering.

## Contributing

Contributions of any kind are welcome.
If you're submitting a PR, please follow [Go's commit message structure](https://go.dev/wiki/CommitMessage).