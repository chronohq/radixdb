# RadixDB

[![go workflow](https://github.com/chronohq/radixdb/actions/workflows/go.yml/badge.svg)](https://github.com/chronohq/radixdb/actions/workflows/go.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/chronohq/radixdb.svg)](https://pkg.go.dev/github.com/chronohq/radixdb)
[![mit license](https://img.shields.io/badge/license-MIT-green)](/LICENSE)

RadixDB is a lightweight database built on a [Radix tree](https://en.wikipedia.org/wiki/Radix_tree) structure.
While initially implemented in Go, the database file format is platform-agnostic.
This allows easy read and write from any programming language, without special bindings.

🏗️ RadixDB is currently in active development. We plan to announce its readiness for
general availability when appropriate. Stay tuned for updates.

## Design Principles

RadixDB is designed with a focus on portability and cross-platform,
language-agnostic usage. When a developer constructs a Radix tree in their
application, it's typically for a specific purpose. Therefore, the ability
to persist the tree structure is valuable, as it allows applications to avoid
rebuilding the tree on each restart. This is particularly beneficial when the
tree's construction relies on external data sources that may not always be
reliably available. RadixDB addresses this need by offering a straightforward
mechanism to persist and reload Radix trees. Alternatively, RadixDB can be used
exclusively for in-memory operations when persistence is not required.

## Concurrency Model

The Go implementation of RadixDB employs a concurrency model based on the
[single-writer, multi-reader](https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock)
paradigm. This model allows multiple readers to access the database concurrently without
locking, while write operations are limited to a single writer at a time to ensure
data integrity. Other implementations of RadixDB might adopt different concurrency
models to better suit specific performance needs or language constraints.

## Persistence Model

RadixDB employs a dual-representation persistence model. It maintains a complete
in-memory representation of the Radix tree for fast operations, while also providing
a platform-agnostic on-disk format for durability. When saving the database, RadixDB
traverses the tree to compute its on-disk layout, ensuring data integrity and consistency.
Future versions may implement techniques such as [memory mapping](https://en.wikipedia.org/wiki/Memory-mapped_file)
to allow partial loading of the tree, while supporting the existing RadixDB file format.

## Data Integrity

RadixDB ensures data integrity using two [IEEE CRC32](https://en.wikipedia.org/wiki/Cyclic_redundancy_check) checksum mechanisms. 

The in-memory tree checksum verifies the integrity of the Radix tree during
runtime operations. It detects potential corruption or tampering of the tree
structure by enforcing checksum verification on read and write operations.

The storage checksum verifies the integrity of the database file and detects
any corruption or tampering that might occur at the filesystem level.

Both checksum types are calculated across all nodes in the Radix tree, providing
comprehensive coverage of the entire database structure.

## Contributing

Contributions of any kind are welcome.
If you're submitting a PR, please follow [Go's commit message structure](https://go.dev/wiki/CommitMessage).