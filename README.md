# RadixDB

[![go workflow](https://github.com/chronohq/radixdb/actions/workflows/go.yml/badge.svg)](https://github.com/chronohq/radixdb/actions/workflows/go.yml)
[![mit license](https://img.shields.io/badge/license-MIT-green)](/LICENSE)

RadixDB is a lightweight database built on a [Radix tree](https://en.wikipedia.org/wiki/Radix_tree) structure.
While initially implemented in Go, the database file format is platform-agnostic.
This allows easy read and write from any programming language, without special bindings.

🏗️ RadixDB is currently in active development. We plan to announce its readiness for
general availability when appropriate. Stay tuned for updates.

## Concurrency Model

The Go implementation of RadixDB employs a concurrency model based on the
[single-writer, multi-reader](https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock)
paradigm. This model allows multiple readers to access the database concurrently without
locking, while write operations are limited to a single writer at a time to ensure
data integrity. Other implementations of RadixDB might adopt different concurrency
models to better suit specific performance needs or language constraints.

## Contributing

Contributions of any kind are welcome.
If you're submitting a PR, please follow [Go's commit message structure](https://go.dev/wiki/CommitMessage).