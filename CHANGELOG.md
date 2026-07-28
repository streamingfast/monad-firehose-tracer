# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased (v0.15.2)

### Changed

* Bump Monad BFT / execution event crates to git tag **`v0.15.2-fh3.0`** on streamingfast/monad-bft.
* Tracks Monad execution Firehose fork `v0.15.2-fh3.0`. `monad-exec-events` public API unchanged for this bump.
* Bump `firehose-tracer` / `firehose-tracer-test` to **5.3.0** from crates.io (was git `main`).

## Unreleased (v0.14.0)

### Added

* First release of `monad-firehose-tracer` as a standalone repository, tracking Monad BFT `v0.14.0`.
* Reads Monad execution events from the event ring and outputs Firehose Protocol blocks.
