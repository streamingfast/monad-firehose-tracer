# Monad Firehose Tracer

A Monad execution tracer that produces [Firehose](https://firehose.streamingfast.io/) protobuf blocks for blockchain indexing and analytics.

## Build

```bash
cargo build --release
```

### Mac OS X Prerequisites

Requires a recent LLVM version. The one that comes from stock OSX is often outdated:

```bash
brew install llvm@20 zstd
```

Then configure your environment (e.g., via `.envrc`):

```bash
llvm_path="$(brew --prefix llvm@20)"
zstd_path="$(brew --prefix zstd)"

path_add PATH "$llvm_path"/bin
path_add LIBRARY_PATH "$llvm_path"/lib
path_add LIBRARY_PATH "$zstd_path"/lib

export CC="${llvm_path}/bin/clang"
export CXX="${llvm_path}/bin/clang++"
export LLVM_CONFIG_PATH="${llvm_path}/bin/llvm-config"
```

## Testing

```bash
# Run all tests
cargo test

# Run with debug output
RUST_LOG=debug cargo test
```

## Repository

https://github.com/streamingfast/monad-firehose-tracer

## License

Apache 2.0

## Resources

- [Firehose Documentation](https://firehose.streamingfast.io/)
- [Protobuf Definitions](https://github.com/streamingfast/firehose-ethereum/tree/develop/types/pb)
- [StreamingFast](https://www.streamingfast.io/)
