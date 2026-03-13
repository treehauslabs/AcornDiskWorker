# AcornDiskWorker

A high-performance, filesystem-backed Content-Addressed Storage (CAS) worker for the [Acorn](../Acorn) framework. Stores and retrieves data on disk using SHA256 content identifiers, with optional LFU decay eviction for capacity-bounded caches.

## Overview

AcornDiskWorker implements the `AcornCASWorker` protocol as a Swift actor, providing thread-safe disk persistence for content-addressed data. It is designed to slot into Acorn's layered caching chain — typically as a mid-tier between a fast in-memory worker and a slower network worker.

```
[MemoryWorker] <-near/far-> [DiskCASWorker] <-near/far-> [NetworkWorker]
```

## Requirements

- Swift 6.0+
- macOS 13+ / iOS 16+
- [Acorn](../Acorn) package (local dependency)

## Installation

Add AcornDiskWorker as a dependency in your `Package.swift`:

```swift
.package(path: "../AcornDiskWorker"),
```

Then add it to your target's dependencies:

```swift
.target(name: "YourTarget", dependencies: ["AcornDiskWorker"])
```

## Usage

### Basic storage and retrieval

```swift
import AcornDiskWorker
import Acorn

let worker = try DiskCASWorker(directory: cacheDir)

let data = Data("hello, world".utf8)
let cid = ContentIdentifier(for: data)

await worker.storeLocal(cid: cid, data: data)
let retrieved = await worker.get(cid: cid) // Data("hello, world")
```

### Capacity-bounded cache with LFU eviction

```swift
let worker = try DiskCASWorker(
    directory: cacheDir,
    capacity: 1000,          // max entries before eviction
    maxBytes: 50_000_000,    // max total bytes on disk (50 MB)
    halfLife: .seconds(300), // frequency scores decay with this half-life
    sampleSize: 5            // candidates sampled per eviction decision
)
```

Eviction triggers when either the entry count or byte limit is exceeded. The least-frequently-used entry (with time decay) is evicted.

### High-throughput mode (skip integrity verification)

```swift
let worker = try DiskCASWorker(
    directory: cacheDir,
    verifyReads: false  // skip SHA256 re-hash on reads
)
```

When `verifyReads` is `false`, reads return data without re-hashing to verify the CID. Use this for trusted-local caches where bit rot is not a concern.

### Persisting state for fast restart

```swift
// Before shutdown — save bloom filter + size index to disk
try await worker.persistState()

// On next init, state is loaded from disk instead of scanning all files
let worker2 = try DiskCASWorker(directory: cacheDir, capacity: 10_000)
```

### Explicit deletion

```swift
await worker.delete(cid: someCID)
```

### Observability

```swift
let m = await worker.metrics
print("hits: \(m.hits), misses: \(m.misses), evictions: \(m.evictions)")
print("total bytes on disk: \(await worker.totalBytes)")
```

### Custom file system provider

Inject a custom `FileSystemProvider` for testing or alternative storage backends:

```swift
let worker = try DiskCASWorker(directory: dir, fileSystem: MyCustomFS())
```

### Chaining with other workers

```swift
let memory = MemoryCASWorker()
let disk = try DiskCASWorker(directory: cacheDir, capacity: 10_000)

let chain = await CompositeCASWorker(
    workers: ["memory": memory, "disk": disk],
    order: ["memory", "disk"]
)

// get() walks the chain: memory first, then disk
let data = await chain.get(cid: someCID)
```

## API

### `DiskCASWorker`

| Method | Description |
|--------|-------------|
| `init(directory:capacity:maxBytes:halfLife:sampleSize:timeout:verifyReads:fileSystem:)` | Create a worker backed by the given directory. Loads persisted state or scans existing files on init. Pre-creates 256 shard directories. |
| `has(cid:) -> Bool` | Check if a CID exists. Uses Bloom filter for fast rejection, falls back to `access()` syscall. |
| `getLocal(cid:) async -> Data?` | Read data from disk. Bloom filter rejects unknown CIDs in nanoseconds. Optionally verifies SHA256 integrity. |
| `storeLocal(cid:data:) async` | Write data to disk via temp file + rename (no fsync). Evicts until within capacity. |
| `delete(cid:)` | Remove an entry from cache, size index, and disk. |
| `persistState()` | Save Bloom filter and item sizes to disk for fast restart. |
| `get(cid:) -> Data?` | Protocol default: checks near worker first, then local, with optional timeout. |
| `store(cid:data:)` | Protocol default: stores locally, then propagates to near worker. |
| `metrics -> CASMetrics` | Hits, misses, stores, evictions, deletions, corruption detections. |
| `totalBytes -> Int` | Current total bytes tracked on disk (O(1) lookup). |

## Design

- **Actor isolation** ensures all state mutations (cache scores, size tracking) are serialized without explicit locks.
- **POSIX I/O** — raw `open`/`read`/`write`/`close`/`rename` syscalls bypass Foundation overhead. No fsync on writes — CAS is self-verifying, so partial writes are caught by the hash check on read.
- **Bloom filter** — in-memory probabilistic filter eliminates filesystem `stat` calls for unknown CIDs. Misses resolve in ~80 nanoseconds. Serializable to disk for fast restart.
- **Generic over `FileSystemProvider`** — `DiskCASWorker<F>` is specialized by the compiler in release mode, eliminating existential dispatch overhead on every I/O call.
- **Configurable integrity verification** — SHA256 re-hash on reads can be disabled for trusted-local caches where throughput matters more than bit-rot detection.
- **Data integrity** (when enabled) — every read verifies the SHA256 hash matches the CID. Corrupted files are auto-deleted.
- **Directory sharding** — files are stored in `<dir>/<2-char-prefix>/<hash>` with all 256 shard directories pre-created at init.
- **LFU with exponential decay** means recently-accessed items are favored over historically-popular-but-stale items. Scores decay continuously with a configurable half-life.
- **Dual capacity limits** — eviction can be triggered by entry count (`capacity`), total bytes (`maxBytes`), or both.
- **State persistence** — Bloom filter and item sizes can be saved to disk, turning O(n) init scans into O(1) file loads on restart.
- **Sampled eviction** (like Redis) avoids scanning the entire cache — a random sample is drawn and the lowest-scored item is evicted.
- **Zero-allocation hex encoding** — `ContentIdentifier` uses a byte lookup table with `String(unsafeUninitializedCapacity:)` for zero intermediate allocations.
- **O(1) byte tracking** — `totalBytes` is a running counter, not a reduction over the size dictionary.
- **Injectable file system** — `FileSystemProvider` protocol allows swapping the I/O layer for testing or alternative backends.

## Performance

Benchmarked on Apple Silicon (M-series), release mode:

| Operation | Latency | Notes |
|-----------|---------|-------|
| store (64B) | ~300us | POSIX write + rename, no fsync |
| store (4KB) | ~306us | Dominated by syscall overhead, not data size |
| get hit (64B) | ~41us | Includes SHA256 verification |
| get hit (256KB) | ~129us | SHA256 scales with data size |
| get miss | ~86ns | Bloom filter rejects without touching disk |
| has (hit) | ~15us | Bloom filter + `access()` syscall |
| has (miss) | ~75ns | Bloom filter only |
| delete | ~69us | `unlink()` syscall |
| eviction store | ~284us | Store with LFU eviction |
| mixed (80r/20w) | ~94us | Realistic workload |

### Running benchmarks

```bash
swift run -c release DiskCASBenchmarks --save-baseline
swift run -c release DiskCASBenchmarks --check-baseline
```

The `--check-baseline` flag compares against a saved baseline and flags regressions greater than 15%.

## Testing

```bash
swift test
```

15 tests covering: round-trip storage, missing key lookups, cross-instance persistence, existence checks, LFU eviction, explicit deletion, corruption detection, size-based eviction, metrics tracking, init scan reconciliation, Bloom filter misses, totalBytes tracking, state persistence, and configurable verification.
