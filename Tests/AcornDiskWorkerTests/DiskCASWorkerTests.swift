import Testing
import Foundation
import Acorn
@testable import AcornDiskWorker

@Suite("DiskCASWorker")
struct DiskCASWorkerTests {

    private func tempDir() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("AcornDiskWorkerTests-\(UUID().uuidString)")
    }

    private func withTempDir(_ body: (URL) async throws -> Void) async throws {
        let dir = tempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        try await body(dir)
    }

    @Test("Put and get round-trip")
    func testPutGet() async throws {
        try await withTempDir { dir in
            let worker = try DiskCASWorker(directory: dir)
            let data = Data("disk hello".utf8)
            let cid = ContentIdentifier(for: data)

            await worker.storeLocal(cid: cid, data: data)
            let result = await worker.get(cid: cid)
            #expect(result == data)
        }
    }

    @Test("Get missing CID returns nil")
    func testGetMissing() async throws {
        try await withTempDir { dir in
            let worker = try DiskCASWorker(directory: dir)
            let cid = ContentIdentifier(for: Data("nope".utf8))
            #expect(await worker.get(cid: cid) == nil)
        }
    }

    @Test("Data persists across worker instances")
    func testPersistence() async throws {
        try await withTempDir { dir in
            let data = Data("persistent".utf8)
            let cid = ContentIdentifier(for: data)

            let worker1 = try DiskCASWorker(directory: dir)
            await worker1.storeLocal(cid: cid, data: data)

            let worker2 = try DiskCASWorker(directory: dir)
            let result = await worker2.get(cid: cid)
            #expect(result == data)
        }
    }

    @Test("has returns true for stored CID")
    func testHasTrue() async throws {
        try await withTempDir { dir in
            let worker = try DiskCASWorker(directory: dir)
            let data = Data("on disk".utf8)
            let cid = ContentIdentifier(for: data)

            await worker.storeLocal(cid: cid, data: data)
            #expect(await worker.has(cid: cid) == true)
        }
    }

    @Test("has returns false for missing CID")
    func testHasFalse() async throws {
        try await withTempDir { dir in
            let worker = try DiskCASWorker(directory: dir)
            let cid = ContentIdentifier(for: Data("not here".utf8))
            #expect(await worker.has(cid: cid) == false)
        }
    }

    @Test("Evicts at capacity")
    func testEviction() async throws {
        try await withTempDir { dir in
            let worker = try DiskCASWorker(directory: dir, capacity: 2, sampleSize: 10)
            let a = ContentIdentifier(for: Data("a".utf8))
            let b = ContentIdentifier(for: Data("b".utf8))
            let c = ContentIdentifier(for: Data("c".utf8))

            await worker.storeLocal(cid: a, data: Data("a".utf8))
            await worker.storeLocal(cid: b, data: Data("b".utf8))

            for _ in 0..<10 {
                _ = await worker.getLocal(cid: a)
            }

            await worker.storeLocal(cid: c, data: Data("c".utf8))

            #expect(await worker.getLocal(cid: a) != nil)
            #expect(await worker.getLocal(cid: c) != nil)
            #expect(await worker.getLocal(cid: b) == nil)
        }
    }

    @Test("Delete removes entry from disk and cache")
    func testDelete() async throws {
        try await withTempDir { dir in
            let worker = try DiskCASWorker(directory: dir)
            let data = Data("deleteme".utf8)
            let cid = ContentIdentifier(for: data)

            await worker.storeLocal(cid: cid, data: data)
            #expect(await worker.has(cid: cid) == true)

            await worker.delete(cid: cid)
            #expect(await worker.has(cid: cid) == false)
            #expect(await worker.getLocal(cid: cid) == nil)
        }
    }

    @Test("Corruption detection removes invalid file")
    func testCorruptionDetection() async throws {
        try await withTempDir { dir in
            let worker = try DiskCASWorker(directory: dir)
            let data = Data("original".utf8)
            let cid = ContentIdentifier(for: data)

            await worker.storeLocal(cid: cid, data: data)

            let hex = cid.rawValue
            let prefix = String(hex.prefix(2))
            let filePath = dir.appendingPathComponent(prefix).appendingPathComponent(hex)
            try Data("corrupted".utf8).write(to: filePath)

            let result = await worker.getLocal(cid: cid)
            #expect(result == nil)
            #expect(await worker.has(cid: cid) == false)
        }
    }

    @Test("Size-based eviction respects maxBytes")
    func testMaxBytesEviction() async throws {
        try await withTempDir { dir in
            let worker = try DiskCASWorker(directory: dir, maxBytes: 2, sampleSize: 10)
            let a = ContentIdentifier(for: Data("a".utf8))
            let b = ContentIdentifier(for: Data("b".utf8))
            let c = ContentIdentifier(for: Data("c".utf8))

            await worker.storeLocal(cid: a, data: Data("a".utf8))
            await worker.storeLocal(cid: b, data: Data("b".utf8))

            for _ in 0..<10 {
                _ = await worker.getLocal(cid: a)
            }

            await worker.storeLocal(cid: c, data: Data("c".utf8))

            #expect(await worker.getLocal(cid: a) != nil)
            #expect(await worker.getLocal(cid: c) != nil)
            #expect(await worker.getLocal(cid: b) == nil)
        }
    }

    @Test("Metrics track operations correctly")
    func testMetrics() async throws {
        try await withTempDir { dir in
            let worker = try DiskCASWorker(directory: dir, capacity: 2, sampleSize: 10)
            let data = Data("metric".utf8)
            let cid = ContentIdentifier(for: data)
            let missing = ContentIdentifier(for: Data("nope".utf8))

            await worker.storeLocal(cid: cid, data: data)
            _ = await worker.getLocal(cid: cid)
            _ = await worker.getLocal(cid: missing)
            await worker.delete(cid: cid)

            let m = await worker.metrics
            #expect(m.stores == 1)
            #expect(m.hits == 1)
            #expect(m.misses == 1)
            #expect(m.deletions == 1)
        }
    }

    @Test("Init scan reconciles cache with existing files")
    func testInitScan() async throws {
        try await withTempDir { dir in
            let worker1 = try DiskCASWorker(directory: dir, capacity: 3)
            let a = ContentIdentifier(for: Data("a".utf8))
            let b = ContentIdentifier(for: Data("b".utf8))
            await worker1.storeLocal(cid: a, data: Data("a".utf8))
            await worker1.storeLocal(cid: b, data: Data("b".utf8))

            let worker2 = try DiskCASWorker(directory: dir, capacity: 3, sampleSize: 10)
            let c = ContentIdentifier(for: Data("c".utf8))
            let d = ContentIdentifier(for: Data("d".utf8))
            await worker2.storeLocal(cid: c, data: Data("c".utf8))

            for _ in 0..<10 {
                _ = await worker2.getLocal(cid: a)
                _ = await worker2.getLocal(cid: b)
                _ = await worker2.getLocal(cid: c)
            }

            await worker2.storeLocal(cid: d, data: Data("d".utf8))

            let evictions = await worker2.metrics.evictions
            #expect(evictions > 0)
        }
    }

    @Test("Bloom filter prevents stat for unknown CIDs")
    func testBloomFilterMiss() async throws {
        try await withTempDir { dir in
            let worker = try DiskCASWorker(directory: dir)
            let cid = ContentIdentifier(for: Data("unknown".utf8))
            #expect(await worker.has(cid: cid) == false)
            #expect(await worker.getLocal(cid: cid) == nil)
        }
    }

    @Test("Persisted state enables fast restart")
    func testPersistedState() async throws {
        try await withTempDir { dir in
            let data = Data("persist-me".utf8)
            let cid = ContentIdentifier(for: data)

            let worker1 = try DiskCASWorker(directory: dir, capacity: 100)
            await worker1.storeLocal(cid: cid, data: data)
            try await worker1.persistState()

            let worker2 = try DiskCASWorker(directory: dir, capacity: 100)
            #expect(await worker2.has(cid: cid) == true)
            let result = await worker2.getLocal(cid: cid)
            #expect(result == data)
            #expect(await worker2.totalBytes == data.count)
        }
    }

    @Test("verifyReads=false skips integrity check")
    func testSkipVerification() async throws {
        try await withTempDir { dir in
            let worker = try DiskCASWorker(directory: dir, verifyReads: false)
            let data = Data("original".utf8)
            let cid = ContentIdentifier(for: data)

            await worker.storeLocal(cid: cid, data: data)

            let hex = cid.rawValue
            let prefix = String(hex.prefix(2))
            let filePath = dir.appendingPathComponent(prefix).appendingPathComponent(hex)
            try Data("corrupted".utf8).write(to: filePath)

            // With verifyReads=false, corrupted data is returned without checking
            let result = await worker.getLocal(cid: cid)
            #expect(result == Data("corrupted".utf8))
        }
    }

    @Test("totalBytes tracks correctly across operations")
    func testTotalBytes() async throws {
        try await withTempDir { dir in
            let worker = try DiskCASWorker(directory: dir)
            let dataA = Data("aaaa".utf8)
            let dataB = Data("bb".utf8)
            let cidA = ContentIdentifier(for: dataA)
            let cidB = ContentIdentifier(for: dataB)

            #expect(await worker.totalBytes == 0)

            await worker.storeLocal(cid: cidA, data: dataA)
            #expect(await worker.totalBytes == 4)

            await worker.storeLocal(cid: cidB, data: dataB)
            #expect(await worker.totalBytes == 6)

            await worker.delete(cid: cidA)
            #expect(await worker.totalBytes == 2)
        }
    }
}
