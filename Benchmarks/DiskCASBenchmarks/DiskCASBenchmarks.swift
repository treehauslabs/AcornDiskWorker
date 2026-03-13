import Foundation
import AcornDiskWorker
import Acorn

func makeData(size: Int) -> Data {
    Data((0..<size).map { _ in UInt8.random(in: 0...255) })
}

func taggedData(base: Data, tag: Int) -> Data {
    var d = base
    withUnsafeBytes(of: tag) { d.append(contentsOf: $0) }
    return d
}

@main
struct DiskCASBenchmarks {
    static let baselineURL = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
        .appendingPathComponent(".benchmark-baseline.json")

    static func main() async throws {
        let args = CommandLine.arguments
        let doSave = args.contains("--save-baseline")
        let doCheck = args.contains("--check-baseline")

        let results = try await runBenchmarks()

        printReport(results)

        if doSave { try saveBaseline(results, to: baselineURL) }
        if doCheck { try compareBaseline(results, from: baselineURL) }
        if !doSave && !doCheck {
            print("Tip: Run with --save-baseline to save, --check-baseline to compare.\n")
        }
    }
}

func runBenchmarks() async throws -> [BenchmarkResult] {
    print("DiskCASWorker Performance Benchmarks")
    print("=====================================\n")

    let tempRoot = FileManager.default.temporaryDirectory
        .appendingPathComponent("DiskCASBench-\(UUID().uuidString)")
    defer { try? FileManager.default.removeItem(at: tempRoot) }

    var dirCount = 0
    func dir() -> URL { dirCount += 1; return tempRoot.appendingPathComponent("b\(dirCount)") }

    let clock = ContinuousClock()
    var results: [BenchmarkResult] = []

    let small = makeData(size: 64)
    let medium = makeData(size: 4_096)
    let large = makeData(size: 262_144)
    let smallCID = ContentIdentifier(for: small)
    let mediumCID = ContentIdentifier(for: medium)
    let largeCID = ContentIdentifier(for: large)
    let missCID = ContentIdentifier(for: Data("nonexistent".utf8))

    let storeItems64 = (0..<500).map { i -> (ContentIdentifier, Data) in
        let d = taggedData(base: makeData(size: 64), tag: i)
        return (ContentIdentifier(for: d), d)
    }
    let storeItems4K = (0..<300).map { i -> (ContentIdentifier, Data) in
        let d = taggedData(base: makeData(size: 4_096), tag: i)
        return (ContentIdentifier(for: d), d)
    }
    let storeItems256K = (0..<100).map { i -> (ContentIdentifier, Data) in
        let d = taggedData(base: makeData(size: 262_144), tag: i)
        return (ContentIdentifier(for: d), d)
    }

    func usPerOp(_ start: ContinuousClock.Instant, _ end: ContinuousClock.Instant, _ n: Int) -> Double {
        let elapsed = end - start
        return (Double(elapsed.components.seconds) * 1e6 + Double(elapsed.components.attoseconds) * 1e-12) / Double(n)
    }

    let warmup = 2
    let sampleCount = 5

    // -------------------------------------------------------
    print("[Store operations]")
    // -------------------------------------------------------

    do {
        let name = "store-64B"
        let iters = 100
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir())
        for _ in 0..<(iters * warmup) {
            let idx = Int.random(in: 0..<storeItems64.count)
            await w.storeLocal(cid: storeItems64[idx].0, data: storeItems64[idx].1)
        }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters {
                let idx = Int.random(in: 0..<storeItems64.count)
                await w.storeLocal(cid: storeItems64[idx].0, data: storeItems64[idx].1)
            }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    do {
        let name = "store-4KB"
        let iters = 50
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir())
        for _ in 0..<(iters * warmup) {
            let idx = Int.random(in: 0..<storeItems4K.count)
            await w.storeLocal(cid: storeItems4K[idx].0, data: storeItems4K[idx].1)
        }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters {
                let idx = Int.random(in: 0..<storeItems4K.count)
                await w.storeLocal(cid: storeItems4K[idx].0, data: storeItems4K[idx].1)
            }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    do {
        let name = "store-256KB"
        let iters = 20
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir())
        for _ in 0..<(iters * warmup) {
            let idx = Int.random(in: 0..<storeItems256K.count)
            await w.storeLocal(cid: storeItems256K[idx].0, data: storeItems256K[idx].1)
        }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters {
                let idx = Int.random(in: 0..<storeItems256K.count)
                await w.storeLocal(cid: storeItems256K[idx].0, data: storeItems256K[idx].1)
            }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    // -------------------------------------------------------
    print("[Get (hit) operations — includes SHA256 verification]")
    // -------------------------------------------------------

    do {
        let name = "get-hit-64B"
        let iters = 100
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir())
        await w.storeLocal(cid: smallCID, data: small)
        for _ in 0..<(iters * warmup) { _ = await w.getLocal(cid: smallCID) }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters { _ = await w.getLocal(cid: smallCID) }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    do {
        let name = "get-hit-4KB"
        let iters = 50
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir())
        await w.storeLocal(cid: mediumCID, data: medium)
        for _ in 0..<(iters * warmup) { _ = await w.getLocal(cid: mediumCID) }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters { _ = await w.getLocal(cid: mediumCID) }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    do {
        let name = "get-hit-256KB"
        let iters = 20
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir())
        await w.storeLocal(cid: largeCID, data: large)
        for _ in 0..<(iters * warmup) { _ = await w.getLocal(cid: largeCID) }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters { _ = await w.getLocal(cid: largeCID) }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    // -------------------------------------------------------
    print("[Miss and existence checks]")
    // -------------------------------------------------------

    do {
        let name = "get-miss"
        let iters = 200
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir())
        for _ in 0..<(iters * warmup) { _ = await w.getLocal(cid: missCID) }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters { _ = await w.getLocal(cid: missCID) }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    do {
        let name = "has-hit"
        let iters = 200
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir())
        await w.storeLocal(cid: smallCID, data: small)
        for _ in 0..<(iters * warmup) { _ = await w.has(cid: smallCID) }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters { _ = await w.has(cid: smallCID) }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    do {
        let name = "has-miss"
        let iters = 200
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir())
        for _ in 0..<(iters * warmup) { _ = await w.has(cid: missCID) }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters { _ = await w.has(cid: missCID) }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    // -------------------------------------------------------
    print("[Delete operations]")
    // -------------------------------------------------------

    do {
        let name = "delete"
        let iters = 100
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir())
        for item in storeItems64.prefix(400) { await w.storeLocal(cid: item.0, data: item.1) }
        var idx = 0
        for _ in 0..<(iters * warmup) { await w.delete(cid: storeItems64[idx % 400].0); idx += 1 }
        for item in storeItems64.prefix(400) { await w.storeLocal(cid: item.0, data: item.1) }
        idx = 0
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters { await w.delete(cid: storeItems64[idx % 400].0); idx += 1 }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    // -------------------------------------------------------
    print("[Eviction under pressure]")
    // -------------------------------------------------------

    do {
        let name = "store-evict-count"
        let iters = 50
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir(), capacity: 50, sampleSize: 5)
        for i in 0..<50 { await w.storeLocal(cid: storeItems64[i].0, data: storeItems64[i].1) }
        var idx = 50
        for _ in 0..<(iters * warmup) {
            await w.storeLocal(cid: storeItems64[idx % storeItems64.count].0, data: storeItems64[idx % storeItems64.count].1); idx += 1
        }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters {
                await w.storeLocal(cid: storeItems64[idx % storeItems64.count].0, data: storeItems64[idx % storeItems64.count].1); idx += 1
            }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    do {
        let name = "store-evict-bytes"
        let iters = 50
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir(), maxBytes: 50 * 72, sampleSize: 5)
        for i in 0..<50 { await w.storeLocal(cid: storeItems64[i].0, data: storeItems64[i].1) }
        var idx = 50
        for _ in 0..<(iters * warmup) {
            await w.storeLocal(cid: storeItems64[idx % storeItems64.count].0, data: storeItems64[idx % storeItems64.count].1); idx += 1
        }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters {
                await w.storeLocal(cid: storeItems64[idx % storeItems64.count].0, data: storeItems64[idx % storeItems64.count].1); idx += 1
            }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    // -------------------------------------------------------
    print("[Init scan with pre-existing files]")
    // -------------------------------------------------------

    do {
        let name = "init-scan-100"
        let iters = 10
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let d = dir()
        let seeder = try DiskCASWorker(directory: d, capacity: 200)
        for i in 0..<100 { await seeder.storeLocal(cid: storeItems64[i].0, data: storeItems64[i].1) }
        for _ in 0..<(iters * warmup) { _ = try DiskCASWorker(directory: d, capacity: 200) }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters { _ = try DiskCASWorker(directory: d, capacity: 200) }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    // -------------------------------------------------------
    print("[Mixed workload — 80% read, 20% write]")
    // -------------------------------------------------------

    do {
        let name = "mixed-80r-20w-4KB"
        let iters = 100
        print("  Running: \(name) ...", terminator: ""); fflush(stdout)
        let w = try DiskCASWorker(directory: dir(), capacity: 500)
        let readItems = Array(storeItems4K.prefix(50))
        for item in readItems { await w.storeLocal(cid: item.0, data: item.1) }
        var step = 0; var writeIdx = 50
        for _ in 0..<(iters * warmup) {
            step += 1
            if step % 5 == 0 {
                let item = storeItems4K[writeIdx % storeItems4K.count]; writeIdx += 1
                await w.storeLocal(cid: item.0, data: item.1)
            } else {
                _ = await w.getLocal(cid: readItems[step % readItems.count].0)
            }
        }
        var durations: [Double] = []
        for _ in 0..<sampleCount {
            let start = clock.now
            for _ in 0..<iters {
                step += 1
                if step % 5 == 0 {
                    let item = storeItems4K[writeIdx % storeItems4K.count]; writeIdx += 1
                    await w.storeLocal(cid: item.0, data: item.1)
                } else {
                    _ = await w.getLocal(cid: readItems[step % readItems.count].0)
                }
            }
            durations.append(usPerOp(start, clock.now, iters))
        }
        let r = BenchmarkResult(name: name, iterations: iters * sampleCount, samples: durations)
        print(" done (\(formatMicros(r.median)) median)")
        results.append(r)
    }

    return results
}
