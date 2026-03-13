import Foundation

struct BenchmarkResult: Codable, Sendable {
    let name: String
    let iterations: Int
    let samples: [Double]

    var min: Double { samples.min() ?? 0 }
    var max: Double { samples.max() ?? 0 }
    var mean: Double { samples.reduce(0, +) / Double(samples.count) }
    var median: Double { percentile(0.5) }
    var p95: Double { percentile(0.95) }
    var p99: Double { percentile(0.99) }
    var stddev: Double {
        let m = mean
        let variance = samples.map { ($0 - m) * ($0 - m) }.reduce(0, +) / Double(samples.count)
        return variance.squareRoot()
    }

    func percentile(_ p: Double) -> Double {
        let sorted = samples.sorted()
        guard !sorted.isEmpty else { return 0 }
        let index = p * Double(sorted.count - 1)
        let lower = Int(index)
        let upper = Swift.min(lower + 1, sorted.count - 1)
        let fraction = index - Double(lower)
        return sorted[lower] * (1 - fraction) + sorted[upper] * fraction
    }
}

func formatMicros(_ us: Double) -> String {
    if us >= 1_000_000 {
        return String(format: "%.1fs", us / 1_000_000)
    } else if us >= 1_000 {
        return String(format: "%.1fms", us / 1_000)
    } else if us >= 1 {
        return String(format: "%.1fus", us)
    } else {
        return String(format: "%.0fns", us * 1000)
    }
}

func pad(_ s: String, width: Int, right: Bool = false) -> String {
    if s.count >= width { return s }
    let padding = String(repeating: " ", count: width - s.count)
    return right ? (padding + s) : (s + padding)
}

func printReport(_ results: [BenchmarkResult]) {
    let nameWidth = max(results.map(\.name.count).max() ?? 0, 9)
    let cols = [
        pad("Benchmark", width: nameWidth),
        pad("Min", width: 10, right: true),
        pad("Median", width: 10, right: true),
        pad("Mean", width: 10, right: true),
        pad("P95", width: 10, right: true),
        pad("P99", width: 10, right: true),
        pad("StdDev", width: 10, right: true)
    ]
    let header = cols.joined(separator: " ")
    let separator = String(repeating: "-", count: header.count)
    print("\n\(separator)")
    print(header)
    print(separator)
    for r in results {
        let row = [
            pad(r.name, width: nameWidth),
            pad(formatMicros(r.min), width: 10, right: true),
            pad(formatMicros(r.median), width: 10, right: true),
            pad(formatMicros(r.mean), width: 10, right: true),
            pad(formatMicros(r.p95), width: 10, right: true),
            pad(formatMicros(r.p99), width: 10, right: true),
            pad(formatMicros(r.stddev), width: 10, right: true)
        ]
        print(row.joined(separator: " "))
    }
    print(separator)
    print("All times in microseconds (us) per operation.\n")
}

func saveBaseline(_ results: [BenchmarkResult], to url: URL) throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
    let data = try encoder.encode(results)
    try data.write(to: url, options: .atomic)
    print("Baseline saved to \(url.path(percentEncoded: false))")
}

func compareBaseline(_ results: [BenchmarkResult], from url: URL) throws {
    guard FileManager.default.fileExists(atPath: url.path(percentEncoded: false)) else {
        print("No baseline found at \(url.path(percentEncoded: false)). Run with --save-baseline first.")
        return
    }
    let data = try Data(contentsOf: url)
    let baseline = try JSONDecoder().decode([BenchmarkResult].self, from: data)
    let baselineMap = Dictionary(uniqueKeysWithValues: baseline.map { ($0.name, $0) })

    let nameWidth = max(results.map(\.name.count).max() ?? 0, 9)
    let headerCols = [
        pad("Benchmark", width: nameWidth),
        pad("Baseline", width: 12, right: true),
        pad("Current", width: 12, right: true),
        pad("Delta", width: 10, right: true),
        pad("Status", width: 8, right: true)
    ]
    let header = headerCols.joined(separator: " ")
    let separator = String(repeating: "-", count: header.count)
    print("\n\(separator)")
    print(header)
    print(separator)

    var regressions = 0
    for r in results {
        guard let b = baselineMap[r.name] else {
            let row = [
                pad(r.name, width: nameWidth),
                pad("N/A", width: 12, right: true),
                pad(formatMicros(r.median), width: 12, right: true),
                pad("new", width: 10, right: true),
                pad("---", width: 8, right: true)
            ]
            print(row.joined(separator: " "))
            continue
        }
        let delta = ((r.median - b.median) / b.median) * 100
        let threshold = 15.0
        let status: String
        if delta > threshold {
            status = "REGRESS"
            regressions += 1
        } else if delta < -threshold {
            status = "FASTER"
        } else {
            status = "OK"
        }
        let deltaStr = String(format: "%+.1f%%", delta)
        let row = [
            pad(r.name, width: nameWidth),
            pad(formatMicros(b.median), width: 12, right: true),
            pad(formatMicros(r.median), width: 12, right: true),
            pad(deltaStr, width: 10, right: true),
            pad(status, width: 8, right: true)
        ]
        print(row.joined(separator: " "))
    }
    print(separator)
    if regressions > 0 {
        print("WARNING: \(regressions) regression(s) detected (>15% slower than baseline).")
    } else {
        print("All benchmarks within threshold.")
    }
    print("")
}
