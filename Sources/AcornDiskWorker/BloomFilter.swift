import Foundation

struct BloomFilter: Sendable {
    private var bits: [UInt64]
    private let bitCount: Int
    private let hashCount: Int

    init(expectedCount: Int, falsePositiveRate: Double = 0.01) {
        let m = max(64, Self.optimalBitCount(expectedCount: expectedCount, fpRate: falsePositiveRate))
        let k = max(1, Self.optimalHashCount(bitCount: m, expectedCount: expectedCount))
        self.bitCount = m
        self.hashCount = k
        self.bits = [UInt64](repeating: 0, count: (m + 63) / 64)
    }

    private init(bits: [UInt64], bitCount: Int, hashCount: Int) {
        self.bits = bits
        self.bitCount = bitCount
        self.hashCount = hashCount
    }

    mutating func insert(_ value: String) {
        let (h1, h2) = hashes(value)
        for i in 0..<hashCount {
            let idx = Int((h1 &+ UInt64(i) &* h2) % UInt64(bitCount))
            bits[idx / 64] |= 1 << (idx % 64)
        }
    }

    func mightContain(_ value: String) -> Bool {
        let (h1, h2) = hashes(value)
        for i in 0..<hashCount {
            let idx = Int((h1 &+ UInt64(i) &* h2) % UInt64(bitCount))
            if bits[idx / 64] & (1 << (idx % 64)) == 0 { return false }
        }
        return true
    }

    func serialize() -> Data {
        var data = Data()
        data.reserveCapacity(16 + bits.count * 8)
        var bc = UInt64(bitCount)
        var hc = UInt64(hashCount)
        withUnsafeBytes(of: &bc) { data.append(contentsOf: $0) }
        withUnsafeBytes(of: &hc) { data.append(contentsOf: $0) }
        for var word in bits {
            withUnsafeBytes(of: &word) { data.append(contentsOf: $0) }
        }
        return data
    }

    static func deserialize(from data: Data) -> BloomFilter? {
        guard data.count >= 16 else { return nil }
        let bc = data.withUnsafeBytes { $0.load(fromByteOffset: 0, as: UInt64.self) }
        let hc = data.withUnsafeBytes { $0.load(fromByteOffset: 8, as: UInt64.self) }
        let bitCount = Int(bc)
        let hashCount = Int(hc)
        let expectedWords = (bitCount + 63) / 64
        guard data.count == 16 + expectedWords * 8 else { return nil }
        var bits = [UInt64](repeating: 0, count: expectedWords)
        data.withUnsafeBytes { buf in
            for i in 0..<expectedWords {
                bits[i] = buf.load(fromByteOffset: 16 + i * 8, as: UInt64.self)
            }
        }
        return BloomFilter(bits: bits, bitCount: bitCount, hashCount: hashCount)
    }

    private func hashes(_ value: String) -> (UInt64, UInt64) {
        var h: UInt64 = 14695981039346656037
        for byte in value.utf8 {
            h ^= UInt64(byte)
            h &*= 1099511628211
        }
        let h1 = h
        h ^= h >> 33
        h &*= 0xff51afd7ed558ccd
        h ^= h >> 33
        h &*= 0xc4ceb9fe1a85ec53
        h ^= h >> 33
        return (h1, h)
    }

    private static func optimalBitCount(expectedCount n: Int, fpRate p: Double) -> Int {
        guard n > 0, p > 0 else { return 64 }
        return Int(ceil(-Double(n) * log(p) / (log(2) * log(2))))
    }

    private static func optimalHashCount(bitCount m: Int, expectedCount n: Int) -> Int {
        guard n > 0 else { return 1 }
        return max(1, Int(round(Double(m) / Double(n) * log(2))))
    }
}
