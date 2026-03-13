import Foundation
import Acorn

public actor DiskCASWorker<F: FileSystemProvider>: AcornCASWorker {
    private let directoryPath: String
    private let fs: F
    private var cache: LFUDecayCache?
    private let maxBytes: Int?
    private let verifyReads: Bool
    private var itemSizes: [ContentIdentifier: Int] = [:]
    private var _totalBytes: Int = 0
    private var bloom: BloomFilter
    public let timeout: Duration?
    public var near: (any AcornCASWorker)?
    public var far: (any AcornCASWorker)?
    public private(set) var metrics = CASMetrics()
    public var totalBytes: Int { _totalBytes }

    public init(
        directory: URL,
        capacity: Int? = nil,
        maxBytes: Int? = nil,
        halfLife: Duration = .seconds(300),
        sampleSize: Int = 5,
        timeout: Duration? = nil,
        verifyReads: Bool = true,
        fileSystem: F
    ) throws {
        self.directoryPath = directory.path(percentEncoded: false)
        self.fs = fileSystem
        self.maxBytes = maxBytes
        self.verifyReads = verifyReads
        self.bloom = BloomFilter(expectedCount: max(capacity ?? 10_000, 1_000))
        if let capacity {
            self.cache = LFUDecayCache(capacity: capacity, halfLife: halfLife, sampleSize: sampleSize)
        } else if maxBytes != nil {
            self.cache = LFUDecayCache(capacity: .max, halfLife: halfLife, sampleSize: sampleSize)
        }
        self.timeout = timeout
        try fs.createDirectory(atPath: directoryPath)

        // Try to load persisted state first
        let loaded = Self.loadPersistedState(directoryPath: directoryPath, fs: fs)

        if let loaded {
            self.bloom = loaded.bloom
            self.itemSizes = loaded.sizes
            self._totalBytes = loaded.sizes.values.reduce(0, +)
            if cache != nil {
                for cid in loaded.sizes.keys {
                    cache?.recordAccess(cid)
                }
            }
        } else {
            // Pre-create all 256 shard directories
            let hexChars: [Character] = Array("0123456789abcdef")
            for a in hexChars {
                for b in hexChars {
                    let shard = directoryPath + "/" + String(a) + String(b)
                    try? fs.createDirectory(atPath: shard)
                }
            }

            // Scan existing files
            let shardDirs = (try? fs.contentsOfDirectory(atPath: directoryPath)) ?? []
            for shardDir in shardDirs {
                let shardName = (shardDir as NSString).lastPathComponent
                guard shardName.count == 2 else { continue }
                let files = (try? fs.contentsOfDirectory(atPath: shardDir)) ?? []
                for file in files {
                    let fileName = (file as NSString).lastPathComponent
                    guard fileName.count == 64 else { continue }
                    let cid = ContentIdentifier(rawValue: fileName)
                    bloom.insert(cid.rawValue)
                    if cache != nil {
                        cache?.recordAccess(cid)
                        if let size = fs.fileSize(atPath: file) {
                            itemSizes[cid] = size
                            _totalBytes += size
                        }
                    }
                }
            }
        }
    }

    public func has(cid: ContentIdentifier) -> Bool {
        guard bloom.mightContain(cid.rawValue) else { return false }
        return fs.fileExists(atPath: filePath(for: cid))
    }

    public func getLocal(cid: ContentIdentifier) async -> Data? {
        guard bloom.mightContain(cid.rawValue) else {
            metrics.misses += 1
            return nil
        }
        let path = filePath(for: cid)
        guard let data = try? fs.contentsOfFile(atPath: path) else {
            metrics.misses += 1
            return nil
        }
        if verifyReads {
            guard ContentIdentifier(for: data) == cid else {
                try? fs.removeItem(atPath: path)
                cache?.remove(cid)
                let oldSize = itemSizes.removeValue(forKey: cid) ?? 0
                _totalBytes -= oldSize
                metrics.corruptionDetections += 1
                return nil
            }
        }
        cache?.recordAccess(cid)
        scheduleRenormalizationIfNeeded()
        metrics.hits += 1
        return data
    }

    public func storeLocal(cid: ContentIdentifier, data: Data) async {
        if var c = cache {
            while c.needsEviction(for: cid) || isOverByteLimit(adding: data.count, for: cid) {
                guard let victim = c.evictionCandidate(), victim != cid else { break }
                try? fs.removeItem(atPath: filePath(for: victim))
                c.remove(victim)
                let oldSize = itemSizes.removeValue(forKey: victim) ?? 0
                _totalBytes -= oldSize
                metrics.evictions += 1
            }
            c.recordAccess(cid)
            cache = c
            scheduleRenormalizationIfNeeded()
        }
        let path = filePath(for: cid)
        try? fs.writeFile(data, toPath: path)
        let oldSize = itemSizes[cid] ?? 0
        itemSizes[cid] = data.count
        _totalBytes += data.count - oldSize
        bloom.insert(cid.rawValue)
        metrics.stores += 1
    }

    public func delete(cid: ContentIdentifier) {
        cache?.remove(cid)
        let oldSize = itemSizes.removeValue(forKey: cid) ?? 0
        _totalBytes -= oldSize
        try? fs.removeItem(atPath: filePath(for: cid))
        metrics.deletions += 1
    }

    public func persistState() throws {
        let bloomPath = directoryPath + "/.bloom"
        let sizesPath = directoryPath + "/.sizes"

        let bloomData = bloom.serialize()
        try fs.writeFile(bloomData, toPath: bloomPath)

        let encoder = JSONEncoder()
        let sizesDict = Dictionary(uniqueKeysWithValues: itemSizes.map { ($0.key.rawValue, $0.value) })
        let sizesData = try encoder.encode(sizesDict)
        try fs.writeFile(sizesData, toPath: sizesPath)
    }

    private static func loadPersistedState(directoryPath: String, fs: F) -> (bloom: BloomFilter, sizes: [ContentIdentifier: Int])? {
        let bloomPath = directoryPath + "/.bloom"
        let sizesPath = directoryPath + "/.sizes"

        guard let bloomData = try? fs.contentsOfFile(atPath: bloomPath),
              let sizesData = try? fs.contentsOfFile(atPath: sizesPath),
              let bloom = BloomFilter.deserialize(from: bloomData),
              let rawSizes = try? JSONDecoder().decode([String: Int].self, from: sizesData) else {
            return nil
        }

        let sizes = Dictionary(uniqueKeysWithValues: rawSizes.map { (ContentIdentifier(rawValue: $0.key), $0.value) })
        return (bloom, sizes)
    }

    private func isOverByteLimit(adding newSize: Int, for cid: ContentIdentifier) -> Bool {
        guard let maxBytes else { return false }
        let currentTotal = _totalBytes - (itemSizes[cid] ?? 0) + newSize
        return currentTotal > maxBytes
    }

    private func scheduleRenormalizationIfNeeded() {
        guard var c = cache else { return }
        if let work = c.claimRenormalization() {
            for key in work.keys {
                c.applyRenormFactor(key, factor: work.factor)
            }
            cache = c
        }
    }

    @inline(__always)
    private func filePath(for cid: ContentIdentifier) -> String {
        let hex = cid.rawValue
        return directoryPath + "/" + String(hex[hex.startIndex..<hex.index(hex.startIndex, offsetBy: 2)]) + "/" + hex
    }
}

// Convenience initializer with default file system
public extension DiskCASWorker where F == DefaultFileSystem {
    init(
        directory: URL,
        capacity: Int? = nil,
        maxBytes: Int? = nil,
        halfLife: Duration = .seconds(300),
        sampleSize: Int = 5,
        timeout: Duration? = nil,
        verifyReads: Bool = true
    ) throws {
        try self.init(
            directory: directory,
            capacity: capacity,
            maxBytes: maxBytes,
            halfLife: halfLife,
            sampleSize: sampleSize,
            timeout: timeout,
            verifyReads: verifyReads,
            fileSystem: DefaultFileSystem()
        )
    }
}
