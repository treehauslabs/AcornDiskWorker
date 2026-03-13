public struct CASMetrics: Sendable, Equatable {
    public var hits: Int = 0
    public var misses: Int = 0
    public var stores: Int = 0
    public var evictions: Int = 0
    public var deletions: Int = 0
    public var corruptionDetections: Int = 0
}
