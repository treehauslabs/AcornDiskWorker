import Foundation
#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

public protocol FileSystemProvider: Sendable {
    func fileExists(atPath path: String) -> Bool
    func createDirectory(atPath path: String) throws
    func contentsOfFile(atPath path: String) throws -> Data
    func writeFile(_ data: Data, toPath path: String) throws
    func removeItem(atPath path: String) throws
    func contentsOfDirectory(atPath path: String) throws -> [String]
    func fileSize(atPath path: String) -> Int?
}

public struct DefaultFileSystem: FileSystemProvider {
    public init() {}

    public func fileExists(atPath path: String) -> Bool {
        access(path, F_OK) == 0
    }

    public func createDirectory(atPath path: String) throws {
        var isDir: ObjCBool = false
        if FileManager.default.fileExists(atPath: path, isDirectory: &isDir), isDir.boolValue {
            return
        }
        try FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: true, attributes: nil)
    }

    public func contentsOfFile(atPath path: String) throws -> Data {
        let fd = open(path, O_RDONLY)
        guard fd >= 0 else { throw CocoaError(.fileReadNoSuchFile) }
        defer { close(fd) }

        var st = stat()
        guard fstat(fd, &st) == 0 else { throw CocoaError(.fileReadUnknown) }
        let size = Int(st.st_size)
        guard size > 0 else { return Data() }

        var data = Data(count: size)
        let bytesRead = data.withUnsafeMutableBytes { buf in
            read(fd, buf.baseAddress!, size)
        }
        guard bytesRead == size else { throw CocoaError(.fileReadUnknown) }
        return data
    }

    public func writeFile(_ data: Data, toPath path: String) throws {
        let tempPath = path + ".t\(UInt32.random(in: 0...UInt32.max))"
        let fd = open(tempPath, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
        guard fd >= 0 else { throw CocoaError(.fileWriteUnknown) }

        let written = data.withUnsafeBytes { buf in
            write(fd, buf.baseAddress!, data.count)
        }
        close(fd)

        guard written == data.count else {
            unlink(tempPath)
            throw CocoaError(.fileWriteUnknown)
        }

        if rename(tempPath, path) != 0 {
            unlink(tempPath)
            throw CocoaError(.fileWriteUnknown)
        }
    }

    public func removeItem(atPath path: String) throws {
        if unlink(path) != 0 && errno != ENOENT {
            throw CocoaError(.fileWriteUnknown)
        }
    }

    public func contentsOfDirectory(atPath path: String) throws -> [String] {
        guard let dir = opendir(path) else { throw CocoaError(.fileReadNoSuchFile) }
        defer { closedir(dir) }
        var results: [String] = []
        while let entry = readdir(dir) {
            let name = withUnsafePointer(to: entry.pointee.d_name) { ptr in
                String(cString: UnsafeRawPointer(ptr).assumingMemoryBound(to: CChar.self))
            }
            if name == "." || name == ".." { continue }
            results.append(path + "/" + name)
        }
        return results
    }

    public func fileSize(atPath path: String) -> Int? {
        var st = stat()
        guard stat(path, &st) == 0 else { return nil }
        return Int(st.st_size)
    }
}
