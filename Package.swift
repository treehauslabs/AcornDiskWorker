// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "AcornDiskWorker",
    platforms: [.macOS(.v13), .iOS(.v16)],
    products: [
        .library(name: "AcornDiskWorker", targets: ["AcornDiskWorker"]),
    ],
    dependencies: [
        .package(url: "https://github.com/treehauslabs/Acorn.git", from: "1.1.0"),
    ],
    targets: [
        .target(
            name: "AcornDiskWorker",
            dependencies: ["Acorn"]
        ),
        .testTarget(
            name: "AcornDiskWorkerTests",
            dependencies: ["AcornDiskWorker"]
        ),
        .executableTarget(
            name: "DiskCASBenchmarks",
            dependencies: ["AcornDiskWorker", "Acorn"],
            path: "Benchmarks/DiskCASBenchmarks"
        ),
    ]
)
