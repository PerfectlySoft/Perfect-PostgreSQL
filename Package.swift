// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "PerfectPostgreSQL",
    platforms: [.macOS(.v26)],
    products: [
        .library(name: "PerfectPostgreSQL", targets: ["PerfectPostgreSQL"]),
    ],
    dependencies: [
        .package(url: "https://github.com/taplin/Perfect-CRUD.git", branch: "main"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.5.0"),
    ],
    targets: [
        .systemLibrary(
            name: "libpq",
            pkgConfig: "libpq",
            providers: [
                .brew(["libpq"]),
                .apt(["libpq-dev"]),
            ]
        ),
        .target(
            name: "PerfectPostgreSQL",
            dependencies: [
                .product(name: "PerfectCRUD", package: "Perfect-CRUD"),
                .product(name: "Logging", package: "swift-log"),
                "libpq",
            ],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .testTarget(
            name: "PerfectPostgreSQLTests",
            dependencies: ["PerfectPostgreSQL"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
    ]
)
