# Perfect - PostgreSQL Connector

<p align="center">
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Swift-6.2-orange.svg?style=flat" alt="Swift 6.2">
    </a>
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Platforms-macOS%2012%2B-lightgray.svg?style=flat" alt="Platforms macOS 12+">
    </a>
    <a href="LICENSE" target="_blank">
        <img src="https://img.shields.io/badge/License-Apache-lightgrey.svg?style=flat" alt="License Apache">
    </a>
</p>

This package is part of the [Perfect-Resurrection](https://github.com/taplin) project, a modernized fork of the original [PerfectlySoft Perfect](https://github.com/PerfectlySoft/Perfect) server-side Swift libraries.

It provides a Swift wrapper around the `libpq` client library for connecting to PostgreSQL servers, **and** a full [Perfect-CRUD](https://github.com/taplin/Perfect-CRUD) SQL provider backend for PostgreSQL. `Sources/PerfectPostgreSQL/PostgresCRUD.swift` implements Perfect-CRUD's `SQLGenDelegate`, `SQLExeDelegate`, and `DatabaseConfigurationProtocol`, so this is no longer a bare, stand-alone libpq connector — it's a CRUD driver that happens to also expose the lower-level `PGConnection`/`PGResult` libpq wrapper (`Sources/PerfectPostgreSQL/PerfectPostgreSQL.swift`, using `swift-log` for diagnostics rather than `print`).

**Usage status:** this driver is real, tested, and consumed today — [Perfect-Session](https://github.com/taplin/Perfect-Session)'s `PostgreSQLSessionDriver` (`Sources/PerfectSessionPostgreSQL/PostgreSQLSessionDriver.swift`) imports `PerfectPostgreSQL` directly and is one of Perfect-Session's four supported backend drivers. It is **not** the backend currently configured in Perfect-Lasso's development/validation setup (scrubsSite), which uses the MySQL driver instead — this PostgreSQL driver is a supported, ready-to-use alternative, not deprecated or dead code.

Both `libpq` wrapper types (`PGConnection`, `PGResult`) are synchronous/blocking and marked `@unchecked Sendable`; there is no async/await API in this package.

This package builds with Swift Package Manager. Ensure you have installed and activated a Swift 6.2 (or later) tool chain.

## Requirements

- swift-tools-version: 6.2, built with `swiftLanguageMode(.v6)` (strict concurrency)
- Platform: `platforms: [.macOS(.v12)]` — macOS 12 or later (no other Apple platforms declared)
- Linux is buildable via SwiftPM/systemLibrary (the `libpq` system-library target declares an `.apt(["libpq-dev"])` provider) but is not asserted in the `platforms` array, so Linux support is implicit, not a guaranteed contract of this manifest.

## Dependencies

Declared in `Package.swift`:

- [`Perfect-CRUD`](https://github.com/taplin/Perfect-CRUD) (`.package(url:, branch: "main")`), product `PerfectCRUD`.
- [`swift-log`](https://github.com/apple/swift-log.git) `from: 1.5.0`, product `Logging`.
- A `.systemLibrary` target `libpq` (via `pkgConfig: "libpq"`) providing the C libpq bindings.

## macOS Build Notes

This package requires the `libpq` client library, available via Homebrew:

```
brew install libpq
```

## Linux Build Notes

Ensure that you have installed `libpq-dev`.

```
sudo apt-get install libpq-dev
```

## Building

Add this project as a dependency in your `Package.swift` file:

```swift
dependencies: [
    // No tagged releases exist yet, so pin a branch rather than a version:
    .package(url: "https://github.com/taplin/Perfect-PostgreSQL.git", branch: "main"),
],
targets: [
    .target(
        name: "YourTarget",
        dependencies: [
            .product(name: "PerfectPostgreSQL", package: "Perfect-PostgreSQL"),
        ]
    ),
]
```

## Testing

A test target is included. Run it with:

```
swift test
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
