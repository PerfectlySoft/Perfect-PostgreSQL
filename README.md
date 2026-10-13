# Perfect - PostgreSQL Connector

<p align="center">
    <img src="https://img.shields.io/badge/Swift-6.2-orange.svg?style=flat" alt="Swift 6.2">
    <img src="https://img.shields.io/badge/Platforms-macOS%2012%2B%20%7C%20Linux-lightgray.svg?style=flat" alt="Platforms macOS 12+ | Linux">
    <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg?style=flat" alt="License Apache 2.0"></a>
</p>

A PostgreSQL connector for Swift, built on the `libpq` client library. It has two layers:

- a [Perfect-CRUD](https://github.com/PerfectlySoft/Perfect-CRUD) backend, `PostgresDatabaseConfiguration`, so
  `Codable` types can be created, inserted, queried, joined, updated and deleted through CRUD, plus
  PostgreSQL-only `returning(...)` variants of insert and update;
- the lower-level `PGConnection` / `PGResult` wrapper around libpq.

[Perfect-Session](https://github.com/PerfectlySoft/Perfect-Session)'s PostgreSQL session driver is built on it.

The pre-Swift-6 version of this package (4.0.0 and earlier) is preserved on the [`legacy`](../../tree/legacy) branch.

**Requirements:** Swift 6.2 or newer (Swift 6 language mode). macOS 12+ and Linux, with the libpq client library
installed (see below). CI runs the tests on Linux (Swift 6.2 and 6.4, Ubuntu 24.04, against PostgreSQL 17) and
macOS (Xcode 26.0.1 and 26.6) in debug and release.

```swift
dependencies: [
    .package(url: "https://github.com/PerfectlySoft/Perfect-PostgreSQL.git", from: "5.0.0")
],
targets: [
    .target(name: "MyTarget", dependencies: [
        .product(name: "PerfectPostgreSQL", package: "Perfect-PostgreSQL")
    ])
]
```

## Installing libpq

On macOS, install it with Homebrew:

```
brew install libpq
```

Homebrew's libpq is keg-only (not linked into `/opt/homebrew`). Swift 6.4's SwiftPM finds it anyway, but Swift 6.2's
doesn't, and the build fails with `'libpq-fe.h' file not found`. Pointing pkg-config at it works with both:

```
export PKG_CONFIG_PATH="$(brew --prefix libpq)/lib/pkgconfig"
```

On Linux (Debian/Ubuntu):

```
sudo apt-get install libpq-dev pkg-config
```

## Usage

```swift
import PerfectCRUD
import PerfectPostgreSQL

struct Person: Codable {
    let id: Int
    let name: String
}

let db = Database(configuration: try PostgresDatabaseConfiguration(
    database: "mydb", host: "localhost", username: "me", password: "secret"))
try db.create(Person.self, policy: .reconcileTable)
let people = db.table(Person.self)
try people.insert(Person(id: 1, name: "Ada"))
let ada = try people.where(\Person.id == 1).first()
```

`PostgresDatabaseConfiguration("host=localhost dbname=mydb")` takes a libpq connection string instead. libpq's
environment variables (`PGPORT`, `PGUSER`, `PGPASSWORD` and so on) fill in anything the connection string leaves out.

See [Perfect-CRUD](https://github.com/PerfectlySoft/Perfect-CRUD) for the query API. What the PostgreSQL backend
doesn't support:

- **Dynamic Select:** `select(_: DynamicQuery)` throws "Dynamic rows are not supported by this connector" as soon as
  a row comes back, and so does `execute(_: DynamicSQL)` for a statement that returns rows. The error comes only
  after the statement has run (and, outside a transaction, been committed): an `INSERT … RETURNING` through
  `execute` has inserted its row when it throws, so retrying it inserts the row again. `mutate(_: DynamicMutation)`
  and row-less `DynamicSQL` work, but the result's `affectedRows` is always 0 and its `insertedID` is nil.
  `DynamicSQL` with `allowsMultipleStatements: true` throws (PostgreSQL's extended protocol takes one statement per
  call) and runs none of them.
- **Isolation levels:** `transaction(isolation:)` ignores the level and uses the server's default.

`PGConnection` and `PGResult` are synchronous (blocking) and `@unchecked Sendable`. Every `PGConnection` method holds
an internal lock, and `doWithTransaction` holds it for the whole transaction, so a connection can be shared between
tasks. Separate calls from different tasks can still interleave, so give each task its own connection (for example
through Perfect-CRUD's `DatabaseConnectionPool`) when that matters.

That lock does not cover Perfect-CRUD's `Database.transaction` / `transactionAsync`: they send `BEGIN`, the body's
statements and `COMMIT` or `ROLLBACK` as separate calls, so another task's statement on the same `Database` runs
inside the transaction and is committed or rolled back with it. Don't share a `Database` that runs transactions
between tasks; take one connection per task from a `DatabaseConnectionPool`. `PostgresDatabaseConfiguration` is
`Sendable`, so the pool and CRUD's async entry points (`transactionAsync`, `insertAsync`, `fetchAll` and so on) work
with it.

Known limitations:

- A `String` value containing a NUL character is cut off at the NUL when it's sent.
- `[UInt8]` / `Data` columns are decoded from PostgreSQL's default hex `bytea` output; with `bytea_output = escape`
  they decode as empty.
- A NULL read into a non-optional `Int` or `String` property gives 0 or `""` rather than an error.
- `Date` values are stored to the millisecond.
- A `UInt` or `UInt64` value above `Int64.max` binds (Perfect-CRUD 3.0.1 sends it as unsigned), but a `bigint`
  column can't hold it, so the server rejects it with an error.

## Testing

```
swift test
```

The tests that need a server only run with `PG_TESTS=1`. They connect with `host=localhost` and the
`postgres` and `testing123` databases (the tests drop and recreate `testing123` and
`perfect_subtable_order_test`), so use a throwaway server. libpq's environment variables set the rest, for example:

```
docker run -d --rm -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:17
PG_TESTS=1 PGUSER=postgres swift test
```

Use `PGPORT` for a server on another port, and `PGHOSTADDR` for one that isn't on localhost.

## Migrating from 4.x

5.0.0 is a major release. The [release notes](../../releases/tag/5.0.0) list every change; the ones most likely to
need attention when upgrading:

* **Toolchain and platforms.** Swift tools 6.2 and the Swift 6 language mode are required, and the minimum macOS
  is now 12 (was 10.15).
* **libpq comes from the system.** The Perfect-libpq and Perfect-libpq-linux packages are no longer used: install
  libpq as above, and remove those packages from your own manifest if you added them.
* **Perfect-CRUD 3.0.0.** CRUD's own breaking changes apply; see its
  [migration notes](https://github.com/PerfectlySoft/Perfect-CRUD#migrating-from-2x).
* **`Sendable`.** Closures passed to `PGConnection.setReceiver` and `setProcessor` must be `@Sendable`.
* **Shared connections are serialized.** `PGConnection` calls from different threads now wait for each other, and
  `doWithTransaction` blocks other callers on that connection until it commits or rolls back. Code inside the
  transaction closure that waits for another thread using the same connection will deadlock.
* **`create()` with sub-tables.** Tables are created in foreign-key order, and `.dropTable` drops every table in the
  set before creating any. `.reconcileTable` now reconciles existing sub-tables too, which drops columns their models
  no longer have. When it needs to alter a table the role doesn't own, it fails with "must be owner".
* **Identifiers containing `"`.** `quote(identifier:)` now doubles an embedded `"`, so such a name refers to the
  table or column of that exact name. Before, it ended the quoted identifier and let a Dynamic API name inject SQL.
* **Connection parameters are quoted.** `PostgresDatabaseConfiguration(database:host:port:username:password:)` now
  quotes each value, so a value is used exactly as given. If you escaped values yourself, or appended options inside
  one (such as `host: "localhost port=5433"`), pass the plain value and use the matching parameter or a connection
  string with `PostgresDatabaseConfiguration(_:)` instead.
* **Logging.** The connector's one diagnostic of its own (an unhandled result status) goes through
  [swift-log](https://github.com/apple/swift-log) (label `perfect.postgresql`) instead of `print`. Errors and queries
  logged by the CRUD layer still go through Perfect-CRUD's `CRUDLogging`.

## License

Apache 2.0. See [LICENSE](LICENSE).
