import Foundation
import Testing
import PerfectCRUD
@testable import PerfectPostgreSQL

// How Perfect-CRUD's Dynamic API behaves on this connector. The connector doesn't implement
// `nextDynamicRow()`, so reading rows throws (CRUD's default) rather than building a
// `DynamicRow`; a statement string with several statements is rejected by the server; and
// unsigned values are bound as their decimal text, so even UInt64.max can't trap.
// Uses only `dbname=postgres` and TEMP tables, which are private to the connection.
@Suite struct DynamicAPITests {
	let pgEnabled = ProcessInfo.processInfo.environment["PG_TESTS"] == "1"

	func connect() throws -> Database<PostgresDatabaseConfiguration> {
		Database(configuration: try PostgresDatabaseConfiguration(postgresInitConnInfo))
	}

	private struct Count: Codable { let c: Int }

	@Test func selectThrowsOnceARowComesBack() throws {
		guard pgEnabled else { return }
		let db = try connect()
		try db.sql("CREATE TEMP TABLE dyn_select (id int, name text)")
		#expect(try db.select(DynamicQuery(table: "dyn_select")).rows.isEmpty)
		_ = try db.mutate(DynamicMutation(action: .insert, table: "dyn_select",
										  values: ["id": .int(1), "name": .string("a")]))
		#expect(throws: CRUDSQLExeError.self) {
			try db.select(DynamicQuery(table: "dyn_select"))
		}
		// Columns differing only in case would trap in DynamicRow.init; here no row is built.
		#expect(throws: CRUDSQLExeError.self) {
			try db.execute(DynamicSQL(sql: #"SELECT 1 AS a, 2 AS "A""#))
		}
	}

	@Test func mutationCountsAreNotReported() throws {
		guard pgEnabled else { return }
		let db = try connect()
		try db.sql("CREATE TEMP TABLE dyn_count (id int)")
		let insert = try db.mutate(DynamicMutation(action: .insert, table: "dyn_count", values: ["id": .int(1)]))
		#expect(insert.affectedRows == 0)
		#expect(insert.insertedID == nil)
		_ = try db.mutate(DynamicMutation(action: .insert, table: "dyn_count", values: ["id": .int(2)]))
		let update = try db.mutate(DynamicMutation(action: .update, table: "dyn_count", values: ["id": .int(3)],
												   predicates: [.init(field: "id", comparison: .greaterThan, value: .int(0))]))
		#expect(update.affectedRows == 0)
		#expect(try db.sql("SELECT count(*)::int AS c FROM dyn_count WHERE id = 3", Count.self).first?.c == 2)
	}

	// PQexecParams accepts one statement only, so neither statement runs and the call throws.
	@Test func multipleStatementsThrowAndRunNothing() throws {
		guard pgEnabled else { return }
		let db = try connect()
		try db.sql("CREATE TEMP TABLE dyn_multi (id int)")
		let error = #expect(throws: CRUDSQLExeError.self) {
			try db.execute(DynamicSQL(sql: "INSERT INTO dyn_multi VALUES (1); INSERT INTO dyn_multi VALUES (2)",
									  allowsMultipleStatements: true))
		}
		#expect("\(error.map { $0 as Any } ?? "")".contains("cannot insert multiple commands into a prepared statement"))
		#expect(throws: CRUDSQLGenError.self) {
			try db.execute(DynamicSQL(sql: "INSERT INTO dyn_multi VALUES (1); INSERT INTO dyn_multi VALUES (2)"))
		}
		#expect(try db.sql("SELECT count(*)::int AS c FROM dyn_multi", Count.self).first?.c == 0)
	}

	@Test func unsignedValuesBindExactly() throws {
		guard pgEnabled else { return }
		let db = try connect()
		try db.sql("CREATE TEMP TABLE dyn_uint (id int, n numeric, b bigint)")
		_ = try db.mutate(DynamicMutation(action: .insert, table: "dyn_uint",
										  values: ["id": .int(1), "n": .uint(UInt64.max)]))
		struct Text: Codable { let t: String }
		#expect(try db.sql("SELECT n::text AS t FROM dyn_uint WHERE id = 1", Text.self).first?.t == "\(UInt64.max)")
		// Too big for bigint: the server rejects it, nothing traps.
		#expect(throws: CRUDSQLExeError.self) {
			try db.mutate(DynamicMutation(action: .insert, table: "dyn_uint",
										  values: ["id": .int(2), "b": .uint(UInt64.max)]))
		}
		// The same through the typed API's unsigned expressions.
		let conn = db.configuration.connection
		for (index, value) in [UInt64.max, UInt64(UInt32.max), UInt64(UInt16.max), UInt64(UInt8.max)].enumerated() {
			let exe = PostgresExeDelegate(connection: conn, sql: "SELECT $1::numeric::text AS t")
			let expr: CRUDExpression = switch index {
			case 0: .uinteger64(value)
			case 1: .uinteger32(UInt32(value))
			case 2: .uinteger16(UInt16(value))
			default: .uinteger8(UInt8(value))
			}
			exe.nextBindings = [("$1", expr)]
			#expect(try exe.hasNext())
			#expect(exe.results?.getFieldString(tupleIndex: 0, fieldIndex: 0) == "\(value)")
		}
		let exe = PostgresExeDelegate(connection: conn, sql: "SELECT $1::numeric::text AS t")
		exe.nextBindings = [("$1", .uinteger(UInt.max))]
		#expect(try exe.hasNext())
		#expect(exe.results?.getFieldString(tupleIndex: 0, fieldIndex: 0) == "\(UInt.max)")
	}
}
