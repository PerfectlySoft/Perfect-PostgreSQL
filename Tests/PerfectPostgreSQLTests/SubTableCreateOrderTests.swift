import Foundation
import Testing
@testable import PerfectPostgreSQL
@testable import PerfectCRUD

// MARK: - create() statement order for sub-tables, and .reconcileTable
//
// `create(Parent.self)` also creates the tables of Parent's `[Child]` properties. A table's
// FOREIGN KEY needs its target to exist, and with `.dropTable` a table dropped with CASCADE
// takes the FOREIGN KEY constraints that reference it with it.

private struct OrderParent: Codable {
	let id: Int
	var name: String
	var children: [OrderChild]?
}

private struct OrderChild: Codable {
	let id: Int
	@ForeignKey(OrderParent.self, onDelete: setNull, onUpdate: restrict)
	var parentId: Int?
}

// The reverse dependency: the parent references a sub-table type that doesn't reference
// it back, so that sub-table has to be created first.
private struct OrderOwner: Codable {
	let id: Int
	@ForeignKey(OrderPet.self, onDelete: setNull, onUpdate: restrict)
	var favoritePetId: Int?
	var pets: [OrderPet]?
}

private struct OrderPet: Codable {
	let id: Int
	var ownerId: Int
}

private struct ReconcileRow: Codable {
	let id: Int
	var camelName: String
}

private struct ColumnName: Codable {
	let column_name: String
}

private func statementHeads(_ statements: [String]) -> [String] {
	statements.map { $0.split(separator: "(").first.map { String($0).trimmingCharacters(in: .whitespaces) } ?? "" }
}

@Suite("PostgresGenDelegate sub-table create order")
struct SubTableCreateOrderDDLTests {

	@Test("a sub-table that references its parent is created after the parent")
	func childReferencingParentIsCreatedAfterIt() throws {
		let delegate = PostgresGenDelegate(connection: PGConnection())
		let statements = try delegate.getCreateTableSQL(forTable: try OrderParent.CRUDTableStructure(), policy: .dropTable)
		#expect(statementHeads(statements) == [
			"DROP TABLE IF EXISTS \"orderchild\" CASCADE",
			"DROP TABLE IF EXISTS \"orderparent\" CASCADE",
			"CREATE TABLE IF NOT EXISTS \"orderparent\"",
			"CREATE TABLE IF NOT EXISTS \"orderchild\"",
		])
	}

	@Test("a parent that references a sub-table is created after that sub-table")
	func parentReferencingSubTableIsCreatedAfterIt() throws {
		let delegate = PostgresGenDelegate(connection: PGConnection())
		let statements = try delegate.getCreateTableSQL(forTable: try OrderOwner.CRUDTableStructure(), policy: .dropTable)
		#expect(statementHeads(statements) == [
			"DROP TABLE IF EXISTS \"orderowner\" CASCADE",
			"DROP TABLE IF EXISTS \"orderpet\" CASCADE",
			"CREATE TABLE IF NOT EXISTS \"orderpet\"",
			"CREATE TABLE IF NOT EXISTS \"orderowner\"",
		])
	}

	@Test(".shallow creates only the table itself")
	func shallowSkipsSubTables() throws {
		let delegate = PostgresGenDelegate(connection: PGConnection())
		let statements = try delegate.getCreateTableSQL(forTable: try OrderParent.CRUDTableStructure(), policy: [.shallow, .dropTable])
		#expect(statementHeads(statements) == [
			"DROP TABLE IF EXISTS \"orderparent\" CASCADE",
			"CREATE TABLE IF NOT EXISTS \"orderparent\"",
		])
	}
}

@Suite("create() with sub-tables on a live server", .serialized)
struct SubTableCreateOrderLiveTests {
	static let databaseName = "perfect_subtable_order_test"

	private func freshDatabase() throws -> Database<PostgresDatabaseConfiguration> {
		let admin = Database(configuration: try PostgresDatabaseConfiguration(postgresInitConnInfo))
		try admin.sql("DROP DATABASE IF EXISTS \(Self.databaseName)")
		try admin.sql("CREATE DATABASE \(Self.databaseName)")
		return Database(configuration: try PostgresDatabaseConfiguration("host=localhost dbname=\(Self.databaseName)"))
	}

	private func dropDatabase() {
		guard let admin = try? Database(configuration: PostgresDatabaseConfiguration(postgresInitConnInfo)) else {
			return
		}
		try? admin.sql("DROP DATABASE IF EXISTS \(Self.databaseName) WITH (FORCE)")
	}

	// The FOREIGN KEY constraints on a table, as "column -> target table".
	private func foreignKeys(_ db: Database<PostgresDatabaseConfiguration>, _ table: String) throws -> [String] {
		struct Row: Codable { let fk: String }
		return try db.sql("""
			SELECT a.attname || ' -> ' || c.confrelid::regclass::text AS fk
			FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
			WHERE c.contype = 'f' AND c.conrelid = '\(table)'::regclass
			ORDER BY 1
			""", Row.self).map(\.fk)
	}

	@Test(.enabled(if: ProcessInfo.processInfo.environment["PG_TESTS"] == "1"))
	func createParentWithReferencingChildren() throws {
		let db = try freshDatabase()
		defer { dropDatabase() }

		try db.create(OrderParent.self)
		try db.table(OrderParent.self).insert(OrderParent(id: 1, name: "a", children: nil))
		try db.table(OrderChild.self).insert(OrderChild(id: 1, parentId: ForeignKey(OrderParent.self, onDelete: setNull, onUpdate: restrict, wrappedValue: 1)))
		try db.table(OrderParent.self).where(\OrderParent.id == 1).delete()
		let child = try #require(try db.table(OrderChild.self).where(\OrderChild.id == 1).first())
		#expect(child.parentId == nil)

		try db.create(OrderParent.self, policy: .reconcileTable)
		try db.create(OrderParent.self, policy: .dropTable)
		#expect(try db.table(OrderChild.self).count() == 0)
		#expect(try foreignKeys(db, "orderchild") == ["parentid -> orderparent"])
	}

	@Test(.enabled(if: ProcessInfo.processInfo.environment["PG_TESTS"] == "1"))
	func createParentReferencingASubTable() throws {
		let db = try freshDatabase()
		defer { dropDatabase() }

		try db.create(OrderOwner.self)
		#expect(try foreignKeys(db, "orderowner") == ["favoritepetid -> orderpet"])
	}

	@Test(.enabled(if: ProcessInfo.processInfo.environment["PG_TESTS"] == "1"))
	func dropTableKeepsTheParentsConstraintOnASubTable() throws {
		let db = try freshDatabase()
		defer { dropDatabase() }

		// Tables made one at a time, so this doesn't depend on create()'s own order.
		try db.create(OrderPet.self)
		try db.create(OrderOwner.self, policy: .shallow)
		#expect(try foreignKeys(db, "orderowner") == ["favoritepetid -> orderpet"])
		// Recreating the tables must not lose the owner's constraint: dropping the pet table
		// with CASCADE after the owner's table was recreated took it away.
		try db.create(OrderOwner.self, policy: .dropTable)
		#expect(try foreignKeys(db, "orderowner") == ["favoritepetid -> orderpet"])
	}

	@Test(.enabled(if: ProcessInfo.processInfo.environment["PG_TESTS"] == "1"))
	func reconcileKeepsMixedCaseColumns() throws {
		let db = try freshDatabase()
		defer { dropDatabase() }

		try db.create(ReconcileRow.self)
		try db.table(ReconcileRow.self).insert(ReconcileRow(id: 1, camelName: "kept"))
		try db.sql("ALTER TABLE reconcilerow ADD COLUMN oldcolumn bigint")
		try db.create(ReconcileRow.self, policy: .reconcileTable)

		let row = try #require(try db.table(ReconcileRow.self).where(\ReconcileRow.id == 1).first())
		#expect(row.camelName == "kept")
		let columns = try db.sql("SELECT column_name FROM information_schema.columns WHERE table_name = 'reconcilerow' ORDER BY ordinal_position", ColumnName.self).map(\.column_name)
		#expect(columns == ["id", "camelname"])
	}

	@Test(.enabled(if: ProcessInfo.processInfo.environment["PG_TESTS"] == "1"))
	func reconcileIgnoresATableOfTheSameNameInAnotherSchema() throws {
		let db = try freshDatabase()
		defer { dropDatabase() }

		try db.create(ReconcileRow.self)
		try db.table(ReconcileRow.self).insert(ReconcileRow(id: 1, camelName: "kept"))
		try db.sql("CREATE SCHEMA other")
		try db.sql("CREATE TABLE other.reconcilerow (id bigint, camelname text, unrelated text)")
		try db.create(ReconcileRow.self, policy: .reconcileTable)

		let row = try #require(try db.table(ReconcileRow.self).where(\ReconcileRow.id == 1).first())
		#expect(row.camelName == "kept")
		let other = try db.sql("SELECT column_name FROM information_schema.columns WHERE table_schema = 'other' AND table_name = 'reconcilerow' ORDER BY ordinal_position", ColumnName.self).map(\.column_name)
		#expect(other == ["id", "camelname", "unrelated"])
	}
}
