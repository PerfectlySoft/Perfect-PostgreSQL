import Foundation
import Testing
import PerfectCRUD
@testable import PerfectPostgreSQL

// Identifier quoting. The Dynamic API passes caller-supplied table and field
// names through quote(identifier:), so a `"` in a name must stay part of the
// identifier rather than end it. quote() also lowercases, matching Postgres's
// folding of unquoted names.
//
// In the serialized suite: getDB() drops and recreates the shared database.
extension PerfectPostgreSQLTests {
    private struct Count: Codable { let c: Int }
    private struct IdName: Codable { let id: Int; let name: String }

    // No server needed: quote() doesn't touch the connection.
    @Test func quoteIdentifierDoublesEmbeddedQuotes() throws {
        let gen = PostgresGenDelegate(connection: PGConnection())
        #expect(try gen.quote(identifier: "plain") == #""plain""#)
        #expect(try gen.quote(identifier: "MixedCase") == #""mixedcase""#)
        #expect(try gen.quote(identifier: #"A"b"#) == #""a""b""#)
        #expect(try gen.quote(identifier: #"""#) == #""""""#)
        #expect(try gen.quote(identifier: #"x" = "x" OR "x"#) == #""x"" = ""x"" or ""x""#)
        #expect(try gen.quote(identifier: "") == #""""#)
    }

    @Test func dynamicMutationWithQuoteInTableAndFieldNames() throws {
        guard pgEnabled else { return }
        let db = try getDB()
        try db.sql(#"CREATE TABLE "we""ird" ("id" INT PRIMARY KEY, "na""me" TEXT)"#)
        for (id, name) in [(1, "one"), (2, "two")] {
            _ = try db.mutate(DynamicMutation(
                action: .insert, table: #"We"ird"#,
                values: ["id": .int(Int64(id)), #"Na"me"#: .string(name)]))
        }
        _ = try db.mutate(DynamicMutation(
            action: .update, table: #"we"ird"#,
            values: [#"na"me"#: .string("uno")],
            predicates: [.init(field: #"na"me"#, comparison: .equal, value: .string("one"))]))
        _ = try db.mutate(DynamicMutation(
            action: .delete, table: #"we"ird"#,
            predicates: [.init(field: "id", comparison: .equal, value: .int(2))]))
        let rows = try db.sql(#"SELECT "id", "na""me" AS name FROM "we""ird""#, IdName.self)
        #expect(rows.map(\.id) == [1])
        #expect(rows.map(\.name) == ["uno"])
    }

    // On main each of these names closed the quoted identifier and added SQL,
    // turning a one-row DELETE/UPDATE into one that hits the whole table.
    @Test func dynamicNamesCannotInjectSQL() throws {
        guard pgEnabled else { return }
        let db = try getDB()
        try db.sql(#"CREATE TABLE "quote_victim" ("id" INT PRIMARY KEY, "name" TEXT)"#)
        try db.sql(#"INSERT INTO "quote_victim" ("id", "name") VALUES (1, 'a'), (2, 'b'), (3, 'c')"#)
        func count() throws -> Int {
            try db.sql(#"SELECT COUNT(*)::int AS c FROM "quote_victim""#, Count.self)[0].c
        }
        // Field name: `"id" = "id" or "id" = $1` would match every row.
        #expect(throws: (any Error).self) {
            try db.mutate(DynamicMutation(
                action: .delete, table: "quote_victim",
                predicates: [.init(field: #"id" = "id" OR "id"#, comparison: .equal, value: .int(1))]))
        }
        #expect(try count() == 3)
        // Table name: `DELETE FROM "quote_victim" --" WHERE ...` drops the WHERE.
        #expect(throws: (any Error).self) {
            try db.mutate(DynamicMutation(
                action: .delete, table: #"quote_victim" --"#,
                predicates: [.init(field: "id", comparison: .equal, value: .int(1))]))
        }
        #expect(try count() == 3)
        // Value key: `SET "id" = 99, "name" = $1` would rewrite a second column.
        #expect(throws: (any Error).self) {
            try db.mutate(DynamicMutation(
                action: .update, table: "quote_victim",
                values: [#"id" = 99, "name"#: .string("z")],
                predicates: [.init(field: "id", comparison: .equal, value: .int(1))]))
        }
        #expect(try db.sql(#"SELECT COUNT(*)::int AS c FROM "quote_victim" WHERE "id" = 99"#, Count.self)[0].c == 0)
        // The same table name works as a plain identifier when the table exists.
        try db.sql(#"CREATE TABLE "quote_victim"" --" ("id" INT PRIMARY KEY)"#)
        try db.sql(#"INSERT INTO "quote_victim"" --" ("id") VALUES (1), (2)"#)
        _ = try db.mutate(DynamicMutation(
            action: .delete, table: #"quote_victim" --"#,
            predicates: [.init(field: "id", comparison: .equal, value: .int(1))]))
        #expect(try db.sql(#"SELECT COUNT(*)::int AS c FROM "quote_victim"" --""#, Count.self)[0].c == 1)
        #expect(try count() == 3)
    }

    // The typed API (create, FK references, index names, reconcile) with a `"`
    // in table and column names and a keyword column name.
    struct QuoteParent: Codable, TableNameProvider {
        static let tableName = #"quote"parent"#
        @PrimaryKey var id: Int
        let order: String
        let children: [QuoteChild]?
        init(id: Int, order: String) { _id = .init(wrappedValue: id); self.order = order; children = nil }
    }
    struct QuoteChild: Codable, TableNameProvider {
        static let tableName = #"quote"child"#
        enum CodingKeys: String, CodingKey { case id, parentId = #"parent"id"#, note = #"no"te"# }
        @PrimaryKey var id: Int
        @ForeignKey(QuoteParent.self, onDelete: cascade, onUpdate: cascade) var parentId: Int
        let note: String
        init(id: Int, parentId: Int, note: String) {
            _id = .init(wrappedValue: id)
            _parentId = .init(QuoteParent.self, onDelete: cascade, onUpdate: cascade, wrappedValue: parentId)
            self.note = note
        }
    }
    struct QuoteChildSlim: Codable, TableNameProvider {
        static let tableName = #"quote"child"#
        enum CodingKeys: String, CodingKey { case id, parentId = #"parent"id"# }
        let id: Int
        let parentId: Int
    }

    @Test func schemaWithQuoteAndKeywordNames() throws {
        guard pgEnabled else { return }
        let db = try getDB()
        try db.create(QuoteParent.self, policy: .dropTable)
        try db.table(QuoteChild.self).index(\.note)
        try db.table(QuoteParent.self).insert(QuoteParent(id: 1, order: "first"))
        try db.table(QuoteChild.self).insert([
            QuoteChild(id: 10, parentId: 1, note: "a"),
            QuoteChild(id: 11, parentId: 1, note: "b")])
        let joined = try db.table(QuoteParent.self)
            .join(\.children, on: \.id, equals: \.parentId)
            .where(\QuoteParent.id == 1).select().map { $0 }
        #expect(joined.map(\.order) == ["first"])
        #expect(joined.first?.children?.map(\.note).sorted() == ["a", "b"])
        try db.table(QuoteParent.self).where(\QuoteParent.id == 1).delete()
        #expect(try db.table(QuoteChild.self).count() == 0)

        try db.table(QuoteParent.self).insert(QuoteParent(id: 2, order: "second"))
        try db.table(QuoteChild.self).insert(QuoteChild(id: 20, parentId: 2, note: "c"))
        try db.create(QuoteChildSlim.self, policy: .reconcileTable)
        let slim = try db.table(QuoteChildSlim.self).select().map { $0 }
        #expect(slim.map(\.id) == [20])
        #expect(slim.map(\.parentId) == [2])
        let columns = try db.sql(
            "SELECT COUNT(*)::int AS c FROM information_schema.columns WHERE table_name = $1",
            bindings: [("$1", .string(#"quote"child"#))], Count.self)[0].c
        #expect(columns == 2)
    }
}
