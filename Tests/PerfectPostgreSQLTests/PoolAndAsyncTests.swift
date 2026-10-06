import Foundation
import Testing
import PerfectCRUD
@testable import PerfectPostgreSQL

// Perfect-CRUD's pool and async entry points need a Sendable configuration. These would not
// compile if PostgresDatabaseConfiguration weren't Sendable.
// Pooled connections are separate sessions, so this uses a real table in `dbname=postgres`
// with a name no other suite uses.
@Suite(.enabled(if: ProcessInfo.processInfo.environment["PG_TESTS"] == "1"), .serialized)
struct PoolAndAsyncTests {
	struct PoolItem: Codable, Sendable, TableNameProvider {
		static let tableName = "pool_async_item"
		let id: Int
		let name: String
	}

	private func makePool(max: Int) throws -> DatabaseConnectionPool<PostgresDatabaseConfiguration> {
		try DatabaseConnectionPool(configuration: .init(minConnections: 1, maxConnections: max)) {
			try PostgresDatabaseConfiguration(postgresInitConnInfo)
		}
	}

	@Test func poolTransactionAsyncRoundTrip() async throws {
		let pool = try makePool(max: 3)
		try await pool.prewarm()
		try await pool.withConnection { db in
			try db.create(PoolItem.self, policy: .dropTable)
			let table = db.table(PoolItem.self)
			try await db.transactionAsync {
				try table.insert(PoolItem(id: 1, name: "one"))
				try table.insert(PoolItem(id: 2, name: "two"))
			}
			try await table.insertAsync(PoolItem(id: 3, name: "three"))
			let all = try await table.order(by: \.id).fetchAll()
			#expect(all.map(\.id) == [1, 2, 3])
			let two = try await table.where(\PoolItem.id == 2).firstAsync()
			#expect(two?.name == "two")
			// A failing transaction rolls back.
			struct Boom: Error {}
			await #expect(throws: Boom.self) {
				try await db.transactionAsync {
					try table.insert(PoolItem(id: 4, name: "four"))
					throw Boom()
				}
			}
			#expect(try table.count() == 3)
		}
		// Many tasks share the pool; each gets its own connection.
		try await withThrowingTaskGroup(of: Void.self) { group in
			for id in 10..<30 {
				group.addTask {
					try await pool.withConnection { db in
						try await db.table(PoolItem.self).insertAsync(PoolItem(id: id, name: "n\(id)"))
					}
				}
			}
			try await group.waitForAll()
		}
		#expect(await pool.currentSize <= 3)
		let count = try await pool.withConnection { db in
			let n = try db.table(PoolItem.self).count()
			try db.sql("DROP TABLE pool_async_item")
			return n
		}
		#expect(count == 23)
	}
}
