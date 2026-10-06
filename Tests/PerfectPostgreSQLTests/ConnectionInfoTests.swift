import Foundation
import Testing
import libpq
@testable import PerfectPostgreSQL

// PostgresDatabaseConfiguration(database:host:port:username:password:) builds a libpq conninfo
// string. Every value must come back out of libpq's parser unchanged, and must not be able to
// add options of its own.
@Suite struct ConnectionInfoTests {
	// Parses with libpq itself (no server needed) and returns the options that were set.
	private func parse(_ conninfo: String) throws -> [String: String] {
		var errmsg: UnsafeMutablePointer<CChar>?
		guard let options = PQconninfoParse(conninfo, &errmsg) else {
			let message = errmsg.map { String(cString: $0) } ?? "unknown error"
			PQfreemem(errmsg)
			throw PostgresCRUDError("conninfo didn't parse: \(message)")
		}
		defer { PQconninfoFree(options) }
		var ret: [String: String] = [:]
		var option = options
		while let keyword = option.pointee.keyword {
			if let value = option.pointee.val {
				ret[String(cString: keyword)] = String(cString: value)
			}
			option += 1
		}
		return ret
	}

	@Test func plainValues() throws {
		let info = PostgresDatabaseConfiguration.connectionInfo(
			database: "db", host: "localhost", port: 5432, username: "me", password: "secret")
		#expect(try parse(info) == ["host": "localhost", "dbname": "db", "port": "5432", "user": "me", "password": "secret"])
	}

	@Test func valuesWithSpacesQuotesAndBackslashes() throws {
		let password = #"a b'c\d "e" \'"#
		let info = PostgresDatabaseConfiguration.connectionInfo(
			database: "my db", host: "localhost", port: nil, username: #"o'brien"#, password: password)
		#expect(try parse(info) == ["host": "localhost", "dbname": "my db", "user": #"o'brien"#, "password": password])
	}

	@Test func valueCannotAddAnOption() throws {
		let info = PostgresDatabaseConfiguration.connectionInfo(
			database: "db", host: "localhost", port: nil, username: nil, password: "x sslmode=disable")
		let options = try parse(info)
		#expect(options["password"] == "x sslmode=disable")
		#expect(options["sslmode"] == nil)
	}

	@Test func emptyValues() throws {
		let info = PostgresDatabaseConfiguration.connectionInfo(
			database: "", host: "", port: nil, username: "", password: "")
		#expect(try parse(info) == ["host": "", "dbname": "", "user": "", "password": ""])
	}

	// With trust authentication (the test server's), the password is ignored once parsed; before
	// the fix, this one made the conninfo unparseable and the connection failed.
	@Test(.enabled(if: ProcessInfo.processInfo.environment["PG_TESTS"] == "1"))
	func connectsWithAPasswordContainingASpace() throws {
		let config = try PostgresDatabaseConfiguration(
			database: "postgres", host: "localhost",
			port: ProcessInfo.processInfo.environment["PGPORT"].flatMap { Int($0) },
			username: ProcessInfo.processInfo.environment["PGUSER"],
			password: "two words")
		#expect(config.connection.status() == .ok)
	}
}
