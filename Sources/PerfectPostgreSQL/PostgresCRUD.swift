//
//  PostgresCRUD.swift
//  PerfectCRUD
//
//  Created by Kyle Jessup on 2017-12-04.
//

import Foundation
import PerfectCRUD

public struct PostgresCRUDError: Error, CustomStringConvertible {
	public let description: String
	public init(_ msg: String) {
		description = msg
		CRUDLogging.log(.error, msg)
	}
}

extension PGResult {
	public func getFieldBlobUInt8(tupleIndex: Int, fieldIndex: Int) -> [UInt8]? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		let sc = s.utf8
		guard sc.count % 2 == 0, sc.count >= 2, s[s.startIndex] == "\\", s[s.index(after: s.startIndex)] == "x" else {
			return nil
		}
		var ret = [UInt8]()
		var index = sc.index(sc.startIndex, offsetBy: 2)
		while index != sc.endIndex {
			let c1 = UInt8(sc[index])
			index = sc.index(after: index)
			let c2 = UInt8(sc[index])
			guard let byte = byteFromHexDigits(one: c1, two: c2) else {
				return nil
			}
			ret.append(byte)
			index = sc.index(after: index)
		}
		return ret
	}

	private func byteFromHexDigits(one c1v: UInt8, two c2v: UInt8) -> UInt8? {
		let capA: UInt8 = 65
		let capF: UInt8 = 70
		let lowA: UInt8 = 97
		let lowF: UInt8 = 102
		let zero: UInt8 = 48
		let nine: UInt8 = 57
		var newChar = UInt8(0)
		if c1v >= capA && c1v <= capF {
			newChar = c1v - capA + 10
		} else if c1v >= lowA && c1v <= lowF {
			newChar = c1v - lowA + 10
		} else if c1v >= zero && c1v <= nine {
			newChar = c1v - zero
		} else {
			return nil
		}
		newChar *= 16
		if c2v >= capA && c2v <= capF {
			newChar += c2v - capA + 10
		} else if c2v >= lowA && c2v <= lowF {
			newChar += c2v - lowA + 10
		} else if c2v >= zero && c2v <= nine {
			newChar += c2v - zero
		} else {
			return nil
		}
		return newChar
	}
}

class PostgresCRUDRowReader<K: CodingKey>: KeyedDecodingContainerProtocol {
	typealias Key = K
	var codingPath: [CodingKey] = []
	var allKeys: [Key] = []
	let results: PGResult
	let tupleIndex: Int
	let fieldNames: [String: Int]
	init(results r: PGResult, tupleIndex ti: Int, fieldNames fn: [String: Int]) {
		results = r
		tupleIndex = ti
		fieldNames = fn
	}
	func ensureIndex(forKey key: Key) throws -> Int {
		guard let index = fieldNames[key.stringValue.lowercased()] else {
			throw PostgresCRUDError("No index for column \(key.stringValue)")
		}
		return index
	}
	func contains(_ key: Key) -> Bool {
		return fieldNames[key.stringValue.lowercased()] != nil
	}
	func decodeNil(forKey key: Key) throws -> Bool {
		return results.fieldIsNull(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key))
	}
	func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
		return results.getFieldBool(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? false
	}
	func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
		return results.getFieldInt(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? 0
	}
	func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
		return results.getFieldInt8(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? 0
	}
	func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
		return results.getFieldInt16(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? 0
	}
	func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
		return results.getFieldInt32(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? 0
	}
	func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
		return results.getFieldInt64(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? 0
	}
	func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
		return results.getFieldUInt(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? 0
	}
	func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
		return results.getFieldUInt8(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? 0
	}
	func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
		return results.getFieldUInt16(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? 0
	}
	func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
		return results.getFieldUInt32(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? 0
	}
	func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
		return results.getFieldUInt64(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? 0
	}
	func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
		return results.getFieldFloat(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? 0
	}
	func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
		return results.getFieldDouble(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? 0
	}
	func decode(_ type: String.Type, forKey key: Key) throws -> String {
		return results.getFieldString(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) ?? ""
	}
	func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T: Decodable {
		let index = try ensureIndex(forKey: key)
		guard let special = SpecialType(type) else {
			throw CRUDDecoderError("Unsupported type: \(type) for key: \(key.stringValue)")
		}
		switch special {
		case .uint8Array:
			let ret: [UInt8] = results.getFieldBlobUInt8(tupleIndex: tupleIndex, fieldIndex: index) ?? []
			return ret as! T
		case .int8Array:
			let ret: [Int8] = results.getFieldBlob(tupleIndex: tupleIndex, fieldIndex: index) ?? []
			return ret as! T
		case .data:
			let bytes: [UInt8] = results.getFieldBlobUInt8(tupleIndex: tupleIndex, fieldIndex: index) ?? []
			return Data(bytes) as! T
		case .uuid:
			let str = results.getFieldString(tupleIndex: tupleIndex, fieldIndex: index) ?? ""
			guard let ret = UUID(uuidString: str) else {
				throw CRUDDecoderError("Invalid UUID string \(str).")
			}
			return ret as! T
		case .date:
			let str = results.getFieldString(tupleIndex: tupleIndex, fieldIndex: index) ?? ""
			guard let date = Date(fromISO8601: str) else {
				throw CRUDDecoderError("Invalid Date string \(str).")
			}
			return date as! T
		case .url:
			let str = results.getFieldString(tupleIndex: tupleIndex, fieldIndex: index) ?? ""
			guard let url = URL(string: str) else {
				throw CRUDDecoderError("Invalid URL string \(str).")
			}
			return url as! T
		case .codable:
			guard let data0 = results.getFieldString(tupleIndex: tupleIndex, fieldIndex: try ensureIndex(forKey: key)) else {
				throw CRUDDecoderError("Unsupported type: \(type) for key: \(key.stringValue)")
			}
			if type == String.self {
				return data0 as! T
			}
			let container = data0.count >= 1 && (data0[data0.startIndex] == "[" || data0[data0.startIndex] == "{")
			guard let data = data0.data(using: .utf8) else {
				throw CRUDDecoderError("Invalid data for type: \(type) for key: \(key.stringValue)")
			}
			if container {
				return try JSONDecoder().decode(type, from: data)
			}
			guard let obj = try JSONSerialization.jsonObject(with: data, options: .allowFragments) as? T else {
				throw CRUDDecoderError("Invalid data for type: \(type) for key: \(key.stringValue)")
			}
			return obj
		case .wrapped:
			let decoder = CRUDColumnValueDecoder(source: KeyedDecodingContainer(self), key: key)
			return try T(from: decoder)
		}
	}
	func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
		throw CRUDDecoderError("Unimplimented nestedContainer")
	}
	func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
		throw CRUDDecoderError("Unimplimented nestedUnkeyedContainer")
	}
	func superDecoder() throws -> Decoder {
		throw CRUDDecoderError("Unimplimented superDecoder")
	}
	func superDecoder(forKey key: Key) throws -> Decoder {
		throw CRUDDecoderError("Unimplimented superDecoder")
	}
}

struct PostgresColumnInfo: Codable {
	let column_name: String
	let data_type: String
}

class PostgresGenDelegate: SQLGenDelegate {
	let connection: PGConnection
	var parentTableStack: [TableStructure] = []
	var bindings: Bindings = []

	init(connection c: PGConnection) {
		connection = c
	}
	func getBinding(for expr: Expression) throws -> String {
		let id = "$\(bindings.count+1)"
		bindings.append((id, expr))
		return id
	}
	func quote(identifier: String) throws -> String {
		return "\"\(identifier.lowercased())\""
	}
	func getCreateTableSQL(forTable: TableStructure, policy: TableCreatePolicy) throws -> [String] {
		parentTableStack.append(forTable)
		defer {
			parentTableStack.removeLast()
		}
		var sub: [String] = []
		if policy.contains(.dropTable) {
			sub += ["DROP TABLE IF EXISTS \(try quote(identifier: forTable.tableName)) CASCADE"]
		}
		if !policy.contains(.dropTable),
			policy.contains(.reconcileTable),
			let existingColumns = getExistingColumnData(forTable: forTable.tableName) {
			let existingColumnMap: [String: PostgresColumnInfo] = .init(uniqueKeysWithValues: existingColumns.map { ($0.column_name, $0) })
			let newColumnMap: [String: TableStructure.Column] = .init(uniqueKeysWithValues: forTable.columns.map { ($0.name.lowercased(), $0) })

			let addColumns = newColumnMap.keys.filter { existingColumnMap[$0] == nil }
			let removeColumns: [String] = existingColumnMap.keys.filter { newColumnMap[$0] == nil }

			sub += try removeColumns.map {
				return """
				ALTER TABLE \(try quote(identifier: forTable.tableName)) DROP COLUMN \(try quote(identifier: $0))
				"""
			}
			sub += try addColumns.compactMap { newColumnMap[$0] }.map {
				let nameType = try getColumnDefinition($0)
				return """
				ALTER TABLE \(try quote(identifier: forTable.tableName)) ADD COLUMN \(nameType)
				"""
			}
			return sub
		} else {
			sub += [
				"""
				CREATE TABLE IF NOT EXISTS \(try quote(identifier: forTable.tableName)) (
				\(try forTable.columns.map { try getColumnDefinition($0) }.joined(separator: ",\n\t"))
				)
				"""]
		}
		if !policy.contains(.shallow) {
			sub += try forTable.subTables.flatMap {
				try getCreateTableSQL(forTable: $0, policy: policy)
			}
		}

		return sub
	}
	func getExistingColumnData(forTable: String) -> [PostgresColumnInfo]? {
		do {
			let statement =
				"""
				SELECT column_name, data_type
				FROM INFORMATION_SCHEMA.COLUMNS
				WHERE table_name = $1
				"""
			let exeDelegate = PostgresExeDelegate(connection: connection, sql: statement)
			exeDelegate.nextBindings = [("$1", .string(forTable.lowercased()))]
			var ret: [PostgresColumnInfo] = []
			while try exeDelegate.hasNext() {
				let rowDecoder: CRUDRowDecoder<ColumnKey> = CRUDRowDecoder(delegate: exeDelegate)
				ret.append(try PostgresColumnInfo(from: rowDecoder))
			}
			guard !ret.isEmpty else {
				return nil
			}
			return ret
		} catch {
			return nil
		}
	}
	private func getTypeName(_ type: Any.Type) throws -> String {
		let typeName: String
		switch type {
		case is Int.Type:
			typeName = "bigint"
		case is Int8.Type:
			typeName = "smallint"
		case is Int16.Type:
			typeName = "smallint"
		case is Int32.Type:
			typeName = "integer"
		case is Int64.Type:
			typeName = "bigint"
		case is UInt.Type:
			typeName = "bigint"
		case is UInt8.Type:
			typeName = "smallint"
		case is UInt16.Type:
			typeName = "integer"
		case is UInt32.Type:
			typeName = "bigint"
		case is UInt64.Type:
			typeName = "bigint"
		case is Double.Type:
			typeName = "double precision"
		case is Float.Type:
			typeName = "real"
		case is Bool.Type:
			typeName = "boolean"
		case is String.Type:
			typeName = "text"
		default:
			guard let special = SpecialType(type) else {
				throw PostgresCRUDError("Unsupported SQL column type \(type)")
			}
			switch special {
			case .uint8Array:
				typeName = "bytea"
			case .int8Array:
				typeName = "bytea"
			case .data:
				typeName = "bytea"
			case .uuid:
				typeName = "uuid"
			case .date:
				typeName = "timestamp with time zone"
			case .url:
				typeName = "text"
			case .codable:
				typeName = "jsonb"
			case .wrapped:
				guard let w = type as? WrappedCodableProvider.Type else {
					throw PostgresCRUDError("Unsupported SQL column type \(type)")
				}
				return try getTypeName(w)
			}
		}
		return typeName
	}
	func getColumnDefinition(_ column: TableStructure.Column) throws -> String {
		let name = column.name
		let type = column.type
		let typeName = try getTypeName(type)
		var addendum = ""
		if !column.properties.contains(.primaryKey) && !column.optional {
			addendum += " NOT NULL"
		}
		for prop in column.properties {
			switch prop {
			case .primaryKey:
				addendum += " PRIMARY KEY"
			case .foreignKey(let table, let column, let onDelete, let onUpdate):
				addendum += " REFERENCES \(try quote(identifier: table))(\(try quote(identifier: column)))"
				let scenarios = [(" ON DELETE ", onDelete), (" ON UPDATE ", onUpdate)]
				for (scenario, action) in scenarios {
					addendum += scenario
					switch action {
					case .ignore:
						addendum += "NO ACTION"
					case .restrict:
						addendum += "RESTRICT"
					case .setNull:
						addendum += "SET NULL"
					case .setDefault:
						addendum += "SET DEFAULT"
					case .cascade:
						addendum += "CASCADE"
					}
				}
			}
		}
		return "\(try quote(identifier: name)) \(typeName)\(addendum)"
	}
	func getCreateIndexSQL(forTable name: String, on columns: [String], unique: Bool) throws -> [String] {
		let stat =
		"""
		CREATE \(unique ? "UNIQUE " : "")INDEX IF NOT EXISTS \(try quote(identifier: "index_\(columns.joined(separator: "_"))"))
		ON \(try quote(identifier: name)) (\(try columns.map{try quote(identifier: $0)}.joined(separator: ",")))
		"""
		return [stat]
	}
}

class PostgresExeDelegate: SQLExeDelegate {
    func asyncExecute(completion: @escaping (SQLExeDelegate) -> ()) {
        completion(self)
    }

	var nextBindings: Bindings = []
	let connection: PGConnection
	let sql: String
	var results: PGResult?
	var tupleIndex = -1
	var numTuples = 0
	var fieldNames: [String: Int] = [:]
	init(connection c: PGConnection, sql s: String) {
		connection = c
		sql = s
	}
	func bind(_ bindings: Bindings, skip: Int) throws {
		results = nil
		if skip == 0 {
			nextBindings = bindings
		} else {
			nextBindings = nextBindings[0..<skip] + bindings
		}
	}

	func resetResults() {
		tupleIndex = -1
		numTuples = 0
		results = nil
	}

	func hasNext() throws -> Bool {
		tupleIndex += 1
		if nil == results {
			let r = try connection.exec(statement: sql,
										params: nextBindings.map {
											try bindOne(expr: $0.1) })
			results = r
			guard r.isValid() else {
				switch connection.status() {
				case .ok:
					throw CRUDSQLExeError("Fatal error on SQL execution")
				case .bad:
					throw CRUDSQLExeError(connection.errorMessage())
				}
			}
			let status = r.status()
			switch status {
			case .emptyQuery:
				return false
			case .commandOK, .tuplesOK, .singleTuple:
				numTuples = r.numTuples()
				for i in 0..<r.numFields() {
					guard let fieldName = r.fieldName(index: i) else {
						continue
					}
					fieldNames[fieldName] = i
				}
			case .badResponse, .fatalError:
				throw CRUDSQLExeError(r.errorMessage())
			case .nonFatalError:
				CRUDLogging.log(.warning, r.errorMessage())
			case .unknown:
				return false
			}
		}
		return tupleIndex < numTuples
	}

	func next<A>() throws -> KeyedDecodingContainer<A>? where A: CodingKey {
		guard let results = self.results else {
			return nil
		}
		let ret = KeyedDecodingContainer(PostgresCRUDRowReader<A>(results: results,
																  tupleIndex: tupleIndex,
																  fieldNames: fieldNames))
		return ret
	}

	private func bindOne(expr: CRUDExpression) throws -> Any? {
		switch expr {
		case .lazy(let e):
			return try bindOne(expr: e())
		case .integer(let i):
			return i
		case .uinteger(let i):
			return i
		case .integer64(let i):
			return i
		case .uinteger64(let i):
			return i
		case .integer32(let i):
			return i
		case .uinteger32(let i):
			return i
		case .integer16(let i):
			return i
		case .uinteger16(let i):
			return i
		case .integer8(let i):
			return i
		case .uinteger8(let i):
			return i
		case .decimal(let d):
			return d
		case .float(let f):
			return f
		case .string(let s):
			return s
		case .blob(let b):
			return b
		case .sblob(let b):
			return b
		case .bool(let b):
			return b
		case .date(let d):
			return d.iso8601()
		case .url(let u):
			return u.absoluteString
		case .uuid(let u):
			return u.uuidString
		case .null:
			return nil as String?
		case .column(_), .and(_, _), .or(_, _),
			 .equality(_, _), .inequality(_, _),
			 .not(_), .lessThan(_, _), .lessThanEqual(_, _),
			 .greaterThan(_, _), .greaterThanEqual(_, _),
			 .keyPath(_), .in(_, _), .like(_, _, _, _):
			throw PostgresCRUDError("Asked to bind unsupported expression type: \(expr)")
		}
	}
}

public struct PostgresDatabaseConfiguration: DatabaseConfigurationProtocol {
	let connection: PGConnection

	public init(url: String?,
				 name: String?,
				 host: String?,
				 port: Int?,
				 user: String?,
				 pass: String?) throws {
		if let connectionInfo = url {
			try self.init(connectionInfo)
		} else {
			guard let database = name, let host = host else {
				throw PostgresCRUDError("Database name and host must be provided.")
			}
			try self.init(database: database, host: host, port: port, username: user, password: pass)
		}
	}

	public init(database: String, host: String, port: Int? = nil, username: String? = nil, password: String? = nil) throws {
		var s = "host=\(host) dbname=\(database)"
		if let p = port {
			s += " port=\(p)"
		}
		if let u = username {
			s += " user=\(u)"
		}
		if let p = password {
			s += " password=\(p)"
		}
		try self.init(s)
	}
	public init(_ connectionInfo: String) throws {
		let con = PGConnection()
		guard case .ok = con.connectdb(connectionInfo) else {
			throw PostgresCRUDError("Could not connect. \(con.errorMessage())")
		}
		connection = con
	}
	public var sqlGenDelegate: SQLGenDelegate {
		return PostgresGenDelegate(connection: connection)
	}
	public func sqlExeDelegate(forSQL: String) throws -> SQLExeDelegate {
		return PostgresExeDelegate(connection: connection, sql: forSQL)
	}
}
