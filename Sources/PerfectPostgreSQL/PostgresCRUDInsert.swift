//
//  PostgresCRUDInsert.swift
//  PerfectPostgreSQL
//
//  Created by Kyle Jessup on 2019-01-29.
//

import Foundation
import PerfectCRUD

// Promises that instances.count == returnValue.count on exit
// otherwise it will throw
private func _insert<OverAllForm: Codable, FromTableType: TableProtocol, R: Decodable>(fromTable ft: FromTableType,
								   instances: [OverAllForm],
								   returning: KeyPath<OverAllForm, R>,
								   includeKeys: [PartialKeyPath<OverAllForm>],
								   excludeKeys: [PartialKeyPath<OverAllForm>]) throws -> [R] {
	typealias OAF = OverAllForm
	let delegate = ft.databaseConfiguration.sqlGenDelegate
	var state = SQLGenState(delegate: delegate)
	state.command = .insert
	try ft.setState(state: &state)
	let td = state.tableData[0]
	let kpDecoder = td.keyPathDecoder
	guard let kpInstance = td.modelInstance else {
		throw CRUDSQLGenError("Could not get model instance for key path decoder \(OAF.self)")
	}
	guard let returningName = try kpDecoder.getKeyPathName(kpInstance, keyPath: returning) else {
		throw CRUDSQLGenError("Could not get column name for `returning` key path \(returning).")
	}
	guard let databaseConfiguration = ft.databaseConfiguration as? PostgresDatabaseConfiguration else {
		throw CRUDSQLGenError("This is for Postgres only.")
	}
	let includeNames: [String]
	if includeKeys.isEmpty {
		let columnDecoder = CRUDColumnNameDecoder()
		_ = try OverAllForm.init(from: columnDecoder)
		includeNames = columnDecoder.collectedKeys.map { $0.name }
	} else {
		includeNames = try includeKeys.map {
			guard let n = try kpDecoder.getKeyPathName(kpInstance, keyPath: $0) else {
				throw CRUDSQLGenError("Could not get key path name for \(OAF.self) \($0)")
			}
			return n
		}
	}
	let excludeNames: [String] = try excludeKeys.map {
		guard let n = try kpDecoder.getKeyPathName(kpInstance, keyPath: $0) else {
			throw CRUDSQLGenError("Could not get key path name for \(OAF.self) \($0)")
		}
		return n
	}
	
	let encoder = try CRUDBindingsEncoder(delegate: delegate)
	try instances[0].encode(to: encoder)
	
	let bindings = try encoder.completedBindings(allKeys: includeNames, ignoreKeys: Set(excludeNames))
	let columnNames = try bindings.map { try delegate.quote(identifier: $0.column) }
	let bindIdentifiers = bindings.map { $0.identifier }
	
	let nameQ = try delegate.quote(identifier: "\(OAF.CRUDTableName)")
	let sqlStr = """
	INSERT INTO \(nameQ) (\(columnNames.joined(separator: ", ")))
		VALUES (\(bindIdentifiers.joined(separator: ", ")))
		RETURNING \(nameQ).\(try delegate.quote(identifier: returningName))
	"""
	CRUDLogging.log(.query, sqlStr)
	let exeDelegate = PostgresExeDelegate(connection: databaseConfiguration.connection, sql: sqlStr)
	try exeDelegate.bind(delegate.bindings)
	guard try exeDelegate.hasNext(), let next: KeyedDecodingContainer<ColumnKey> = try exeDelegate.next() else {
		throw CRUDSQLGenError("Did not get return value from statement \(sqlStr).")
	}
	var ret: [R] = []
	let value = try next.decode(R.self, forKey: ColumnKey(stringValue: returningName)!)
	ret.append(value)	
	for instance in instances[1...] {
		exeDelegate.resetResults()
		let delegate = databaseConfiguration.sqlGenDelegate
		let encoder = try CRUDBindingsEncoder(delegate: delegate)
		try instance.encode(to: encoder)
		_ = try encoder.completedBindings(allKeys: includeNames, ignoreKeys: Set(excludeNames))
		try exeDelegate.bind(delegate.bindings)
		guard try exeDelegate.hasNext(), let next: KeyedDecodingContainer<ColumnKey> = try exeDelegate.next() else {
			throw CRUDSQLGenError("Did not get return value from statement \(sqlStr).")
		}
		let value = try next.decode(R.self, forKey: ColumnKey(stringValue: returningName)!)
		ret.append(value)
	}
	return ret
}

public extension Table where C.Configuration == PostgresDatabaseConfiguration {
	func insert<R: Decodable>(_ instance: Form, returning: KeyPath<OverAllForm, R>) throws -> R {
		return try _insert(fromTable: self, instances: [instance], returning: returning, includeKeys: [], excludeKeys: []).first!
	}
	func insert<R: Decodable>(_ instance: Form, returning: KeyPath<OverAllForm, R>,
							  setKeys: PartialKeyPath<OverAllForm>, _ rest: PartialKeyPath<OverAllForm>...) throws -> R {
		return try _insert(fromTable: self, instances: [instance], returning: returning, includeKeys: [setKeys] + rest, excludeKeys: []).first!
	}
	func insert<R: Decodable>(_ instance: Form, returning: KeyPath<OverAllForm, R>,
							  ignoreKeys: PartialKeyPath<OverAllForm>, _ rest: PartialKeyPath<OverAllForm>...) throws -> R {
		return try _insert(fromTable: self, instances: [instance], returning: returning, includeKeys: [], excludeKeys: [ignoreKeys] + rest).first!
	}
	
	func insert<R: Decodable>(_ instances: [Form], returning: KeyPath<OverAllForm, R>) throws -> [R] {
		return try _insert(fromTable: self, instances: instances, returning: returning, includeKeys: [], excludeKeys: [])
	}
	func insert<R: Decodable>(_ instances: [Form], returning: KeyPath<OverAllForm, R>,
							  setKeys: PartialKeyPath<OverAllForm>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [R] {
		return try _insert(fromTable: self, instances: instances, returning: returning, includeKeys: [setKeys] + rest, excludeKeys: [])
	}
	func insert<R: Decodable>(_ instances: [Form], returning: KeyPath<OverAllForm, R>,
							  ignoreKeys: PartialKeyPath<OverAllForm>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [R] {
		return try _insert(fromTable: self, instances: instances, returning: returning, includeKeys: [], excludeKeys: [ignoreKeys] + rest)
	}
}
