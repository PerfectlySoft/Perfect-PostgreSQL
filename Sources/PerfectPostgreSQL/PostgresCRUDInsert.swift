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
private func _insert<OverAllForm: Codable, FromTableType: TableProtocol>(fromTable ft: FromTableType,
																		 instances: [OverAllForm],
																		 includeKeys: [PartialKeyPath<OverAllForm>],
																		 excludeKeys: [PartialKeyPath<OverAllForm>]) throws -> [OverAllForm] {
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
	RETURNING *
	"""
	CRUDLogging.log(.query, sqlStr)
	let exeDelegate = PostgresExeDelegate(connection: databaseConfiguration.connection, sql: sqlStr)
	try exeDelegate.bind(delegate.bindings)
	guard try exeDelegate.hasNext() else {
		throw CRUDSQLGenError("Did not get return value from statement \(sqlStr).")
	}
	var ret: [OverAllForm] = []
	ret.append(try OverAllForm(from: CRUDRowDecoder<ColumnKey>(delegate: exeDelegate)))
	for instance in instances[1...] {
		exeDelegate.resetResults()
		let delegate = databaseConfiguration.sqlGenDelegate
		let encoder = try CRUDBindingsEncoder(delegate: delegate)
		try instance.encode(to: encoder)
		_ = try encoder.completedBindings(allKeys: includeNames, ignoreKeys: Set(excludeNames))
		try exeDelegate.bind(delegate.bindings)
		guard try exeDelegate.hasNext() else {
			throw CRUDSQLGenError("Did not get return value from statement \(sqlStr).")
		}
		ret.append(try OverAllForm(from: CRUDRowDecoder<ColumnKey>(delegate: exeDelegate)))
	}
	return ret
}

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
	let sqlStr: String
	if columnNames.isEmpty {
		sqlStr = "INSERT INTO \(nameQ) DEFAULT VALUES RETURNING \(nameQ).\(try delegate.quote(identifier: returningName))"
	} else {
		sqlStr = """
		INSERT INTO \(nameQ) (\(columnNames.joined(separator: ", ")))
		VALUES (\(bindIdentifiers.joined(separator: ", ")))
		RETURNING \(nameQ).\(try delegate.quote(identifier: returningName))
		"""
	}
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
	/// Insert the instance and return the new column value.
	func returning<R: Decodable>(_ returning: KeyPath<OverAllForm, R>, insert instance: Form) throws -> R {
		return try _insert(fromTable: self, instances: [instance], returning: returning, includeKeys: [], excludeKeys: []).first!
	}
	/// Insert the instance and return the new column value.
	func returning<R: Decodable, Z: Decodable>(_ returning: KeyPath<OverAllForm, R>, insert instance: Form,
											setKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> R {
		return try _insert(fromTable: self, instances: [instance], returning: returning, includeKeys: [setKeys] + rest, excludeKeys: []).first!
	}
	/// Insert the instance and return the new column value.
	func returning<R: Decodable, Z: Decodable>(_ returning: KeyPath<OverAllForm, R>, insert instance: Form,
											ignoreKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> R {
		return try _insert(fromTable: self, instances: [instance], returning: returning, includeKeys: [], excludeKeys: [ignoreKeys] + rest).first!
	}
	/// Insert the instances and return the new column values.
	/// Guarantees that insert.count == returnValue.count.
	func returning<R: Decodable>(_ returning: KeyPath<OverAllForm, R>, insert instances: [Form]) throws -> [R] {
		return try _insert(fromTable: self, instances: instances, returning: returning, includeKeys: [], excludeKeys: [])
	}
	/// Insert the instances and return the new column values.
	/// Guarantees that insert.count == returnValue.count.
	func returning<R: Decodable, Z: Decodable>(_ returning: KeyPath<OverAllForm, R>, insert instances: [Form],
											setKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [R] {
		return try _insert(fromTable: self, instances: instances, returning: returning, includeKeys: [setKeys] + rest, excludeKeys: [])
	}
	/// Insert the instances and return the new column values.
	/// Guarantees that insert.count == returnValue.count.
	func returning<R: Decodable, Z: Decodable>(_ returning: KeyPath<OverAllForm, R>, insert instances: [Form],
											ignoreKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [R] {
		return try _insert(fromTable: self, instances: instances, returning: returning, includeKeys: [], excludeKeys: [ignoreKeys] + rest)
	}
}

public extension Table where C.Configuration == PostgresDatabaseConfiguration {
	/// Insert the instance and return the new object value.
	func returning(insert instance: Form) throws -> OverAllForm {
		return try _insert(fromTable: self, instances: [instance], includeKeys: [], excludeKeys: []).first!
	}
	/// Insert the instance and return the new object value.
	func returning<Z: Decodable>(insert instance: Form,
								 setKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> OverAllForm {
		return try _insert(fromTable: self, instances: [instance], includeKeys: [setKeys] + rest, excludeKeys: []).first!
	}
	/// Insert the instance and return the new object value.
	func returning<Z: Decodable>(insert instance: Form,
								 ignoreKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> OverAllForm {
		return try _insert(fromTable: self, instances: [instance], includeKeys: [], excludeKeys: [ignoreKeys] + rest).first!
	}
	/// Insert the instances and return the new object values.
	/// Guarantees that insert.count == returnValue.count.
	func returning(insert instances: [Form]) throws -> [OverAllForm] {
		return try _insert(fromTable: self, instances: instances, includeKeys: [], excludeKeys: [])
	}
	/// Insert the instances and return the new object values.
	/// Guarantees that insert.count == returnValue.count.
	func returning<Z: Decodable>(insert instances: [Form],
											   setKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [OverAllForm] {
		return try _insert(fromTable: self, instances: instances, includeKeys: [setKeys] + rest, excludeKeys: [])
	}
	/// Insert the instances and return the new object values.
	/// Guarantees that insert.count == returnValue.count.
	func returning<Z: Decodable>(insert instances: [Form],
											   ignoreKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [OverAllForm] {
		return try _insert(fromTable: self, instances: instances, includeKeys: [], excludeKeys: [ignoreKeys] + rest)
	}
}
