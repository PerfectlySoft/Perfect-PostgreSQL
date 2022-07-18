//
//  PostgresCRUDUpdate.swift
//  PerfectPostgreSQL
//
//  Created by Kyle Jessup on 2019-01-30.
//

import Foundation
import PerfectCRUD

// Promises that instances.count == returnValue.count on exit
// otherwise it will throw
private func _update<OverAllForm: Codable, FromTableType: TableProtocol>(fromTable ft: FromTableType,
																		 instance: OverAllForm,
																		 includeKeys: [PartialKeyPath<OverAllForm>],
																		 excludeKeys: [PartialKeyPath<OverAllForm>]) throws -> [OverAllForm] {
	typealias OAF = OverAllForm
	let delegate = ft.databaseConfiguration.sqlGenDelegate
	var state = SQLGenState(delegate: delegate)
	state.command = .update
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
	try instance.encode(to: encoder)
	state.bindingsEncoder = encoder
	state.columnFilters = (include: includeNames, exclude: excludeNames)
	try ft.setSQL(state: &state)
	var ret: [OverAllForm] = []
	if let stat = state.statements.first { // multi statements?!
		let sql = stat.sql + " RETURNING *"
		let exeDelegate = try databaseConfiguration.sqlExeDelegate(forSQL: sql)
		try exeDelegate.bind(stat.bindings)
		while try exeDelegate.hasNext() {
			ret.append(try OverAllForm(from: CRUDRowDecoder<ColumnKey>(delegate: exeDelegate)))
		}
	}
	return ret
}

// Promises that instances.count == returnValue.count on exit
// otherwise it will throw
private func _update<OverAllForm: Codable, FromTableType: TableProtocol, R: Decodable>(fromTable ft: FromTableType,
																					   instance: OverAllForm,
																					   returning: KeyPath<OverAllForm, R>,
																					   includeKeys: [PartialKeyPath<OverAllForm>],
																					   excludeKeys: [PartialKeyPath<OverAllForm>]) throws -> [R] {
	typealias OAF = OverAllForm
	let delegate = ft.databaseConfiguration.sqlGenDelegate
	var state = SQLGenState(delegate: delegate)
	state.command = .update
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
	try instance.encode(to: encoder)
	state.bindingsEncoder = encoder
	state.columnFilters = (include: includeNames, exclude: excludeNames)
	try ft.setSQL(state: &state)
	var ret: [R] = []
	if let stat = state.statements.first { // multi statements?!
		let nameQ = try delegate.quote(identifier: "\(OAF.CRUDTableName)")
		let sql = stat.sql + " RETURNING \(nameQ).\(try delegate.quote(identifier: returningName))"
		let exeDelegate = try databaseConfiguration.sqlExeDelegate(forSQL: sql)
		try exeDelegate.bind(stat.bindings)
		while try exeDelegate.hasNext(), let next: KeyedDecodingContainer<ColumnKey> = try exeDelegate.next() {
			let value = try next.decode(R.self, forKey: ColumnKey(stringValue: returningName)!)
			ret.append(value)
		}
	}
	return ret
}

public extension Table where C.Configuration == PostgresDatabaseConfiguration {
	/// Update the instance and return the new column value.
	func returning<R: Decodable>(_ returning: KeyPath<OverAllForm, R>, update instance: Form) throws -> [R] {
		return try _update(fromTable: self, instance: instance, returning: returning, includeKeys: [], excludeKeys: [])
	}
	/// Update the instance and return the new column value.
	func returning<R: Decodable, Z: Decodable>(_ returning: KeyPath<OverAllForm, R>, update instance: Form,
											   setKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [R] {
		return try _update(fromTable: self, instance: instance, returning: returning, includeKeys: [setKeys] + rest, excludeKeys: [])
	}
	/// Update the instance and return the new column value.
	func returning<R: Decodable, Z: Decodable>(_ returning: KeyPath<OverAllForm, R>, update instance: Form,
											   ignoreKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [R] {
		return try _update(fromTable: self, instance: instance, returning: returning, includeKeys: [], excludeKeys: [ignoreKeys] + rest)
	}
	/// Update the instance and return the new object value.
	func returning(update instance: Form) throws -> [OverAllForm] {
		return try _update(fromTable: self, instance: instance, includeKeys: [], excludeKeys: [])
	}
	/// Update the instance and return the new object value.
	func returning<Z: Decodable>(update instance: Form,
								 setKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [OverAllForm] {
		return try _update(fromTable: self, instance: instance, includeKeys: [setKeys] + rest, excludeKeys: [])
	}
	/// Update the instance and return the new object value.
	func returning<Z: Decodable>(update instance: Form,
								 ignoreKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [OverAllForm] {
		return try _update(fromTable: self, instance: instance, includeKeys: [], excludeKeys: [ignoreKeys] + rest)
	}
}

public extension Where where OverAllForm == FromTableType.Form {
	/// Update the instance and return the new column value.
	func returning<R: Decodable>(_ returning: KeyPath<OverAllForm, R>, update instance: Form) throws -> [R] {
		return try _update(fromTable: self, instance: instance, returning: returning, includeKeys: [], excludeKeys: [])
	}
	/// Update the instance and return the new column value.
	func returning<R: Decodable, Z: Decodable>(_ returning: KeyPath<OverAllForm, R>, update instance: Form,
											   setKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [R] {
		return try _update(fromTable: self, instance: instance, returning: returning, includeKeys: [setKeys] + rest, excludeKeys: [])
	}
	/// Update the instance and return the new column value.
	func returning<R: Decodable, Z: Decodable>(_ returning: KeyPath<OverAllForm, R>, update instance: Form,
											   ignoreKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [R] {
		return try _update(fromTable: self, instance: instance, returning: returning, includeKeys: [], excludeKeys: [ignoreKeys] + rest)
	}
	/// Update the instance and return the new object value.
	func returning(update instance: Form) throws -> [OverAllForm] {
		return try _update(fromTable: self, instance: instance, includeKeys: [], excludeKeys: [])
	}
	/// Update the instance and return the new object value.
	func returning<Z: Decodable>(update instance: Form,
								 setKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [OverAllForm] {
		return try _update(fromTable: self, instance: instance, includeKeys: [setKeys] + rest, excludeKeys: [])
	}
	/// Update the instance and return the new object value.
	func returning<Z: Decodable>(update instance: Form,
								 ignoreKeys: KeyPath<OverAllForm, Z>, _ rest: PartialKeyPath<OverAllForm>...) throws -> [OverAllForm] {
		return try _update(fromTable: self, instance: instance, includeKeys: [], excludeKeys: [ignoreKeys] + rest)
	}
}
