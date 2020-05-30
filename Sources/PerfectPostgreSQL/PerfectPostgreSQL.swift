//
//  PostgreSQL.swift
//  PostgreSQL
//
//  Created by Kyle Jessup on 2015-07-29.
//	Copyright (C) 2015 PerfectlySoft, Inc.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2015 - 2016 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import Foundation
import libpq

/// This enum type indicates an exception when dealing with a PostgreSQL database
public enum PostgreSQLError : Error {
	/// Error with detail message.
	case error(String)
}

/// result object
public final class PGResult {
	
	/// Result Status enum
	public enum StatusType {
		case emptyQuery
		case commandOK
		case tuplesOK
		case badResponse
		case nonFatalError
		case fatalError
		case singleTuple
		case unknown
	}
	
	private var res: OpaquePointer? = OpaquePointer(bitPattern: 0)
	private var borrowed = false
	
	init(_ res: OpaquePointer?, isBorrowed: Bool = false) {
		self.res = res
		self.borrowed = isBorrowed
	}
	
	deinit {
		close()
	}
	
	func isValid() -> Bool {
		return res != nil
	}

	/// close result object
	public func close() {
		clear()
	}
	
	/// clear and disconnect result object
	public func clear() {
		if let res = self.res {
			if !self.borrowed {
				PQclear(res)
			}
			self.res = OpaquePointer(bitPattern: 0)
		}
	}
	
	/// Result Status number as Int
	public func statusInt() -> Int {
		guard let res = self.res else {
			return 0
		}
		let s = PQresultStatus(res)
		return Int(s.rawValue)
	}
	
	/// Result Status Value
	public func status() -> StatusType {
		guard let res = self.res else {
			return .unknown
		}
		let s = PQresultStatus(res)
		switch(s.rawValue) {
		case PGRES_EMPTY_QUERY.rawValue:
			return .emptyQuery
		case PGRES_COMMAND_OK.rawValue:
			return .commandOK
		case PGRES_TUPLES_OK.rawValue:
			return .tuplesOK
		case PGRES_BAD_RESPONSE.rawValue:
			return .badResponse
		case PGRES_NONFATAL_ERROR.rawValue:
			return .nonFatalError
		case PGRES_FATAL_ERROR.rawValue:
			return .fatalError
		case PGRES_SINGLE_TUPLE.rawValue:
			return .singleTuple
		default:
			print("Unhandled PQresult status type \(s.rawValue)")
		}
		return .unknown
	}
	
	/// Result Status Message
	public func errorMessage() -> String {
		guard let res = self.res else {
			return ""
		}
		return String(validatingUTF8: PQresultErrorMessage(res)) ?? ""
	}
	
	/// Result field count
	public func numFields() -> Int {
		guard let res = self.res else {
			return 0
		}
		return Int(PQnfields(res))
	}
	
	/// Field name for index value
	public func fieldName(index: Int) -> String? {
		guard let res = self.res,
			let fn = PQfname(res, Int32(index)),
			let ret = String(validatingUTF8: fn) else {
				return nil
		}
		return ret
	}
	
	/// Field type for index value
	public func fieldType(index: Int) -> Oid? {
		guard let res = self.res else {
			return nil
		}
		let fn = PQftype(res, Int32(index))
		return fn
	}
	
	/// number of rows (Tuples) returned in result
	public func numTuples() -> Int {
		guard let res = self.res else {
			return 0
		}
		return Int(PQntuples(res))
	}
	
	/// test null field at row index for field index
	public func fieldIsNull(tupleIndex: Int, fieldIndex: Int) -> Bool {
		return 1 == PQgetisnull(res, Int32(tupleIndex), Int32(fieldIndex))
	}
	
	/// return value for String field type with row and field indexes provided
	public func getFieldString(tupleIndex: Int, fieldIndex: Int) -> String? {
		guard !fieldIsNull(tupleIndex: tupleIndex, fieldIndex: fieldIndex),
			let v = PQgetvalue(res, Int32(tupleIndex), Int32(fieldIndex)) else {
				return nil
		}
		return String(validatingUTF8: v)
	}
	
	/// return value for Bool field type with row and field indexes provided
	public func getFieldBool(tupleIndex: Int, fieldIndex: Int) -> Bool? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return s == "t"
	}
	
	/// return value for Int field type with row and field indexes provided
	public func getFieldInt(tupleIndex: Int, fieldIndex: Int) -> Int? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return Int(s)
	}
	
	/// return value for Int8 field type with row and field indexes provided
	public func getFieldInt8(tupleIndex: Int, fieldIndex: Int) -> Int8? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return Int8(s)
	}
	
	/// return value for Int16 field type with row and field indexes provided
	public func getFieldInt16(tupleIndex: Int, fieldIndex: Int) -> Int16? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return Int16(s)
	}
	
	/// return value for Int32 field type with row and field indexes provided
	public func getFieldInt32(tupleIndex: Int, fieldIndex: Int) -> Int32? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return Int32(s)
	}
	
	/// return value for Int64 field type with row and field indexes provided
	public func getFieldInt64(tupleIndex: Int, fieldIndex: Int) -> Int64? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return Int64(s)
	}
	
	/// return value for Int field type with row and field indexes provided
	public func getFieldUInt(tupleIndex: Int, fieldIndex: Int) -> UInt? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return UInt(s)
	}
	
	/// return value for Int8 field type with row and field indexes provided
	public func getFieldUInt8(tupleIndex: Int, fieldIndex: Int) -> UInt8? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return UInt8(s)
	}
	
	/// return value for Int16 field type with row and field indexes provided
	public func getFieldUInt16(tupleIndex: Int, fieldIndex: Int) -> UInt16? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return UInt16(s)
	}
	
	/// return value for Int32 field type with row and field indexes provided
	public func getFieldUInt32(tupleIndex: Int, fieldIndex: Int) -> UInt32? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return UInt32(s)
	}
	
	/// return value for Int64 field type with row and field indexes provided
	public func getFieldUInt64(tupleIndex: Int, fieldIndex: Int) -> UInt64? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return UInt64(s)
	}
	
	/// return value for Double field type with row and field indexes provided
	public func getFieldDouble(tupleIndex: Int, fieldIndex: Int) -> Double? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return Double(s)
	}
	
	/// return value for Float field type with row and field indexes provided
	public func getFieldFloat(tupleIndex: Int, fieldIndex: Int) -> Float? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return Float(s)
	}
	
	/// return value for Blob field type with row and field indexes provided
	public func getFieldBlob(tupleIndex: Int, fieldIndex: Int) -> [Int8]? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		let sc = s.utf8
		guard sc.count % 2 == 0, sc.count >= 2, s[s.startIndex] == "\\", s[s.index(after: s.startIndex)] == "x" else {
			return nil
		}
		var ret = [Int8]()
		var index = sc.index(sc.startIndex, offsetBy: 2)
		while index != sc.endIndex {
			let c1 = Int8(sc[index])
			index = sc.index(after: index)
			let c2 = Int8(sc[index])
			guard let byte = byteFromHexDigits(one: c1, two: c2) else {
				return nil
			}
			ret.append(byte)
			index = sc.index(after: index)
		}
		return ret
	}
	
	private func byteFromHexDigits(one c1v: Int8, two c2v: Int8) -> Int8? {
		
		let capA: Int8 = 65
		let capF: Int8 = 70
		let lowA: Int8 = 97
		let lowF: Int8 = 102
		let zero: Int8 = 48
		let nine: Int8 = 57
		
		var newChar = Int8(0)
		
		if c1v >= capA && c1v <= capF {
			newChar = c1v - capA + 10
		} else if c1v >= lowA && c1v <= lowF {
			newChar = c1v - lowA + 10
		} else if c1v >= zero && c1v <= nine {
			newChar = c1v - zero
		} else {
			return nil
		}
		
		newChar = newChar &* 16
		
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

/// connection management class
public final class PGConnection {

	/// Connection Status enum
	public enum StatusType {
		case ok
		case bad
	}

	// Take care that conn is not thread-safe.
	var conn = OpaquePointer(bitPattern: 0)
	var connectInfo: String = ""

	// Acquire this lock before accessing conn.
	// Need to be recursive to support transaction.
	private var lock = NSRecursiveLock()

	/// empty init
	public init() {

	}

	deinit {
		close()
	}

	/// Makes a new connection to the database server.
	public func connectdb(_ info: String) -> StatusType {
		lock.lock()
		defer { lock.unlock() }

		conn = PQconnectdb(info)
		connectInfo = info
		return status()
	}

	/// Close db connection
	public func close() {
		finish()
	}

	/// Closes the connection to the server. Also frees memory used by the PGconn object.
	public func finish() {
		lock.lock()
		defer { lock.unlock() }

		if conn != nil {
			PQfinish(conn)
			conn = OpaquePointer(bitPattern: 0)
		}
	}

	/// Returns the status of the connection.
	public func status() -> StatusType {
		lock.lock()
		defer { lock.unlock() }

		let status = PQstatus(conn)
		return status == CONNECTION_OK ? .ok : .bad
	}

	/// Returns the error message most recently generated by an operation on the connection.
	public func errorMessage() -> String {
		lock.lock()
		defer { lock.unlock() }

		return String(validatingUTF8: PQerrorMessage(conn)) ?? ""
	}

	/// Submits a command to the server and waits for the result.
	public func exec(statement: String) -> PGResult {
		lock.lock()
		defer { lock.unlock() }

		return PGResult(PQexec(conn, statement))
	}

	/// Sends data to the server during COPY_IN state.
	public func putCopyData(data: String) {
		lock.lock()
		defer { lock.unlock() }

		PQputCopyData(self.conn, data, Int32(data.count))
	}

	/// Sends end-of-data indication to the server during COPY_IN state.
	/// If withError is set, the copy is forced to fail with the error description supplied.
	public func putCopyEnd(withError: String? = nil) -> PGResult {
		lock.lock()
		defer { lock.unlock() }

		PQputCopyEnd(self.conn, withError)
		let result = PGResult(PQgetResult(self.conn))
		return result
	}

	// !FIX! does not handle binary data
	/// Submits a command to the server and waits for the result, with the ability to pass parameters separately from the SQL command text.
	public func exec(statement: String, params: [Any?]) -> PGResult {
		let count = params.count
		let values = UnsafeMutablePointer<UnsafePointer<Int8>?>.allocate(capacity: count)
		let types = UnsafeMutablePointer<Oid>.allocate(capacity: count)
		let lengths = UnsafeMutablePointer<Int32>.allocate(capacity: count)
		let formats = UnsafeMutablePointer<Int32>.allocate(capacity: count)
		defer {
			values.deinitialize(count: count) ; values.deallocate()
			types.deinitialize(count: count) ; types.deallocate()
			lengths.deinitialize(count: count) ; lengths.deallocate()
			formats.deinitialize(count: count) ; formats.deallocate()
		}
		var asStrings = [String]()
		var temps = [[UInt8]]()
		for idx in 0..<count {
			switch params[idx] {
			case let s as String:
				var aa = [UInt8](s.utf8)
				aa.append(0)
				temps.append(aa)
				values[idx] = UnsafePointer<Int8>(OpaquePointer(temps.last!))
				types[idx] = 0
				lengths[idx] = 0
				formats[idx] = 0
			case let a as [UInt8]:
				let length = Int32(a.count)
				values[idx] = UnsafePointer<Int8>(OpaquePointer(a))
				types[idx] = 17
				lengths[idx] = length
				formats[idx] = 1
			case let a as [Int8]:
				let length = Int32(a.count)
				values[idx] = UnsafePointer<Int8>(OpaquePointer(a))
				types[idx] = 17
				lengths[idx] = length
				formats[idx] = 1
			case let d as Data:
				let a = d.map { $0 }
				let length = Int32(a.count)
				temps.append(a)
				values[idx] = UnsafePointer<Int8>(OpaquePointer(temps.last!))
				types[idx] = 17
				lengths[idx] = length
				formats[idx] = 1
			default:
				if let pm = params[idx] {
					asStrings.append("\(pm)")
					var aa = [UInt8](asStrings.last!.utf8)
					aa.append(0)
					temps.append(aa)
					values[idx] = UnsafePointer<Int8>(OpaquePointer(temps.last!))
				} else {
					values[idx] = nil
				}//end if
				types[idx] = 0
				lengths[idx] = 0
				formats[idx] = 0
			}
		}

		lock.lock()
		defer { lock.unlock() }

		let r = PQexecParams(conn, statement, Int32(count), nil, values, lengths, formats, Int32(0))
		return PGResult(r)
	}

	/// Assert that the connection status is OK
	///
	/// - throws: If the connection status is bad
	public func ensureStatusIsOk() throws {
		switch status() {
		case .ok:
			// connection status is good
			return
		case .bad:
			throw PostgreSQLError.error("Connection status is bad")
		}
	}
	
	/// The binding values
	public typealias ExecuteParameterArray = [Any?]
	
	/// Execute the given statement.
	///
	/// - parameter statement: String statement to be executed
	/// - parameter params: ExecuteParameterArray? optional bindings
	/// - throws: If the status is an error
	@discardableResult
	public func execute(statement: String, params: ExecuteParameterArray? = nil) throws -> PGResult {
		try ensureStatusIsOk()
		let res: PGResult
		if let params = params {
			res = exec(statement: statement, params: params)
		} else {
			res = exec(statement: statement)
		}
		let status: PGResult.StatusType = res.status()
		switch status {
		case .emptyQuery, .commandOK, .tuplesOK:
			return res
		case .badResponse, .nonFatalError, .fatalError, .singleTuple, .unknown:
			throw PostgreSQLError.error("Failed to execute statement. status: \(status)")
		}
	}

	/// Executes a BEGIN, calls the provided closure and executes a ROLLBACK if an exception occurs or a COMMIT if no exception occurs.
	///
	/// - parameter closure: Block to be executed inside transaction
	/// - throws: If the provided closure fails
	/// - returns: If successful then the return value from the `closure`
	public func doWithTransaction<Result>(closure: () throws -> Result) throws -> Result {
		lock.lock()
		defer { lock.unlock() }

		try ensureStatusIsOk()
		try execute(statement: "BEGIN")
		do {
			let result: Result = try closure()
			try execute(statement: "COMMIT")
			return result
		} catch {
			try execute(statement: "ROLLBACK")
			throw error
		}
	}

	/// Handler for receiving a PGResult
	public typealias ReceiverProc = (PGResult) -> Void

	/// Handler for processing a text message
	public typealias ProcessorProc = (String) -> Void

	/// internal callback for notice receiving
	internal var receiver: ReceiverProc = { _ in }

	/// internal callback for notice processing
	internal var processor: ProcessorProc = { _ in }

	/// Set a new notice receiver
	/// - parameter handler: a closure to handle the incoming notice
	/// - returns: a C convention function pointer; would be nil if failed to set.
	public func setReceiver(_ handler: @escaping ReceiverProc) -> PQnoticeReceiver? {
		lock.lock()
		defer { lock.unlock() }

		guard let cn = self.conn else {
			return nil
		}
		self.receiver = handler
		let me = Unmanaged.passUnretained(self).toOpaque()
		return PQsetNoticeReceiver(cn, { arg, result in
			guard let pointer = arg, let res = result else {
				return
			}
			let this = Unmanaged<PGConnection>.fromOpaque(pointer).takeUnretainedValue()
			let pgresult = PGResult(res, isBorrowed: true)
			this.receiver(pgresult)
		}, me)
	}

	/// Set a new notice processor
	/// - parameter handler: a closure to handle the incoming notice
	/// - returns: a C convention function pointer; would be nil if failed to set.
	public func setProcessor(_ handler: @escaping ProcessorProc) -> PQnoticeProcessor?{
		lock.lock()
		defer { lock.unlock() }

		guard let cn = self.conn else {
			return nil
		}
		self.processor = handler
		let me = Unmanaged.passUnretained(self).toOpaque()
		return PQsetNoticeProcessor(cn, {arg, msg in
			guard let pointer = arg, let message = msg else {
				return
			}
			let this = Unmanaged<PGConnection>.fromOpaque(pointer).takeUnretainedValue()
			let strmsg = String(cString: message)
			this.processor(strmsg)
		}, me)
	}
}

#if !swift(>=4.1)
// Added for Swift 4.0/4.1 compat
extension UnsafeMutableRawBufferPointer {
	static func allocate(byteCount: Int, alignment: Int) -> UnsafeMutableRawBufferPointer {
		return allocate(count: byteCount)
	}
}
extension UnsafeMutablePointer {
	func deallocate() {
		deallocate(capacity: 0)
	}
}
extension Collection {
	func compactMap<ElementOfResult>(_ transform: (Element) throws -> ElementOfResult?) rethrows -> [ElementOfResult] {
		return try flatMap(transform)
	}
}
#endif
