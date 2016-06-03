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


import libpq

#if swift(>=3.0)
	extension UnsafeMutablePointer {
		public static func allocatingCapacity(_ num: Int) -> UnsafeMutablePointer<Pointee> {
			return UnsafeMutablePointer<Pointee>(allocatingCapacity: num)
		}
	}
#else
	typealias ErrorProtocol = ErrorType
	typealias OpaquePointer = COpaquePointer
	
	extension String {
		init?(validatingUTF8: UnsafePointer<Int8>) {
			if let s = String.fromCString(validatingUTF8) {
				self.init(s)
			} else {
				return nil
			}
		}
	}
	extension UnsafeMutablePointer {
		public static func allocatingCapacity(num: Int) -> UnsafeMutablePointer<Memory> {
			return UnsafeMutablePointer<Memory>.alloc(num)
		}
	
		func deallocateCapacity(num: Int) {
			self.dealloc(num)
		}
		
		func deinitialize(count count: Int) {
			self.destroy(count)
		}
	}
#endif

/// result object
public final class PGResult {
	
    /// Result Status enum
	public enum StatusType {
		case EmptyQuery
		case CommandOK
		case TuplesOK
		case BadResponse
		case NonFatalError
		case FatalError
		case SingleTuple
		case Unknown
	}
	
	var res: OpaquePointer? = OpaquePointer(bitPattern: 0)
	
	init(_ res: OpaquePointer?) {
		self.res = res
	}
	
	deinit {
		self.close()
	}
	
    /// close result object
	public func close() {
		self.clear()
	}
	
    /// clear and disconnect result object
	public func clear() {
		if self.res != nil {
			PQclear(self.res!)
			self.res = OpaquePointer(bitPattern: 0)
		}
	}
	
    /// Result Status number as Int
	public func statusInt() -> Int {
		let s = PQresultStatus(self.res!)
		return Int(s.rawValue)
	}
	
    /// Result Status Value
	public func status() -> StatusType {
		let s = PQresultStatus(self.res!)
		switch(s.rawValue) {
		case PGRES_EMPTY_QUERY.rawValue:
			return .EmptyQuery
		case PGRES_COMMAND_OK.rawValue:
			return .CommandOK
		case PGRES_TUPLES_OK.rawValue:
			return .TuplesOK
		case PGRES_BAD_RESPONSE.rawValue:
			return .BadResponse
		case PGRES_NONFATAL_ERROR.rawValue:
			return .NonFatalError
		case PGRES_FATAL_ERROR.rawValue:
			return .FatalError
		case PGRES_SINGLE_TUPLE.rawValue:
			return .SingleTuple
		default:
			print("Unhandled PQresult status type \(s.rawValue)")
		}
		return .Unknown
	}
	
    /// Result Status Message
	public func errorMessage() -> String {
		return String(validatingUTF8: PQresultErrorMessage(self.res!)) ?? ""
	}
	
    /// Result field count
	public func numFields() -> Int {
		return Int(PQnfields(self.res!))
	}
	
    /// Field name for index value
	public func fieldName(index: Int) -> String? {
	#if swift(>=3.0)
		if let fn = PQfname(self.res!, Int32(index)) {
			return String(validatingUTF8: fn) ?? ""
		}
	#else
		let fn = PQfname(self.res!, Int32(index))
		if nil != fn {
			return String(validatingUTF8: fn) ?? ""
		}
	#endif
		return nil
	}
	
    /// Field type for index value
	public func fieldType(index: Int) -> Oid? {
		let fn = PQftype(self.res!, Int32(index))
		return fn
	}
	
    /// number of rows (Tuples) returned in result
	public func numTuples() -> Int {
		return Int(PQntuples(self.res!))
	}
	
    /// test null field at row index for field index
	public func fieldIsNull(tupleIndex: Int, fieldIndex: Int) -> Bool {
		return 1 == PQgetisnull(self.res!, Int32(tupleIndex), Int32(fieldIndex))
	}
	
    /// return value for String field type with row and field indexes provided
	public func getFieldString(tupleIndex: Int, fieldIndex: Int) -> String? {
	#if swift(>=3.0)
		guard let v = PQgetvalue(self.res, Int32(tupleIndex), Int32(fieldIndex)) else {
			return nil
		}
	#else
		let v = PQgetvalue(self.res!, Int32(tupleIndex), Int32(fieldIndex))
		guard nil != v else {
			return nil
		}
	#endif
		return String(validatingUTF8: v)
	}
	
    /// return value for Int field type with row and field indexes provided
	public func getFieldInt(tupleIndex: Int, fieldIndex: Int) -> Int? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return Int(s)
	}
	
    /// return value for Bool field type with row and field indexes provided
	public func getFieldBool(tupleIndex: Int, fieldIndex: Int) -> Bool? {
		guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
			return nil
		}
		return s == "t"
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
	#if swift(>=3.0)
		guard let ip = UnsafePointer<Int8>(PQgetvalue(self.res!, Int32(tupleIndex), Int32(fieldIndex))) else {
			return nil
		}
	#else
		let ip = UnsafePointer<Int8>(PQgetvalue(self.res!, Int32(tupleIndex), Int32(fieldIndex)))
		guard nil != ip else {
			return nil
		}
	#endif
		let length = Int(PQgetlength(self.res!, Int32(tupleIndex), Int32(fieldIndex)))
		var ret = [Int8]()
		for idx in 0..<length {
			ret.append(ip[idx])
		}
		return ret
	}
}

/// connection management class
public final class PGConnection {
	
    /// Connection Status enum
	public enum StatusType {
		case OK
		case Bad
	}
	
	var conn = OpaquePointer(bitPattern: 0)
	var connectInfo: String = ""
	
    /// empty init
	public init() {
		
	}
	
	deinit {
		self.close()
	}
	
    /// Makes a new connection to the database server.
	public func connectdb(_ info: String) -> StatusType {
		self.conn = PQconnectdb(info)
		self.connectInfo = info
		return self.status()
	}
	
    /// Close db connection
	public func close() {
		self.finish()
	}
	
    /// Closes the connection to the server. Also frees memory used by the PGconn object.
	public func finish() {
		if self.conn != nil {
			PQfinish(self.conn)
			self.conn = OpaquePointer(bitPattern: 0)
		}
	}
	
    /// Returns the status of the connection.
	public func status() -> StatusType {
		let status = PQstatus(self.conn)
		return status == CONNECTION_OK ? .OK : .Bad
	}
	
    /// Returns the error message most recently generated by an operation on the connection.
	public func errorMessage() -> String {
		return String(validatingUTF8: PQerrorMessage(self.conn)) ?? ""
	}
	
    /// Submits a command to the server and waits for the result.
	public func exec(statement: String) -> PGResult {
		return PGResult(PQexec(self.conn, statement))
	}
	
	// !FIX! does not handle binary data
    /// Submits a command to the server and waits for the result, with the ability to pass parameters separately from the SQL command text.
	public func exec(statement: String, params: [String]) -> PGResult {
		var asStrings = [String]()
		for item in params {
			asStrings.append(String(item))
		}
		let count = asStrings.count
	#if swift(>=3.0)
		let values = UnsafeMutablePointer<UnsafePointer<Int8>?>.allocatingCapacity(count)
	#else
		let values = UnsafeMutablePointer<UnsafePointer<Int8>>.allocatingCapacity(count)	
	#endif
		defer {
			values.deinitialize(count: count) ; values.deallocateCapacity(count)
		}
		var temps = [Array<UInt8>]()
		for idx in 0..<count {
			let s = asStrings[idx]
			let utf8 = s.utf8
			var aa = Array<UInt8>(utf8)
			aa.append(0)
			temps.append(aa)
			values[idx] = UnsafePointer<Int8>(temps.last!)
		}
		
		let r = PQexecParams(self.conn, statement, Int32(count), nil, values, nil, nil, Int32(0))
		return PGResult(r)
	}
}







