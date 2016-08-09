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
		if let res = self.res {
			PQclear(res)
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
		if let fn = PQfname(self.res!, Int32(index)) {
			return String(validatingUTF8: fn) ?? ""
		}
		return nil
	}
	
    /// Field type for index value
	public func fieldType(index: Int) -> Oid? {
		let fn = PQftype(self.res!, Int32(index))
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
		return 1 == PQgetisnull(self.res!, Int32(tupleIndex), Int32(fieldIndex))
	}
	
    /// return value for String field type with row and field indexes provided
	public func getFieldString(tupleIndex: Int, fieldIndex: Int) -> String? {
		guard let v = PQgetvalue(self.res, Int32(tupleIndex), Int32(fieldIndex)) else {
			return nil
		}
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
		guard let ip = UnsafePointer<Int8>(PQgetvalue(self.res!, Int32(tupleIndex), Int32(fieldIndex))) else {
			return nil
		}
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
		case ok
		case bad
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
		return status == CONNECTION_OK ? .ok : .bad
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
		let values = UnsafeMutablePointer<UnsafePointer<Int8>?>.allocate(capacity: count)
		defer {
			values.deinitialize(count: count) ; values.deallocate(capacity: count)
		}
		var temps = [Array<UInt8>]()
		for idx in 0..<count {
			let s = asStrings[idx]
			let utf8 = s.utf8
			var aa = Array<UInt8>(utf8)
			aa.append(0)
			temps.append(aa)
			values[idx] = UnsafePointer<Int8>(OpaquePointer(temps.last!))
		}
		
		let r = PQexecParams(self.conn, statement, Int32(count), nil, values, nil, nil, Int32(0))
		return PGResult(r)
	}
}


/// Wraps PGResult in an iterable object that also has subscript access to individual rows
class PGResultSet: Sequence, IteratorProtocol {
    var count:Int
    let res :PGResult
    
    /// Pass in a PGResult to get access to Sequence and IteratorProtocol conformance, use of for loops, and subscript access to rows by index
    init(res:PGResult) {
        self.res = res
        count = 0
    }
    ///provides basic index based retrieval of rows in result set
    func getRow(rowIndex: Int) -> PGRow? {
        
        return PGRow(fromResultSet: self, row: rowIndex)
    }
    
    ///returns next row in the result set. Required for Sequence and IteratorProtocol conformance. Allows use of for - in syntax without having to iterate thru a range of index numbers
    public func next() -> PGRow? {
        if (count == res.numTuples()) {
            return nil
        } else {
            defer { count += 1}
            return PGRow(fromResultSet: self, row: count)
        }
    }
    /// returns specified row by index
    public subscript(rowIndex: Int) -> PGRow? {
        return getRow(rowIndex: rowIndex)
        
    }
}

///Provides Sequence and Iterator access to the row data from a PGResultSet
class PGRow: Sequence, IteratorProtocol {
    var rowPosition:Int
    let row:Int
    let res:PGResult
    var fields = [String:Any?]()
    
    ///access fields from a specified row in PGResultSet
    init(fromResultSet set: PGResultSet, row:Int){
        self.res = set.res
        self.row = row
        rowPosition = 0
        
        while let f = self.next() {
            
            if(res.fieldIsNull(tupleIndex: self.row, fieldIndex: rowPosition-1)) {
                fields[f.0] = nil
            } else {
                fields[f.0] = f.2
            }
            
        }
    }
    
    ///Returns a Tuple made up of (fieldName:String, fieldType:Int, fieldValue:Any?) for a field specified by index. This method attempts to return proper type thru use of fieldType Integer, but needs a more complete reference to the field type list to be complete
    func getFieldTuple(fieldIndex: Int)-> (String, Int, Any?)? {
        if(res.fieldIsNull(tupleIndex: row, fieldIndex: fieldIndex)) {
            return (res.fieldName(index: rowPosition)!, Int(res.fieldType(index: fieldIndex)!), nil)
        } else {
            let fieldtype = Int(res.fieldType(index: fieldIndex)!)
            switch fieldtype {
            case 23:
                return (res.fieldName(index: fieldIndex)!, Int(res.fieldType(index: fieldIndex)!), res.getFieldInt(tupleIndex: row, fieldIndex: fieldIndex))
            case 16:
                return (res.fieldName(index: fieldIndex)!, Int(res.fieldType(index: fieldIndex)!), res.getFieldBool(tupleIndex: row, fieldIndex: fieldIndex))
            default:
                return (res.fieldName(index: fieldIndex)!, Int(res.fieldType(index: fieldIndex)!), res.getFieldString(tupleIndex: row, fieldIndex: fieldIndex))
            }
            
        }
    }
    ///returns next field in the row. Required for Sequence and IteratorProtocol conformance. Allows use of for - in syntax without having to iterate thru a range of index numbers
    public func next() -> (String,Int,Any?)? {
        let curIndex = rowPosition
        if (curIndex >= res.numFields()) {
            return nil
        } else {
            rowPosition += 1
            return getFieldTuple(fieldIndex: curIndex)
        }
    }
    
    /// subscript by field Index, returns field Tuple
    public subscript(fieldIndex: Int) -> (String,Int,Any?)? {
        return getFieldTuple(fieldIndex: fieldIndex)
        
    }
    
    /// subscript by field Name, returns field Tuple
    public subscript(fieldName: String) -> Any? {
        return fields[fieldName]
        
    }
}


