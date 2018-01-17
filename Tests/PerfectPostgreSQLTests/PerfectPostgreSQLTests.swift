//
//  PostgreSQLTests.swift
//  PostgreSQLTests
//
//  Created by Kyle Jessup on 2015-10-19.
//  Copyright © 2015 PerfectlySoft. All rights reserved.
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
import XCTest
@testable import PerfectPostgreSQL

func randomStr(_ size: Int = 8190) -> String? {
  guard let f = fopen("/dev/urandom", "rb") else { return nil }
  let buf = UnsafeMutablePointer<UInt8>.allocate(capacity: size)
  defer {
    buf.deallocate(capacity: size)
    fclose(f)
  }
  _ = fread(buf, 1, size, f)
  let buffer = UnsafeBufferPointer(start: buf, count: size)
  let array: [UInt8] = Array(buffer)
  let str:[String] = array.map { String(format: "%02x", $0) }
  let s = str.joined(separator: "")
  return s
}

class PerfectPostgreSQLTests: XCTestCase {
    
    var postgresTestConnInfo = "host=localhost dbname=postgres"
    
	override func setUp() {
		super.setUp()
		// Put setup code here. This method is called before the invocation of each test method in the class.
    if let env = getenv("PSQL_TEST"), let connection = String(validatingUTF8: env) {
      postgresTestConnInfo = connection
    }
	}
	
	override func tearDown() {
		// Put teardown code here. This method is called after the invocation of each test method in the class.
		super.tearDown()
	}
	
	func testConnect() {
		let p = PGConnection()
		let status = p.connectdb(postgresTestConnInfo)
		
		XCTAssert(status == .ok)
		print(p.errorMessage())
		p.finish()
	}

  func testLeak() {
    let db = PGConnection()
    let status = db.connectdb(postgresTestConnInfo)
    XCTAssert(status == .ok)
    var sql = "DROP TABLE IF EXISTS leaky"
    var res = db.exec(statement: sql)
    var s = res.status()
    XCTAssertTrue(s == .commandOK || s == .tuplesOK)
    res.clear()
    res.close()
    sql =
    """
    CREATE TABLE IF NOT EXISTS leaky (
    id INT PRIMARY KEY NOT NULL, payload VARCHAR(16384))
    """
    res = db.exec(statement: sql)
    s = res.status()
    XCTAssertTrue(s == .commandOK || s == .tuplesOK)
    res.clear()
    res.close()
    print("inserting 1000 lines, may take about 1 minute ...")
    var now = time(nil)
    for i in 1 ... 1000 {
      sql = "INSERT INTO leaky(id, payload) VALUES($1, $2)"
      res = db.exec(statement: sql, params: [i, randomStr() ?? "\(i)"])
      s = res.status()
      XCTAssertTrue(s == .commandOK || s == .tuplesOK)
      res.clear()
      res.close()
      if i % 100 == 0 {
        print("inserting ... ", i / 10, "%")
      }
    }
    db.close()
    var then = time(nil)
    print("inserted in ", then - now, "seconds")
    print("fetched 100k records, hold on please ...")
    now = then
    for j in 0 ... 100 {
      #if os(OSX)
      autoreleasepool {
        let p = PGConnection()
        let sta = p.connectdb(postgresTestConnInfo)
        XCTAssert(sta == .ok)
        let r = p.exec(statement: "SELECT id, payload FROM leaky ORDER BY id")
        let x = r.status()
        XCTAssertTrue(x == .commandOK || x == .tuplesOK)
        XCTAssertEqual(r.numTuples(), 1000)
        if j % 10 == 0 {
          print("fetching ... ", j, "%")
        }
        for i in 0 ... 999 {
          let id = r.getFieldInt(tupleIndex: i, fieldIndex: 0)
          guard let text = r.getFieldString(tupleIndex: i, fieldIndex: 1) else {
            XCTFail("unexpected text fetching")
            break
          }
          XCTAssertEqual(id, i + 1)
          XCTAssertEqual(text.count, 16380)
        }
        r.clear()
        r.close()
        p.close()
      }
      #else
        let p = PGConnection()
        let sta = p.connectdb(postgresTestConnInfo)
        XCTAssert(sta == .ok)
        let r = p.exec(statement: "SELECT id, payload FROM leaky ORDER BY id")
        let x = r.status()
        XCTAssertTrue(x == .commandOK || x == .tuplesOK)
        XCTAssertEqual(r.numTuples(), 1000)
        if j % 10 == 0 {
          print("fetching ... ", j, "%")
        }
        for i in 0 ... 999 {
          let id = r.getFieldInt(tupleIndex: i, fieldIndex: 0)
          guard let text = r.getFieldString(tupleIndex: i, fieldIndex: 1) else {
            XCTFail("unexpected text fetching")
            break
          }
          XCTAssertEqual(id, i + 1)
          XCTAssertEqual(text.count, 16380)
        }
        r.clear()
        r.close()
        p.close()
      #endif
    }
    then = time(nil)
    print("fetched 100k lines in ", then - now, "seconds")
  }
	
	func testExec() {
		let p = PGConnection()
		let status = p.connectdb(postgresTestConnInfo)
		XCTAssert(status == .ok)
		
		let res = p.exec(statement: "select * from pg_database")
		XCTAssertEqual(res.status(), PGResult.StatusType.tuplesOK)
		
		let num = res.numFields()
		XCTAssert(num > 0)
		for x in 0..<num {
			let fn = res.fieldName(index: x)
			XCTAssertNotNil(fn)
			print(fn!)
		}
		res.clear()
		p.finish()
	}
	
	func testExecGetValues() {
        let p = PGConnection()
        let status = p.connectdb(postgresTestConnInfo)
		XCTAssert(status == .ok)
		// name, oid, integer, boolean
		let res = p.exec(statement: "select datname,datdba,encoding,datistemplate from pg_database")
		XCTAssertEqual(res.status(), PGResult.StatusType.tuplesOK)
		
		let num = res.numTuples()
		XCTAssert(num > 0)
		for x in 0..<num {
			let c1 = res.getFieldString(tupleIndex: x, fieldIndex: 0)
			XCTAssertTrue((c1?.count)! > 0)
			let c2 = res.getFieldInt(tupleIndex: x, fieldIndex: 1)
			let c3 = res.getFieldInt(tupleIndex: x, fieldIndex: 2)
			let c4 = res.getFieldBool(tupleIndex: x, fieldIndex: 3)
			print("c1=\(String(describing: c1)) c2=\(String(describing: c2)) c3=\(String(describing: c3)) c4=\(String(describing: c4))")
		}
		res.clear()
		p.finish()
	}
	
	func testExecGetValuesParams() {
        let p = PGConnection()
        let status = p.connectdb(postgresTestConnInfo)
		XCTAssert(status == .ok)
		// name, oid, integer, boolean
		let res = p.exec(statement: "select datname,datdba,encoding,datistemplate from pg_database where encoding = $1", params: ["6"])
		XCTAssertEqual(res.status(), PGResult.StatusType.tuplesOK, res.errorMessage())
		
		let num = res.numTuples()
		XCTAssert(num > 0)
		for x in 0..<num {
			let c1 = res.getFieldString(tupleIndex: x, fieldIndex: 0)
			XCTAssertTrue((c1?.count)! > 0)
			let c2 = res.getFieldInt(tupleIndex: x, fieldIndex: 1)
			let c3 = res.getFieldInt(tupleIndex: x, fieldIndex: 2)
			let c4 = res.getFieldBool(tupleIndex: x, fieldIndex: 3)
			print("c1=\(String(describing: c1)) c2=\(String(describing: c2)) c3=\(String(describing: c3)) c4=\(String(describing: c4))")
		}
		res.clear()
		p.finish()
	}
	
	func testAnyBinds() {
		let p = PGConnection()
		let status = p.connectdb(postgresTestConnInfo)
		guard case .ok = status else {
			return XCTAssert(status == .ok)
		}
		// name, oid, integer, boolean
		_ = p.exec(statement: "DROP TABLE IF EXISTS films")
		
		for _ in 0..<200 {
			
			let res = p.exec(statement: "CREATE TABLE films (code char(5) PRIMARY KEY, title varchar(40) NOT NULL, did integer NOT NULL, date_prod date, kind1 bytea, kind2 bytea)")
			XCTAssertEqual(res.status(), PGResult.StatusType.commandOK, res.errorMessage())
			
			let u = "ABCDEFGH".utf8.map { Int8($0) }
			do {
				let res = p.exec(statement: "INSERT INTO films (code, title, did, kind1, kind2) VALUES ($1, $2, $3, $4, $5)",
				                 params: [1, "film title", 42, Data(bytes: u, count: u.count), u])
				XCTAssertEqual(res.status(), PGResult.StatusType.commandOK, res.errorMessage())
			}
			
			do {
				let res = p.exec(statement: "SELECT code, title, did, kind1, kind2 FROM films")
				XCTAssertEqual(res.status(), PGResult.StatusType.tuplesOK, res.errorMessage())
				let num = res.numTuples()
				XCTAssert(num == 1)
				
				let c1 = res.getFieldString(tupleIndex: 0, fieldIndex: 0)
				XCTAssert(c1 == "1    ")
				let c2 = res.getFieldString(tupleIndex: 0, fieldIndex: 1)
				XCTAssert(c2 == "film title")
				let c3 = res.getFieldInt(tupleIndex: 0, fieldIndex: 2)
				XCTAssert(c3 == 42)
				let c4 = res.getFieldBlob(tupleIndex: 0, fieldIndex: 3)
				XCTAssert(c4! == u)
				let c5 = res.getFieldBlob(tupleIndex: 0, fieldIndex: 4)
				XCTAssert(c5! == u)
			}
			_ = p.exec(statement: "DROP TABLE films")
		}
		p.finish()
	}

	func testEnsureStatusIsOk() {
		let p = PGConnection()
		XCTAssertThrowsError(try p.ensureStatusIsOk())
		_ =  p.connectdb(postgresTestConnInfo)
		XCTAssertNoThrow(try p.ensureStatusIsOk())
		p.finish()
		XCTAssertThrowsError(try p.ensureStatusIsOk())
	}
	
	func testExecute() {
		let p = PGConnection()
		let status = p.connectdb(postgresTestConnInfo)
		XCTAssert(status == .ok)
		
		// Exercise with statements that executes successfully
		XCTAssertNoThrow(try p.execute(statement: "SELECT 3 * 4"))
		XCTAssertNoThrow(try p.execute(statement: ""))
		
		// Exercise with statements that fails
		XCTAssertThrowsError(try p.execute(statement: "INVALID_SQL_GARBAGE"))

		p.finish()
	}
	
	func testTransaction() {
		// Setup
		let p = PGConnection()
		let status = p.connectdb(postgresTestConnInfo)
		XCTAssert(status == .ok)
		try! p.execute(statement: "DROP TABLE IF EXISTS planets")
		try! p.execute(statement: "CREATE TABLE planets (id INTEGER PRIMARY KEY, name TEXT)")
		
		// Exercise COMMIT
		let exerciseCommitClosure: () throws -> String = {
			try p.execute(statement: "INSERT INTO planets (id, name) VALUES ($1, $2)", params: [1, "Mercury"])
			try p.execute(statement: "INSERT INTO planets (id, name) VALUES ($1, $2)", params: [2, "Venus"])
			return "I'm a value returned from the closure"
		}
		let result: String = try! p.doWithTransaction(closure: exerciseCommitClosure)
		XCTAssertEqual(result, "I'm a value returned from the closure")
		
		// Exercise ROLLBACK
		let exerciseRollbackClosure: () throws -> () = {
			try p.execute(statement: "INSERT INTO planets (id, name) VALUES ($1, $2)", params: [3, "Earth"])
			try p.execute(statement: "INSERT INTO planets (id, name) VALUES ($1, $2)", params: [4, "Mars"])
			// The following line exercises the ROLLBACK code by triggering a duplicate id error
			try p.execute(statement: "INSERT INTO planets (id, name) VALUES ($1, $2)", params: [4, "Mars"])
		}
		XCTAssertThrowsError(try p.doWithTransaction(closure: exerciseRollbackClosure))
		
		// Verify that the `exerciseRollbackClosure` got ROLLBACK'ed by checking Earth does not exist
		do {
			let res = try! p.execute(statement: "SELECT name FROM planets WHERE name = $1", params: ["Earth"])
			let num = res.numTuples()
			XCTAssertEqual(num, 0)
		}
		// Verify that the `exerciseCommitClosure` got COMMIT'ted by checking that Mercury does exist
		do {
			let res = try! p.execute(statement: "SELECT name FROM planets WHERE name = $1", params: ["Mercury"])
			let num = res.numTuples()
			XCTAssertEqual(num, 1)
		}

		// Teardown
		try! p.execute(statement: "DROP TABLE planets")
		p.finish()
	}
}

extension PerfectPostgreSQLTests {
    static var allTests : [(String, (PerfectPostgreSQLTests) -> () throws -> ())] {
        return [
            ("testConnect", testConnect),
            ("testExec", testExec),
            ("testExecGetValues", testExecGetValues),
            ("testExecGetValuesParams", testExecGetValuesParams),
            ("testAnyBinds", testAnyBinds),
            ("testEnsureStatusIsOk", testEnsureStatusIsOk),
            ("testExecute", testExecute),
            ("testTransaction", testTransaction),
            ("testLeak", testLeak)
        ]
    }
}

