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

class PerfectPostgreSQLTests: XCTestCase {
    
    let postgresTestConnInfo = "host=localhost dbname=postgres"
    
	override func setUp() {
		super.setUp()
		// Put setup code here. This method is called before the invocation of each test method in the class.
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
			XCTAssertTrue((c1?.characters.count)! > 0)
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
			XCTAssertTrue((c1?.characters.count)! > 0)
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
}

extension PerfectPostgreSQLTests {
    static var allTests : [(String, (PerfectPostgreSQLTests) -> () throws -> ())] {
        return [
            ("testConnect", testConnect),
            ("testExec", testExec),
            ("testExecGetValues", testExecGetValues),
            ("testExecGetValuesParams", testExecGetValuesParams),
            ("testAnyBinds", testAnyBinds)
        ]
    }
}

