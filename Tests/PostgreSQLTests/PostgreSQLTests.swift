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

import XCTest
@testable import PostgreSQL

class PostgreSQLTests: XCTestCase {
    
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
			print("c1=\(c1) c2=\(c2) c3=\(c3) c4=\(c4)")
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
			print("c1=\(c1) c2=\(c2) c3=\(c3) c4=\(c4)")
		}
		res.clear()
		p.finish()
	}
}

extension PostgreSQLTests {
    static var allTests : [(String, (PostgreSQLTests) -> () throws -> ())] {
        return [
            ("testConnect", testConnect),
            ("testExec", testExec),
            ("testExecGetValues", testExecGetValues),
            ("testExecGetValuesParams", testExecGetValuesParams)
        ]
    }
}

