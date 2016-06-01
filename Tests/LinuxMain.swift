import XCTest

import PostgreSQLTestSuite

var tests = [XCTestCaseEntry]()
tests += PostgreSQLTestSuite.allTests()
XCTMain(tests)
