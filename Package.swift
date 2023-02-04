// swift-tools-version:5.4
//  Package.swift
//  Perfect-PostgreSQL
//
//  Created by Kyle Jessup on 3/22/16.
//	Copyright (C) 2016 PerfectlySoft, Inc.
//
// ===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2015 - 2016 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
// ===----------------------------------------------------------------------===//
//

import PackageDescription

let package = Package(
	name: "PerfectPostgreSQL",
	platforms: [
		.macOS(.v10_15)
	],
	products: [
		.library(name: "PerfectPostgreSQL", targets: ["PerfectPostgreSQL"])
	],
	dependencies: [
		.package(url: "https://github.com/RockfordWei/Perfect.git", from: "5.6.13")
	],
	targets: [
		.systemLibrary(name: "libpq", pkgConfig: "libpq", providers: [
            .apt(["libpq-dev"]),
            .brew(["openssl", "postgres"])
        ]),
		.target(name: "PerfectPostgreSQL", dependencies: ["libpq", .product(name: "PerfectCRUD", package: "Perfect")]),
		.testTarget(name: "PerfectPostgreSQLTests", dependencies: ["PerfectPostgreSQL"])
	]
)
