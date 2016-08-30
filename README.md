# Perfect - PostgreSQL Connector

[![Perfect logo](http://www.perfect.org/github/Perfect_GH_header_854.jpg)](http://perfect.org/get-involved.html)

[![Perfect logo](http://www.perfect.org/github/Perfect_GH_button_1_Star.jpg)](https://github.com/PerfectlySoft/Perfect)
[![Perfect logo](http://www.perfect.org/github/Perfect_GH_button_2_Git.jpg)](https://gitter.im/PerfectlySoft/Perfect)
[![Perfect logo](http://www.perfect.org/github/Perfect_GH_button_3_twit.jpg)](https://twitter.com/perfectlysoft)
[![Perfect logo](http://www.perfect.org/github/Perfect_GH_button_4_slack.jpg)](http://perfect.ly)


[![Swift 3.0](https://img.shields.io/badge/Swift-3.0-orange.svg?style=flat)](https://developer.apple.com/swift/)
[![Platforms OS X | Linux](https://img.shields.io/badge/Platforms-OS%20X%20%7C%20Linux%20-lightgray.svg?style=flat)](https://developer.apple.com/swift/)
[![License Apache](https://img.shields.io/badge/License-Apache-lightgrey.svg?style=flat)](http://perfect.org/licensing.html)
[![Twitter](https://img.shields.io/badge/Twitter-@PerfectlySoft-blue.svg?style=flat)](http://twitter.com/PerfectlySoft)
[![Join the chat at https://gitter.im/PerfectlySoft/Perfect](https://img.shields.io/badge/Gitter-Join%20Chat-brightgreen.svg)](https://gitter.im/PerfectlySoft/Perfect?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Slack Status](http://perfect.ly/badge.svg)](http://perfect.ly)
[![GitHub version](https://badge.fury.io/gh/PerfectlySoft%2FPerfect-PostgreSQL.svg)](https://badge.fury.io/gh/PerfectlySoft%2FPerfect-PostgreSQL)


This project provides a Swift wrapper around the libpq client library, enabling access to PostgreSQL servers.

This package builds with Swift Package Manager and is part of the [Perfect](https://github.com/PerfectlySoft/Perfect) project. It was written to be stand-alone and so does not require PerfectLib or any other components.

Ensure you have installed and activated the latest Swift 3.0 tool chain.


## Issues

We are transitioning to using JIRA for all bugs and support related issues, therefore the GitHub issues has been disabled.

If you find a mistake, bug, or any other helpful suggestion you'd like to make on the docs please head over to [http://jira.perfect.org:8080/servicedesk/customer/portal/1](http://jira.perfect.org:8080/servicedesk/customer/portal/1) and raise it.

A comprehensive list of open issues can be found at [http://jira.perfect.org:8080/projects/ISS/issues](http://jira.perfect.org:8080/projects/ISS/issues)


## OS X Build Notes

This package requires the [Home Brew](http://brew.sh) build of PostgreSQL.

To install Home Brew:

```
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

To install postgres:

```
brew install postgres
```

## Linux Build Notes

Ensure that you have installed libpq-dev.

```
sudo apt-get install libpq-dev
```

## Building

Add this project as a dependency in your Package.swift file.

```
.Package(url: "https://github.com/PerfectlySoft/Perfect-PostgreSQL.git", versions: Version(0,0,0)..<Version(10,0,0))
```

## QuickStart

Add a file to your project, making sure that it is stored in the Sources directory of your file structure. Lets name it pg_quickstart.swift for example.

Import required libraries:
```swift
import PostgreSQL
```

Setup the credentials for your connection: 
```swift
let postgresTestConnInfo = "host=localhost dbname=postgres"

let dataPG = PGConnection()
```

This function will setup and use a PGConnection

```swift
public func usePostgres() -> PGResult? {
    
    // need to make sure something is available.
    guard dataPG.connectdb(postgresTestConnInfo) == PGConnection.StatusType.ok else {
        Log.info(message: "Failure connecting to data server \(postgresTestConnInfo)")
        
        return nil
    }

    defer {
        dataPG.close()  // defer ensures we close our db connection at the end of this request
    }
    // setup basic query
    //retrieving fields with type name, oid, integer, boolean
    let queryResult:PGResult = dataPG.exec(statement: "select datname,datdba,encoding,datistemplate from pg_database")
    
    defer { queryResult.clear() }
    return queryResult
}
```

Use the queryResult to access the row data using PGRow
Here is a function that uses several different methods to view the row contents

```swift
public func useRows(result: PGResult?) {
    //get rows
    guard result != nil else {
        return
    }
    
     for row:PGRow in result! {
        for f in row {
            //print field tuples
            //this returns a tuple (fieldName:String, fieldType:Int, fieldValue:Any?)
            print("Field: \(f)")
        }
     
     }
    
    for row:PGRow in result! {
    
        //raw description
        Log.info(message: "Row description: \(row)")
        
        //retrieve field values by name
        Log.info(message: "row( datname: \(row["datname"]), datdba: \(row["datdba"]), encoding: \(row["encoding"]), datistemplate: \(row["datistemplate"])")
        
        //retrieve field values by index
        Log.info(message: "row( datname: \(row[0]), datdba: \(row[1]), encoding: \(row[2]), datistemplate: \(row[3])")
        
        // field values are properly typed, but you have to cast to tell the compiler what we have
        let c1 = row["datname"] as? String
        let c2 = row["datdba"] as? Int
        let c3 = row["encoding"] as? Int
        let c4 = row["datistemplate"] as? Bool
        print("c1=\(c1) c2=\(c2) c3=\(c3) c4=\(c4)")
        
    }
}
```

Rows can also be accessed by index using subscript syntax:
```swift
 let secondRow = result[1]
```


Additionally, there are more complex Statement constructors, and potential object designs which can further abstract the process of interacting with your data.
