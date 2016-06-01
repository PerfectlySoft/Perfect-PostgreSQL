# Perfect - PostgreSQL Connector

[![GitHub version](https://badge.fury.io/gh/PerfectlySoft%2FPerfect-PostgreSQL.svg)](https://badge.fury.io/gh/PerfectlySoft%2FPerfect-PostgreSQL)

This project provides a Swift wrapper around the libpq client library, enabling access to PostgreSQL servers.

This package builds with Swift Package Manager and is part of the [Perfect](https://github.com/PerfectlySoft/Perfect) project. It was written to be stand-alone and so does not require PerfectLib or any other components.

Ensure you have installed and activated the latest Swift 3.0 tool chain.

## OS X Build Notes

This package requires the [Home Brew](http://brew.sh) build of posstgres.

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
.Package(url: "https://github.com/PerfectlySoft/Perfect-PostgreSQL.git", majorVersion: 0, minor: 1)
```
