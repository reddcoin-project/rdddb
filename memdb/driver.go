// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package memdb

import (
	"fmt"

	"github.com/conformal/btclog"
	"github.com/reddcoin-project/rdddb"
)

var log = btclog.Disabled

func init() {
	driver := rdddb.DriverDB{DbType: "memdb", CreateDB: CreateDB, OpenDB: OpenDB}
	rdddb.AddDBDriver(driver)
}

// parseArgs parses the arguments from the rdddb Open/Create methods.
func parseArgs(funcName string, args ...interface{}) error {
	if len(args) != 0 {
		return fmt.Errorf("memdb.%s does not accept any arguments",
			funcName)
	}

	return nil
}

// OpenDB opens an existing database for use.
func OpenDB(args ...interface{}) (rdddb.Db, error) {
	if err := parseArgs("OpenDB", args...); err != nil {
		return nil, err
	}

	// A memory database is not persistent, so let CreateDB handle it.
	return CreateDB()
}

// CreateDB creates, initializes, and opens a database for use.
func CreateDB(args ...interface{}) (rdddb.Db, error) {
	if err := parseArgs("CreateDB", args...); err != nil {
		return nil, err
	}

	log = rdddb.GetLog()
	return newMemDb(), nil
}
