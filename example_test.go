// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package rdddb_test

import (
	"fmt"
	"github.com/reddcoin-project/rdddb"
	_ "github.com/reddcoin-project/rdddb/memdb"
	"github.com/reddcoin-project/rddnet"
	"github.com/reddcoin-project/rddutil"
)

// This example demonstrates creating a new database and inserting the genesis
// block into it.
func ExampleCreateDB() {
	// Notice in these example imports that the memdb driver is loaded.
	// Ordinarily this would be whatever driver(s) your application
	// requires.
	// import (
	//	"github.com/reddcoin-project/rdddb"
	// 	_ "github.com/reddcoin-project/rdddb/memdb"
	// )

	// Create a database and schedule it to be closed on exit.  This example
	// uses a memory-only database to avoid needing to write anything to
	// the disk.  Typically, you would specify a persistent database driver
	// such as "leveldb" and give it a database name as the second
	// parameter.
	db, err := rdddb.CreateDB("memdb")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	// Insert the main network genesis block.
	genesis := rddutil.NewBlock(rddnet.MainNetParams.GenesisBlock)
	newHeight, err := db.InsertBlock(genesis)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("New height:", newHeight)

	// Output:
	// New height: 0
}

// exampleLoadDB is used in the example to elide the setup code.
func exampleLoadDB() (rdddb.Db, error) {
	db, err := rdddb.CreateDB("memdb")
	if err != nil {
		return nil, err
	}

	// Insert the main network genesis block.
	genesis := rddutil.NewBlock(rddnet.MainNetParams.GenesisBlock)
	_, err = db.InsertBlock(genesis)
	if err != nil {
		return nil, err
	}

	return db, err
}

// This example demonstrates querying the database for the most recent best
// block height and hash.
func ExampleDb_newestSha() {
	// Load a database for the purposes of this example and schedule it to
	// be closed on exit.  See the CreateDB example for more details on what
	// this step is doing.
	db, err := exampleLoadDB()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	latestHash, latestHeight, err := db.NewestSha()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Latest hash:", latestHash)
	fmt.Println("Latest height:", latestHeight)

	// Output:
	// Latest hash: b868e0d95a3c3c0e0dadc67ee587aaf9dc8acbf99e3b4b3110fad4eb74c1decc
	// Latest height: 0
}
