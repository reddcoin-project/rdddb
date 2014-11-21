// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

/*
Package rdddb provides a database interface for the Reddcoin block chain.

rdddb provides a database layer to store and retrieve this data in a fairly
simple and efficient manner.  The use of this should not require specific
knowledge of the database backend.

Basic Design

The basic design of rdddb is to provide two classes of items in a database;
blocks and transactions (tx) where the block number increases monotonically.
Each transaction belongs to a single block although a block can have a variable
number of transactions.  Along with these two items, several convenience
functions for dealing with the database are provided as well as functions to
query specific items that may be present in a block or tx.

Usage

At the highest level, the use of this packages just requires that you import it,
setup a database, insert some data into it, and optionally, query the data back.
The first block inserted into the database will be treated as the genesis block.
Every subsequent block insert requires the referenced parent block to already
exist.
*/
package rdddb
