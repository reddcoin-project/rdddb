// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package rdddb

import (
	"errors"

	"github.com/reddcoin-project/rddutil"
	"github.com/reddcoin-project/rddwire"
)

// Errors that the various database functions may return.
var (
	ErrPrevShaMissing  = errors.New("previous sha missing from database")
	ErrTxShaMissing    = errors.New("requested transaction does not exist")
	ErrBlockShaMissing = errors.New("requested block does not exist")
	ErrDuplicateSha    = errors.New("duplicate insert attempted")
	ErrDbDoesNotExist  = errors.New("non-existent database")
	ErrDbUnknownType   = errors.New("non-existent database type")
)

// AllShas is a special value that can be used as the final sha when requesting
// a range of shas by height to request them all.
const AllShas = int64(^uint64(0) >> 1)

// Db defines a generic interface that is used to request and insert data into
// the Reddcoin block chain.  This interface is intended to be agnostic to actual
// mechanism used for backend data storage.  The AddDBDriver function can be
// used to add a new backend data storage method.
type Db interface {
	// Close cleanly shuts down the database and syncs all data.
	Close() (err error)

	// DropAfterBlockBySha will remove any blocks from the database after
	// the given block.  It terminates any existing transaction and performs
	// its operations in an atomic transaction which is commited before
	// the function returns.
	DropAfterBlockBySha(*rddwire.ShaHash) (err error)

	// ExistsSha returns whether or not the given block hash is present in
	// the database.
	ExistsSha(sha *rddwire.ShaHash) (exists bool, err error)

	// FetchBlockBySha returns a rddutil Block.  The implementation may
	// cache the underlying data if desired.
	FetchBlockBySha(sha *rddwire.ShaHash) (blk *rddutil.Block, err error)

	// FetchBlockHeightBySha returns the block height for the given hash.
	FetchBlockHeightBySha(sha *rddwire.ShaHash) (height int64, err error)

	// FetchBlockHeaderBySha returns a rddwire.BlockHeader for the given
	// sha.  The implementation may cache the underlying data if desired.
	FetchBlockHeaderBySha(sha *rddwire.ShaHash) (bh *rddwire.BlockHeader, err error)

	// FetchBlockShaByHeight returns a block hash based on its height in the
	// block chain.
	FetchBlockShaByHeight(height int64) (sha *rddwire.ShaHash, err error)

	// FetchHeightRange looks up a range of blocks by the start and ending
	// heights.  Fetch is inclusive of the start height and exclusive of the
	// ending height. To fetch all hashes from the start height until no
	// more are present, use the special id `AllShas'.
	FetchHeightRange(startHeight, endHeight int64) (rshalist []rddwire.ShaHash, err error)

	// ExistsTxSha returns whether or not the given tx hash is present in
	// the database
	ExistsTxSha(sha *rddwire.ShaHash) (exists bool, err error)

	// FetchTxBySha returns some data for the given transaction hash. The
	// implementation may cache the underlying data if desired.
	FetchTxBySha(txsha *rddwire.ShaHash) ([]*TxListReply, error)

	// FetchTxByShaList returns a TxListReply given an array of transaction
	// hashes.  The implementation may cache the underlying data if desired.
	// This differs from FetchUnSpentTxByShaList in that it will return
	// the most recent known Tx, if it is fully spent or not.
	//
	// NOTE: This function does not return an error directly since it MUST
	// return at least one TxListReply instance for each requested
	// transaction.  Each TxListReply instance then contains an Err field
	// which can be used to detect errors.
	FetchTxByShaList(txShaList []*rddwire.ShaHash) []*TxListReply

	// FetchUnSpentTxByShaList returns a TxListReply given an array of
	// transaction hashes.  The implementation may cache the underlying
	// data if desired. Fully spent transactions will not normally not
	// be returned in this operation.
	//
	// NOTE: This function does not return an error directly since it MUST
	// return at least one TxListReply instance for each requested
	// transaction.  Each TxListReply instance then contains an Err field
	// which can be used to detect errors.
	FetchUnSpentTxByShaList(txShaList []*rddwire.ShaHash) []*TxListReply

	// InsertBlock inserts raw block and transaction data from a block
	// into the database.  The first block inserted into the database
	// will be treated as the genesis block.  Every subsequent block insert
	// requires the referenced parent block to already exist.
	InsertBlock(block *rddutil.Block) (height int64, err error)

	// NewestSha returns the hash and block height of the most recent (end)
	// block of the block chain.  It will return the zero hash, -1 for
	// the block height, and no error (nil) if there are not any blocks in
	// the database yet.
	NewestSha() (sha *rddwire.ShaHash, height int64, err error)

	// RollbackClose discards the recent database changes to the previously
	// saved data at last Sync and closes the database.
	RollbackClose() (err error)

	// Sync verifies that the database is coherent on disk and no
	// outstanding transactions are in flight.
	Sync() (err error)
}

// DriverDB defines a structure for backend drivers to use when they registered
// themselves as a backend which implements the Db interface.
type DriverDB struct {
	DbType   string
	CreateDB func(args ...interface{}) (pbdb Db, err error)
	OpenDB   func(args ...interface{}) (pbdb Db, err error)
}

// TxListReply is used to return individual transaction information when
// data about multiple transactions is requested in a single call.
type TxListReply struct {
	Sha     *rddwire.ShaHash
	Tx      *rddwire.MsgTx
	BlkSha  *rddwire.ShaHash
	Height  int64
	TxSpent []bool
	Err     error
}

// driverList holds all of the registered database backends.
var driverList []DriverDB

// AddDBDriver adds a back end database driver to available interfaces.
func AddDBDriver(instance DriverDB) {
	// TODO(drahn) Does this really need to check for duplicate names ?
	for _, drv := range driverList {
		// TODO(drahn) should duplicates be an error?
		if drv.DbType == instance.DbType {
			return
		}
	}
	driverList = append(driverList, instance)
}

// CreateDB intializes and opens a database.
func CreateDB(dbtype string, args ...interface{}) (pbdb Db, err error) {
	for _, drv := range driverList {
		if drv.DbType == dbtype {
			return drv.CreateDB(args...)
		}
	}
	return nil, ErrDbUnknownType
}

// OpenDB opens an existing database.
func OpenDB(dbtype string, args ...interface{}) (pbdb Db, err error) {
	for _, drv := range driverList {
		if drv.DbType == dbtype {
			return drv.OpenDB(args...)
		}
	}
	return nil, ErrDbUnknownType
}

// SupportedDBs returns a slice of strings that represent the database drivers
// that have been registered and are therefore supported.
func SupportedDBs() []string {
	var supportedDBs []string
	for _, drv := range driverList {
		supportedDBs = append(supportedDBs, drv.DbType)
	}
	return supportedDBs
}
