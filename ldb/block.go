// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ldb

import (
	"bytes"
	"encoding/binary"

	"github.com/reddcoin-project/rdddb"
	"github.com/reddcoin-project/rddutil"
	"github.com/reddcoin-project/rddwire"
	"github.com/conformal/goleveldb/leveldb"
)

// FetchBlockBySha - return a rddutil Block
func (db *LevelDb) FetchBlockBySha(sha *rddwire.ShaHash) (blk *rddutil.Block, err error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()
	return db.fetchBlockBySha(sha)
}

// fetchBlockBySha - return a rddutil Block
// Must be called with db lock held.
func (db *LevelDb) fetchBlockBySha(sha *rddwire.ShaHash) (blk *rddutil.Block, err error) {

	buf, height, err := db.fetchSha(sha)
	if err != nil {
		return
	}

	blk, err = rddutil.NewBlockFromBytes(buf)
	if err != nil {
		return
	}
	blk.SetHeight(height)

	return
}

// FetchBlockHeightBySha returns the block height for the given hash.  This is
// part of the rdddb.Db interface implementation.
func (db *LevelDb) FetchBlockHeightBySha(sha *rddwire.ShaHash) (int64, error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	return db.getBlkLoc(sha)
}

// FetchBlockHeaderBySha - return a rddwire ShaHash
func (db *LevelDb) FetchBlockHeaderBySha(sha *rddwire.ShaHash) (bh *rddwire.BlockHeader, err error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	// Read the raw block from the database.
	buf, _, err := db.fetchSha(sha)
	if err != nil {
		return nil, err
	}

	// Only deserialize the header portion and ensure the transaction count
	// is zero since this is a standalone header.
	var blockHeader rddwire.BlockHeader
	err = blockHeader.Deserialize(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	bh = &blockHeader

	return bh, err
}

func (db *LevelDb) getBlkLoc(sha *rddwire.ShaHash) (int64, error) {
	key := shaBlkToKey(sha)

	data, err := db.lDb.Get(key, db.ro)
	if err != nil {
		if err == leveldb.ErrNotFound {
			err = rdddb.ErrBlockShaMissing
		}
		return 0, err
	}

	// deserialize
	blkHeight := binary.LittleEndian.Uint64(data)

	return int64(blkHeight), nil
}

func (db *LevelDb) getBlkByHeight(blkHeight int64) (rsha *rddwire.ShaHash, rbuf []byte, err error) {
	var blkVal []byte

	key := int64ToKey(blkHeight)

	blkVal, err = db.lDb.Get(key, db.ro)
	if err != nil {
		log.Tracef("failed to find height %v", blkHeight)
		return // exists ???
	}

	var sha rddwire.ShaHash

	sha.SetBytes(blkVal[0:32])

	blockdata := make([]byte, len(blkVal[32:]))
	copy(blockdata[:], blkVal[32:])

	return &sha, blockdata, nil
}

func (db *LevelDb) getBlk(sha *rddwire.ShaHash) (rblkHeight int64, rbuf []byte, err error) {
	var blkHeight int64

	blkHeight, err = db.getBlkLoc(sha)
	if err != nil {
		return
	}

	var buf []byte

	_, buf, err = db.getBlkByHeight(blkHeight)
	if err != nil {
		return
	}
	return blkHeight, buf, nil
}

func (db *LevelDb) setBlk(sha *rddwire.ShaHash, blkHeight int64, buf []byte) {
	// serialize
	var lw [8]byte
	binary.LittleEndian.PutUint64(lw[0:8], uint64(blkHeight))

	shaKey := shaBlkToKey(sha)
	blkKey := int64ToKey(blkHeight)

	shaB := sha.Bytes()
	blkVal := make([]byte, len(shaB)+len(buf))
	copy(blkVal[0:], shaB)
	copy(blkVal[len(shaB):], buf)

	db.lBatch().Put(shaKey, lw[:])
	db.lBatch().Put(blkKey, blkVal)
}

// insertSha stores a block hash and its associated data block with a
// previous sha of `prevSha'.
// insertSha shall be called with db lock held
func (db *LevelDb) insertBlockData(sha *rddwire.ShaHash, prevSha *rddwire.ShaHash, buf []byte) (int64, error) {
	oBlkHeight, err := db.getBlkLoc(prevSha)
	if err != nil {
		// check current block count
		// if count != 0  {
		//	err = rdddb.PrevShaMissing
		//	return
		// }
		oBlkHeight = -1
		if db.nextBlock != 0 {
			return 0, err
		}
	}

	// TODO(drahn) check curfile filesize, increment curfile if this puts it over
	blkHeight := oBlkHeight + 1

	db.setBlk(sha, blkHeight, buf)

	// update the last block cache
	db.lastBlkShaCached = true
	db.lastBlkSha = *sha
	db.lastBlkIdx = blkHeight
	db.nextBlock = blkHeight + 1

	return blkHeight, nil
}

// fetchSha returns the datablock for the given ShaHash.
func (db *LevelDb) fetchSha(sha *rddwire.ShaHash) (rbuf []byte,
	rblkHeight int64, err error) {
	var blkHeight int64
	var buf []byte

	blkHeight, buf, err = db.getBlk(sha)
	if err != nil {
		return
	}

	return buf, blkHeight, nil
}

// ExistsSha looks up the given block hash
// returns true if it is present in the database.
func (db *LevelDb) ExistsSha(sha *rddwire.ShaHash) (bool, error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	// not in cache, try database
	return db.blkExistsSha(sha)
}

// blkExistsSha looks up the given block hash
// returns true if it is present in the database.
// CALLED WITH LOCK HELD
func (db *LevelDb) blkExistsSha(sha *rddwire.ShaHash) (bool, error) {
	_, err := db.getBlkLoc(sha)
	switch err {
	case nil:
		return true, nil
	case leveldb.ErrNotFound, rdddb.ErrBlockShaMissing:
		return false, nil
	}
	return false, err
}

// FetchBlockShaByHeight returns a block hash based on its height in the
// block chain.
func (db *LevelDb) FetchBlockShaByHeight(height int64) (sha *rddwire.ShaHash, err error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	return db.fetchBlockShaByHeight(height)
}

// fetchBlockShaByHeight returns a block hash based on its height in the
// block chain.
func (db *LevelDb) fetchBlockShaByHeight(height int64) (rsha *rddwire.ShaHash, err error) {
	key := int64ToKey(height)

	blkVal, err := db.lDb.Get(key, db.ro)
	if err != nil {
		log.Tracef("failed to find height %v", height)
		return // exists ???
	}

	var sha rddwire.ShaHash
	sha.SetBytes(blkVal[0:32])

	return &sha, nil
}

// FetchHeightRange looks up a range of blocks by the start and ending
// heights.  Fetch is inclusive of the start height and exclusive of the
// ending height. To fetch all hashes from the start height until no
// more are present, use the special id `AllShas'.
func (db *LevelDb) FetchHeightRange(startHeight, endHeight int64) (rshalist []rddwire.ShaHash, err error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	var endidx int64
	if endHeight == rdddb.AllShas {
		endidx = startHeight + 500
	} else {
		endidx = endHeight
	}

	shalist := make([]rddwire.ShaHash, 0, endidx-startHeight)
	for height := startHeight; height < endidx; height++ {
		// TODO(drahn) fix blkFile from height

		key := int64ToKey(height)
		blkVal, lerr := db.lDb.Get(key, db.ro)
		if lerr != nil {
			break
		}

		var sha rddwire.ShaHash
		sha.SetBytes(blkVal[0:32])
		shalist = append(shalist, sha)
	}

	if err != nil {
		return
	}
	//log.Tracef("FetchIdxRange idx %v %v returned %v shas err %v", startHeight, endHeight, len(shalist), err)

	return shalist, nil
}

// NewestSha returns the hash and block height of the most recent (end) block of
// the block chain.  It will return the zero hash, -1 for the block height, and
// no error (nil) if there are not any blocks in the database yet.
func (db *LevelDb) NewestSha() (rsha *rddwire.ShaHash, rblkid int64, err error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	if db.lastBlkIdx == -1 {
		return &rddwire.ShaHash{}, -1, nil
	}
	sha := db.lastBlkSha

	return &sha, db.lastBlkIdx, nil
}
