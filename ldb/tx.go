// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ldb

import (
	"bytes"
	"encoding/binary"

	"github.com/conformal/goleveldb/leveldb"
	"github.com/reddcoin-project/rdddb"
	"github.com/reddcoin-project/rddwire"
)

type txUpdateObj struct {
	txSha     *rddwire.ShaHash
	blkHeight int64
	txoff     int
	txlen     int
	ntxout    int
	spentData []byte
	delete    bool
}

type spentTx struct {
	blkHeight int64
	txoff     int
	txlen     int
	numTxO    int
	delete    bool
}
type spentTxUpdate struct {
	txl    []*spentTx
	delete bool
}

// InsertTx inserts a tx hash and its associated data into the database.
func (db *LevelDb) InsertTx(txsha *rddwire.ShaHash, height int64, txoff int, txlen int, spentbuf []byte) (err error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	return db.insertTx(txsha, height, txoff, txlen, spentbuf)
}

// insertTx inserts a tx hash and its associated data into the database.
// Must be called with db lock held.
func (db *LevelDb) insertTx(txSha *rddwire.ShaHash, height int64, txoff int, txlen int, spentbuf []byte) (err error) {
	var txU txUpdateObj

	txU.txSha = txSha
	txU.blkHeight = height
	txU.txoff = txoff
	txU.txlen = txlen
	txU.spentData = spentbuf

	db.txUpdateMap[*txSha] = &txU

	return nil
}

// formatTx generates the value buffer for the Tx db.
func (db *LevelDb) formatTx(txu *txUpdateObj) []byte {
	blkHeight := uint64(txu.blkHeight)
	txOff := uint32(txu.txoff)
	txLen := uint32(txu.txlen)
	spentbuf := txu.spentData

	txW := make([]byte, 16+len(spentbuf))
	binary.LittleEndian.PutUint64(txW[0:8], blkHeight)
	binary.LittleEndian.PutUint32(txW[8:12], txOff)
	binary.LittleEndian.PutUint32(txW[12:16], txLen)
	copy(txW[16:], spentbuf)

	return txW[:]
}

func (db *LevelDb) getTxData(txsha *rddwire.ShaHash) (int64, int, int, []byte, error) {
	key := shaTxToKey(txsha)
	buf, err := db.lDb.Get(key, db.ro)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	blkHeight := binary.LittleEndian.Uint64(buf[0:8])
	txOff := binary.LittleEndian.Uint32(buf[8:12])
	txLen := binary.LittleEndian.Uint32(buf[12:16])

	spentBuf := make([]byte, len(buf)-16)
	copy(spentBuf, buf[16:])

	return int64(blkHeight), int(txOff), int(txLen), spentBuf, nil
}

func (db *LevelDb) getTxFullySpent(txsha *rddwire.ShaHash) ([]*spentTx, error) {

	var badTxList, spentTxList []*spentTx

	key := shaSpentTxToKey(txsha)
	buf, err := db.lDb.Get(key, db.ro)
	if err == leveldb.ErrNotFound {
		return badTxList, rdddb.ErrTxShaMissing
	} else if err != nil {
		return badTxList, err
	}
	txListLen := len(buf) / 20

	spentTxList = make([]*spentTx, txListLen, txListLen)
	for i := range spentTxList {
		offset := i * 20

		blkHeight := binary.LittleEndian.Uint64(buf[offset : offset+8])
		txOff := binary.LittleEndian.Uint32(buf[offset+8 : offset+12])
		txLen := binary.LittleEndian.Uint32(buf[offset+12 : offset+16])
		numTxO := binary.LittleEndian.Uint32(buf[offset+16 : offset+20])

		sTx := spentTx{
			blkHeight: int64(blkHeight),
			txoff:     int(txOff),
			txlen:     int(txLen),
			numTxO:    int(numTxO),
		}

		spentTxList[i] = &sTx
	}

	return spentTxList, nil
}

func (db *LevelDb) formatTxFullySpent(sTxList []*spentTx) []byte {
	txW := make([]byte, 20*len(sTxList))

	for i, sTx := range sTxList {
		blkHeight := uint64(sTx.blkHeight)
		txOff := uint32(sTx.txoff)
		txLen := uint32(sTx.txlen)
		numTxO := uint32(sTx.numTxO)
		offset := i * 20

		binary.LittleEndian.PutUint64(txW[offset:offset+8], blkHeight)
		binary.LittleEndian.PutUint32(txW[offset+8:offset+12], txOff)
		binary.LittleEndian.PutUint32(txW[offset+12:offset+16], txLen)
		binary.LittleEndian.PutUint32(txW[offset+16:offset+20], numTxO)
	}

	return txW
}

// ExistsTxSha returns if the given tx sha exists in the database
func (db *LevelDb) ExistsTxSha(txsha *rddwire.ShaHash) (bool, error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	return db.existsTxSha(txsha)
}

// existsTxSha returns if the given tx sha exists in the database.o
// Must be called with the db lock held.
func (db *LevelDb) existsTxSha(txSha *rddwire.ShaHash) (bool, error) {
	_, _, _, _, err := db.getTxData(txSha)
	switch err {
	case nil:
		return true, nil
	case leveldb.ErrNotFound:
		return false, nil
	}
	return false, err
}

// FetchTxByShaList returns the most recent tx of the name fully spent or not
func (db *LevelDb) FetchTxByShaList(txShaList []*rddwire.ShaHash) []*rdddb.TxListReply {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	// until the fully spent separation of tx is complete this is identical
	// to FetchUnSpentTxByShaList
	replies := make([]*rdddb.TxListReply, len(txShaList))
	for i, txsha := range txShaList {
		tx, blockSha, height, txspent, err := db.fetchTxDataBySha(txsha)
		btxspent := []bool{}
		if err == nil {
			btxspent = make([]bool, len(tx.TxOut), len(tx.TxOut))
			for idx := range tx.TxOut {
				byteidx := idx / 8
				byteoff := uint(idx % 8)
				btxspent[idx] = (txspent[byteidx] & (byte(1) << byteoff)) != 0
			}
		}
		if err == rdddb.ErrTxShaMissing {
			// if the unspent pool did not have the tx,
			// look in the fully spent pool (only last instance

			sTxList, fSerr := db.getTxFullySpent(txsha)
			if fSerr == nil && len(sTxList) != 0 {
				idx := len(sTxList) - 1
				stx := sTxList[idx]

				tx, blockSha, _, _, err = db.fetchTxDataByLoc(
					stx.blkHeight, stx.txoff, stx.txlen, []byte{})
				if err == nil {
					btxspent = make([]bool, len(tx.TxOut))
					for i := range btxspent {
						btxspent[i] = true
					}
				}
			}
		}
		txlre := rdddb.TxListReply{Sha: txsha, Tx: tx, BlkSha: blockSha, Height: height, TxSpent: btxspent, Err: err}
		replies[i] = &txlre
	}
	return replies
}

// FetchUnSpentTxByShaList given a array of ShaHash, look up the transactions
// and return them in a TxListReply array.
func (db *LevelDb) FetchUnSpentTxByShaList(txShaList []*rddwire.ShaHash) []*rdddb.TxListReply {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	replies := make([]*rdddb.TxListReply, len(txShaList))
	for i, txsha := range txShaList {
		tx, blockSha, height, txspent, err := db.fetchTxDataBySha(txsha)
		btxspent := []bool{}
		if err == nil {
			btxspent = make([]bool, len(tx.TxOut), len(tx.TxOut))
			for idx := range tx.TxOut {
				byteidx := idx / 8
				byteoff := uint(idx % 8)
				btxspent[idx] = (txspent[byteidx] & (byte(1) << byteoff)) != 0
			}
		}
		txlre := rdddb.TxListReply{Sha: txsha, Tx: tx, BlkSha: blockSha, Height: height, TxSpent: btxspent, Err: err}
		replies[i] = &txlre
	}
	return replies
}

// fetchTxDataBySha returns several pieces of data regarding the given sha.
func (db *LevelDb) fetchTxDataBySha(txsha *rddwire.ShaHash) (rtx *rddwire.MsgTx, rblksha *rddwire.ShaHash, rheight int64, rtxspent []byte, err error) {
	var blkHeight int64
	var txspent []byte
	var txOff, txLen int

	blkHeight, txOff, txLen, txspent, err = db.getTxData(txsha)
	if err != nil {
		if err == leveldb.ErrNotFound {
			err = rdddb.ErrTxShaMissing
		}
		return
	}
	return db.fetchTxDataByLoc(blkHeight, txOff, txLen, txspent)
}

// fetchTxDataByLoc returns several pieces of data regarding the given tx
// located by the block/offset/size location
func (db *LevelDb) fetchTxDataByLoc(blkHeight int64, txOff int, txLen int, txspent []byte) (rtx *rddwire.MsgTx, rblksha *rddwire.ShaHash, rheight int64, rtxspent []byte, err error) {
	var blksha *rddwire.ShaHash
	var blkbuf []byte

	blksha, blkbuf, err = db.getBlkByHeight(blkHeight)
	if err != nil {
		if err == leveldb.ErrNotFound {
			err = rdddb.ErrTxShaMissing
		}
		return
	}

	//log.Trace("transaction %v is at block %v %v txoff %v, txlen %v\n",
	//	txsha, blksha, blkHeight, txOff, txLen)

	if len(blkbuf) < txOff+txLen {
		err = rdddb.ErrTxShaMissing
		return
	}
	rbuf := bytes.NewReader(blkbuf[txOff : txOff+txLen])

	var tx rddwire.MsgTx
	err = tx.Deserialize(rbuf)
	if err != nil {
		log.Warnf("unable to decode tx block %v %v txoff %v txlen %v",
			blkHeight, blksha, txOff, txLen)
		return
	}

	return &tx, blksha, blkHeight, txspent, nil
}

// FetchTxBySha returns some data for the given Tx Sha.
func (db *LevelDb) FetchTxBySha(txsha *rddwire.ShaHash) ([]*rdddb.TxListReply, error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	replylen := 0
	replycnt := 0

	tx, blksha, height, txspent, txerr := db.fetchTxDataBySha(txsha)
	if txerr == nil {
		replylen++
	} else {
		if txerr != rdddb.ErrTxShaMissing {
			return []*rdddb.TxListReply{}, txerr
		}
	}

	sTxList, fSerr := db.getTxFullySpent(txsha)

	if fSerr != nil {
		if fSerr != rdddb.ErrTxShaMissing {
			return []*rdddb.TxListReply{}, fSerr
		}
	} else {
		replylen += len(sTxList)
	}

	replies := make([]*rdddb.TxListReply, replylen)

	if fSerr == nil {
		for _, stx := range sTxList {
			tx, blksha, _, _, err := db.fetchTxDataByLoc(
				stx.blkHeight, stx.txoff, stx.txlen, []byte{})
			if err != nil {
				if err != leveldb.ErrNotFound {
					return []*rdddb.TxListReply{}, err
				}
				continue
			}
			btxspent := make([]bool, len(tx.TxOut), len(tx.TxOut))
			for i := range btxspent {
				btxspent[i] = true
			}
			txlre := rdddb.TxListReply{Sha: txsha, Tx: tx, BlkSha: blksha, Height: stx.blkHeight, TxSpent: btxspent, Err: nil}
			replies[replycnt] = &txlre
			replycnt++
		}
	}
	if txerr == nil {
		btxspent := make([]bool, len(tx.TxOut), len(tx.TxOut))
		for idx := range tx.TxOut {
			byteidx := idx / 8
			byteoff := uint(idx % 8)
			btxspent[idx] = (txspent[byteidx] & (byte(1) << byteoff)) != 0
		}
		txlre := rdddb.TxListReply{Sha: txsha, Tx: tx, BlkSha: blksha, Height: height, TxSpent: btxspent, Err: nil}
		replies[replycnt] = &txlre
		replycnt++
	}
	return replies, nil
}
