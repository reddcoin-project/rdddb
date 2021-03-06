// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ldb_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/reddcoin-project/rdddb"
	_ "github.com/reddcoin-project/rdddb/ldb"
	"github.com/reddcoin-project/rddutil"
	"github.com/reddcoin-project/rddwire"
)

var tstBlocks []*rddutil.Block

func loadblocks(t *testing.T) []*rddutil.Block {
	if len(tstBlocks) != 0 {
		return tstBlocks
	}

	testdatafile := filepath.Join("..", "testdata", "blocks1-189.bz2")
	blocks, err := loadBlocks(t, testdatafile)
	if err != nil {
		t.Errorf("Unable to load blocks from test data: %v", err)
		return nil
	}
	tstBlocks = blocks
	return blocks
}

func TestUnspentInsert(t *testing.T) {
	testUnspentInsert(t)
}

// insert every block in the test chain
// after each insert, fetch all the tx affected by the latest
// block and verify that the the tx is spent/unspent
// new tx should be fully unspent, referenced tx should have
// the associated txout set to spent.
func testUnspentInsert(t *testing.T) {
	// Ignore db remove errors since it means we didn't have an old one.
	dbname := fmt.Sprintf("tstdbuspnt1")
	dbnamever := dbname + ".ver"
	_ = os.RemoveAll(dbname)
	_ = os.RemoveAll(dbnamever)
	db, err := rdddb.CreateDB("leveldb", dbname)
	if err != nil {
		t.Errorf("Failed to open test database %v", err)
		return
	}
	defer os.RemoveAll(dbname)
	defer os.RemoveAll(dbnamever)
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: unexpected error: %v", err)
		}
	}()

	blocks := loadblocks(t)
endtest:
	for height := int64(0); height < int64(len(blocks)); height++ {

		block := blocks[height]
		// look up inputs to this x
		mblock := block.MsgBlock()
		var txneededList []*rddwire.ShaHash
		var txlookupList []*rddwire.ShaHash
		var txOutList []*rddwire.ShaHash
		var txInList []*rddwire.OutPoint
		for _, tx := range mblock.Transactions {
			for _, txin := range tx.TxIn {
				if txin.PreviousOutPoint.Index == uint32(4294967295) {
					continue
				}
				origintxsha := &txin.PreviousOutPoint.Hash

				txInList = append(txInList, &txin.PreviousOutPoint)
				txneededList = append(txneededList, origintxsha)
				txlookupList = append(txlookupList, origintxsha)

				exists, err := db.ExistsTxSha(origintxsha)
				if err != nil {
					t.Errorf("ExistsTxSha: unexpected error %v ", err)
				}
				if !exists {
					t.Errorf("referenced tx not found %v ", origintxsha)
				}
			}
			txshaname, _ := tx.TxSha()
			txlookupList = append(txlookupList, &txshaname)
			txOutList = append(txOutList, &txshaname)
		}

		txneededmap := map[rddwire.ShaHash]*rdddb.TxListReply{}
		txlist := db.FetchUnSpentTxByShaList(txneededList)
		for _, txe := range txlist {
			if txe.Err != nil {
				t.Errorf("tx list fetch failed %v err %v ", txe.Sha, txe.Err)
				break endtest
			}
			txneededmap[*txe.Sha] = txe
		}
		for _, spend := range txInList {
			itxe := txneededmap[spend.Hash]
			if itxe.TxSpent[spend.Index] == true {
				t.Errorf("txin %v:%v is already spent", spend.Hash, spend.Index)
			}
		}

		newheight, err := db.InsertBlock(block)
		if err != nil {
			t.Errorf("failed to insert block %v err %v", height, err)
			break endtest
		}
		if newheight != height {
			t.Errorf("height mismatch expect %v returned %v", height, newheight)
			break endtest
		}

		txlookupmap := map[rddwire.ShaHash]*rdddb.TxListReply{}
		txlist = db.FetchTxByShaList(txlookupList)
		for _, txe := range txlist {
			if txe.Err != nil {
				t.Errorf("tx list fetch failed %v err %v ", txe.Sha, txe.Err)
				break endtest
			}
			txlookupmap[*txe.Sha] = txe
		}
		for _, spend := range txInList {
			itxe := txlookupmap[spend.Hash]
			if itxe.TxSpent[spend.Index] == false {
				t.Errorf("txin %v:%v is unspent %v", spend.Hash, spend.Index, itxe.TxSpent)
			}
		}
		for _, txo := range txOutList {
			itxe := txlookupmap[*txo]
			for i, spent := range itxe.TxSpent {
				if spent == true {
					t.Errorf("freshly inserted tx %v already spent %v", txo, i)
				}
			}

		}
		if len(txInList) == 0 {
			continue
		}
		dropblock := blocks[height-1]
		dropsha, _ := dropblock.Sha()

		err = db.DropAfterBlockBySha(dropsha)
		if err != nil {
			t.Errorf("failed to drop block %v err %v", height, err)
			break endtest
		}

		txlookupmap = map[rddwire.ShaHash]*rdddb.TxListReply{}
		txlist = db.FetchUnSpentTxByShaList(txlookupList)
		for _, txe := range txlist {
			if txe.Err != nil {
				if _, ok := txneededmap[*txe.Sha]; ok {
					t.Errorf("tx list fetch failed %v err %v ", txe.Sha, txe.Err)
					break endtest
				}
			}
			txlookupmap[*txe.Sha] = txe
		}
		for _, spend := range txInList {
			itxe := txlookupmap[spend.Hash]
			if itxe.TxSpent[spend.Index] == true {
				t.Errorf("txin %v:%v is unspent %v", spend.Hash, spend.Index, itxe.TxSpent)
			}
		}
		newheight, err = db.InsertBlock(block)
		if err != nil {
			t.Errorf("failed to insert block %v err %v", height, err)
			break endtest
		}
		txlookupmap = map[rddwire.ShaHash]*rdddb.TxListReply{}
		txlist = db.FetchTxByShaList(txlookupList)
		for _, txe := range txlist {
			if txe.Err != nil {
				t.Errorf("tx list fetch failed %v err %v ", txe.Sha, txe.Err)
				break endtest
			}
			txlookupmap[*txe.Sha] = txe
		}
		for _, spend := range txInList {
			itxe := txlookupmap[spend.Hash]
			if itxe.TxSpent[spend.Index] == false {
				t.Errorf("txin %v:%v is unspent %v", spend.Hash, spend.Index, itxe.TxSpent)
			}
		}
	}
}
