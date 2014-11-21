// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ldb

import (
	"fmt"

	"github.com/reddcoin-project/rdddb"
	"github.com/reddcoin-project/rddwire"
)

// FetchSha returns the datablock and pver for the given ShaHash.
// This is a testing only interface.
func FetchSha(db rdddb.Db, sha *rddwire.ShaHash) (buf []byte, pver uint32,
	blkid int64, err error) {
	sqldb, ok := db.(*LevelDb)
	if !ok {
		err = fmt.Errorf("invalid data type")
		return
	}
	buf, blkid, err = sqldb.fetchSha(sha)
	return
}
