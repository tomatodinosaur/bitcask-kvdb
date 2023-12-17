package bitcaskkvdb

import (
	"bitcask/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_Merge(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 1000; i++ {
		db.Put(utils.GetTestKey(i), utils.RandomValue(4))
	}
	for i := 0; i < 800; i++ {
		db.Delete(utils.GetTestKey(i))
	}

	keys := db.ListKeys()
	t.Log(len(keys))
	db.Merge()

	db.Close()

	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	keys = db2.ListKeys()
	t.Log(len(keys))
}
