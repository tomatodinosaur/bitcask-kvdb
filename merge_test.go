package bitcaskkvdb

import (
	"bitcask/utils"
	"os"
	"sync"
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

func TestWriteWhileMerge(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 1024 * 1024
	opts.DataFileMergeRatio = 0
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 60000; i < 70000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
			assert.Nil(t, err)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50000; i++ {
			err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
	}()
	//time.Sleep(time.Millisecond * 100)
	//t.Log(len(db.ListKeys()))
	err = db.Merge()
	assert.Nil(t, err)
	wg.Wait()

	keys := db.ListKeys()
	assert.Equal(t, 10000, len(keys))

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys = db2.ListKeys()
	assert.Equal(t, 10000, len(keys))

	for i := 60000; i < 70000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}
