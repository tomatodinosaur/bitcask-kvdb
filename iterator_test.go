package bitcaskkvdb

import (
	"bitcask/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	Iterator := db.NewIterator(DefalutIteratorOptions)
	assert.NotNil(t, Iterator)
	assert.Equal(t, false, Iterator.Valid())

	val := utils.RandomValue(128)
	db.Put(utils.GetTestKey(10), val)

	iter := db.NewIterator(DefalutIteratorOptions)
	assert.NotNil(t, iter)
	val1, _ := iter.Value()
	assert.Equal(t, val1, val)

	db.Put(utils.GetTestKey(1), utils.RandomValue(4))
	db.Put(utils.GetTestKey(2), utils.RandomValue(4))
	db.Put(utils.GetTestKey(3), utils.RandomValue(4))
	db.Put(utils.GetTestKey(4), utils.RandomValue(4))
	db.Put(utils.GetTestKey(5), utils.RandomValue(4))

	// for iter = db.NewIterator(DefalutIteratorOptions); iter.Valid(); iter.Next() {
	// 	value, _ := iter.Value()
	// 	t.Log(string(value))
	// }

	iteropts := DefalutIteratorOptions
	iteropts.Prefix = []byte("b")
	iter = db.NewIterator(iteropts)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, _ := iter.Value()
		t.Log(string(value))
	}
}
