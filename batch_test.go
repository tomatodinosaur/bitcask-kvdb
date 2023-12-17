package bitcaskkvdb

import (
	"bitcask/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_WriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//写数据后并不提交
	wb := db.NewWriteBatch(DefalutWriteBatchOptions)
	wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	wb.Delete(utils.GetTestKey(2))
	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, err, ErrKeyNotFind)

	//正常提交数据
	t.Log(db.seqNo)
	wb.Commit()
	val, err := db.Get(utils.GetTestKey(1))
	t.Log(string(val))
	assert.Nil(t, err)

	wb2 := db.NewWriteBatch(DefalutWriteBatchOptions)
	wb2.Delete(utils.GetTestKey(1))

	val, err = db.Get(utils.GetTestKey(1))
	t.Log(string(val))
	assert.Nil(t, err)
	t.Log(db.seqNo)
	wb2.Commit()
	t.Log(db.seqNo)

	val, err = db.Get(utils.GetTestKey(1))
	t.Log(string(val))
	assert.Equal(t, err, ErrKeyNotFind)

	wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	wb.Put(utils.GetTestKey(3), utils.RandomValue(10))
	wb.Commit()
	t.Log(db.seqNo)

	//重启
	db.Close()
	db2, _ := Open(opts)
	val, err = db2.Get(utils.GetTestKey(1))
	t.Log(string(val))
	assert.Equal(t, err, ErrKeyNotFind)
	assert.Equal(t, db2.seqNo, int64(3))
	t.Log(db2.seqNo)
}

func TestDB_WriteBatchBig(t *testing.T) {
	opts := DefaultOptions
	dir := "/tmp/bitcask-go-batch1"
	opts.Dirpath = dir
	db, err := Open(opts)

	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys := db.ListKeys()
	t.Log(len(keys))

	// wbops := DefalutWriteBatchOptions
	// wbops.MaxBatchNum = 1000000
	// wb := db.NewWriteBatch(wbops)
	// for i := 0; i < 500000; i++ {
	// 	err := wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
	// 	assert.Nil(t, err)
	// }
	// err = wb.Commit()
	assert.Nil(t, err)
}
