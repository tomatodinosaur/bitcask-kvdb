package bitcaskkvdb

import (
	"bitcask/utils"
	"bytes"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 测试完成之后销毁DB数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activefile != nil {
			_ = db.activefile.Close()
		}
		err := os.RemoveAll(db.options.Dirpath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//1、正常Put一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	//2、重复Put key相同的数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val, err = db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	//3.key为空
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, err, ErrKeyIsEmpty)

	//4.value为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)

	//5.写到数据文件进行了转化
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(1), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderfile))

	//6、重启后再Put
	err = db.activefile.Close()
	assert.Nil(t, err)
	db.Close()
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val = utils.RandomValue(128)
	db2.Put(utils.GetTestKey(10), val)
	val1, _ := db2.Get(utils.GetTestKey(10))
	assert.Equal(t, val1, val)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//1、读取不存在的key
	val, err := db.Get([]byte("key"))
	assert.Nil(t, val)
	assert.Equal(t, ErrKeyNotFind, err)

	//2、重复Put后读取
	db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val, err = db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	//3、删除后再读取
	err = db.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	val, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, err, ErrKeyNotFind)
	assert.Equal(t, 0, len(val))
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//删除不存在的一个key
	err = db.Delete([]byte("name"))
	assert.Nil(t, err)

	//空key
	err = db.Delete(nil)
	assert.Equal(t, err, ErrKeyIsEmpty)

	//删除后再Put
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete([]byte(utils.GetTestKey(1)))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	db.Close()
	//重启之后在进行校验
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val1, _ := db2.Get(utils.GetTestKey(1))
	assert.Equal(t, val1, val)
}

func Test_ListKeys(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//1、正常Put一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	keys := db.ListKeys()
	assert.Equal(t, len(keys), 1)

	//2、重复Put key相同的数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val, err = db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	keys = db.ListKeys()
	assert.Equal(t, len(keys), 1)

	//3.key为空
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, err, ErrKeyIsEmpty)
	keys = db.ListKeys()
	assert.Equal(t, len(keys), 1)

	//4.value为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	keys = db.ListKeys()
	assert.Equal(t, len(keys), 2)

	//5.写到数据文件进行了转化
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(1), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderfile))

	//6、重启后再Put
	db.Close()
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val = utils.RandomValue(128)
	db2.Put(utils.GetTestKey(10), val)
	val1, _ := db2.Get(utils.GetTestKey(10))
	assert.Equal(t, val1, val)
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(4))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(2), utils.RandomValue(4))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(3), utils.RandomValue(4))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(4), utils.RandomValue(4))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		t.Log(string(key))
		t.Log(string(value))
		return !bytes.Equal(key, utils.GetTestKey(3))
	})

	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	db.Close()
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(4))
	assert.Nil(t, err)
}

func TestDB_Multithreading(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	var wg sync.WaitGroup
	wg.Add(300)
	for j := 0; j < 100; j++ {
		start := j * 10000
		go func() {
			for i := start; i < start+10000; i++ {
				db.Put(utils.GetTestKey(i), utils.RandomValue(4))
			}
			wg.Done()
		}()
		go func() {
			for i := start; i < start+10000; i++ {
				db.Get(utils.GetTestKey(i))
			}
			wg.Done()
		}()
		go func() {
			for i := start; i < start+10000; i++ {
				db.Delete(utils.GetTestKey(i))
			}
			wg.Done()
		}()
	}
	wg.Wait()

	stat := db.Stat()
	t.Log(stat)
}

func TestMultiDelete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		db.Put(utils.GetTestKey(i), utils.RandomValue(4))
	}
	wg.Add(100)

	for j := 0; j < 100; j++ {
		go func() {
			for i := 0; i < 10000; i++ {
				db.Delete(utils.GetTestKey(i))
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestDB_Easythreading(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		for i := 0; i < 10000; i++ {
			db.Put(utils.GetTestKey(i), utils.RandomValue(4))
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < 10000; i++ {
			db.Get(utils.GetTestKey(i))
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 10000; i++ {
			db.Delete(utils.GetTestKey(i))
		}
		wg.Done()
	}()

	wg.Wait()
}

func TestDB_FileLock(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	db2, err := Open(opts)
	assert.Nil(t, db2)
	assert.Equal(t, err, ErrDataBaseIsUsing)

	db.Close()
	db2, err = Open(opts)
	assert.NotNil(t, db2)
	assert.Nil(t, err)

}

func TestMMapIo(t *testing.T) {
	opts := DefaultOptions
	opts.Dirpath = "/tmp/bit"
	opts.DataFileSize = 64 * 1024
	opts.MMapOpen = true
	db, err := Open(opts)

	// //defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// for i := 0; i < 100000000; i++ {
	// 	db.Put(utils.GetTestKey(i), utils.RandomValue(4))
	// }
}

func TestStat(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	for i := 0; i < 10000; i++ {
		db.Put(utils.GetTestKey(i), utils.RandomValue(4))
	}
	for i := 0; i < 10000; i++ {
		db.Get(utils.GetTestKey(i))
	}

	for i := 0; i < 10000; i++ {
		db.Delete(utils.GetTestKey(i))
	}

	// var wg sync.WaitGroup
	// wg.Add(2)
	// go func() {
	// 	for i := 0; i < 100000; i++ {
	// 		db.Put(utils.GetTestKey(i), utils.RandomValue(4))
	// 	}
	// 	wg.Done()
	// }()

	// go func() {
	// 	for i := 0; i < 100000; i++ {
	// 		db.Delete(utils.GetTestKey(i))
	// 	}
	// 	wg.Done()
	// }()
	// wg.Wait()

	stat := db.Stat()
	t.Log(stat)
}

func TestBackUp(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.Dirpath = dir
	opts.DataFileSize = 64 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	for i := 0; i < 10000; i++ {
		db.Put(utils.GetTestKey(i), utils.RandomValue(4))
	}
	for i := 0; i < 10000; i++ {
		db.Get(utils.GetTestKey(i))
	}

	for i := 0; i < 10000; i++ {
		db.Delete(utils.GetTestKey(i))
	}
	stat := db.Stat()

	backupdir, _ := os.MkdirTemp("", "bitcask-go-backup")
	db.BackUp(backupdir)
	db.Close()
	opts.Dirpath = backupdir
	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	stat2 := db2.Stat()

	assert.Equal(t, stat, stat2)
}
