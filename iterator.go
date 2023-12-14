package bitcaskkvdb

import (
	"bitcask/index"
	"bytes"
)

//供用户调用的Iterator <数据迭代器>

type Iterator struct {
	indexIter index.Iterator //<索引迭代器>
	db        *DB
	Options   IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexiter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexiter,
		Options:   opts,
	}
}

// 重新回到迭代器的起点，即第一个数据
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// 根据传入的key，跳转到>= 或（<=）key的第一个位置
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// 跳转到下一个key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// 是否有效，是否已经遍历完所有的key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// 返回当前位置的Key
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// 返回当前位置的Value数据
func (it *Iterator) Value() ([]byte, error) {
	logpos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPostion(logpos)
}

// 关闭迭代器，释放相应资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixlen := len(it.Options.Prefix)
	if prefixlen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixlen <= len(key) && bytes.Equal(it.Options.Prefix, key[:prefixlen]) {
			break
		}
	}
}
