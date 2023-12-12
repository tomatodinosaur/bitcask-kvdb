package index

import (
	"bitcask/data"
	"bytes"

	"github.com/google/btree"
)

// 内存索引接口
type index interface {
	//向内存索引中存储key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	//根据key值取出内存中对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	//根据key值删除对应的索引位置信息
	Delete(key []byte) bool
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (a *Item) Less(b btree.Item) bool {
	//if a<b ,compare return -1 ,so Less return 1
	return bytes.Compare(a.key, b.(*Item).key) == -1
}
