package index

import (
	"bitcask/data"
	"bytes"

	"github.com/google/btree"
)

// 内存索引接口
type Indexer interface {
	//向内存索引中存储key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	//根据key值取出内存中对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	//根据key值删除对应的索引位置信息
	Delete(key []byte) bool

	//返回索引中的个数
	Size() int

	//索引迭代器
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	Btree IndexType = iota + 1

	ART
)

// NEWIndexer 根据类型初始化索引
func NEWIndexer(tp IndexType) Indexer {
	switch tp {
	case Btree:
		return NewBtree()
	case ART:
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (a *Item) Less(b btree.Item) bool {
	//if a<b ,compare return -1 ,so Less return 1
	return bytes.Compare(a.key, b.(*Item).key) == -1
}

// 通用<索引迭代器>
type Iterator interface {
	//重新回到迭代器的起点，即第一个数据
	Rewind()

	//根据传入的key，跳转到>= 或（<=）key的第一个位置
	Seek(key []byte)

	//跳转到下一个key
	Next()

	//是否有效，是否已经遍历完所有的key，用于退出遍历
	Valid() bool

	//返回当前位置的Key
	Key() []byte

	//返回当前位置的Value数据
	Value() *data.LogRecordPos

	//关闭迭代器，释放相应资源
	Close()
}
