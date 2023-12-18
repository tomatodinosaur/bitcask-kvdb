package index

import (
	"bitcask/data"
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
)

//BTree实现内存索引，主要封装了google的btree kv

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// INIT
func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := Item{key, pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(&it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := Item{key: key}
	res := bt.tree.Get(&it)
	if res == nil {
		return nil
	}
	return res.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := Item{key: key}
	bt.lock.Lock()
	oldterm := bt.tree.Delete(&it)
	bt.lock.Unlock()
	return oldterm != nil
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Close() error {
	return nil
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return NewBtreeIterator(bt.tree, reverse)
}

// BTREE 索引迭代器
type btreeIterator struct {
	curIndex int     //当前位置
	reverse  bool    //是否是反向遍历
	values   []*Item //key+LogRecordPos
}

func NewBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	//将所有数据存放在数组中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

// 重新回到迭代器的起点，即第一个数据
func (bti *btreeIterator) Rewind() {
	bti.curIndex = 0
}

// 根据传入的key，跳转到>= 或（<=）key的第一个位置
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

// 跳转到下一个key
func (bti *btreeIterator) Next() {
	bti.curIndex++
}

// 是否有效，是否已经遍历完所有的key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.curIndex >= 0 && bti.curIndex < len(bti.values)
}

// 返回当前位置的Key
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.curIndex].key
}

// 返回当前位置的Value数据
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.curIndex].pos
}

// 关闭迭代器，释放相应资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}
