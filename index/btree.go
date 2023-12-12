package index

import (
	"bitcask/data"
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
