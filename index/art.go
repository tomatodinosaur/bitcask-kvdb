package index

import (
	"bitcask/data"
	"bytes"
	"sort"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree"
)

// AdaptiveRadixTree 自适应基数树索引
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

// 向内存索引中存储key对应的数据位置信息
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	art.tree.Insert(key, pos)
	art.lock.Unlock()
	return true
}

// 根据key值取出内存中对应的索引位置信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

// 根据key值删除对应的索引位置信息
func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	_, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	return deleted
}

// 返回索引中的个数
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// 索引迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return NewArtIterator(art.tree, reverse)
}

// Art 索引迭代器
type artIterator struct {
	curIndex int     //当前位置
	reverse  bool    //是否是反向遍历
	values   []*Item //key+LogRecordPos
}

func NewArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())

	//将所有数据存放在数组中
	saveValues := func(node goart.Node) bool {
		Item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = Item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValues)

	return &artIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

// 重新回到迭代器的起点，即第一个数据
func (ai *artIterator) Rewind() {
	ai.curIndex = 0
}

// 根据传入的key，跳转到>= 或（<=）key的第一个位置
func (ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		ai.curIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.curIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

// 跳转到下一个key
func (ai *artIterator) Next() {
	ai.curIndex++
}

// 是否有效，是否已经遍历完所有的key，用于退出遍历
func (ai *artIterator) Valid() bool {
	return ai.curIndex >= 0 && ai.curIndex < len(ai.values)
}

// 返回当前位置的Key
func (ai *artIterator) Key() []byte {
	return ai.values[ai.curIndex].key
}

// 返回当前位置的Value数据
func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.curIndex].pos
}

// 关闭迭代器，释放相应资源
func (ai *artIterator) Close() {
	ai.values = nil
}
