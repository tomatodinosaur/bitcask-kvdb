package index

import (
	"bitcask/data"
	"bytes"
	"container/heap"
	"sort"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree"
)

// AdaptiveRadixTree 自适应基数树索引
type AdaptiveRadixTree struct {
	tree     []goart.Tree
	lock     []*sync.RWMutex
	IndexNum int64
}

func NewART(num int64) *AdaptiveRadixTree {
	art := &AdaptiveRadixTree{
		IndexNum: num,
	}

	art.lock = make([]*sync.RWMutex, num)
	art.tree = make([]goart.Tree, num)

	for i := 0; i < int(num); i++ {
		art.lock[i] = new(sync.RWMutex)
		art.tree[i] = goart.New()
	}
	return art
}

// 向内存索引中存储key对应的数据位置信息
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	index := Hash(key, art.IndexNum)
	art.lock[index].Lock()
	old, _ := art.tree[index].Insert(key, pos)
	art.lock[index].Unlock()
	if old == nil {
		return nil
	}
	return old.(*data.LogRecordPos)
}

// 根据key值取出内存中对应的索引位置信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	index := Hash(key, art.IndexNum)
	art.lock[index].RLock()
	defer art.lock[index].RUnlock()
	value, found := art.tree[index].Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

// 根据key值删除对应的索引位置信息
func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	index := Hash(key, art.IndexNum)
	art.lock[index].Lock()
	old, deleted := art.tree[index].Delete(key)
	art.lock[index].Unlock()
	if old == nil {
		return nil, false
	}
	return old.(*data.LogRecordPos), deleted
}

// 返回索引中的个数
func (art *AdaptiveRadixTree) Size() int {
	var size int
	for i := 0; i < int(art.IndexNum); i++ {
		art.lock[i].RLock()
		size += art.tree[i].Size()
		art.lock[i].RUnlock()
	}
	return size
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// 索引迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock[0].RLock()
	defer art.lock[0].RUnlock()
	return NewArtIterator(art.tree, reverse)
}

// Art 索引迭代器
type artIterator struct {
	curIndex int     //当前位置
	reverse  bool    //是否是反向遍历
	values   []*Item //key+LogRecordPos
}

func NewArtIterator(tree []goart.Tree, reverse bool) *artIterator {
	n := len(tree)
	values := make([][]*Item, n)
	for i := 0; i < n; i++ {
		size := tree[i].Size()
		values[i] = make([]*Item, size)
		var idx int
		//将所有数据存放在数组中
		saveValues := func(node goart.Node) bool {
			Item := &Item{
				key: node.Key(),
				pos: node.Value().(*data.LogRecordPos),
			}
			values[i][idx] = Item
			idx++
			return true
		}
		tree[i].ForEach(saveValues)
	}

	output := MergeSortedArrays(values)
	if reverse {
		Reverse(output)
	}
	return &artIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   output,
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

// ItemArray is a slice of Items
type heapItem struct {
	item  *Item
	index int
}

type ItemArray []heapItem

// ItemHeap is a struct that implements heap.Interface
type ItemHeap struct {
	items []heapItem // the slice of items
}

// Len returns the length of the ItemHeap
func (h ItemHeap) Len() int {
	return len(h.items)
}

// Less returns true if the item at i is smaller than the item at j
func (h ItemHeap) Less(i, j int) bool {
	return string(h.items[i].item.key) < string(h.items[j].item.key)
}

// Swap swaps the items at i and j
func (h ItemHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

// Push adds an item to the ItemHeap
func (h *ItemHeap) Push(x interface{}) {
	item := x.(heapItem)
	h.items = append(h.items, item)
}

// Pop removes and returns the smallest item from the ItemHeap
func (h *ItemHeap) Pop() interface{} {
	n := len(h.items)
	item := h.items[n-1]
	h.items = h.items[:n-1]
	return item
}

// MergeSortedArrays merges n sorted ItemArrays into one sorted ItemArray
func MergeSortedArrays(arrays [][]*Item) []*Item {
	// initialize the output array
	n := len(arrays) // the number of arrays
	k := 0           // the maximum length of each array
	for _, array := range arrays {
		if len(array) > k {
			k = len(array)
		}
	}
	output := make([]*Item, 0, n*k)

	// initialize the min heap
	h := &ItemHeap{}
	heap.Init(h)
	for i, array := range arrays {
		if len(array) > 0 {
			// push the first item of each array to the heap
			heap.Push(h, heapItem{array[0], i})
		}
	}

	// repeat n*k times
	for h.Len() > 0 {
		// pop the smallest item from the heap
		item := heap.Pop(h).(heapItem)
		// append it to the output array
		output = append(output, item.item)
		// get the index of the array that the item belongs to
		idx := item.index
		// get the next item from the same array
		arrays[idx] = arrays[idx][1:]
		if len(arrays[idx]) > 0 {
			// push the next item to the heap
			heap.Push(h, heapItem{arrays[idx][0], idx})
		}
	}
	// return the output array
	return output
}

func Reverse(s []*Item) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
