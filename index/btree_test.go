package index

import (
	"bitcask/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBtree_Put(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res2)
}

func TestBtree_Get(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	pos2 := bt.Get([]byte("abc"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(2), pos2.Offset)

	res3 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 200})
	assert.True(t, res3)
	pos3 := bt.Get([]byte("abc"))
	assert.Equal(t, uint32(1), pos3.Fid)
	assert.Equal(t, int64(200), pos3.Offset)
}

func TestBtree_Delete(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res3)
	res4 := bt.Delete(nil)
	assert.True(t, !res4)

	res5 := bt.Delete([]byte("abc"))
	assert.True(t, res5)
}
