package index

import (
	"bitcask/data"
	"bitcask/utils"
	"testing"
)

func TestArt(t *testing.T) {

	art := NewART()
	art.Put(utils.GetTestKey(1), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put(utils.GetTestKey(2), &data.LogRecordPos{Fid: 2, Offset: 12})
	art.Put(utils.GetTestKey(3), &data.LogRecordPos{Fid: 3, Offset: 12})

	art.Delete(utils.GetTestKey(2))
	pos := art.Get(utils.GetTestKey(2))
	t.Log(pos)

	iter := art.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}
}
