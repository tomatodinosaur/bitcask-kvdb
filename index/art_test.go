package index

import (
	"bitcask/data"
	"bitcask/utils"
	"testing"
)

func TestArt(t *testing.T) {

	art := NewART()
	art.Put(utils.GetTestKey(1), &data.LogRecordPos{1, 12})
	art.Put(utils.GetTestKey(2), &data.LogRecordPos{2, 12})
	art.Put(utils.GetTestKey(3), &data.LogRecordPos{3, 12})

	art.Delete(utils.GetTestKey(2))
	pos := art.Get(utils.GetTestKey(2))
	t.Log(pos)

	iter := art.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}
}
