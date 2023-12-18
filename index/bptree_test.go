package index

import (
	"bitcask/data"
	"bitcask/utils"
	"os"
	"path/filepath"
	"testing"
)

func TestNew(t *testing.T) {
	path := filepath.Join("/tmp")
	tree := NewBPlusTree(path, false)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree.Put(utils.GetTestKey(1), &data.LogRecordPos{Fid: 1, Offset: 12})
	tree.Put(utils.GetTestKey(2), &data.LogRecordPos{Fid: 2, Offset: 12})
	tree.Put(utils.GetTestKey(3), &data.LogRecordPos{Fid: 3, Offset: 12})
	val := tree.Get(utils.GetTestKey(2))
	t.Log(val)

	tree.Delete(utils.GetTestKey(2))
	val = tree.Get(utils.GetTestKey(2))
	t.Log(val)
	t.Log(tree.Size())

	iter := tree.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}
}
