package index

import (
	"bitcask/data"
	"bitcask/utils"
	"sync"
	"testing"
)

func TestArt(t *testing.T) {

	art := NewART(10)
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

func TestArt_MulitiThread(t *testing.T) {
	art := NewART(10)
	var wg sync.WaitGroup
	wg.Add(300)
	for j := 0; j < 100; j++ {
		go func() {
			for i := 0; i < 100000; i++ {
				art.Put(utils.GetTestKey(i), &data.LogRecordPos{Fid: uint32(i), Offset: 12})
			}
			wg.Done()
		}()
	}
	for j := 0; j < 100; j++ {
		go func() {
			for i := 0; i < 100000; i++ {
				art.Get(utils.GetTestKey(i))
			}
			wg.Done()
		}()
	}
	for j := 0; j < 100; j++ {
		go func() {
			for i := 0; i < 100000; i++ {
				art.Delete(utils.GetTestKey(i))
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestArt_EasyThread(t *testing.T) {
	art := NewART(10)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		for i := 0; i < 1000000; i++ {
			art.Put(utils.GetTestKey(i), &data.LogRecordPos{Fid: uint32(i), Offset: 12})
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 1000000; i++ {
			art.Get(utils.GetTestKey(i))
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < 1000000; i++ {
			art.Delete(utils.GetTestKey(i))
		}
		wg.Done()
	}()
	wg.Wait()
}

func TestMultiIterate(t *testing.T) {
	art := NewART(10)

	for i := 0; i < 1000; i++ {
		art.Put(utils.GetTestKey(i), &data.LogRecordPos{Fid: uint32(i), Offset: 12})
	}

	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}

	iter = art.Iterator(true)
	t.Log(string(iter.Key()))
	iter.Next()
	t.Log(string(iter.Key()))
}
