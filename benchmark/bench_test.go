package benchmark

import (
	bitcaskkvdb "bitcask"
	"bitcask/utils"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var db *bitcaskkvdb.DB
var values [][]byte

func init() {
	//初始化用于基准测试的存储引擎
	options := bitcaskkvdb.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-bench")
	options.Dirpath = dir
	db, _ = bitcaskkvdb.Open(options)
	values = make([][]byte, 0)
	for i := 0; i < 10000; i++ {
		values = append(values, utils.RandomValue(128))
	}
}

func Benchmark_Parallel(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), values[i])
		assert.Nil(b, err)
	}
	b.SetParallelism(100000)
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := db.Put(utils.GetTestKey(rand.Int()), values[rand.Int()%10000])
			assert.Nil(b, err)
			_, err = db.Get(utils.GetTestKey(rand.Int()))
			if err != nil && err != bitcaskkvdb.ErrKeyNotFind {
				b.Fatal(err)
			}
			err = db.Delete(utils.GetTestKey(rand.Int()))
			assert.Nil(b, err)
		}
	})
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != bitcaskkvdb.ErrKeyNotFind {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}
}
