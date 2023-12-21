package redis

import (
	bitcaskkvdb "bitcask"
	"bitcask/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisDataStrcture_Get(t *testing.T) {
	opts := bitcaskkvdb.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.Dirpath = dir
	rds, err := NewRedisDataStructure(opts)
	defer assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(4))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), 3, utils.RandomValue(4))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	time.Sleep(time.Second * 3)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Nil(t, val2)

}

func TestRedisDataStrcture_Del_Type(t *testing.T) {
	opts := bitcaskkvdb.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.Dirpath = dir
	rds, err := NewRedisDataStructure(opts)
	defer assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(4))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, typ)
	t.Log(typ)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	val1, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, err, bitcaskkvdb.ErrKeyNotFind)
	assert.Nil(t, val1)

}

func TestRedisDataStrcture_HGet(t *testing.T) {
	opts := bitcaskkvdb.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.Dirpath = dir
	rds, err := NewRedisDataStructure(opts)
	defer assert.Nil(t, err)

	ok, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(4))
	assert.Nil(t, err)
	assert.True(t, ok)

	val := utils.RandomValue(4)
	ok, err = rds.HSet(utils.GetTestKey(1), []byte("field1"), val)
	assert.Nil(t, err)
	assert.False(t, ok)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, val1, val)

	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, val2)
	assert.Error(t, err, bitcaskkvdb.ErrKeyNotFind)

	// val3, err := rds.HGet(utils.GetTestKey(1), []byte("field3"))
	// // assert.Nil(t, err)
	// // assert.Nil(t, val2)
	// t.Log(val3, err)

}

func TestRedisDataStrcture_HDel(t *testing.T) {
	opts := bitcaskkvdb.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.Dirpath = dir
	rds, err := NewRedisDataStructure(opts)
	defer assert.Nil(t, err)

	ok, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(4))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.HDel(utils.GetTestKey(100), []byte("field1"))
	assert.False(t, ok)
	assert.Nil(t, err)

	val := utils.RandomValue(4)
	ok, err = rds.HSet(utils.GetTestKey(1), []byte("field1"), val)
	assert.Nil(t, err)
	assert.False(t, ok)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	assert.Equal(t, val1, val)

	ok, err = rds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.True(t, ok)
	assert.Nil(t, err)

}

func TestRedisDataStrcture_SIsMember(t *testing.T) {
	opts := bitcaskkvdb.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.Dirpath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)

	val := []byte("val-2")
	ok, err = rds.SAdd(utils.GetTestKey(1), val)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), val)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("not"))
	assert.Nil(t, err)
	assert.False(t, ok)

}

func TestRedisDataStrcture_SRem(t *testing.T) {
	opts := bitcaskkvdb.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.Dirpath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)

	val := []byte("val-2")
	ok, err = rds.SAdd(utils.GetTestKey(1), val)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), val)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("not"))
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(0), val)
	assert.False(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SRem(utils.GetTestKey(1), []byte("d"))
	assert.False(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SRem(utils.GetTestKey(1), val)
	assert.True(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SIsMember(utils.GetTestKey(1), val)
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestList(t *testing.T) {
	opts := bitcaskkvdb.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.Dirpath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	val1 := utils.RandomValue(4)
	val2 := utils.RandomValue(4)

	rds.LPush(utils.GetTestKey(1), val1)
	res, _ := rds.RPush(utils.GetTestKey(1), val2)
	assert.Equal(t, res, uint32(2))

	val, err := rds.LPop(utils.GetTestKey(1))
	assert.Equal(t, val, val1)
	assert.Nil(t, err)

	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Equal(t, val, val2)
	assert.Nil(t, err)

	val, _ = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, val)

}

func Test_Zcore(t *testing.T) {
	opts := bitcaskkvdb.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.Dirpath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	val1 := utils.RandomValue(4)
	val2 := utils.RandomValue(4)

	ok, err := rds.ZAdd(utils.GetTestKey(1), 113, val1)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.ZAdd(utils.GetTestKey(1), 213, val2)
	assert.Nil(t, err)
	assert.True(t, ok)

	score, _ := rds.ZScore(utils.GetTestKey(1), val1)
	assert.Equal(t, score, float64(113))

	score, _ = rds.ZScore(utils.GetTestKey(1), val2)
	assert.Equal(t, score, float64(213))
}
