package redis

import (
	bitcask "bitcask"
	"encoding/binary"
	"errors"
	"time"

	"github.com/saint-yellow/baradb/utils"
)

var (
	ErrWrongTypeOperation = errors.New("WrongType Opeartion against a key holding the wrong kind of the value")
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	Zset
)

// Redis 数据结构服务
type RedisDataSrtucture struct {
	db *bitcask.DB
}

func NewRedisDataStructure(options bitcask.Options) (*RedisDataSrtucture, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}

	return &RedisDataSrtucture{db: db}, nil
}

func (rds *RedisDataSrtucture) Close() error {
	return rds.db.Close()
}

//===========================String 数据结构===================================

func (rds *RedisDataSrtucture) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	//编码Value : type +expire +payload
	//[string,expire,value]
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	//调用存储引擎接口进行Push <key,[string,expire,value]>
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataSrtucture) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	//解码ENCValue[string,expire,value]
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	//判断是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], nil
}

//===========================Hash 数据结构===================================

func (rds *RedisDataSrtucture) HSet(key, field, value []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}

	//构造 Hash RealKey <key,version,field>
	hk := &hashRealKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	realKey := hk.encode()

	//先查找是否存在数据
	var exist = true
	if _, err = rds.db.Get(realKey); err == bitcask.ErrKeyNotFind {
		exist = false
	}

	//事务：一次性写入元数据和数据
	wb := rds.db.NewWriteBatch(bitcask.DefalutWriteBatchOptions)
	//不存在则更新元数据(新增filed)
	if !exist {
		meta.size++
		//写入新的元数据
		wb.Put(key, meta.encode())
	}
	wb.Put(realKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataSrtucture) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil //
	}
	//构造 Hash RealKey <key,version,field>
	hk := &hashRealKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())

}

func (rds *RedisDataSrtucture) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil //存疑
	}
	//构造 Hash RealKey <key,version,field>
	hk := &hashRealKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	realKey := hk.encode()

	//先查找是否存在数据
	var exist = true
	if _, err = rds.db.Get(realKey); err == bitcask.ErrKeyNotFind {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(bitcask.DefalutWriteBatchOptions)
		meta.size--
		wb.Put(key, meta.encode())
		wb.Delete(realKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil

}

// ===========================Set 数据结构===================================
func (rds *RedisDataSrtucture) SAdd(key, member []byte) (bool, error) {

	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}

	sk := &setRealKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	var ok bool

	if _, err := rds.db.Get(sk.encode()); err == bitcask.ErrKeyNotFind {
		//不存在则更新
		//事务：一次性写入元数据和数据
		wb := rds.db.NewWriteBatch(bitcask.DefalutWriteBatchOptions)
		//不存在则更新元数据(新增member)
		meta.size++
		//写入新的元数据
		wb.Put(key, meta.encode())
		wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil

}

func (rds *RedisDataSrtucture) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	sk := &setRealKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && err != bitcask.ErrKeyNotFind {
		return false, err
	}
	if err == bitcask.ErrKeyNotFind {
		return false, nil
	}
	return true, nil
}

func (rds *RedisDataSrtucture) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	sk := &setRealKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); err == bitcask.ErrKeyNotFind {
		return false, nil
	}

	//更新
	wb := rds.db.NewWriteBatch(bitcask.DefalutWriteBatchOptions)
	//不存在则更新元数据(新增member)
	meta.size--
	//写入新的元数据
	wb.Put(key, meta.encode())
	wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil

}

// ===========================List 数据结构===================================

func (rds *RedisDataSrtucture) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataSrtucture) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataSrtucture) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataSrtucture) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *RedisDataSrtucture) pushInner(key, element []byte, isleft bool) (uint32, error) {
	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return 0, err
	}

	lk := &listRealKey{
		key:     key,
		version: meta.version,
	}
	if isleft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	//更新
	wb := rds.db.NewWriteBatch(bitcask.DefalutWriteBatchOptions)
	//不存在则更新元数据(新增member)
	meta.size++
	//写入新的元数据
	if isleft {
		meta.head--
	} else {
		meta.tail++
	}
	wb.Put(key, meta.encode())
	wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	return meta.size, nil
}

func (rds *RedisDataSrtucture) popInner(key []byte, isleft bool) ([]byte, error) {
	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	lk := &listRealKey{
		key:     key,
		version: meta.version,
	}
	if isleft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	meta.size--
	//写入新的元数据
	if isleft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}
	return element, nil
}

// ===========================ZSet 数据结构===================================

func (rds *RedisDataSrtucture) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Zset)
	if err != nil {
		return false, err
	}

	zk := &zsetRealKey{
		key:     key,
		version: meta.version,
		member:  member,
		score:   score,
	}
	var exist bool
	Value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && err != bitcask.ErrKeyNotFind {
		return false, err
	}
	if err == bitcask.ErrKeyNotFind {
		exist = false
	}
	if exist {
		if score == utils.Float64FromBytes(Value) {
			return false, nil
		}
	}

	wb := rds.db.NewWriteBatch(bitcask.DefalutWriteBatchOptions)
	//不存在则更新元数据(新增member)
	meta.size++
	//写入新的元数据
	if !exist {
		meta.size++
		wb.Put(key, meta.encode())

	}
	if exist {
		oldKey := &zsetRealKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.Float64FromBytes(Value),
		}
		wb.Delete(oldKey.encodeWithScore())
	}
	wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil

}

func (rds *RedisDataSrtucture) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetaData(key, Zset)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}
	zk := &zsetRealKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}
	return utils.Float64FromBytes(value), nil
}

func (rds *RedisDataSrtucture) findMetaData(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != bitcask.ErrKeyNotFind {
		return nil, err
	}

	var meta *metadata
	var exist = true
	//Hset
	if err == bitcask.ErrKeyNotFind {
		exist = false
	} else {
		//Hget
		meta = DecodeMetadata(metaBuf)
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		//判断元数据过期
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
