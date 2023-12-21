package redis

import (
	"bitcask/utils"
	"encoding/binary"
	"math"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListMark   = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte   //数据类型
	expire   int64  //过期时间
	version  int64  //版本号--快速删除
	size     uint32 //field的数量
	//List数据专用
	head uint64
	tail uint64
}

func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)
	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))
	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}
	return buf[:index]
}

func DecodeMetadata(buf []byte) *metadata {
	dataType := buf[0]
	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64 = 0
	var tail uint64 = 0

	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, n = binary.Uvarint(buf[index:])
		index += n
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		size:     uint32(size),
		version:  version,
		head:     head,
		tail:     tail,
	}
}

type hashRealKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashRealKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8)

	var index = 0
	copy(buf[index:], hk.key)
	index += len(hk.key)
	binary.LittleEndian.PutUint64(buf[index:], uint64(hk.version))
	index += 8
	copy(buf[index:], hk.field)
	index += len(hk.field)

	return buf

}

type setRealKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setRealKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4)

	var index = 0
	copy(buf[index:], sk.key)
	index += len(sk.key)
	binary.LittleEndian.PutUint64(buf[index:], uint64(sk.version))
	index += 8
	copy(buf[index:], sk.member)
	index += len(sk.member)

	//member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf

}

type listRealKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listRealKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)

	var index = 0
	copy(buf[index:], lk.key)
	index += len(lk.key)
	binary.LittleEndian.PutUint64(buf[index:], uint64(lk.version))
	index += 8
	binary.LittleEndian.PutUint64(buf[index:], lk.index)
	index += 8

	return buf
}

type zsetRealKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *zsetRealKey) encodeWithScore() []byte {
	scorebuf := utils.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+len(zk.member)+len(scorebuf)+8+4)

	var index = 0
	copy(buf[index:], zk.key)
	index += len(zk.key)
	binary.LittleEndian.PutUint64(buf[index:], uint64(zk.version))
	index += 8
	copy(buf[index:], scorebuf)
	index += len(scorebuf)

	copy(buf[index:], zk.member)
	index += len(zk.member)

	//member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))

	return buf
}

func (zk *zsetRealKey) encodeWithMember() []byte {

	//<key,version,member>
	buf := make([]byte, len(zk.key)+len(zk.member)+8)

	var index = 0
	copy(buf[index:], zk.key)
	index += len(zk.key)
	binary.LittleEndian.PutUint64(buf[index:], uint64(zk.version))
	index += 8
	copy(buf[index:], zk.member)
	index += len(zk.member)

	return buf
}
