package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// crc type keysize valuesize
// 4   1     5				5     =15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// 数据内存索引，描述数据在磁盘的位置
type LogRecordPos struct {
	Fid    uint32 //文件id,表示将数据存到了哪个文件
	Offset int64  //偏移，存储到了文件中的哪个位置
}

// 写入到数据文件的Entry
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType //标记Entry是否被替代
}

// LogRecordHeader Entry头部字段
type LogRecordHeader struct {
	crc       uint32        //crc校验值
	Type      LogRecordType //标识LogRecord类型
	keySize   uint32        //key长度
	valueSize uint32        //value长度
}

// 暂存事务结构
type TransactionRecords struct {
	Key  []byte
	Type LogRecordType
	Pos  *LogRecordPos
}

// 对位置信息进行编码
func Encode_LogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	return buf[:index]
}

// 解码LogRecordPos
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])
	return &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: offset,
	}
}

// 对LogRecord进行编码，返回字节数组和长度
func Encode_LogRecord(logrecord *LogRecord) ([]byte, int64) {
	/*-------------------------------------------------------------
	| crc   type    keysize      valuesize  |   key       value		|
	|	4			1			变长(最大5)		变长(最大5)	 | keysize		valuesize|
	------------------------------------------------------------*/

	//初始化一个header部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	//第5个字节储存Type
	header[4] = logrecord.Type
	var index = 5

	//5字节后，写入size信息
	index += binary.PutVarint(header[index:], int64(len(logrecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logrecord.Value)))

	var realsize = index + len(logrecord.Key) + len(logrecord.Value)
	EncodeBytes := make([]byte, realsize)

	//将header切片拷贝过来
	copy(EncodeBytes, header[:index])
	copy(EncodeBytes[index:], logrecord.Key)
	copy(EncodeBytes[index+len(logrecord.Key):], logrecord.Value)

	//crc校验
	crc := crc32.ChecksumIEEE(EncodeBytes[4:])
	binary.LittleEndian.PutUint32(EncodeBytes[:4], crc)

	return EncodeBytes, int64(realsize)
}

// 将字节数组解码成LogRecordHeader
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:  binary.LittleEndian.Uint32(buf[:4]),
		Type: buf[4],
	}

	var index = 5
	//取出实际的	key size
	keysize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keysize)
	index += n

	//取出实际的	value size
	valuesize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valuesize)
	index += n

	return header, int64(index)
}

func getLogRecordCrc(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
