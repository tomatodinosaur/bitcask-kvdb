package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
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

// 对LogRecord进行编码，返回字节数组和长度
func Encode_LogRecord(logrecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// 将字节数组解码成LogRecordHeader
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCrc(lr *LogRecord, header []byte) uint32 {
	return 0
}
