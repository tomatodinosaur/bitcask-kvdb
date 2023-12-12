package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

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

// 对LogRecord进行编码，返回字节数组和长度
func Encode_LogRecord(logrecord LogRecord) ([]byte, int64) {
	return nil, 0
}
