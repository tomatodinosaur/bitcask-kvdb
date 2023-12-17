package bitcaskkvdb

import "os"

type Options struct {
	//数据库数据目录
	Dirpath string

	//数据文件的阈值
	DataFileSize int64

	//每次写入都需持久化
	SyncWrites bool

	//索引数据结构类型
	IndexType IndexType
}

// Iterator配置项
type IteratorOptions struct {
	//遍历前缀为指定值的Key，默认为空
	Prefix []byte
	//是否反向遍历，false=正向
	Reverse bool
}

type WriteBatchOptions struct {
	//单批次最大数据量
	MaxBatchNum uint

	//提交时 是否持久化
	SyncWrites bool
}

type IndexType = int8

const (
	//BTREE 索引
	Btree IndexType = iota + 1

	//ART 自适应基数树
	ART
)

var DefaultOptions = Options{
	Dirpath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    Btree,
}

var DefalutIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefalutWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
