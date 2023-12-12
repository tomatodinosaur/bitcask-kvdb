package bitcaskkvdb

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

type IndexType = int8

const (
	//BTREE 索引
	Btree IndexType = iota + 1

	//ART 自适应基数树
	ART
)
