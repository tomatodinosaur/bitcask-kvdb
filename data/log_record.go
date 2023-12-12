package data

type LogRecordPos struct {
	Fid    uint32 //文件id,表示将数据存到了哪个文件
	Offset int64  //偏移，存储到了文件中的哪个位置
}
