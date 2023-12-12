package data

import "bitcask/fio"

const DataFileNameSuffix = ".data"

// DataFile数据文件
type DataFile struct {
	FileId uint32 //文件ID

	Writeoff int64 //文件写入偏移

	IoManager fio.IOManager //Io读写接口，通过此接口进行文件的读写
}

// 打开新的数据文件
func OpenDataFile(dirpath string, fileid uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(b []byte) error {
	return nil
}

func (df *DataFile) ReadRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
