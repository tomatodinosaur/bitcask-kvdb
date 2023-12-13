package fio

import "os"

// FileIo标准系统文件Io
type FileIO struct {
	fd *os.File
}

// INIT
func NewFileIOManger(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFileperm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd}, nil

}

// Read从文件指定位置读取对应的数据
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Write写入字符数组到文件中
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// Sync持久化数据
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

// Size获取文件大小
func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
