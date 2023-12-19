package fio

import (
	"os"

	"golang.org/x/exp/mmap"
)

// MMap Io 内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManger(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFileperm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

// Read从文件指定位置读取对应的数据
func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

// Write写入字符数组到文件中
func (mmap *MMap) Write([]byte) (int, error) {
	panic("No need to implemented")
}

// Sync持久化数据
func (mmap *MMap) Sync() error {
	panic("No need to implemented")

}

// Close关闭文件
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()

}

// Size获取文件大小
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
