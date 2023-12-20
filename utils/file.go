package utils

import (
	"io/fs"
	"path/filepath"
	"syscall"
)

// 获取一个目标的大小
func DirSize(dirpath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirpath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// 获取磁盘可用空间
func AvailableDiskSize() (uint64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	var stat syscall.Statfs_t
	if err = syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
