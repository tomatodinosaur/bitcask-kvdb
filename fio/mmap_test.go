package fio

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/tmp", "mmap-a.data")
	defer destoryFile(path)

	mmapIo, err := NewMMapIOManger(path)
	assert.Nil(t, err)

	//文件为空
	b1 := make([]byte, 10)
	n1, err := mmapIo.Read(b1, 0)
	t.Log(n1)
	t.Log(err)

}

func TestMMap_Read2(t *testing.T) {
	path := filepath.Join("/tmp", "mmap-a.data")
	defer destoryFile(path)

	fio, _ := NewFileIOManger(path)
	fio.Write([]byte("aa"))
	fio.Write([]byte("bb"))
	fio.Write([]byte("cc"))

	mmapIo, err := NewMMapIOManger(path)
	assert.Nil(t, err)

	//文件为空
	b1 := make([]byte, 10)
	n1, err := mmapIo.Read(b1, 2)
	t.Log(n1)
	t.Log(mmapIo.Size())
}
