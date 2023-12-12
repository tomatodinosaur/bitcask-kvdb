package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destoryFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIoManager(t *testing.T) {
	fio, err := NewFileIOManger(filepath.Join("/tmp", "a.data"))
	path := filepath.Join("/tmp", "a.data")
	defer destoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIo_write(t *testing.T) {
	fio, _ := NewFileIOManger(filepath.Join("/tmp", "a.data"))
	path := filepath.Join("/tmp", "a.data")
	defer destoryFile(path)
	n, _ := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	n, _ = fio.Write([]byte("kv"))
	assert.Equal(t, 2, n)

}

func TestFileIo_read(t *testing.T) {
	fio, _ := NewFileIOManger(filepath.Join("/tmp", "a.data"))
	path := filepath.Join("/tmp", "a.data")
	defer destoryFile(path)
	n, _ := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	n, _ = fio.Write([]byte("kv"))
	assert.Equal(t, 2, n)
	b := make([]byte, 2)
	fio.Read(b, 0)
	assert.Equal(t, []byte("kv"), b)
}

func TestFileIo_sync(t *testing.T) {
	fio, _ := NewFileIOManger(filepath.Join("/tmp", "a.data"))
	path := filepath.Join("/tmp", "a.data")
	defer destoryFile(path)
	err := fio.Sync()
	assert.Nil(t, err)
}

func TestFileIo_close(t *testing.T) {
	fio, _ := NewFileIOManger(filepath.Join("/tmp", "a.data"))
	path := filepath.Join("/tmp", "a.data")
	defer destoryFile(path)
	err := fio.Close()
	assert.Nil(t, err)
}
