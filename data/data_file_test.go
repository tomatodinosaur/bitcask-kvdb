package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	datafile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, datafile1)

	datafile2, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, datafile2)

	datafile3, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, datafile3)
}

func TestDataFile_Write(t *testing.T) {
	datafile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, datafile1)
	err = datafile1.Write([]byte("aaa"))
	assert.Nil(t, err)
	err = datafile1.Write([]byte("aaa"))
	assert.Nil(t, err)

	assert.Equal(t, datafile1.Writeoff, int64(6))
}

func TestDataFile_Close(t *testing.T) {
	datafile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, datafile1)

	err = datafile1.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	datafile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, datafile1)

	err = datafile1.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadRecord(t *testing.T) {
	datafile, err := OpenDataFile(os.TempDir(), 12)
	assert.Nil(t, err)
	assert.NotNil(t, datafile)

	//只有一条logrecord
	rec := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv go"),
	}

	res, size1 := Encode_LogRecord(rec)

	err = datafile.Write(res)
	assert.Nil(t, err)
	readres, readsize, err := datafile.ReadRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, size1, readsize)
	assert.Equal(t, rec, readres)
	t.Log(datafile.Writeoff)

	//多条LogRecord，从不同位置读取
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
	}
	res2, size2 := Encode_LogRecord(rec2)
	err = datafile.Write(res2)
	assert.Nil(t, err)

	readres, readsize, err = datafile.ReadRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, size2, readsize)
	assert.Equal(t, rec2, readres)

	//删除后的数据在数据文件的末尾
	rec3 := &LogRecord{
		Key:   []byte("1"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	res3, size3 := Encode_LogRecord(rec3)
	err = datafile.Write(res3)
	assert.Nil(t, err)

	readres, readsize, err = datafile.ReadRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, size3, readsize)
	assert.Equal(t, rec3, readres)
}
