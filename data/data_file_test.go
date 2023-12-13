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
