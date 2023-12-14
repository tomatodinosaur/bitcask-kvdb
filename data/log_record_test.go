package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
-------------------------------------------------------------
| crc   type    keysize      valuesize  |   key       value		|
|	4			1			变长(最大5)		变长(最大5)	 | keysize		valuesize|
------------------------------------------------------------
*/
func TestEncodeLogRecord(t *testing.T) {
	//正常情况
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("go"),
		Type:  LogRecordNormal,
	}
	res1, n1 := Encode_LogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))
	t.Log(n1)

	//value为空
	rec1 = &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res1, n1 = Encode_LogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))
	t.Log(n1)

	//对Deleted的测试
	rec1 = &LogRecord{
		Key:   []byte("name"),
		Value: []byte("go"),
		Type:  LogRecordDeleted,
	}
	res1, n1 = Encode_LogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))
	t.Log(n1)
}

func TestDecodeLogRecordHeader(t *testing.T) {

	headerbuf := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, size1 := decodeLogRecordHeader(headerbuf)
	assert.NotNil(t, h1)
	assert.Equal(t, size1, int64(7))
	t.Log(h1.crc)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)
	assert.Equal(t, h1.crc, uint32(2532332136))

	headerbuf = []byte{9, 252, 88, 14, 0, 8, 0}
	h1, size1 = decodeLogRecordHeader(headerbuf)
	assert.NotNil(t, h1)
	assert.Equal(t, size1, int64(7))
	t.Log(h1.crc)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(0), h1.valueSize)
	assert.Equal(t, h1.crc, uint32(240712713))

}

func TestGetLogRecordCrc(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}

	headerbuf := []byte{104, 82, 240, 150, 0, 8, 20}
	crc := getLogRecordCrc(rec1, headerbuf[crc32.Size:])
	assert.Equal(t, crc, uint32(2532332136))

	rec1 = &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}

	headerbuf = []byte{9, 252, 88, 14, 0, 8, 0}
	crc = getLogRecordCrc(rec1, headerbuf[crc32.Size:])
	assert.Equal(t, crc, uint32(240712713))

}
