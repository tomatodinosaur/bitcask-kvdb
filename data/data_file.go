package data

import (
	"bitcask/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const DataFileNameSuffix = ".data"

var ErrInvalidCrc = errors.New("Invalid Crc,log Record may be corrupted")

// DataFile数据文件
type DataFile struct {
	FileId uint32 //文件ID

	Writeoff int64 //文件写入偏移

	IoManager fio.IOManager //Io读写接口，通过此接口进行文件的读写
}

// 打开新的数据文件
func OpenDataFile(dirpath string, fileid uint32) (*DataFile, error) {
	fileName := filepath.Join(dirpath, fmt.Sprintf("%09d", fileid)+DataFileNameSuffix)
	ioManager, err := fio.NewFileIOManger(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileid,
		Writeoff:  0,
		IoManager: ioManager,
	}, nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) Write(b []byte) error {
	n, err := df.IoManager.Write(b)
	if err != nil {
		return err
	}
	df.Writeoff += int64(n)
	return nil
}

// ReadRecord 根据offset从数据文件中读取LogRecord并返回字节数
func (df *DataFile) ReadRecord(offset int64) (*LogRecord, int64, error) {
	size, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	//如果读取的最大header长度超过文件范围，则只需要读取到文件的末尾
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > size {
		headerBytes = size - offset
	}

	//读取Header信息【crc type keysize valuesize】
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	//解码Header信息
	header, headerSize := decodeLogRecordHeader(headerBuf)
	//下面两个条件表示读到了文件末尾，直接返回EOF错误
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	//取出KeySize和ValueSize
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.Type}
	//开始读取用户的实际存储的 Key/Value数据
	if keySize > 0 || valueSize > 0 {
		kvbuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		//分割Key和Value
		logRecord.Key = kvbuf[:keySize]
		logRecord.Value = kvbuf[keySize:]
	}

	//检验数据的有效性
	crc := getLogRecordCrc(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCrc
	}
	return logRecord, recordSize, nil
}

// readNBytes 调用IOManager接口，实现从OFFSET读取N个字节
func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IoManager.Read(b, offset)
	if err != nil {
		return nil, err
	}
	return b, err
}
