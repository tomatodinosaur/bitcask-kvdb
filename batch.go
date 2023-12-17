package bitcaskkvdb

import (
	"bitcask/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const NonTransactionSewNo int64 = 0

var txnFinKey = []byte("txn-fin")

// Writebatch 原子批量写数据，保证原子性
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord //内存暂存用户写入的数据
}

// 初始化
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	logrecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logrecord
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//数据不存在直接返回
	logrecordPos := wb.db.index.Get(key)
	if logrecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	//暂存log
	logrecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logrecord
	return nil
}

// Commit 提交事务，将暂存数据写道数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if len(wb.pendingWrites) > int(wb.options.MaxBatchNum) {
		return ErrExceedMaxBatchNum
	}

	//加锁保证事物提交串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	//获取最新的事物SEQ
	seqNo := atomic.AddInt64(&wb.db.seqNo, 1)

	//磁盘位置暂存于此，等到全部写完后再写入Index-Table
	positons := make(map[string]*data.LogRecordPos)

	//开始写数据到数据文件
	for _, record := range wb.pendingWrites {
		logrecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   LogRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positons[string(record.Key)] = logrecordPos
	}

	//追加一条标识事务完成的数据
	finishedRecord := &data.LogRecord{
		Key:  LogRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}

	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	//根据配置决定是否持久化
	if wb.options.SyncWrites && wb.db.activefile != nil {
		if err := wb.db.activefile.Sync(); err != nil {
			return err
		}
	}

	//整个事物写入之后更新Index-table
	for _, record := range wb.pendingWrites {
		pos := positons[string(record.Key)]
		if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		}
	}

	//清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil

}

// key + seq Number编码
func LogRecordKeyWithSeq(key []byte, seqNo int64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], uint64(seqNo))

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 启动时，分开seqNo 和 key
func parseLogRecordKey(key []byte) ([]byte, int64) {
	seqNo, n := binary.Uvarint(key)
	realkey := key[n:]
	return realkey, int64(seqNo)
}
