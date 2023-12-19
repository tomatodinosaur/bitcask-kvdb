package bitcaskkvdb

import (
	"bitcask/data"
	"bitcask/index"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

/*
	db.mutex 只负责对磁盘文件上锁
	db.index 独立实现对内存索引上锁

	减小表的加锁粒度，增加并发
*/

// DB bitcask存储引擎实例

const SeqNoKey = "seq-no"

type DB struct {
	options         Options
	mu              *sync.RWMutex
	fileIds         []int                     //文件id,只能在加载索引的使用，递增
	activefile      *data.DataFile            //当前活跃文件，用于读写
	olderfile       map[uint32]*data.DataFile //旧文件，只能用于读
	index           index.Indexer             //内存索引接口
	seqNo           int64                     //事务序列号，全局递增
	isMerging       bool                      //是否正在Merge
	seqNoFileExists bool                      //存储事务序列号的文件是否存在
	isInitial       bool                      //是否是第一次初始化此数据目录
}

// Open 启动 bitcask 存储引擎实例 :检查、安装
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行检查
	if err := checkoptions(options); err != nil {
		return nil, err
	}
	var isInitial bool

	//判断数据目录是否存在，如果不存在，创建该目录
	if _, err := os.Stat(options.Dirpath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.Dirpath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	entries, err := os.ReadDir(options.Dirpath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	//初始化DB实例的结构体
	db := &DB{
		options:   options,
		mu:        new(sync.RWMutex),
		olderfile: make(map[uint32]*data.DataFile),
		index:     index.NEWIndexer(options.IndexType, options.Dirpath, options.SyncWrites, options.IndexNum),
		isInitial: isInitial,
	}

	//加载 merge 数据
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if options.IndexType != BPlusTree {
		//从Hint文件加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		//从数据文件中加载索引
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	}
	//取出当前事务的序列号
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activefile != nil {
			size, err := db.activefile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activefile.Writeoff = size
		}
	}

	return db, nil
}

// 写入 KEY/VALUE 数据总体方法
func (db *DB) Put(key []byte, value []byte) error {
	//如果 key 无效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//构造logRecord 结构体
	log_record := data.LogRecord{
		Key:   LogRecordKeyWithSeq(key, NonTransactionSewNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//追加写入到活跃文件
	db.mu.Lock()
	pos, err := db.appendLogRecord(&log_record)
	db.mu.Unlock()
	if err != nil {
		return err
	}

	//更新内存索引
	ok := db.index.Put(key, pos)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Delete 根据Key 删除对应的数据
// 通过增加一条新的tomb Entry (key,空，deleted)[用来merge]
func (db *DB) Delete(key []byte) error {
	//判断key的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//先检查key是否存在，如果不存在直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	//构造LogRecord ，标识为tombEntry
	logRecord := data.LogRecord{
		Key:  LogRecordKeyWithSeq(key, NonTransactionSewNo),
		Type: data.LogRecordDeleted,
	}
	db.mu.Lock()
	_, err := db.appendLogRecord(&logRecord)
	if err != nil {
		db.mu.Unlock()
		return nil
	}
	db.mu.Unlock()

	//从内存索引中将对应的key删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// 根据索引找到数据文件并读取Value
func (db *DB) Get(key []byte) ([]byte, error) {
	//判断key有效
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//从内存索引数据结构中取出key对应的索引信息
	logpos := db.index.Get(key)
	if logpos == nil {
		return nil, ErrKeyNotFind
	}
	// 根据索引协议获取对应的Value
	db.mu.Lock()
	ans, err := db.getValueByPostion(logpos)
	db.mu.Unlock()
	return ans, err
}

// 获取 数据库中所有的key
func (db *DB) ListKeys() [][]byte {
	iter := db.index.Iterator(false)
	defer iter.Close()
	ans := make([][]byte, db.index.Size())
	idx := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		key := iter.Key()
		ans[idx] = key
		idx++
	}
	return ans
}

// 获取 所有的数据，并执行用户指定的操作
func (db *DB) Fold(f func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iter := db.NewIterator(DefalutIteratorOptions)
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value, err := iter.Value()
		if err != nil {
			return err
		}
		if !f(key, value) {
			break
		}
	}
	return nil
}

// 关闭数据库
func (db *DB) Close() error {
	if db.activefile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	db.index.Close()

	//保存当前事务的序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.Dirpath)
	if err != nil {
		return err
	}

	record := &data.LogRecord{
		Key:   []byte(SeqNoKey),
		Value: []byte(strconv.FormatUint(uint64(db.seqNo), 10)),
	}

	encRecord, _ := data.Encode_LogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}

	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	//关闭活跃文件
	if err := db.activefile.Close(); err != nil {
		return err
	}
	for _, file := range db.olderfile {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// 持久化
func (db *DB) Sync() error {
	if db.activefile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activefile.Sync()
}

// 根据索引协议获取对应的Value
func (db *DB) getValueByPostion(logpos *data.LogRecordPos) ([]byte, error) {
	//根据文件ID找到数据文件
	var dataFile *data.DataFile

	if db.activefile.FileId == logpos.Fid {
		dataFile = db.activefile
	} else {
		dataFile = db.olderfile[logpos.Fid]
	}

	//判断数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//根据偏移量去读取数据
	logrecord, _, err := dataFile.ReadRecord(logpos.Offset)
	if err != nil {
		return nil, err
	}

	//判断logrecord 是墓碑
	if logrecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFind
	}

	return logrecord.Value, nil
}

// 写入数据到活跃文件
func (db *DB) appendLogRecord(logrecord *data.LogRecord) (*data.LogRecordPos, error) {
	//判断当前活跃数据文件是否存在，因为数据库在没有写入过的时候无活跃文件
	//如果为空则初始化数据文件
	if db.activefile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//写入数据编码
	enRecord, size := data.Encode_LogRecord(logrecord)

	//如果写入的数据超过活跃文件阈值，则关闭活跃文件，并打开新的文件
	if db.activefile.Writeoff+size > db.options.DataFileSize {
		//先持久数据文件，保证数据持久化到磁盘
		if err := db.activefile.Sync(); err != nil {
			return nil, err
		}

		//当前活跃文件编程旧的数据文件
		db.olderfile[db.activefile.FileId] = db.activefile

		//打开新的活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	res_writeoff := db.activefile.Writeoff
	if err := db.activefile.Write(enRecord); err != nil {
		return nil, err
	}

	//判断是否需要安全持久化
	if db.options.SyncWrites {
		if err := db.activefile.Sync(); err != nil {
			return nil, err
		}
	}
	return &data.LogRecordPos{
		Fid:    db.activefile.FileId,
		Offset: res_writeoff,
	}, nil
}

// 创建一个新文件并作为Active
// （在访问此方法前必须持有互斥锁）
func (db *DB) setActiveDataFile() error {
	var initialFiled uint32 = 0
	if db.activefile != nil {
		//当前活跃文件ID为最大文件ID
		initialFiled = db.activefile.FileId + 1
	}
	//打开新的数据文件,由用户传数据文件dir
	dataFile, err := data.OpenDataFile(db.options.Dirpath, initialFiled)
	if err != nil {
		return err
	}
	db.activefile = dataFile
	return nil
}

func checkoptions(options Options) error {
	if options.Dirpath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}

// 加载磁盘中的数据文件，构建File表
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.Dirpath)
	if err != nil {
		return err
	}

	var fileIds []int
	//遍历目录中的所有文件，找到所有以.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			//00001.data，分割name
			splitNames := strings.Split(entry.Name(), ".")
			fileid, err := strconv.Atoi(splitNames[0])
			//数据目录可能被损坏
			if err != nil {
				return ErrDataDirCorrupted
			}
			fileIds = append(fileIds, fileid)
		}
	}

	//对文件id进行从小到大排序
	sort.Ints(fileIds)
	db.fileIds = fileIds
	//遍历每个文件id,创建对应的DataFile,分配资源、权限

	for i, fid := range fileIds {
		datafile, err := data.OpenDataFile(db.options.Dirpath, uint32(fid))
		if err != nil {
			return err
		}
		//指定Active文件
		if i == len(fileIds)-1 {
			db.activefile = datafile
		} else {
			//安装旧文件表
			db.olderfile[uint32(fid)] = datafile
		}
	}
	return nil
}

// 从数据文件加载索引
// 遍历文件中的所有记录，并更新到内存索引数据结构中
func (db *DB) loadIndexFromDataFiles() error {
	//没有文件，数据库为空
	if len(db.fileIds) == 0 {
		return nil
	}

	// //检查是否发生过merge
	// hasMerge, nonMergeFileId := false, uint32(0)
	// mergeFinFileName := filepath.Join(db.options.Dirpath, data.MergeFinishedFileName)
	// if _, err := os.Stat(mergeFinFileName); err == nil {
	// 	fid, err := db.getNonMergeFileId(db.options.Dirpath)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	hasMerge = true
	// 	nonMergeFileId = fid
	// }

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index of startup")
		}
	}

	//暂存事务数据的<key,pos>（seqNo != NonTransactionSewNo)
	//待检查到事务完成标记Log后再更新Index-table
	transactionRecords := make(map[int64][]*data.TransactionRecords)
	var maxSeq int64 = NonTransactionSewNo

	//遍历所有文件id,处理文件的记录
	for i, fid := range db.fileIds {
		var fileid = uint32(fid)
		var dataFile *data.DataFile

		// if hasMerge && fileid < nonMergeFileId {
		// 	continue
		// }

		//根据fileid得到 DataFile接口
		if fileid == db.activefile.FileId {
			dataFile = db.activefile
		} else {
			dataFile = db.olderfile[fileid]
		}

		var offset int64 = 0
		//循环遍历Entry，目的是得到每个Entry所在的Offset,装入Index
		for {
			logRecord, size, err := dataFile.ReadRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			//构造内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileid, Offset: offset}

			//解析 Key,拿到事物序列号
			realkey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == NonTransactionSewNo {
				//非事务操作，直接更新内存索引
				updateIndex(realkey, logRecord.Type, logRecordPos)
			} else {
				//事务完成，对应的seq No 的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Key, txnRecord.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					txnRecord := data.TransactionRecords{
						Key:  realkey,
						Type: logRecord.Type,
						Pos:  logRecordPos,
					}
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &txnRecord)
				}
			}
			//维护最新SeqNo
			maxSeq = max(maxSeq, seqNo)

			//递增offset到下一个Entry
			offset += size
		}

		//如果当前是活跃文件，维护活跃文件的Writeoff
		if i == len(db.fileIds)-1 {
			db.activefile.Writeoff = offset
		}
	}

	db.seqNo = maxSeq
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.Dirpath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.options.Dirpath)
	if err != nil {
		return err
	}
	record, _, _ := seqNoFile.ReadRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = int64(seqNo)
	db.seqNoFileExists = true
	return nil
}
