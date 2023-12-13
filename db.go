package bitcaskkvdb

import (
	"bitcask/data"
	"bitcask/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

/*
	Put主方法的并发保护：（Index表、File表、磁盘都需要写）
		在appendLogRecord中使用 DB.mu 保护activefile、olderfile和磁盘文件
		由Btree.Lock保护Index<key,pos>表
	Get主方法的并发保护：（Index表 GET操作无需保护)
		整体使用DB.mu保护

	减小Index表的加锁粒度，增加并发
*/

// DB bitcask存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     //文件id,只能在加载索引的使用，递增
	activefile *data.DataFile            //当前活跃文件，用于读写
	olderfile  map[uint32]*data.DataFile //旧文件，只能用于读
	index      index.Indexer             //内存索引接口
}

// Open 启动 bitcask 存储引擎实例 :检查、安装
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行检查
	if err := checkoptions(options); err != nil {
		return nil, err
	}

	//判断数据目录是否存在，如果不存在，创建该目录
	if _, err := os.Stat(options.Dirpath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.Dirpath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//初始化DB实例的结构体
	db := &DB{
		options:   options,
		mu:        new(sync.RWMutex),
		olderfile: make(map[uint32]*data.DataFile),
		index:     index.NEWIndexer(options.IndexType),
	}

	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
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
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//追加写入到活跃文件
	pos, err := db.appendLogRecord(&log_record)
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
	logRecord := data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	_, err := db.appendLogRecord(&logRecord)
	if err != nil {
		return nil
	}

	//从内存索引中将对应的key删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// 根据索引找到数据文件并读取Value
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	//判断key有效
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//从内存索引数据结构中取出key对应的索引信息
	logpos := db.index.Get(key)
	if logpos == nil {
		return nil, ErrKeyNotFind
	}

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
	db.mu.Lock()
	defer db.mu.Unlock()

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
	return &data.LogRecordPos{db.activefile.FileId, res_writeoff}, nil
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

	//遍历所有文件id,处理文件的记录
	for i, fid := range db.fileIds {
		var fileid = uint32(fid)
		var dataFile *data.DataFile

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
			logRecordPos := &data.LogRecordPos{fileid, offset}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			//递增offset到下一个Entry
			offset += size
		}

		//如果当前是活跃文件，维护活跃文件的Writeoff
		if i == len(db.fileIds)-1 {
			db.activefile.Writeoff = offset
		}
	}
	return nil
}
