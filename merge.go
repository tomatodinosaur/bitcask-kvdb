package bitcaskkvdb

import (
	"bitcask/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据，生成HINT文件
func (db *DB) Merge() error {
	if db.activefile == nil {
		return nil
	}
	db.mu.Lock()

	//如果正在Merge,返回
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	//0 1 [2]-> (0 1 2) [3]
	//关闭当前的活跃文件
	if err := db.activefile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.olderfile[db.activefile.FileId] = db.activefile

	//打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	//记录第一个没有参与 Merge 文件的ID
	nonMergeFileId := db.activefile.FileId

	var MergeFiles []*data.DataFile
	for _, file := range db.olderfile {
		MergeFiles = append(MergeFiles, file)
	}
	db.mu.Unlock()

	//将需要Merge的文件从小到大排序，依次Merge
	sort.Slice(MergeFiles, func(i, j int) bool {
		return MergeFiles[i].FileId < MergeFiles[j].FileId
	})

	mergePath := db.getMergePath()

	//如果目录存在，说明发生过 Merge，将其删除掉
	if _, err := os.Stat(mergePath); err != nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	//新建一个Merge path 目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	//打开一个新的临时 Bitcask 实例
	mergeOpts := db.options
	mergeOpts.Dirpath = mergePath
	mergeOpts.SyncWrites = false
	mergeDB, err := Open(mergeOpts)
	if err != nil {
		return err
	}

	//打开Hint文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	//遍历处理每个数据文件
	for _, datafile := range MergeFiles {
		var offset int64 = 0
		for {
			logrecord, size, err := datafile.ReadRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//解析拿到内存中实际的Key
			realKey, _ := parseLogRecordKey(logrecord.Key)
			logrecordPos := db.index.Get(realKey)

			//和内存中的索引进行比较，如果有效就重写
			if logrecordPos != nil &&
				logrecordPos.Fid == datafile.FileId &&
				logrecordPos.Offset == offset {
				//如果有效，即在内存，则不需要事务序列号
				logrecord.Key = LogRecordKeyWithSeq(realKey, NonTransactionSewNo)
				//重写进Merge实例的ActiveFile
				pos, err := mergeDB.appendLogRecord(logrecord)
				if err != nil {
					return err
				}
				//将当前位置索引写到Hint文件中<key,Pos>
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			//增加Offset
			offset += size
		}
	}

	// sync持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	//新增Hint完成文件
	mergeFinshedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	//写入没有被merge的第一个文件
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}

	encRecord, _ := data.Encode_LogRecord(mergeFinRecord)
	if err := mergeFinshedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinshedFile.Sync(); err != nil {
		return err
	}
	return nil
}

// tmp/bitcask
// tmp/bitcask-merge
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.Dirpath)) // /tmp
	base := path.Base(db.options.Dirpath)           // bitcask
	return filepath.Join(dir, base+mergeDirName)
}

// 加载 merge 数据
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	//查找标识 merge完成的文件
	var mergeFinished bool
	var mergeFileNames []string //存储所有merge数据文件
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	//没有merge完成，直接返回
	if !mergeFinished {
		return nil
	}

	//取出最近 NonMerge Fileid
	NonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	//删除旧的数据文件(FileId < NonMergeFileId)
	var fileId uint32 = 0
	for ; fileId < NonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.Dirpath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	//将新的数据文件移动过来
	for _, fileName := range mergeFileNames {
		// /tmp/bitcask-merge 00.data 01.data
		// /tmp/bitcask 00.data 11.data
		srcpath := filepath.Join(mergePath, fileName)
		destpath := filepath.Join(db.options.Dirpath, fileName)
		if err := os.Rename(srcpath, destpath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirpath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirpath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadRecord(0)
	if err != nil {
		return 0, err
	}

	res, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(res), nil
}

func (db *DB) loadIndexFromHintFile() error {
	//查看Hint文件是否存在
	hintFileName := filepath.Join(db.options.Dirpath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	//打开Hint索引文件<key,pos>
	hintFile, err := data.OpenHintFile(db.options.Dirpath)
	if err != nil {
		return err
	}

	//读取文件中的索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		//解码得到的位置索引
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
