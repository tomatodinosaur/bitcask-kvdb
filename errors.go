package bitcaskkvdb

import "errors"

var ErrKeyIsEmpty = errors.New("key is empty")
var ErrIndexUpdateFailed = errors.New("failed to updata index")
var ErrKeyNotFind = errors.New("key is not found")
var ErrDataFileNotFound = errors.New("datafile is not found")
var ErrDataDirCorrupted = errors.New("the database directory maybe corrupted")
var ErrExceedMaxBatchNum = errors.New("exceed max batch num")
