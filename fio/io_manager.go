package fio

const DataFileperm = 0644

// IOManager 抽象IO管理接口，可以接入不同类型的IO，目前支持标准文件Io
type IOManager interface {

	//Read从文件指定位置读取对应的数据
	Read([]byte, int64) (int, error)

	//Write写入字符数组到文件中
	Write([]byte) (int, error)

	//Sync持久化数据
	Sync() error

	//Close关闭文件
	Close() error

	//Size获取文件大小
	Size() (int64, error)
}

//初始化IOManager,目前只支持标准FileIo

func NewIoManager(fileName string) (IOManager, error) {
	//接口返回标准FIleIo方法
	return NewFileIOManger(fileName)
}
