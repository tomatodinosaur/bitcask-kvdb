package main

import (
	bitcaskkvdb "bitcask"
	bitcask_redis "bitcask/redis"
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6380"

type BitcaskSever struct {
	dbs   map[int]*bitcask_redis.RedisDataSrtucture
	sever *redcon.Server
	mu    sync.RWMutex
}

func main() {
	//打开一个 Redis 数据结构服务
	redisDataStructure, err := bitcask_redis.NewRedisDataStructure(bitcaskkvdb.DefaultOptions)
	if err != nil {
		panic(err)
	}

	//初始哈 BitcaskSever
	bitcaskSever := &BitcaskSever{
		dbs: make(map[int]*bitcask_redis.RedisDataSrtucture),
	}
	bitcaskSever.dbs[0] = redisDataStructure

	//初始化一个 Redis 服务器
	bitcaskSever.sever = redcon.NewServer(addr, execClientCommand, bitcaskSever.accept, bitcaskSever.close)
	bitcaskSever.listen()
}

func (svr *BitcaskSever) listen() {
	log.Println("bitcask sever running,ready to accept connections")
	_ = svr.sever.ListenAndServe()
}

func (svr *BitcaskSever) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.sever = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *BitcaskSever) close(conn redcon.Conn, err error) {

	for _, db := range svr.dbs {
		db.Close()
	}
	svr.sever.Close()

}
