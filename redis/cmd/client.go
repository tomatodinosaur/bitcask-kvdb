package main

import (
	bitcask "bitcask"
	bitcask_redis "bitcask/redis"
	"fmt"
	"strings"

	"github.com/saint-yellow/baradb/utils"
	"github.com/tidwall/redcon"
)

func newWrongNumerOfArgsError(cmd string) error {
	return fmt.Errorf("err wrong number of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportCommands = map[string]cmdHandler{
	"set":       set,
	"get":       get,
	"hset":      hset,
	"hget":      hget,
	"hdel":      hdel,
	"sadd":      sadd,
	"sismember": sismember,
	"srem":      srem,
	"lpush":     lpush,
	"rpush":     rpush,
	"lpop":      lpop,
	"rpop":      rpop,
	"zadd":      zadd,
	"zscore":    zscore,
}

type BitcaskClient struct {
	sever *BitcaskSever
	db    *bitcask_redis.RedisDataSrtucture
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportCommands[command]
	if !ok {
		conn.WriteError("Err unsupported command: '" + command + "' ")
		return
	}

	client, _ := conn.Context().(*BitcaskClient)

	switch command {
	case "quit":
		conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if err == bitcask.ErrKeyNotFind {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumerOfArgsError("set")
	}

	key, value := args[0], args[1]
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumerOfArgsError("get")
	}

	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumerOfArgsError("hset")
	}

	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func hget(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumerOfArgsError("hget")
	}
	key, field := args[0], args[1]

	value, err := cli.db.HGet(key, field)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hdel(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumerOfArgsError("hdel")
	}

	var ok = 0
	key, field := args[0], args[1]

	res, err := cli.db.HDel(key, field)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumerOfArgsError("sadd")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sismember(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumerOfArgsError("sismember")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SIsMember(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func srem(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumerOfArgsError("srem")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SRem(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumerOfArgsError("lpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(res), nil
}

func rpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumerOfArgsError("rpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.RPush(key, value)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(res), nil
}

func lpop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumerOfArgsError("lpop")
	}

	key := args[0]
	res, err := cli.db.LPop(key)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func rpop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumerOfArgsError("rpop")
	}

	key := args[0]
	res, err := cli.db.RPop(key)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumerOfArgsError("zadd")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.Float64FromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

func zscore(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumerOfArgsError("zscore")
	}

	key, member := args[0], args[1]
	res, err := cli.db.ZScore(key, member)
	if err != nil {
		return nil, err
	}

	return utils.Float64ToBytes(res), nil
}
