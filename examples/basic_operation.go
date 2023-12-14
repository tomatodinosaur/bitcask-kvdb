package main

import (
	bitcaskkvdb "bitcask"
	"fmt"
)

func main() {
	opts := bitcaskkvdb.DefaultOptions
	opts.Dirpath = "/tmp/bitcask-go"
	db, err := bitcaskkvdb.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(val))

	db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

	val, err = db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(val))
}
