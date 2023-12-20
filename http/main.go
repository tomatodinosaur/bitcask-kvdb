package main

import (
	bitcaskkvdb "bitcask"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

var db *bitcaskkvdb.DB

func init() {
	//初始化 DB 实例
	var err error
	options := bitcaskkvdb.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-http")
	options.Dirpath = dir
	db, err = bitcaskkvdb.Open(options)
	if err != nil {
		panic(fmt.Errorf("faied to open db %v", err))
	}
}

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put value in db:%v\n", err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := request.URL.Query().Get("key")
	value, err := db.Get([]byte(key))

	if err != nil && err != bitcaskkvdb.ErrKeyNotFind {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value in db:%v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))

	if err != nil && err != bitcaskkvdb.ErrKeyNotFind {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to delete value in db:%v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string("Ok"))
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	var result []string
	for _, key := range keys {
		result = append(result, string(key))
	}
	_ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)
}

func main() {
	//注册处理方法
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listkeys", handleListKeys)
	http.HandleFunc("/bitcask/stat", handleStat)

	//启动http服务
	http.ListenAndServe("172.24.95.63:8080", nil)
}
