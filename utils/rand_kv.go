package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randstr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// 获得测试的Key
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitcask-go-key-%09d", i))
}

// 生成随机value
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randstr.Intn(len(letters))]
	}
	return []byte("bitcask-go-value" + string(b))
}
