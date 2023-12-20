package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirsize(t *testing.T) {
	dir, _ := os.Getwd()
	dirsize, err := DirSize(dir)
	assert.Nil(t, err)
	t.Log(dirsize)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	t.Log(size / 1024 / 1024 / 1024)
}
