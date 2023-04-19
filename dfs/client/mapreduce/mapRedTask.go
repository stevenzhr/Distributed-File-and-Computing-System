package main

import (
	"fmt"
	"strconv"
	"strings"
)

type task string

var (
	args []string
	r    int
)

// go build -buildmode=plugin -o task.so mapRedTask.go

func (t task) Init(strs []string) {
	args = strs
	r = 1
}

func (t task) Map(index int, text string) ([]byte, []byte) {
	var key, value []byte
	key = []byte(strings.Split(text, ",")[1])
	value = []byte("1")
	return key, value
}

func (t task) Reduce(key []byte, values [][]byte) []byte {
	var context []byte
	keyStr := string(key)
	var sum int
	for i := 0; i < len(values); i++ {
		num, _ := strconv.Atoi(string(values[i]))
		sum += num
	}
	context = []byte(fmt.Sprintf("%s,%d", keyStr, sum))
	return context
}

func (t task) GetNumOfReducer() int {
	return r
}

// Do not modify
var Task task
