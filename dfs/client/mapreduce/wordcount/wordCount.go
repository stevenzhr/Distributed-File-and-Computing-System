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

// go build -buildmode=plugin -o wc.so wordCount.go

func (t task) Init(strs []string) {
	args = strs
	r = 5
}

func (t task) Map(index int, text string) ([][]byte, [][]byte) {
	var keys, values [][]byte
	words := strings.Split(text, " ")

	for i := 0; i < len(words); i++ {
		if words[i] == "" {
			continue
		}
		keys = append(keys, []byte(words[i]))
		values = append(values, []byte("1"))
	}

	return keys, values
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

func (t task) Compare(a, b string) bool {
	return b < a
}

func (t task) GetNumOfReducer() int {
	return r
}

// Do not modify
var Task task
