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

// go build -buildmode=plugin -o la2.so logAnalyzer2.go

func (t task) Init(strs []string) {
	args = strs
	r = 1
}

func (t task) Map(index int, text string) ([][]byte, [][]byte) {
	var keys, values [][]byte
	line := strings.Split(text, ",")

	keys = append(keys, []byte(line[1]))     //url
	values = append(values, []byte(line[0])) //count

	return keys, values
}

func (t task) Reduce(key []byte, values [][]byte) []byte {
	var context []byte
	totalOutput := 0

	keyStr := string(key)

	for i := 0; i < len(values); i++ {
		if totalOutput >= 10 {
			break
		}

		context = []byte(fmt.Sprintf("%s,%s", values[i], keyStr))
		totalOutput++
	}

	return context
}

func (t task) Compare(a, b string) bool {
	keyA, _ := strconv.Atoi(a)
	keyB, _ := strconv.Atoi(b)
	return keyB < keyA
}

func (t task) GetNumOfReducer() int {
	return r
}

// Do not modify
var Task task
