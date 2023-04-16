package main

import "fmt"

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
	fmt.Printf("Map task2 \n")
	// FIXME: testing code
	key = []byte(fmt.Sprintf("%d", index))
	value = []byte(text)
	return key, value
}

func (t task) Reduce(key []byte, values [][]byte) [][]byte {
	var context [][]byte
	fmt.Printf("Reduce task1 \n")

	return context
}

func (t task) GetNumOfReducer() int {
	return r
}

// Do not modify
var Task task
