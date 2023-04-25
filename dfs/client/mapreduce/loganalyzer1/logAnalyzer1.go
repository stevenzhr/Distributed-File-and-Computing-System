package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type task string

var (
	args []string
	r    int
)

// go build -buildmode=plugin -o la1.so logAnalyzer1.go

func (t task) Init(strs []string) {
	args = strs
	r = 1
}

func (t task) Map(index int, text string) ([][]byte, [][]byte) {
	var keys, values [][]byte
	// words := strings.Split(text, "\\s+")
	words := strings.Fields(text)

	re := regexp.MustCompile(`^https?://([^/]+)/`)

	for i := 0; i < len(words); i++ {
		if !strings.HasPrefix(words[i], "http") {
			continue
		}
		domain := re.FindStringSubmatch(words[i])
		if len(domain) > 1 && domain[1] != "" {
			keys = append(keys, []byte(domain[1]))
			values = append(values, []byte("1"))
		}

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
	return a < b
}

func (t task) GetNumOfReducer() int {
	return r
}

// Do not modify
var Task task
