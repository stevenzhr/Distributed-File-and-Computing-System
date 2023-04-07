package main

import (
	. "dfs/config"
	. "dfs/controller/modules"
	"dfs/utility"
	"fmt"
	"log"
	"os"
	"sync"
)

var (
	wg    sync.WaitGroup
	mutex sync.RWMutex
	// NodesMap  map[string]Node
	fileMap   map[string]File
	flag      bool
	maxNode   int
	runConfig Config
)

// go run controller.go 28999 28998
func main() {
	// TODO: Commandline argments check

	// init log output
	host, _ := os.Hostname()
	port := os.Args[1]
	logFile, err := utility.SetLogFile(host, port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	// set running config
	runConfig.LoadConfig("./config.json")

	// load parameter
	LoadNodesMap()
	LoadFileMap()

	wg.Add(1)
	go ListenNodesConn()
	go NodeAliveCheck(&wg)
	go ListenClientConn()
	wg.Wait()

	// TODO: controller shutdown procedure(save sth)
	fmt.Println("System shutdown. ")
}
