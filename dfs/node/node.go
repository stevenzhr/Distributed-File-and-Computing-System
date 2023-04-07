package main

import (
	. "dfs/node/modules"
	"dfs/utility"
	"fmt"
	"log"
	"os"
	"sync"
)

var (
	wg sync.WaitGroup
)

//go run node.go orion01:28999 28001(listen port)
func main() {
	// init log output
	host, _ := os.Hostname()
	port := os.Args[2]
	logFile, err := utility.SetLogFile(host, port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	// create listener socket
	chunkListener, err := CreateListener()
	if err != nil {
		nodeShutDown()
		return
	}

	// load running config
	err = LoadRunConfig()
	if err != nil {
		nodeShutDown()
		return
	}

	// Create 2 threads: 1. Listen for connection 2. Heart beat process
	wg.Add(1)
	go StartListening(chunkListener, &wg)
	go Heartbeat(&wg)
	wg.Wait()
	nodeShutDown()
}

func nodeShutDown() {
	// TODO: add shutdown process
	fmt.Println("Shut down. BYE!")
}
