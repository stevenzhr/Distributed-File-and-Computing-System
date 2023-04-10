package main

import (
	. "dfs/MRManager/modules"
	"dfs/utility"
	"log"
	"os"
	"sync"
)

var (
	wg sync.WaitGroup
)

// go run manager.go 28997 28996
// 997 listen to worker， 996 listen to client
func main() {
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

	// manager start up process

	wg.Add(1)
	// start listen worker req
	ListenWorkerConn(&wg)
	// start listen client req
	ListenClientConn(&wg)
	wg.Wait()
	// shutdown process
}
