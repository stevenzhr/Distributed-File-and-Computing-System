package main

import (
	. "dfs/MRWorker/modules"
	"dfs/utility"
	"fmt"
	"log"
	"os"
)

//go run node.go orion02:xx997 xx620(listen port)
// manager listens at 997, worker listens at 620
func main() {
	// init log output
	host, _ := os.Hostname()
	port := os.Args[2]
	logFile, err := utility.SetLogFile("MR/"+host, port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer logFile.Close()

	// create listener socket
	mrListener, err := CreateListener()
	if err != nil {
		workerShutDown()
		return
	}
	Flag = true
	err = JoinServer()
	if err != nil {
		workerShutDown()
		return
	}
	StartListening(mrListener)

}

func workerShutDown() {
	fmt.Println("Shut down MapReduce Worker, BYE!")
}
