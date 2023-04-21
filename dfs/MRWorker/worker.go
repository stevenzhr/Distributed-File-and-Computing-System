package main

import (
	. "dfs/MRWorker/modules"
	"dfs/config"
	"dfs/utility"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	log.SetOutput(logFile)

	err = os.MkdirAll(config.VAULT_PATH+"ws/", os.ModePerm)
	// err = os.MkdirAll("vault/ws/", os.ModePerm) //FIXME: for testing only, re-comment when deploy
	if err != nil {
		log.Println("ERROR: Can't create or open workspace folder. ", err)
		fmt.Println("ERROR: Can't create or open workspace folder. ", err)
		return
	}

	initWorkSpace()

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

func initWorkSpace() error {
	folder := config.VAULT_PATH + "ws"
	// Get a list of all files in the folder
	fileList, err := filepath.Glob(filepath.Join(folder, "*"))
	if err != nil {
		log.Println("ERROR: Fail to get workspace's filelist. ", err)
		return err
	}
	// Remove each file
	for _, file := range fileList {
		err := os.Remove(file)
		if err != nil {
			log.Println("ERROR: Error removing file:", err)
		}
	}
	return nil
}
